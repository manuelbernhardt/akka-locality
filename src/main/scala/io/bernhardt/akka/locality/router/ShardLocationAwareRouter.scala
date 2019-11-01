package io.bernhardt.akka.locality.router

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ ThreadLocalRandom, TimeUnit }

import akka.actor._
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion._
import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.japi.Util.immutableSeq
import akka.pattern.{ ask, AskTimeoutException }
import akka.routing._
import akka.util.Timeout
import io.bernhardt.akka.locality.LocalitySupervisor

import scala.collection.immutable
import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
import scala.util.control.NonFatal

object ShardLocationAwareRouter {
  def extractEntityIdFrom(messageExtractor: MessageExtractor): ShardRegion.ExtractEntityId = {
    case msg if messageExtractor.entityId(msg) ne null =>
      (messageExtractor.entityId(msg), messageExtractor.entityMessage(msg))
  }
}

/**
 * A group router that will route to the routees deployed the closest to the sharded entity they need to interact with
 */
@SerialVersionUID(1L)
final case class ShardLocationAwareGroup(
    routeePaths: immutable.Iterable[String],
    shardRegion: ActorRef,
    extractEntityId: ShardRegion.ExtractEntityId,
    extractShardId: ShardRegion.ExtractShardId,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
    extends Group {
  /**
   * Java API
   *
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *                    sent with [[akka.actor.ActorSelection]] to these paths
   * @param shardRegion the reference to the shard region
   * @param messageExtractor the [[akka.cluster.sharding.ShardRegion.MessageExtractor]] used for the sharding
   *                         of the entities this router should optimize routing for
   */
  def this(routeePaths: java.lang.Iterable[String], shardRegion: ActorRef, messageExtractor: MessageExtractor) =
    this(
      immutableSeq(routeePaths),
      shardRegion,
      ShardLocationAwareRouter.extractEntityIdFrom(messageExtractor),
      extractShardId = msg => messageExtractor.shardId(msg)
    )

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): ShardLocationAwareGroup = copy(routerDispatcher = dispatcherId)

  override def paths(system: ActorSystem): immutable.Iterable[String] = routeePaths

  override def createRouter(system: ActorSystem): Router =
    new Router(ShardLocationAwareRoutingLogic(system, shardRegion, extractEntityId, extractShardId))
}

/**
 * A pool router that will route to the routees deployed the closest to the sharded entity they need to interact with
 */
@SerialVersionUID(1L)
final case class ShardLocationAwarePool(
    nrOfInstances: Int,
    override val resizer: Option[Resizer] = None,
    shardRegion: ActorRef,
    extractEntityId: ShardRegion.ExtractEntityId,
    extractShardId: ShardRegion.ExtractShardId,
    override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
    override val usePoolDispatcher: Boolean = false)
    extends Pool {
  /**
   * Java API
   *
   * @param nrOfInstances how many routees this pool router should have
   * @param shardRegion the reference to the shard region
   * @param messageExtractor the [[akka.cluster.sharding.ShardRegion.MessageExtractor]] used for the sharding
   *                         of the entities this router should optimize routing for
   */
  def this(nrOfInstances: Int, shardRegion: ActorRef, messageExtractor: MessageExtractor) =
    this(
      nrOfInstances = nrOfInstances,
      shardRegion = shardRegion,
      extractEntityId = ShardLocationAwareRouter.extractEntityIdFrom(messageExtractor),
      extractShardId = msg => messageExtractor.shardId(msg))

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): ShardLocationAwarePool = copy(supervisorStrategy = strategy)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): ShardLocationAwarePool = copy(resizer = Some(resizer))

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): ShardLocationAwarePool = copy(routerDispatcher = dispatcherId)

  override def createRouter(system: ActorSystem): Router =
    new Router(ShardLocationAwareRoutingLogic(system, shardRegion, extractEntityId, extractShardId))

  override def nrOfInstances(sys: ActorSystem): Int = this.nrOfInstances
}

/**
 * Router logic that makes its routing decision based on the relative location of routees to the shards they will
 * communicate with, on a best-effort basis.
 * When no shard state information is available this logic falls back to random routing.
 * When there are multiple candidate routees on the same node, one of them is selected at random.
 *
 * @param system the [[akka.actor.ActorSystem]]
 * @param shardRegion the reference to the [[akka.cluster.sharding.ShardRegion]] the local routees will communicate with
 * @param extractEntityId partial function to extract the entity id from a message, should be the same as used for sharding
 * @param extractShardId partial function to extract the shard id based on a message, should be the same as used for sharding
 */
final case class ShardLocationAwareRoutingLogic(
    system: ActorSystem,
    shardRegion: ActorRef,
    extractEntityId: ShardRegion.ExtractEntityId,
    extractShardId: ShardRegion.ExtractShardId
) extends RoutingLogic {
  import io.bernhardt.akka.locality.router.ShardStateMonitor._
  import system.dispatcher

  private lazy val log = Logging(system, getClass)
  private lazy val selfAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
  private val clusterShardingStateRef = new AtomicReference[Map[ShardId, Address]](Map.empty)
  private val shardLocationAwareRouteeRef =
    new AtomicReference[(IndexedSeq[Routee], Map[Address, IndexedSeq[ShardLocationAwareRoutee]])](
      (IndexedSeq.empty, Map.empty))

  watchShardStateChanges()

  override def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    if (routees.isEmpty) {
      NoRoutee
    } else {
      // avoid re-creating routees for each message by checking if they have changed
      def updateShardLocationAwareRoutees(): Map[Address, IndexedSeq[ShardLocationAwareRoutee]] = {
        val oldShardingRouteeTuple = shardLocationAwareRouteeRef.get()
        val (oldRoutees, oldShardLocationAwareRoutees) = oldShardingRouteeTuple
        if ((routees ne oldRoutees) && routees != oldRoutees) {
          val allRoutees = routees.map(ShardLocationAwareRoutee(_, selfAddress))
          val newShardLocationAwareRoutees = allRoutees.groupBy(_.address)
          // don't act on failure of compare and set, as the next call to select will update anyway if the routees remain different
          shardLocationAwareRouteeRef.compareAndSet(oldShardingRouteeTuple, (routees, newShardLocationAwareRoutees))
          newShardLocationAwareRoutees
        } else {
          oldShardLocationAwareRoutees
        }
      }

      val shardId: ShardId = extractShardId(message)
      val shardLocationAwareRoutees = updateShardLocationAwareRoutees()

      val candidateRoutees = for {
        location <- clusterShardingStateRef.get().get(shardId)
        locationAwareRoutees <- shardLocationAwareRoutees.get(location)
      } yield {
        val closeRoutees = locationAwareRoutees.map(_.routee)

        // pick one of the local routees at random
        closeRoutees(ThreadLocalRandom.current.nextInt(closeRoutees.size))
      }

      candidateRoutees.getOrElse {
        // if we couldn't figure out the location of the shard, fall back to random routing
        routees(ThreadLocalRandom.current.nextInt(routees.size))
      }
    }
  }

  private def watchShardStateChanges(): Unit = {
    implicit val timeout: Timeout = Timeout(2 ^ 64, TimeUnit.DAYS)
    val localitySel = system.actorSelection("/system/locality")
    val change: Future[ShardStateChanged] =
      (localitySel ? LocalitySupervisor.MonitorShards(shardRegion)).mapTo[ShardStateChanged]
    change
      .map { stateChanged =>
        if (stateChanged.newState.nonEmpty) {
          log.info("Updating cluster sharding state for {} shards", stateChanged.newState.keys.size)
          clusterShardingStateRef.set(stateChanged.newState)
          watchShardStateChanges()
        }
      }
      .recover {
        case _: AskTimeoutException =>
        // we were shutting down, ignore
        case NonFatal(t) =>
          log.warning("Could not monitor cluster sharding state: {}", t.getMessage)
      }
  }
}

private[locality] final case class ShardLocationAwareRoutee(routee: Routee, selfAddress: Address) {
  // extract the address of the routee. In case of a LocalActorRef, host and port are not provided
  // therefore we fall back to the address of the local node
  val address = {
    val routeeAddress = routee match {
      case ActorRefRoutee(ref)       => ref.path.address
      case ActorSelectionRoutee(sel) => sel.anchorPath.address
    }

    routeeAddress match {
      case Address(_, system, None, None) => selfAddress.copy(system = system)
      case fullAddress                    => fullAddress
    }
  }
}
