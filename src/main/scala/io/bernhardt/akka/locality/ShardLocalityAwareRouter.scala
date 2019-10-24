package io.bernhardt.akka.locality

import java.net.URLEncoder
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import akka.actor._
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion._
import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.routing._
import akka.util.Timeout
import akka.pattern.{AskTimeoutException, ask}
import akka.japi.Util.immutableSeq

import scala.collection.immutable
import scala.concurrent.duration._
import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
import scala.util.control.NonFatal

/*
  doc: router must be deployed on all / same nodes partaking in sharding
*/

object ShardLocalityAwareRouter {
  def extractEntityIdFrom(messageExtractor: MessageExtractor): ShardRegion.ExtractEntityId = {
    case msg if messageExtractor.entityId(msg) ne null =>
      (messageExtractor.entityId(msg), messageExtractor.entityMessage(msg))
  }
}

@SerialVersionUID(1L)
final case class ShardLocalityAwareGroup(
  routeePaths: immutable.Iterable[String],
  shardRegion: ActorRef,
  extractEntityId: ShardRegion.ExtractEntityId,
  extractShardId: ShardRegion.ExtractShardId,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId) extends Group {

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
      ShardLocalityAwareRouter.extractEntityIdFrom(messageExtractor),
      extractShardId = msg => messageExtractor.shardId(msg),
    )

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): ShardLocalityAwareGroup = copy(routerDispatcher = dispatcherId)

  override def paths(system: ActorSystem): immutable.Iterable[String] = routeePaths

  override def createRouter(system: ActorSystem): Router =
    new Router(ShardLocalityAwareRoutingLogic(system, shardRegion, extractEntityId, extractShardId))
}

@SerialVersionUID(1L)
final case class ShardLocalityAwarePool(
  nrOfInstances: Int,
  override val resizer: Option[Resizer] = None,
  shardRegion: ActorRef,
  extractEntityId: ShardRegion.ExtractEntityId,
  extractShardId: ShardRegion.ExtractShardId,
  override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
  override val usePoolDispatcher: Boolean = false) extends Pool {

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
      extractEntityId = ShardLocalityAwareRouter.extractEntityIdFrom(messageExtractor),
      extractShardId = msg => messageExtractor.shardId(msg)
    )

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): ShardLocalityAwarePool = copy(supervisorStrategy = strategy)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): ShardLocalityAwarePool = copy(resizer = Some(resizer))

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): ShardLocalityAwarePool = copy(routerDispatcher = dispatcherId)

  override def createRouter(system: ActorSystem): Router =
    new Router(ShardLocalityAwareRoutingLogic(system, shardRegion, extractEntityId, extractShardId))

  override def nrOfInstances(sys: ActorSystem): Int = this.nrOfInstances
}

final case class ShardLocalityAwareRoutingLogic(
  system: ActorSystem,
  shardRegion: ActorRef,
  extractEntityId: ShardRegion.ExtractEntityId,
  extractShardId: ShardRegion.ExtractShardId,
) extends RoutingLogic {

  import ShardStateMonitor._
  import system.dispatcher

  private lazy val log = Logging(system, getClass)
  private lazy val selfAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
  private val clusterShardingStateRef = new AtomicReference[Map[ShardId, Address]](Map.empty)
  private val shardingAwareRouteeRef = new AtomicReference[(IndexedSeq[Routee], Map[Address, IndexedSeq[ShardingAwareRoutee]])]((IndexedSeq.empty, Map.empty))
  // TODO this should be created as a system actor as part of an extension
  private val shardStateMonitor = system.actorOf(ShardStateMonitor.props(shardRegion), shardRegion.path.name)

  watchShardStateChanges()

  override def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    if(routees.isEmpty) {
      NoRoutee
    } else {
      // avoid re-creating routees for each message by checking if they have changed
      def updateShardingAwareRoutees(): Map[Address, IndexedSeq[ShardingAwareRoutee]] = {
        val oldShardingRouteeTuple = shardingAwareRouteeRef.get()
        val (oldRoutees, oldShardingAwareRoutees) = oldShardingRouteeTuple
        if (routees ne oldRoutees) {
          val newShardingAwareRoutees = if (routees == oldRoutees) {
            oldShardingAwareRoutees
          } else {
            val allRoutees = routees.map(ShardingAwareRoutee(_, selfAddress))
            allRoutees.groupBy(_.address)
          }
          shardingAwareRouteeRef.compareAndSet(oldShardingRouteeTuple, (routees, newShardingAwareRoutees))
          newShardingAwareRoutees
        } else {
          oldShardingAwareRoutees
        }
      }

      val shardId: ShardId = extractShardId(message)
      val shardingAwareRoutees = updateShardingAwareRoutees()

      val candidateRoutees = for {
        location <- clusterShardingStateRef.get().get(shardId)
        routees <- shardingAwareRoutees.get(location)
      } yield {
        val closeRoutees = routees.map(_.routee)

        // pick one at random
        closeRoutees(ThreadLocalRandom.current.nextInt(closeRoutees.size))
      }

      candidateRoutees.getOrElse {
        // if we couldn't figure out the location of the shard, fall back to random routing
        routees(ThreadLocalRandom.current.nextInt(routees.size))
      }

    }
  }

  private def watchShardStateChanges(): Unit = {
    implicit val timeout: Timeout = Timeout(2^64, TimeUnit.DAYS)
    val change: Future[ShardStateChanged] = (shardStateMonitor ? MonitorShards).mapTo[ShardStateChanged]
    change.map { stateChanged =>
      if(stateChanged.newState.nonEmpty) {
        log.info("Updating cached cluster sharding state, got state for {} shards", stateChanged.newState.keys.size)
        clusterShardingStateRef.set(stateChanged.newState)
        watchShardStateChanges()
      }
    }.recover {
      case _: AskTimeoutException =>
        // we were shutting down, ignore
      case NonFatal(t) =>
        log.warning("Could not watch cluster sharding state: {}", t.getMessage)
    }
  }


}

private[locality] class ShardStateMonitor(shardRegion: ActorRef) extends Actor with ActorLogging {

  import ShardStateMonitor._

  val clusterGuardianName: String =
    context.system.settings.config.getString("akka.cluster.sharding.guardian-name")

  // TODO make configurable
  private val ShardStateTimeout = Timeout(5.seconds)

  var watchedShards = Set.empty[ShardId]

  var routerLogicRef: ActorRef = context.system.deadLetters

  def receive: Receive =  {
    case MonitorShards =>
      routerLogicRef = sender()
      if(watchedShards.isEmpty) {
        requestClusterShardingState()
      }
    case ActorIdentity(shardId: ShardId, Some(ref)) =>
      log.debug("Received actor identity for shard {}", shardId)
      context.watch(ref)
      watchedShards += shardId
    case ActorIdentity(shardId, None) => // couldn't get shard ref, not much we can do
      log.warning("Could not watch shard {}, locality-aware routing may not work", shardId)
    case Terminated(ref) =>
      log.debug("Watched shard actor {} terminated", ref)
      watchedShards -= encodeShardId(ref.path.name)
      // TODO optimize this - buffer Terminated messages so as to not trigger a whole bunch of retries should the entire node have gone down
      requestClusterShardingState()
    case ClusterShardingStats(regions) =>
      notifyShardStateChanged(regions)
      watchShards(regions)
  }

  def requestClusterShardingState(): Unit =
    shardRegion ! GetClusterShardingStats(ShardStateTimeout.duration)


  def watchShards(regions: Map[Address, ShardRegionStats]): Unit = {
    val encodedRegionName = shardRegion.path.name
    regions.foreach { case (address, regionStats) =>
      val regionPath = RootActorPath(address) / "system" / clusterGuardianName / encodedRegionName
      regionStats.stats.keys.filterNot(watchedShards).foreach { shardId =>
        val shardPath = regionPath / encodeShardId(shardId)
        context.actorSelection(shardPath) ! Identify(shardId)
      }
    }
  }

  def notifyShardStateChanged(regions: Map[Address, ShardRegionStats]): Unit = {
    val shardsByAddress = regions.flatMap {
      case (address, ShardRegionStats(shards)) =>
        shards.map { case (shardId, _) =>
          shardId -> address
        }
    }
    routerLogicRef ! ShardStateChanged(shardsByAddress)
  }

  private def encodeShardId(id: ShardId): String = URLEncoder.encode(id, "utf-8")

  override def postStop(): Unit = {
    routerLogicRef ! ShardStateChanged(Map.empty)
  }
}

object ShardStateMonitor {
  final case object MonitorShards extends DeadLetterSuppression
  final case class ShardStateChanged(newState: Map[ShardId, Address]) extends DeadLetterSuppression

  private[locality] def props(shardRegion: ActorRef) = Props(new ShardStateMonitor(shardRegion))
}

private[locality] final case class ShardingAwareRoutee(routee: Routee, selfAddress: Address) {

  // extract the address of the routee. In case of a LocalActorRef, host and port are not provided
  // therefore we fall back to the address of the local node
  val address = {
    val routeeAddress = routee match {
      case ActorRefRoutee(ref)       => ref.path.address
      case ActorSelectionRoutee(sel) => sel.anchorPath.address
    }

    routeeAddress match {
      case Address(_, system, None, None) => selfAddress.copy(system = system)
      case fullAddress               => fullAddress

    }
  }

}

