package io.bernhardt.akka.locality.router

import akka.actor.{
  Actor,
  ActorIdentity,
  ActorLogging,
  ActorRef,
  Address,
  DeadLetterSuppression,
  Identify,
  Props,
  RootActorPath,
  Terminated,
  Timers
}
import akka.cluster.sharding.ShardRegion.{ ClusterShardingStats, GetClusterShardingStats, ShardId, ShardRegionStats }
import io.bernhardt.akka.locality._
import io.bernhardt.akka.locality.LocalitySupervisor.MonitorShards

/**
 * Internal: watches shard actors in order to trigger an update. Only trigger the update when the system is stable for a while.
 */
private[locality] class ShardStateMonitor(shardRegion: ActorRef, encodedRegionName: String, settings: LocalitySettings)
    extends Actor
    with ActorLogging
    with Timers {
  import ShardStateMonitor._

  val ClusterGuardianName: String =
    context.system.settings.config.getString("akka.cluster.sharding.guardian-name")

  var watchedShards = Set.empty[ShardId]

  var routerLogic: ActorRef = context.system.deadLetters

  def receive: Receive = {
    case _: MonitorShards =>
      log.debug("Starting to monitor shards for logic {}", routerLogic.path)
      routerLogic = sender()
      if (watchedShards.isEmpty) {
        requestClusterShardingState()
      }
    case UpdateClusterState =>
      requestClusterShardingState()
    case ActorIdentity(shardId: ShardId, Some(ref)) =>
      log.debug("Now watching shard {}", ref.path)
      context.watch(ref)
      watchedShards += shardId
    case ActorIdentity(shardId, None) => // couldn't get shard ref, not much we can do
      log.warning("Could not watch shard {}, shard location aware routing may not work", shardId)
    case Terminated(ref) =>
      log.debug("Watched shard actor {} terminated", ref.path)
      watchedShards -= encodeShardId(ref.path.name)
      // reset the timer - we only want to request state once things are stable
      timers.cancel(UpdateClusterState)
      timers.startSingleTimer(UpdateClusterState, UpdateClusterState, settings.ShardStateUpdateMargin)
    case ClusterShardingStats(regions) =>
      log.debug("Received cluster sharding stats for {} regions", regions.size)
      if (regions.isEmpty) {
        log.warning("Cluster Sharding Stats empty - locality-aware routing will not function correctly")
      }
      notifyShardStateChanged(regions)
      watchShards(regions)
  }

  def requestClusterShardingState(): Unit = {
    log.debug("Requesting cluster state update")
    shardRegion ! GetClusterShardingStats(settings.RetrieveShardStateTimeout)
  }

  def watchShards(regions: Map[Address, ShardRegionStats]): Unit = {
    regions.foreach {
      case (address, regionStats) =>
        val regionPath = RootActorPath(address) / "system" / ClusterGuardianName / encodedRegionName
        regionStats.stats.keys.filterNot(watchedShards).foreach { shardId =>
          val shardPath = regionPath / encodeShardId(shardId)
          context.actorSelection(shardPath) ! Identify(shardId)
        }
    }
  }

  def notifyShardStateChanged(regions: Map[Address, ShardRegionStats]): Unit = {
    val shardsByAddress = regions.flatMap {
      case (address, ShardRegionStats(shards)) =>
        shards.map {
          case (shardId, _) =>
            shardId -> address
        }
    }
    routerLogic ! ShardStateChanged(shardsByAddress)
  }

  override def postStop(): Unit = {
    routerLogic ! ShardStateChanged(Map.empty)
  }
}

object ShardStateMonitor {
  final case class ShardStateChanged(newState: Map[ShardId, Address]) extends DeadLetterSuppression
  final case object UpdateClusterState extends DeadLetterSuppression

  private[locality] def props(shardRegion: ActorRef, entityName: String, settings: LocalitySettings) =
    Props(new ShardStateMonitor(shardRegion, entityName, settings))
}
