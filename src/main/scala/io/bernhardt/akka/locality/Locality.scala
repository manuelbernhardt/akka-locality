package io.bernhardt.akka.locality

import java.util.concurrent.TimeUnit

import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  ActorSystem,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider,
  Props
}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.MessageExtractor
import com.typesafe.config.Config
import io.bernhardt.akka.locality.router.{ ShardLocationAwareGroup, ShardLocationAwarePool, ShardStateMonitor }

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object Locality extends ExtensionId[Locality] with ExtensionIdProvider {
  override def get(system: ActorSystem): Locality = super.get(system)

  override def createExtension(system: ExtendedActorSystem): Locality = new Locality(system)

  override def lookup(): ExtensionId[_ <: Extension] = Locality
}

/**
 * This module provides constructs that help to make better use of the locality of actors within a clustered Akka system.
 */
class Locality(system: ExtendedActorSystem) extends Extension {
  private val settings = LocalitySettings(system.settings.config)

  system.systemActorOf(Props(new LocalitySupervisor(settings)), "locality")

  /**
   * Scala API: Create a shard location aware group
   *
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *                    sent with [[akka.actor.ActorSelection]] to these paths
   * @param shardRegion the reference to the shard region
   * @param extractEntityId the [[akka.cluster.sharding.ShardRegion.ExtractEntityId]] function used to extract the entity id from a message
   * @param extractShardId the [[akka.cluster.sharding.ShardRegion.ExtractShardId]] function used to extract the shard id from a message
   */
  def shardLocationAwareGroup(
      routeePaths: immutable.Iterable[String],
      shardRegion: ActorRef,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ShardLocationAwareGroup =
    ShardLocationAwareGroup(routeePaths, shardRegion, extractEntityId, extractShardId)

  /**
   * Java API: Create a shard location aware group
   *
   * @param routeePaths string representation of the actor paths of the routees, messages are
   *                    sent with [[akka.actor.ActorSelection]] to these paths
   * @param shardRegion the reference to the shard region
   * @param messageExtractor the [[akka.cluster.sharding.ShardRegion.MessageExtractor]] used for the sharding
   *                         of the entities this router should optimize routing for
   */
  def shardLocationAwareGroup(
      routeePaths: java.lang.Iterable[String],
      shardRegion: ActorRef,
      messageExtractor: MessageExtractor): ShardLocationAwareGroup =
    new ShardLocationAwareGroup(routeePaths, shardRegion, messageExtractor)

  /**
   * Scala API: Create a shard location aware pool
   *
   * @param nrOfInstances how many routees this pool router should have
   * @param shardRegion the reference to the shard region
   * @param extractEntityId the [[akka.cluster.sharding.ShardRegion.ExtractEntityId]] function used to extract the entity id from a message
   * @param extractShardId the [[akka.cluster.sharding.ShardRegion.ExtractShardId]] function used to extract the shard id from a message
   */
  def shardLocationAwarePool(
      nrOfInstances: Int,
      shardRegion: ActorRef,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ShardLocationAwarePool =
    ShardLocationAwarePool(
      nrOfInstances = nrOfInstances,
      shardRegion = shardRegion,
      extractEntityId = extractEntityId,
      extractShardId = extractShardId)

  /**
   * Java API: Create a shard location aware pool
   *
   * @param nrOfInstances how many routees this pool router should have
   * @param shardRegion the reference to the shard region
   * @param messageExtractor the [[akka.cluster.sharding.ShardRegion.MessageExtractor]] used for the sharding
   *                         of the entities this router should optimize routing for
   */
  def shardLocationAwarePool(nrOfInstances: Int, shardRegion: ActorRef, messageExtractor: MessageExtractor) =
    new ShardLocationAwarePool(nrOfInstances, shardRegion, messageExtractor)
}

private[locality] final class LocalitySupervisor(settings: LocalitySettings) extends Actor with ActorLogging {
  import LocalitySupervisor._

  def receive: Receive = {
    case m @ MonitorShards(region) =>
      val regionName = encodeRegionName(region)
      context
        .child(regionName)
        .map { monitor =>
          monitor.forward(m)
        }
        .getOrElse {
          log.info("Starting to monitor shards of region {}", regionName)
          context.actorOf(ShardStateMonitor.props(region, regionName, settings), regionName).forward(m)
        }
  }
}

object LocalitySupervisor {
  private[locality] final case class MonitorShards(region: ActorRef)
}

final case class LocalitySettings(config: Config) {
  private val localityConfig = config.getConfig("akka.locality")

  val RetrieveShardStateTimeout: FiniteDuration =
    FiniteDuration(
      localityConfig.getDuration("retrieve-shard-state-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  val ShardStateUpdateMargin =
    FiniteDuration(
      localityConfig.getDuration("shard-state-update-margin", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  val ShardStatePollingInterval =
    FiniteDuration(
      localityConfig.getDuration("shard-state-polling-interval", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)
}
