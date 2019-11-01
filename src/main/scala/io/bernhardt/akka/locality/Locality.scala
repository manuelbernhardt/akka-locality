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
import com.typesafe.config.Config
import io.bernhardt.akka.locality.router.ShardStateMonitor

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
}

private[locality] final class LocalitySupervisor(settings: LocalitySettings) extends Actor with ActorLogging {
  import LocalitySupervisor._

  def receive: Receive = {
    case m @ MonitorShards(region) =>
      context
        .child(region.path.name)
        .map { monitor =>
          monitor.forward(m)
        }
        .getOrElse {
          log.info("Starting to monitor shards of region {}", region.path.name)
          context.actorOf(ShardStateMonitor.props(region, settings), region.path.name).forward(m)
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
}
