package io.bernhardt.akka.locality

import akka.actor.{Actor, ActorRef, Address, Props}
import akka.cluster.{Cluster, MultiNodeClusterSpec}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.cluster.sharding.{MultiNodeClusterShardingConfig, MultiNodeClusterShardingSpec, ShardRegion}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.STMultiNodeSpec
import akka.routing.{GetRoutees, Routees}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import akka.pattern.ask
import akka.serialization.jackson.CborSerializable

import scala.concurrent.Await
import scala.concurrent.duration._

object ShardLocalityAwareRouterSpec {

  class TestEntity extends Actor {
    val cluster = Cluster(context.system)
    def receive: Receive = {

      case ping: Ping =>
        sender() ! Pong(ping.id, ping.sender, cluster.selfAddress, cluster.selfAddress)
    }
  }

  final case class Ping(id: Int, sender: ActorRef) extends CborSerializable
  final case class Pong(id: Int, sender: ActorRef, routeeAddress: Address, entityAddress: Address) extends CborSerializable

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg@Ping(id, _) => (id.toString, msg)
    case msg@Pong(id, _, _, _) => (id.toString, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    // take this simplest mapping on purpose
    case Ping(id, _) => id.toString
    case Pong(id, _, _, _) => id.toString
  }

  class TestRoutee(region: ActorRef) extends Actor {
    val cluster = Cluster(context.system)
    def receive: Receive = {
      case msg: Ping =>
        region ! msg
      case pong: Pong =>
        pong.sender ! pong.copy(routeeAddress = cluster.selfAddress)
    }
  }

}


object ShardLocalityAwareRouterSpecConfig extends MultiNodeClusterShardingConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(ConfigFactory.parseString(
    s"""
    akka.loglevel = INFO
    akka.actor.debug.lifecycle = on
    akka.actor.provider = "cluster"
    akka.cluster.sharding.state-store-mode = ddata
    akka.cluster.sharding.updating-state-timeout = 1s
    akka.cluster.sharding.distributed-data.durable.lmdb.dir = /tmp/ddata

    akka.remote.use-passive-connections=off
    """).withFallback(MultiNodeClusterSpec.clusterConfig))

  testTransport(on = true)
}

class ShardLocalityAwareRouterSpecMultiJvmNode1 extends ShardLocalityAwareRouterSpec
class ShardLocalityAwareRouterSpecMultiJvmNode2 extends ShardLocalityAwareRouterSpec
class ShardLocalityAwareRouterSpecMultiJvmNode3 extends ShardLocalityAwareRouterSpec
class ShardLocalityAwareRouterSpecMultiJvmNode4 extends ShardLocalityAwareRouterSpec
class ShardLocalityAwareRouterSpecMultiJvmNode5 extends ShardLocalityAwareRouterSpec

class ShardLocalityAwareRouterSpec extends MultiNodeClusterShardingSpec(ShardLocalityAwareRouterSpecConfig)
  with STMultiNodeSpec
  with DefaultTimeout
  with ImplicitSender {

  import ShardLocalityAwareRouterSpecConfig._
  import ShardLocalityAwareRouterSpec._

  var region: Option[ActorRef] = None

  def joinAndAllocate(node: RoleName, entityIds: Range): Unit = {
    within(10.seconds) {
      join(node, first)
      runOn(node) {
        val region = startSharding(
          sys = system,
          entityProps = Props[TestEntity],
          dataType = "TestEntity",
          extractEntityId = extractEntityId,
          extractShardId = extractShardId)

        this.region = Some(region)

        entityIds.map { entityId =>
          val probe = TestProbe("test")
          val msg = Ping(entityId, ActorRef.noSender)
          probe.send(region, msg)
          probe.expectMsgType[Pong]
          probe.lastSender.path should be(region.path / s"$entityId" / s"$entityId")
        }
      }
    }
    enterBarrier(s"started")
  }

  "allocate shards" in {

    joinAndAllocate(first, (1 to 10))
    joinAndAllocate(second, (11 to 20))
    joinAndAllocate(third, (21 to 30))
    joinAndAllocate(fourth, (31 to 40))
    joinAndAllocate(fifth, (41 to 50))

    enterBarrier("shards-allocated")
  }

  "route by taking into account shard location" in {
    within(20.seconds) {

      region.map { r =>
        system.actorOf(Props(new TestRoutee(r)), "routee")
        enterBarrier("routee-started")

        val router = system.actorOf(ClusterRouterGroup(ShardLocalityAwareGroup(
          routeePaths = Nil,
          shardRegion = r,
          extractEntityId = extractEntityId,
          extractShardId = extractShardId
        ), ClusterRouterGroupSettings(
          totalInstances = 5,
          routeesPaths = List("/user/routee"),
          allowLocalRoutees = true
        )).props(), "sharding-aware-router")

        awaitAssert {
          currentRoutees(router).size shouldBe 5
        }

        enterBarrier("router-started")

        runOn(first) {
          val probe = TestProbe("probe")
          for (i <- 1 to 50) {
            probe.send(router, Ping(i, probe.ref))
          }

          val msgs: Seq[Pong] = probe.receiveN(50).collect { case p: Pong => p }

          val (same: Seq[Pong], different) = msgs.partition { case Pong(_, _, routeeAddress, entityAddress) =>
            routeeAddress.hostPort == entityAddress.hostPort && routeeAddress.hostPort.nonEmpty
          }

          different.isEmpty shouldBe true

          val byAddress = same.groupBy(_.routeeAddress)

          awaitAssert { byAddress(first).size shouldBe 10 }
          byAddress(first).map(_.id).toSet shouldEqual (1 to 10).toSet
          awaitAssert { byAddress(second).size shouldBe 10 }
          byAddress(second).map(_.id).toSet shouldEqual (11 to 20).toSet
          awaitAssert { byAddress(third).size shouldBe 10 }
          byAddress(third).map(_.id).toSet shouldEqual (21 to 30).toSet
          awaitAssert { byAddress(fourth).size shouldBe 10 }
          byAddress(fourth).map(_.id).toSet shouldEqual (31 to 40).toSet
          awaitAssert { byAddress(fifth).size shouldBe 10 }
          byAddress(fifth).map(_.id).toSet shouldEqual (41 to 50).toSet
        }
        enterBarrier("test-done")

      } getOrElse {
        fail("Region not set")
      }
    }

  }


  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees


}
