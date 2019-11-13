package io.bernhardt.akka.locality.router

import akka.actor.{ActorRef, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.cluster.sharding.{MultiNodeClusterShardingConfig, MultiNodeClusterShardingSpec}
import akka.pattern.ask
import akka.remote.testconductor.RoleName
import akka.remote.testkit.STMultiNodeSpec
import akka.routing.{GetRoutees, Routees}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import io.bernhardt.akka.locality.Locality

import scala.concurrent.Await
import scala.concurrent.duration._

object ShardLocationAwareRouterNewShardsSpecConfig extends MultiNodeClusterShardingConfig(
  overrides = ConfigFactory.parseString(
  """akka.cluster.sharding.distributed-data.majority-min-cap = 2
    |akka.cluster.distributed-data.gossip-interval = 200 ms
    |akka.cluster.distributed-data.notify-subscribers-interval = 200 ms
    |akka.cluster.sharding.updating-state-timeout = 500 ms
    |akka.locality.shard-state-update-margin = 1000 ms
    |akka.locality.shard-state-polling-interval = 2000 ms
    |""".stripMargin)) {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
}

class ShardLocationAwareRouterNewShardsSpecMultiJvmNode1 extends ShardLocationAwareRouterNewShardsSpec
class ShardLocationAwareRouterNewShardsSpecMultiJvmNode2 extends ShardLocationAwareRouterNewShardsSpec
class ShardLocationAwareRouterNewShardsSpecMultiJvmNode3 extends ShardLocationAwareRouterNewShardsSpec
class ShardLocationAwareRouterNewShardsSpecMultiJvmNode4 extends ShardLocationAwareRouterNewShardsSpec
class ShardLocationAwareRouterNewShardsSpecMultiJvmNode5 extends ShardLocationAwareRouterNewShardsSpec

class ShardLocationAwareRouterNewShardsSpec extends MultiNodeClusterShardingSpec(ShardLocationAwareRouterNewShardsSpecConfig)
  with STMultiNodeSpec
  with DefaultTimeout
  with ImplicitSender {

  import ShardLocationAwareRouterNewShardsSpecConfig._
  import ShardLocationAwareRouterSpec._

  var region: Option[ActorRef] = None

  var router: ActorRef = ActorRef.noSender

  Locality(system)

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

        router = system.actorOf(ClusterRouterGroup(ShardLocationAwareGroup(
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
        }
        enterBarrier("test-done")

      } getOrElse {
        fail("Region not set")
      }
    }

  }

  "route randomly when new shards are created" in {
    runOn(first) {
      val probe = TestProbe("probe")
      for (i <- 51 to 100) {
        probe.send(router, Ping(i, probe.ref))
      }

      val msgs: Seq[Pong] = probe.receiveN(50).collect { case p: Pong => p }

      val (_, different) = msgs.partition { case Pong(_, _, routeeAddress, entityAddress) =>
        routeeAddress.hostPort == entityAddress.hostPort && routeeAddress.hostPort.nonEmpty
      }

      different.isEmpty shouldBe false
    }
    enterBarrier("test-done")
  }

  "route with location awareness after the update margin has elapsed" in {

    Thread.sleep(4000)

    runOn(first) {
      val probe = TestProbe("probe")
      for (i <- 51 to 100) {
        probe.send(router, Ping(i, probe.ref))
      }

      val msgs: Seq[Pong] = probe.receiveN(50).collect { case p: Pong => p }

      val (_, different) = msgs.partition { case Pong(_, _, routeeAddress, entityAddress) =>
        routeeAddress.hostPort == entityAddress.hostPort && routeeAddress.hostPort.nonEmpty
      }

      different.isEmpty shouldBe true
    }
    enterBarrier("test-done")
  }



  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees

  def partitionByAddress(msgs: Seq[Pong]) = msgs.partition { case Pong(_, _, routeeAddress, entityAddress) =>
    routeeAddress.hostPort == entityAddress.hostPort && routeeAddress.hostPort.nonEmpty
  }


}
