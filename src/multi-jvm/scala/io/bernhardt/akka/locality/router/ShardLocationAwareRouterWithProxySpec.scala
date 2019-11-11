package io.bernhardt.akka.locality.router

import akka.actor.{ ActorRef, Props }
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
import akka.cluster.sharding.{ ClusterSharding, MultiNodeClusterShardingConfig, MultiNodeClusterShardingSpec }
import akka.pattern.ask
import akka.remote.testconductor.RoleName
import akka.remote.testkit.STMultiNodeSpec
import akka.routing.{ GetRoutees, Routees }
import akka.testkit.{ DefaultTimeout, ImplicitSender, TestProbe }
import com.typesafe.config.ConfigFactory
import io.bernhardt.akka.locality.Locality

import scala.concurrent.Await
import scala.concurrent.duration._

object ShardLocationAwareRouterWithProxySpecConfig
    extends MultiNodeClusterShardingConfig(
      loglevel = "INFO",
      overrides = ConfigFactory.parseString(
        """akka.cluster.sharding.distributed-data.majority-min-cap = 2
          |akka.cluster.distributed-data.gossip-interval = 200 ms
          |akka.cluster.distributed-data.notify-subscribers-interval = 200 ms
          |akka.cluster.sharding.updating-state-timeout = 500 ms
          |akka.locality.shard-state-update-margin = 1000 ms
          |""".stripMargin)) {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

}

class ShardLocationAwareRouterWithProxySpecMultiJvmNode1 extends ShardLocationAwareRouterWithProxySpec
class ShardLocationAwareRouterWithProxySpecMultiJvmNode2 extends ShardLocationAwareRouterWithProxySpec
class ShardLocationAwareRouterWithProxySpecMultiJvmNode3 extends ShardLocationAwareRouterWithProxySpec
class ShardLocationAwareRouterWithProxySpecMultiJvmNode4 extends ShardLocationAwareRouterWithProxySpec
class ShardLocationAwareRouterWithProxySpecMultiJvmNode5 extends ShardLocationAwareRouterWithProxySpec

class ShardLocationAwareRouterWithProxySpec
    extends MultiNodeClusterShardingSpec(ShardLocationAwareRouterWithProxySpecConfig)
    with STMultiNodeSpec
    with DefaultTimeout
    with ImplicitSender {

  import ShardLocationAwareRouterSpec._
  import ShardLocationAwareRouterWithProxySpecConfig._

  var region: Option[ActorRef] = None
  var proxyRegion: Option[ActorRef] = None

  var router: ActorRef = ActorRef.noSender
  var proxyRouter: ActorRef = ActorRef.noSender

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

    join(fifth, first)

    runOn(fifth) {
      val proxy = ClusterSharding(system).startProxy(
        typeName = "TestEntity",
        role = None,
        dataCenter = None,
        extractEntityId = extractEntityId,
        extractShardId = extractShardId)
      proxyRegion = Some(proxy)
    }

    awaitClusterUp(roles: _*)

    enterBarrier("shards-allocated")

  }

  "route by taking into account shard location" in {
    within(20.seconds) {

      runOn(first, second, third, fourth) {
        region
          .map { r =>
            system.actorOf(Props(new TestRoutee(r)), "routee")
            enterBarrier("routee-started")

            router = system.actorOf(
              ClusterRouterGroup(
                ShardLocationAwareGroup(
                  routeePaths = Nil,
                  shardRegion = r,
                  extractEntityId = extractEntityId,
                  extractShardId = extractShardId),
                ClusterRouterGroupSettings(
                  totalInstances = 4,
                  routeesPaths = List("/user/routee"),
                  allowLocalRoutees = true)).props(),
              "sharding-aware-router")

            awaitAssert {
              currentRoutees(router).size shouldBe 4
            }
            enterBarrier("router-started")

          }
          .getOrElse {
            fail("Region not set")
          }

      }
      runOn(fifth) {

        // no routees here
        enterBarrier("routee-started")

        proxyRegion
          .map { proxy =>
            proxyRouter = system.actorOf(
              ClusterRouterGroup(
                ShardLocationAwareGroup(
                  routeePaths = Nil,
                  shardRegion = proxy,
                  extractEntityId = extractEntityId,
                  extractShardId = extractShardId),
                ClusterRouterGroupSettings(
                  totalInstances = 4,
                  routeesPaths = List("/user/routee"),
                  allowLocalRoutees = false)).props(),
              "sharding-aware-router")
          }
          .getOrElse {
            fail("Proxy region not set")
          }

        awaitAssert {
          currentRoutees(proxyRouter).size shouldBe 4
        }
        enterBarrier("router-started")
      }

      // now give time to the new shards to be allocated and time to the router to retrieve new information
      // one second to send out the update shard information and one second of safety margin
      Thread.sleep(2000)

      runOn(fifth) {
        val probe = TestProbe("probe")
        for (i <- 1 to 40) {
          probe.send(proxyRouter, Ping(i, probe.ref))
        }

        val msgs: Seq[Pong] = probe.receiveN(40).collect { case p: Pong => p }

        val (same: Seq[Pong], different) = msgs.partition {
          case Pong(_, _, routeeAddress, entityAddress) =>
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
      }
      enterBarrier("test-done")

    }

  }

  "adjust routing after a topology change" in {
    awaitMemberRemoved(fourth)
    awaitAllReachable()
    runOn(first) {
      testConductor.removeNode(fourth)
    }

    runOn(fifth) {
      // trigger rebalancing the shards of the removed node
      val rebalanceProbe = TestProbe("rebalance")
      for (i <- 31 to 40) {
        rebalanceProbe.send(proxyRouter, Ping(i, rebalanceProbe.ref))
      }

      // we should be receiving messages even in the absence of the updated shard location information
      // random routing should kick in, i.e. we won't have perfect matches
      val randomRoutedMessages: Seq[Pong] = rebalanceProbe.receiveN(10, 20.seconds).collect { case p: Pong => p }
      val (_, differentMsgs) = partitionByAddress(randomRoutedMessages)
      differentMsgs.nonEmpty shouldBe true

      // now give time to the new shards to be allocated and time to the router to retrieve new information
      // one second to send out the update shard information and one second of safety margin
      Thread.sleep(2000)

      val probe = TestProbe("probe")
      for (i <- 1 to 40) {
        probe.send(proxyRouter, Ping(i, probe.ref))
      }

      val msgs: Seq[Pong] = probe.receiveN(40, 10.seconds).collect { case p: Pong => p }

      val (_, different) = msgs.partition {
        case Pong(_, _, routeeAddress, entityAddress) =>
          routeeAddress.hostPort == entityAddress.hostPort && routeeAddress.hostPort.nonEmpty
      }

      different.isEmpty shouldBe true

    }

    runOn(first, second, third, fifth) {
      enterBarrier("finished")

    }

  }

  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees

  def partitionByAddress(msgs: Seq[Pong]) = msgs.partition {
    case Pong(_, _, routeeAddress, entityAddress) =>
      routeeAddress.hostPort == entityAddress.hostPort && routeeAddress.hostPort.nonEmpty
  }

}
