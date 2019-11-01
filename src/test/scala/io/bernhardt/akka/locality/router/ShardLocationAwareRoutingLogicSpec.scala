package io.bernhardt.akka.locality.router

import akka.actor.ActorSystem
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.{ ClusterShardingStats, GetClusterShardingStats, ShardRegionStats }
import akka.routing.ActorRefRoutee
import akka.testkit.{ TestKit, TestProbe }
import io.bernhardt.akka.locality.Locality
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.collection.immutable.IndexedSeq

class ShardLocationAwareRoutingLogicSpec
    extends TestKit(ActorSystem("ShardLocalityAwareRoutingLogicSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  import ShardLocationAwareRoutingLogicSpec._

  "The ShardLocalityAwareRoutingLogic" should {
    "fall back to random routing when no sharding state is available" in {
      Locality(system)

      val shardRegion = TestProbe("region")
      val routee1 = TestProbe("routee1")
      val routee2 = TestProbe("routee2")
      val allRoutees = IndexedSeq(routee1, routee2).map(r => ActorRefRoutee(r.ref))

      val logic = ShardLocationAwareRoutingLogic(system, shardRegion.ref, extractEntityId, extractShardId)

      val runs = 1000
      val selections = for (_ <- 1 to runs) yield {
        logic.select(TestMessage(1), allRoutees)
      }
      val count = selections.count(_ == allRoutees(0))
      val expectedCount = runs * 50 / 100
      val variation = expectedCount / 10

      count shouldEqual expectedCount +- variation
    }

    "retrieve shard state on startup and use it in order to route messages" in {
      // use system identifier in order to simulate running on multiple nodes
      val system1 = ActorSystem("node1")
      val system2 = ActorSystem("node2")
      val system3 = ActorSystem("node3")
      Locality(system1)
      Locality(system2)
      Locality(system3)

      val routee1 = TestProbe("routee1")(system1)
      val routee2 = TestProbe("routee2")(system2)
      val routee3 = TestProbe("routee3")(system3)

      val allRoutees = IndexedSeq(routee1, routee2, routee3).map(r => ActorRefRoutee(r.ref))

      val region1 = TestProbe("region1")(system1)
      val shards1 = for (i <- 1 to 10) yield i
      val region2 = TestProbe("region2")(system2)
      val shards2 = for (i <- 11 to 20) yield i
      val region3 = TestProbe("region3")(system3)
      val shards3 = for (i <- 21 to 30) yield i

      val logic = ShardLocationAwareRoutingLogic(system1, region1.ref, extractEntityId, extractShardId)

      // logic tries to get shard state
      import scala.concurrent.duration._
      region1.expectMsgType[GetClusterShardingStats](5.seconds)
      val monitor1 = region1.sender()

      monitor1 ! ClusterShardingStats(regions = Map(region1.ref.path.address -> ShardRegionStats(shards1.map { id =>
        id.toString -> 0
      }.toMap), region2.ref.path.address -> ShardRegionStats(shards2.map { id =>
        id.toString -> 0
      }.toMap), region3.ref.path.address -> ShardRegionStats(shards3.map { id =>
        id.toString -> 0
      }.toMap)))

      // TODO find a better way to determine that the router is ready
      Thread.sleep(500)

      for (i <- 1 to 10) {
        logic.select(TestMessage(i), allRoutees) shouldEqual ActorRefRoutee(routee1.ref)
      }
      for (i <- 11 to 20) {
        logic.select(TestMessage(i), allRoutees) shouldEqual ActorRefRoutee(routee2.ref)
      }
      for (i <- 21 to 30) {
        logic.select(TestMessage(i), allRoutees) shouldEqual ActorRefRoutee(routee3.ref)
      }

      TestKit.shutdownActorSystem(system1)
      TestKit.shutdownActorSystem(system2)
      TestKit.shutdownActorSystem(system3)
    }
  }

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}

object ShardLocationAwareRoutingLogicSpec {
  final case class TestMessage(id: Int)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TestMessage(id) => (id.toString, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    // take this simplest mapping on purpose
    case TestMessage(id) => id.toString
  }
}
