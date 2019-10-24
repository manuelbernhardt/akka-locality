package io.bernhardt.akka.locality

import akka.actor.{Actor, Props}
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.{MultiNodeClusterShardingConfig, MultiNodeClusterShardingSpec, ShardRegion}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object ShardLocalityAwareRouterSpec {
  class TestEntity extends Actor {
    def receive: Receive = {
      case msg: TestMessage => sender() ! msg
    }
  }
  final case class TestMessage(id: Int)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TestMessage(id) => (id.toString, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    // take this simplest mapping on purpose
    case TestMessage(id) => id.toString
  }

}


object ShardLocalityAwareRouterSpecConfig extends MultiNodeClusterShardingConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.sharding.state-store-mode = ddata
    akka.cluster.sharding.updating-state-timeout = 1s
    akka.cluster.sharding.distributed-data.durable.lmdb.dir = /tmp/ddata
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
  with ImplicitSender {

  import ShardLocalityAwareRouterSpecConfig._
  import ShardLocalityAwareRouterSpec._

  override def initialParticipants: Int = roles.size

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

        entityIds.foreach { entityId =>
          val msg = TestMessage(entityId)
          region ! msg
          expectMsg(msg)
          lastSender.path should be(region.path / s"$entityId" / s"$entityId")
        }
      }
    }
    enterBarrier(s"started-$node")
  }

  "allocate shards" in {
    joinAndAllocate(first, (1 to 10))
    joinAndAllocate(second, (11 to 20))
    joinAndAllocate(third, (21 to 30))
    joinAndAllocate(fourth, (31 to 40))
    joinAndAllocate(fifth, (41 to 50))

    enterBarrier("shards-allocated")
  }




}
