package io.bernhardt.akka

import java.net.URLEncoder

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion.ShardId

package object locality {
  private[locality] def encodeRegionName(region: ActorRef): String =
    URLEncoder.encode(region.path.name.replaceAll("Proxy", ""), "utf-8")

  private[locality] def encodeShardId(id: ShardId): String = URLEncoder.encode(id, "utf-8")
}
