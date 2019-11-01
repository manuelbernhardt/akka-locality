# akka-locality

This module provides constructs that help to make better use of the locality of actors within a clustered Akka system.

### SBT

```sbt
libraryDependencies += "io.bernhardt" %% "akka-locality" % "1.0.0"
```

### Maven

```xml
<dependency>
  <groupId>io.bernhardt</groupId>
  <artifactId>akka-locality_2.12</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Shard location aware routers

This type of router is useful for systems in which the routees of cluster-aware routers need to communicate with sharded 
entities.

With a common routing logic (random, round-robin) there may be an extra network hop (or two when considering replies) 
between a routee and the sharded entities it needs to talk to. Shard location aware routers optimize this by routing 
to the routee closest to the sharded entity. It does so by using the same rules for extracting the `shardId` from a 
message as used by the shard regions themselves.

When the router has not yet retrieved sharding state, it falls back to random routing.
When there are more than one candidate routee close to a sharded entity, one of them is picked at random.

In order to use these routers, the `Locality` extension must be started:

### Scala

```scala
import io.bernhardt.akka.locality._

val system: ActorSystem = ...
Locality(system)
```

### Java

```java
import io.bernhardt.akka.locality;

ActorSystem system = ...;
Locality.get(system);
```
    
You can then use the group or pool routers as a cluster-aware router. These routers must be declared in code, as they
require to be passed elements from the sharding setup:

### Scala

```scala
val system: ActorSystem = ...
val extractEntityId: ShardRegion.ExtractEntityId = ...
val extractShardId: ShardRegion.ExtractShardId = ...
val region: ActorRef = ...

val router = system.actorOf(ClusterRouterGroup(ShardLocationAwareGroup(
  routeePaths = Nil,
  shardRegion = region,
  extractEntityId = extractEntityId,
  extractShardId = extractShardId
), ClusterRouterGroupSettings(
  totalInstances = 5,
  routeesPaths = List("/user/routee"),
  allowLocalRoutees = true
)).props(), "shard-location-aware-router")
```

### Java


```java
ActorRef region = ...;
ShardRegion.MessageExtractor messageExtractor = ...;
int totalInstances = 5;
Iterable<String> routeesPaths = Collections.singletonList("/user/routee");
boolean allowLocalRoutees = true;
Set<String> useRoles = new HashSet<>(Arrays.asList("role"));

ActorRef router = getContext()
        .actorOf(
                new ClusterRouterGroup(
                        new ShardLocationAwareGroup(
                                routeesPaths,
                                region,
                                messageExtractor
                        ).props(),
                        new ClusterRouterGroupSettings(
                                totalInstances,
                                routeesPaths,
                                allowLocalRoutees,
                                useRoles
                        )
                ));
```

Always make sure that:

- you use exactly the same logic for the routers as you use for sharding
- you deploy the routers on all the nodes on which sharding is enabled

### Configuration

See [reference.conf](https://github.com/manuelbernhardt/akka-locality/blob/master/src/main/resources/reference.conf) for more information about the configuration of the routing mechanism.

