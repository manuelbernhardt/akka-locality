import com.typesafe.sbt.MultiJvmPlugin.multiJvmSettings

lazy val akkaVersion = "2.5.26"

lazy val `akka-locality` = project
  .in(file("."))
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .settings(multiJvmSettings: _*)
  .settings(
    name := "akka-locality",
    organization := "io.bernhardt",
    organizationName := "manuel.bernhardt.io",
    version := "1.0",
    startYear := Some(2019),
    scalaVersion := "2.12.6",
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8"
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % Test,
      "org.iq80.leveldb" % "leveldb" % "0.12" % "optional;provided;multi-jvm;test",
      "commons-io" % "commons-io" % "2.6" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )