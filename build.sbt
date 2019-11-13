import com.typesafe.sbt.MultiJvmPlugin.multiJvmSettings

lazy val akkaVersion = "2.5.26"

lazy val `akka-locality` = project
  .in(file("."))
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .settings(multiJvmSettings: _*)
  .settings(publishingSettings: _*)
  .settings(
    name := "akka-locality",
    version := "1.1.0-SNAPSHOT",
    startYear := Some(2019),
    scalaVersion := "2.12.10",
    crossScalaVersions := Seq("2.12.10", "2.13.1"),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8"
    ),
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion % "provided;multi-jvm;test",
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion % "provided;multi-jvm;test",
      "com.typesafe.akka" %% "akka-persistence" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % Test,
      "org.iq80.leveldb" % "leveldb" % "0.12" % "optional;provided;multi-jvm;test",
      "commons-io" % "commons-io" % "2.6" % Test,
      "org.scalatest" %% "scalatest" % "3.0.8" % Test
    ),
    credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")
  )

val publishingSettings = Seq(
  ThisBuild / organization := "io.bernhardt",
  ThisBuild / organizationName := "manuel.bernhardt.io",
  ThisBuild / organizationHomepage := Some(url("https://manuel.bernhardt.io")),

  ThisBuild / scmInfo := Some(
    ScmInfo(
      url("https://github.com/manuelbernhardt/akka-locality"),
      "scm:git@github.com:manuelbernhardt/akka-locality.git"
    )
  ),
  ThisBuild / developers := List(
    Developer(
      id    = "manuel",
      name  = "Manuel Bernhardt",
      email = "manuel@bernhardt.io",
      url   = url("https://manuel.bernhardt.io")
    )
  ),
  ThisBuild / description := "Akka extension to make better use of locality of actors in clustered systems",
  ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  ThisBuild / homepage := Some(url("https://github.com/manuelbernhardt/akka-locality")),

    // Remove all additional repository other than Maven Central from POM
    ThisBuild / pomIncludeRepository := { _ => false },
    ThisBuild / publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  ThisBuild / publishMavenStyle := true
)