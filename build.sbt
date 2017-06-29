organization := "effechecka"

version := "0.2"

scalaVersion := "2.11.11"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.4.18"
  val akkaHttpV = "10.0.7"
  val eelV = "1.2.0-M5"
  val cassandraDriverV = "2.1.10.3"
  val scalaTestV = "3.0.1"
  Seq(
    "org.effechecka" %% "effechecka-selector" % "0.0.1-SNAPSHOT",
    "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverV,
    "io.eels" %% "eel-core" % eelV,
    "io.eels" %% "eel-components" % eelV,
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-xml" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV % "test",
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test",
    "org.scalatest" %% "scalatest" % scalaTestV % "test"
  )
}

resolvers += "effechecka-snapshots" at "https://s3.amazonaws.com/effechecka/snapshots"


