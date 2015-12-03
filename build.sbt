organization := "effechecka"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.4.1"
  val akkaStreamV = "2.0-M2"
  val cassandraDriverV = "2.1.6"
  val sparkV = "1.5.0"
  val scalaTestV = "2.2.5"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-xml-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-testkit-experimental" % akkaStreamV % "test",
    "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverV,
    "org.apache.spark" %% "spark-core" % sparkV excludeAll(
      // only using the submit job functionality at this point
      ExclusionRule("io.dropwizard.metrics"),
      ExclusionRule("org.apache.mesos"),
      ExclusionRule("org.tachyonproject"),
      ExclusionRule("org.apache.avro"),
      ExclusionRule("org.apache.spark", "spark-unsafe"),
      ExclusionRule("org.apache.zookeeper")),
    "org.specs2" %% "specs2-core" % "2.3.11" % "test",
    "org.scalatest" %% "scalatest" % scalaTestV % "test"
  )
}

