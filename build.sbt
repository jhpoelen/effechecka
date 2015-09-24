organization := "effechecka"

version := "0.1"

scalaVersion := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  val phantomV = "1.8.12"
  Seq(
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-testkit" % sprayV % "test",
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "org.apache.spark" %% "spark-core" % "1.5.0" excludeAll(
      // only using the submit job functionality at this point
      ExclusionRule("io.dropwizard.metrics"),
      ExclusionRule("org.apache.hadoop"),
      ExclusionRule("org.apache.mesos"),
      ExclusionRule("org.tachyonproject"),
      ExclusionRule("org.apache.avro"),
      ExclusionRule("org.apache.spark", "spark-unsafe"),
      ExclusionRule("org.apache.zookeeper")),
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.6",
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "org.specs2" %% "specs2-core" % "2.3.11" % "test"
  )
}

Revolver.settings
