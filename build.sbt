organization := "effechecka"

version := "0.2"

scalaVersion := "2.11.11"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.4.19"
  val akkaHttpV = "10.0.9"
  val eelV = "1.2.0-M5"
  val scalaTestV = "3.0.1"
  Seq(
    "org.effechecka" %% "effechecka-selector" % "0.0.3",
    "org.slf4j" % "slf4j-log4j12" % "1.7.25",
    "io.eels" %% "eel-core" % eelV,
    "io.eels" %% "eel-components" % eelV,
    "com.google.guava" % "guava" % "22.0",
    "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-xml" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV % "test",
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test",
    "org.scalatest" %% "scalatest" % scalaTestV % "test"
  )
}

resolvers += "effechecka-releases" at "https://s3.amazonaws.com/effechecka/releases"

