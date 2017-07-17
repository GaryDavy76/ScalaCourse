name := """sharded-kv-store-supervised"""

version := "1.0"

scalaVersion := "2.12.1"

scalacOptions += "-feature"

libraryDependencies ++= {
  val akkaVersion = "2.4.16"     // DRY out the Akka version
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,

    // Logging stuff...
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "ch.qos.logback"    %  "logback-classic" % "1.1.3",

    // Test stuff...
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
}

fork in run := true
