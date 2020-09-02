name := "akka-projections-playground"

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.8"

val akkaProjectionVersion = "1.0.0-RC3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.lightbend.akka" %% "akka-projection-kafka" % akkaProjectionVersion,
  "com.lightbend.akka" %% "akka-projection-jdbc" % akkaProjectionVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.4",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.eclipse.persistence" % "eclipselink" % "2.7.6",
  "org.postgresql" % "postgresql" % "42.2.12",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0" % Test,
  "com.opentable.components" % "otj-pg-embedded" % "0.13.3" % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
