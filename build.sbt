name := "scalafish"

version := "0.0.1"

organization := "org.zymnis"

scalaVersion := "2.9.2"

// Use ScalaCheck
resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor" % "2.0.4",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.specs2" % "specs2_2.9.2" % "1.11" % "test",
  "it.unimi.dsi" % "fastutil" % "6.4.1"
)

parallelExecution in Test := true
