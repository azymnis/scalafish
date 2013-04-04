name := "scalafish"

version := "0.0.1"

organization := "org.zymnis"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Clojars Repository" at "http://clojars.org/repo",
  "Conjars Repository" at "http://conjars.org/repo",
  "Twitter Maven" at "http://maven.twttr.com",
  "Twitter SVN Maven" at "https://svn.twitter.biz/maven-public"
)

libraryDependencies ++= Seq(
  "com.backtype" % "dfs-datastores-cascading" % "1.3.3",
  "com.backtype" % "dfs-datastores" % "1.3.1",
  "com.twitter" %% "scalding-core" % "0.8.4",
  "com.twitter" %% "scalding-commons" % "0.1.5" exclude("org.apache.thrift", "libthrift"),
  "com.twitter" %% "bijection-core" % "0.3.0",
  "com.twitter" %% "bijection-json" % "0.3.0",
  "com.googlecode.json-simple" % "json-simple" % "1.1.1",
  "com.typesafe.akka" % "akka-actor" % "2.0.4",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.specs2" % "specs2_2.9.2" % "1.11" % "test",
  "it.unimi.dsi" % "fastutil" % "6.4.1",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" excludeAll(
      ExclusionRule(organization = "com.sun.jdmk"),
      ExclusionRule(organization = "com.sun.jmx"),
      ExclusionRule(organization = "javax.jms")
    )
)

parallelExecution in Test := true
