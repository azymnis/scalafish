import AssemblyKeys._

name := "scalafish"

version := "0.0.1"

organization := "org.zymnis"

scalaVersion := "2.9.2"

assemblySettings

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter { Set("jsp-2.1-6.1.14.jar", "commons-beanutils-1.7.0.jar") contains _.data.getName }
}

net.virtualvoid.sbt.graph.Plugin.graphSettings

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
  "com.backtype" % "dfs-datastores-cascading" % "1.3.3" excludeAll(
    ExclusionRule(organization = "org.apache.hadoop")
  ),
  "com.backtype" % "dfs-datastores" % "1.3.1",
  "com.twitter" %% "scalding-core" % "0.8.4" excludeAll(
    ExclusionRule(organization = "org.mortbay.jetty"),
    ExclusionRule(organization = "org.apache", name = "hadoop")
  ),
  "com.twitter" %% "scalding-args" % "0.8.4" excludeAll(
    ExclusionRule(organization = "org.mortbay.jetty")
  ),
  "com.twitter" %% "scalding-commons" % "0.1.5" excludeAll(
    ExclusionRule(organization = "org.apache.thrift"),
    ExclusionRule(organization = "org.apache.hadoop"),
    ExclusionRule(organization = "org.mortbay.jetty")
  ),
  "com.twitter" %% "bijection-core" % "0.3.0",
  "com.twitter" %% "bijection-json" % "0.3.0",
  "com.twitter" %% "chill" % "0.2.0",
  "com.googlecode.json-simple" % "json-simple" % "1.1.1",
  "com.typesafe.akka" % "akka-actor" % "2.0.5",
  "com.typesafe.akka" % "akka-kernel" % "2.0.5",
  "com.typesafe.akka" % "akka-remote" % "2.0.5",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.specs2" % "specs2_2.9.2" % "1.11" % "test",
  "it.unimi.dsi" % "fastutil" % "6.4.1",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" excludeAll(
      ExclusionRule(organization = "com.sun.jdmk"),
      ExclusionRule(organization = "com.sun.jmx"),
      ExclusionRule(organization = "javax.jms"),
      ExclusionRule(organization = "org.jboss.netty"),
      ExclusionRule(organization = "org.mortbay.jetty"),
      ExclusionRule(organization = "tomcat")
    )
)

parallelExecution in Test := true

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set("jsp-api-2.1-6.1.14.jar", "jsp-2.1-6.1.14.jar",
                     "jasper-compiler-5.5.12.jar", "janino-2.5.16.jar",
                     "hadoop-core-0.20.2.jar")
  cp filter { jar => excludes(jar.data.getName) }
}

// Some of these files have duplicates, let's ignore:
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith("project.clj") => MergeStrategy.concat
    case s if s.endsWith(".html") => MergeStrategy.last
    case x => old(x)
  }
}
