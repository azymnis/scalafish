package org.zymnis.scalafish.distributed2

import com.typesafe.config.{ Config, ConfigFactory }

object FactorizerApp extends App {
  val shard = args(0).toInt
  val port = args(1).toInt
  val host = "127.0.0.1"
  val nSupervisors = 2
  val nWorkers = 4

  println("Starting Factorizer App.")
  if (shard == 0) {
    println("Am the MASTER!")
    new MasterApp(nSupervisors, nWorkers)
    println("Created the master. Here we go!")
  } else {
    println("Am a supervisor!")
    SupervisorApp(host, port)
    println("Started supervisor app -- waiting for messages!")
  }
}
