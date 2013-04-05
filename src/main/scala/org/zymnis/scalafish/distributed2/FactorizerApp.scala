package org.zymnis.scalafish.distributed2

import com.twitter.scalding.Args
import com.typesafe.config.{ Config, ConfigFactory }

import java.net.InetSocketAddress

object FactorizerApp extends App {
  override def main(arguments: Array[String]) {
    val args = Args(arguments)

    val shard = args("shard").toInt
    val host = args("host")
    val port = args("akkaPort").toInt
    val matPort = args("matrixPort").toInt
    val zkHost = args("zkHost")
    val zkPort = args("zkPort").toInt
    val zkPath = args("zkPath")
    val dataPath = args("dataPath")

    println("Starting Factorizer App.")
    if (shard == 0) {
      println("Am the MASTER!")

      val nSupervisors = 10
      val nWorkers = 4
      val matAd = new InetSocketAddress(host, matPort)
      MasterApp(dataPath, nSupervisors, nWorkers, host, port, zkHost, zkPort, zkPath, matAd)
      println("Created the master. Here we go!")
    } else {
      println("Am a supervisor!")
      SupervisorApp(host, port)
      println("Started supervisor app -- waiting for messages!")
    }
  }
}
