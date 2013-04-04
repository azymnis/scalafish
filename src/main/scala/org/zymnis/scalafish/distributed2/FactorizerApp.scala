package org.zymnis.scalafish.distributed2

import com.twitter.scalding.Args
import com.typesafe.config.{ Config, ConfigFactory }

import java.net.InetSocketAddress

object FactorizerApp extends App {
  override def main(arguments: Array[String]) {
    val args = Args(arguments)

    val shard = args("shard").toInt
    val host = args("host")
    val port = args("factorizerPort").toInt

    println("Starting Factorizer App.")
    if (shard == 0) {
      println("Am the MASTER!")

      val nWorkers = 4
      val supervisorAddresses = Seq(new InetSocketAddress(args("supHost"), args("supPort").toInt))

      MasterApp(supervisorAddresses, nWorkers, host, port)
      println("Created the master. Here we go!")
    } else {
      println("Am a supervisor!")
      SupervisorApp(host, port)
      println("Started supervisor app -- waiting for messages!")
    }
  }
}
