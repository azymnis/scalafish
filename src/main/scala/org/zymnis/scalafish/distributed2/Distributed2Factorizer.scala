package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import scala.util.Random

import org.zymnis.scalafish.matrix._
import org.zymnis.scalafish.ScalafishUpdater

import Syntax._

object Distributed2Factorizer extends App {
  val loader = new TestLoader
  val lwriter = new PrintWriter(-1, -1)
  val rwriter = new PrintWriter(-1, -1)

  val system = ActorSystem("FactorizerSystem")
  val master = system.actorOf(Props[Master], name = "master")
  master ! Start(loader, lwriter, rwriter)
}

// Some constants for testing
/*
 * Master needs memory = COLS * FACTORRANK * 4
 * Supervisors need memory = (COLS + ROWS/SUPERVISORS) * FACTORRANK * 4
 * communicated blocks are = COLS * FACTORRANK * 4/(SUPERVISORS * WORKERS)
 *
 * For example: 10 supervisors, each with 10 workers, and using
 * ROWS = 100M, COLS = 60K, FACTORRANK = 100
 *   then we need 4 GB of raw memory in the supervisors.
 *
 * Note, we are using off-heap memory with a minimum of allocation, so there should
 * not be significant GC pressure.
 *
 * To reach 200M x 1M with FACTORRANK = 200 we need 20 supervisors with 10 workers
 * each with 8.8 GB of matrix memory.
 */
object Distributed2 {
  implicit val rng = new java.util.Random

  val ROWS = 100000000
  val COLS = 60000
  val SUPERVISORS = 10
  val WORKERS = 10
  val REALRANK = 10
  val FACTORRANK = 100
  val DENSITY = 0.1

  // Oscar's settings
  // val ROWS = 320000
  // val COLS = 16000
  // // val ROWS = 2046
  // // val COLS = 10122134
  // val SUPERVISORS = 10
  // val WORKERS = 8
  // val REALRANK = 10
  // val FACTORRANK = REALRANK + 5
  // val DENSITY = 0.01

  val MU = 1e-4f
  val ALPHA = 1e-3
  val ITERS = 100
  //val WORKERS = java.lang.Runtime.getRuntime.availableProcessors

  // TODO: Worker update crashes this isn't true:
  // require(ROWS % (SUPERVISORS * WORKERS) == 0, "Rows must be divisable by supervisors")
  // require(COLS % (SUPERVISORS * WORKERS) == 0, "Cols must be divisable by supervisors")

  def getConfig(host: String, port: Int): Config = {
    val config = ConfigFactory.parseString("""akka {
      loglevel = "DEBUG"
      stdout-loglevel = "DEBUG"
      log-config-on-start = on
      actor.debug.lifecycle = on
      actor.serializers {
        kryo = "org.zymnis.scalafish.serialization.KryoAkkaPooled"
      }
      actor.serialization-bindings {
        "org.zymnis.scalafish.distributed2.Message" = kryo
      }
      actor.provider = "akka.remote.RemoteActorRefProvider"
      remote.netty.message-frame-size = 100 MiB
      remote.netty.hostname = "%s"
      remote.netty.port = %d
    }
    """.format(host, port))
    println("Config is %s".format(config))
    config
  }
}
