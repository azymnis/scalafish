package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import scala.util.Random

import org.zymnis.scalafish.matrix._
import org.zymnis.scalafish.ScalafishUpdater

import Syntax._

object Distributed2Factorizer extends App {
  val loader = HadoopMatrixLoader("/Users/argyris/Downloads/logodds", 10)
  val lwriter = new PrintWriter(-1, -1)
  val rwriter = new PrintWriter(-1, -1)

  val system = ActorSystem("FactorizerSystem")
  val master = system.actorOf(Props[Master], name = "master")
  master ! Start(loader, lwriter, rwriter)
}

// Some constants for testing
object Distributed2 {
  implicit val rng = new java.util.Random

  // val ROWS = 3000
  // val COLS = 1000
  val ROWS = 2046
  val COLS = 10122134
  val SUPERVISORS = 10
  val WORKERS = 4
  val REALRANK = 10
  val FACTORRANK = REALRANK + 5
  val DENSITY = 0.1
  val MU = 1e-4f
  val ALPHA = 1e-2
  val ITERS = 10
  //val WORKERS = java.lang.Runtime.getRuntime.availableProcessors

  // require(ROWS % (SUPERVISORS * WORKERS) == 0, "Rows must be divisable by supervisors")
  // require(COLS % (SUPERVISORS * WORKERS) == 0, "Cols must be divisable by supervisors")
}
