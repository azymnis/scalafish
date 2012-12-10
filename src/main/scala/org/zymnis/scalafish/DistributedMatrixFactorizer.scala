package org.zymnis.scalafish

import breeze.linalg._

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import scala.util.Random

object DistributedMatrixFactorizer extends App {
  val rows = 1000
  val cols = 100
  val realRank = 10
  val factorRank = 20
  val slices = 10
  val p = 0.1
  val noise = 0.001

  val (real, data) = MatrixUtil.randSparseSliced(rows, cols, realRank, p, noise, slices)

  val system = ActorSystem("FactorizerSystem")
  val master = system.actorOf(Props(new Master(cols, factorRank, slices)), name = "master")
  master ! InitializeMaster(data)
}

// Messages are defined below

sealed trait FactorizerMessage
case class UpdateWorkerData(data: CSCMatrix[Double]) extends FactorizerMessage
case class UpdateWorkerR(newR: DenseMatrix[Double]) extends FactorizerMessage
case class InitializeMaster(data: Iterable[CSCMatrix[Double]]) extends FactorizerMessage
case object DataUpdated extends FactorizerMessage

// Actors start here

class Master(cols: Int, rank: Int, slices: Int) extends Actor {
  implicit val timeout = Timeout(5 seconds)
  implicit val ec = ExecutionContext.defaultExecutionContext(context.system)

  var R: DenseMatrix[Double] = _
  val actors = (1 to slices).map{ ind =>
    context.actorOf(Props(new Worker(cols, rank)), name = "worker_" + ind)
  }

  override def preStart() = {
    println("starting up master")
    R = DenseMatrix.rand(cols, rank)
  }

  def receive = {
    case InitializeMaster(data) => {
      val futures = data.zip(actors).flatMap{
        case(mat, worker) =>
          List(worker ? UpdateWorkerData(mat), worker ? UpdateWorkerR(R))
      }
      val fList = Future.sequence(futures)
      Await.ready(fList, 5 seconds)
      context.stop(self)
      context.system.shutdown()
    }
  }
}

class Worker(cols: Int, rank: Int) extends Actor {
  var R: DenseMatrix[Double] = _
  var L: DenseMatrix[Double] = _
  var data: CSCMatrix[Double] = _
  var pat: DenseMatrix[Double] = _

  override def preStart() = {
    println("starting up worker")
    R = DenseMatrix.zeros[Double](cols, rank)
  }

  def receive = {
    case UpdateWorkerData(mat) => {
      println("updating data for worker: " + self.path)
      L = DenseMatrix.rand(mat.rows, rank)

      data = CSCMatrix.zeros[Double](mat.rows, mat.cols)
      pat = DenseMatrix.zeros[Double](data.rows, data.cols)

      mat.activeKeysIterator.foreach{ case(r, c) =>
        data(r, c) = mat(r, c)
        pat(r, c) = 1.0
      }

      sender ! DataUpdated
    }
    case UpdateWorkerR(mat) => {
      println("updating R for worker: " + self.path)
      R := mat
      sender ! DataUpdated
    }
  }

}
