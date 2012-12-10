package org.zymnis.scalafish

import breeze.linalg.{DenseMatrix, CSCMatrix}

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import scala.util.Random

object DistributedMatrixFactorizer extends App {
  val rows = 10000
  val cols = 2000
  val realRank = 20
  val factorRank = 30
  val slices = 10
  val p = 0.1
  val noise = 0.001
  val mu = 1e-3
  val alpha = 1e-2

  val (real, data) = MatrixUtil.randSparseSliced(rows, cols, realRank, p, noise, slices)

  val system = ActorSystem("FactorizerSystem")
  val master = system.actorOf(Props(new Master(cols, factorRank, slices, mu, alpha)), name = "master")
  master ! StartMaster(data)
}

// Messages are defined below

sealed trait FactorizerMessage
case class UpdateWorkerData(data: CSCMatrix[Double]) extends FactorizerMessage
case class UpdateWorkerR(newR: Iterable[DenseMatrix[Double]]) extends FactorizerMessage
case class StartMaster(data: Iterable[CSCMatrix[Double]]) extends FactorizerMessage
case object DoUpdate extends FactorizerMessage
case class UpdateResponse(rSlice: DenseMatrix[Double], rIndex: Int, objSlice: Double) extends FactorizerMessage
case object DataUpdated extends FactorizerMessage

// Actors start here

class Master(cols: Int, rank: Int, slices: Int, mu: Double, alpha: Double) extends Actor {
  val timeToWait = 5.seconds

  implicit val timeout = Timeout(timeToWait)
  implicit val ec = ExecutionContext.defaultExecutionContext(context.system)

  var R: Vector[DenseMatrix[Double]] = _
  val actors = (0 until slices).map{ ind =>
    context.actorOf(Props(new Worker(ind, cols, rank, slices, mu, alpha)), name = "worker_" + ind)
  }

  override def preStart() = {
    println("starting up master")
    R = Vector.fill(slices)(DenseMatrix.rand(cols / slices, rank))
  }

  def receive = {
    case StartMaster(data) => {
      println("initializing workers")
      updateWorkerData(data)
      updateWorkerR

      (1 to 100).foreach { i =>
        val obj = doWorkerUpdates
        println("Master iteration: %s, curObj: %4.3f".format(i, obj))
        updateWorkerR
      }

      context.stop(self)
      context.system.shutdown()
    }
  }

  def updateWorkerData(data: Iterable[CSCMatrix[Double]]) {
    val futures = data.zip(actors).map{
      case(mat, worker) => worker ? UpdateWorkerData(mat)
    }
    val fList = Future.sequence(futures)
    Await.ready(fList, timeToWait)
  }

  def updateWorkerR {
    val futures = actors.map{
      worker => worker ? UpdateWorkerR(R)
    }
    val fList = Future.sequence(futures)
    Await.ready(fList, timeToWait)
  }

  def doWorkerUpdates: Double = {
    val futures = actors.map{ worker =>
      (worker ? DoUpdate).asInstanceOf[Future[UpdateResponse]].map { res =>
        R(res.rIndex) := res.rSlice
        res.objSlice
      }
    }
    Await.result(Future.reduce(futures)(_ + _), timeToWait)
  }

}

class Worker(workerIndex: Int, cols: Int, rank: Int, slices: Int, mu: Double, alpha: Double) extends Actor {
  var R: Vector[DenseMatrix[Double]] = _
  var L: DenseMatrix[Double] = _
  var data: Vector[CSCMatrix[Double]] = _
  var pat: Vector[DenseMatrix[Double]] = _
  var iteration = 0

  val sliceSize = cols / slices

  override def preStart() = {
    R = Vector.fill(slices)(DenseMatrix.zeros[Double](sliceSize, rank))
  }

  def receive = {
    case UpdateWorkerData(mat) => {
      L = DenseMatrix.rand(mat.rows, rank)

      data = Vector.fill(slices)(CSCMatrix.zeros[Double](mat.rows, sliceSize))
      pat = Vector.fill(slices)(DenseMatrix.zeros[Double](mat.rows, sliceSize))

      mat.activeKeysIterator.foreach{ case(r, c) =>
        val itIndex = c / sliceSize
        val colInd = c % sliceSize
        data(itIndex)(r, colInd) = mat(r, c)
        pat(itIndex)(r, colInd) = 1.0
      }

      sender ! DataUpdated
    }
    case UpdateWorkerR(mats) => {
      mats.zipWithIndex.foreach{ case(r, ind) => R(ind) := r }
      sender ! DataUpdated
    }
    case DoUpdate => {
      val updateIndex = (iteration + workerIndex) % slices
      val Rn = R(updateIndex)
      val dataN = data(updateIndex)
      val patN = pat(updateIndex)
      def delta = patN :* (L * Rn.t - dataN)
      val currentAlpha = alpha / (1 + iteration)
      L := L - (L * mu + delta * Rn) * currentAlpha
      Rn := Rn - (Rn * mu + delta.t * L) * currentAlpha
      val curObj =
        0.5 * (MatrixUtil.frobNorm(delta) + mu * (MatrixUtil.frobNorm(L) + MatrixUtil.frobNorm(Rn)))
      iteration += 1
      sender ! UpdateResponse(Rn, updateIndex, curObj)
    }
  }
}
