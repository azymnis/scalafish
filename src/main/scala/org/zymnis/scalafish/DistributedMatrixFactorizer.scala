package org.zymnis.scalafish

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import scala.util.Random

import org.zymnis.scalafish.matrix._

object DistributedMatrixFactorizer extends App {
  import Syntax._

  val rows = 10000
  val cols = 2000
  val realRank = 20
  val factorRank = 30
  val slices = 10
  val p = 0.1
  val noise = 0.001
  val mu = 1e-3
  val alpha = 1e-2

  val real = DenseMatrix.randLowRank(rows, cols, realRank)
  val dataMat = SparseMatrix.sample(p, real)
  val data = dataMat.rowSlice(slices).map { _.asInstanceOf[SparseMatrix] }

  val system = ActorSystem("FactorizerSystem")
  val master = system.actorOf(Props(new Master(cols, factorRank, slices, mu, alpha)), name = "master")
  master ! StartMaster(data)
}

// Messages are defined below

sealed trait FactorizerMessage
case class UpdateWorkerData(data: SparseMatrix) extends FactorizerMessage
case class UpdateWorkerR(newR: IndexedSeq[Matrix]) extends FactorizerMessage
case class StartMaster(data: IndexedSeq[SparseMatrix]) extends FactorizerMessage
case object DoUpdate extends FactorizerMessage
case class UpdateResponse(rSlice: Matrix, rIndex: Int, objSlice: Double) extends FactorizerMessage
case object DataUpdated extends FactorizerMessage

// Actors start here

class Master(cols: Int, rank: Int, slices: Int, mu: Double, alpha: Double) extends Actor {
  import Syntax._

  val timeToWait = 5.seconds

  implicit val timeout = Timeout(timeToWait)
  implicit val ec = ExecutionContext.defaultExecutionContext(context.system)

  var R: Vector[Matrix] = _
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

  def updateWorkerData(data: Iterable[SparseMatrix]) {
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
  import Syntax._

  var R: IndexedSeq[DenseMatrix] = _
  var L: DenseMatrix = _
  var data: IndexedSeq[SparseMatrix] = _
  var currentDelta: IndexedSeq[SparseMatrix] = _

  var iteration = 0

  val sliceSize = cols / slices

  override def preStart() = {
    R = Vector.fill(slices)(DenseMatrix.zeros(sliceSize, rank))
    currentDelta = Vector.fill(slices)(SparseMatrix.zeros(sliceSize, rank))
  }

  def receive = {
    case UpdateWorkerData(mat) => {
      L = DenseMatrix.rand(mat.rows, rank)

      data = mat.colSlice(slices).map { _.asInstanceOf[SparseMatrix] }

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
      val currentDeltaN = currentDelta(updateIndex)
      def delta = new ScalafishUpdater(L, Rn, dataN, rank)
      val currentAlpha = (alpha / (1 + iteration)).toFloat

      L *= (1.0f - mu.toFloat * currentAlpha)
      L -= currentDeltaN * Rn

      currentDeltaN := delta
      currentDeltaN *= currentAlpha

      Rn *= (1.0f - mu.toFloat * currentAlpha)
      Rn -= currentDeltaN.t * L

      val curObj =
        0.5 * (Matrix.frobNorm2(currentDeltaN) + mu.toFloat * (Matrix.frobNorm2(L) + Matrix.frobNorm2(Rn)))
      iteration += 1
      sender ! UpdateResponse(Rn, updateIndex, curObj)
    }
  }
}
