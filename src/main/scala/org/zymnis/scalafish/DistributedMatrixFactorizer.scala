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
  val cols = 1000
  val realRank = 4
  val factorRank = realRank + 4
  val slices = 5
  val p = 0.1
  val mu = 1e-4
  val alpha = 1e-2
  val iters = 400

  val real = DenseMatrix.randLowRank(rows, cols, realRank)
  val dataMat = SparseMatrix.sample(p, real)
  val data = dataMat.rowSlice(slices)

  val system = ActorSystem("FactorizerSystem")
  val master = system.actorOf(Props(new Master(cols, factorRank, slices, mu, alpha, iters)), name = "master")
  master ! StartMaster(data)
}

// Messages are defined below

sealed trait FactorizerMessage
case class UpdateWorkerData(data: Matrix) extends FactorizerMessage
case class UpdateWorkerR(newR: IndexedSeq[Matrix]) extends FactorizerMessage
case class StartMaster(data: IndexedSeq[Matrix]) extends FactorizerMessage
case object DoUpdate extends FactorizerMessage
case class UpdateResponse(rSlice: Matrix, rIndex: Int, objSlice: Double) extends FactorizerMessage
case object DataUpdated extends FactorizerMessage
case object GetCurrentL extends FactorizerMessage
case class CurrentL(myL: Matrix) extends FactorizerMessage

// Actors start here

class Master(cols: Int, rank: Int, slices: Int, mu: Double, alpha: Double, iters: Int) extends Actor {
  import Syntax._

  val timeToWait = 5.seconds

  implicit val timeout = Timeout(timeToWait)
  implicit val ec = ExecutionContext.defaultExecutionContext(context.system)

  var R: Vector[Matrix] = _
  var data: IndexedSeq[Matrix] = _
  val actors = (0 until slices).map{ ind =>
    context.actorOf(Props(new Worker(ind, cols, rank, slices, mu, alpha)), name = "worker_" + ind)
  }

  override def preStart() = {
    println("starting up master")
    R = Vector.fill(slices)(DenseMatrix.rand(cols / slices, rank))
  }

  def receive = {
    case StartMaster(data) => {
      this.data = data

      println("initializing workers")
      updateWorkerData(data).flatMap { _ =>
        updateWorkerR
      }.flatMap { _ =>
        steps(iters) { i =>
          doWorkerUpdates.flatMap { obj =>
            println("Master iteration: %s, curObj: %4.3f".format(iters - i, obj))
            updateWorkerR
          }.flatMap { _ =>
            if (i % 20 == 0) {
              printCurrentError(i)
            } else {
              Future(())
            }
          }
        }
      }
      .foreach { lSeq =>
        context.stop(self)
        context.system.shutdown()
      }
    }
  }

  def steps(numSteps: Int)(f: (Int) => Future[Unit]): Future[Unit] = {
    if(numSteps <= 0) Future(())
    else {
      f(numSteps).flatMap { _ => steps(numSteps - 1)(f) }
    }
  }

  def printCurrentError(iter: Int): Future[Unit] = {
    lookupL.map { lSeq =>
      val dataFrobNorm = data.map { m => Matrix.frobNorm2(m) }.sum
      val rows = data.map { _.rows }.sum
      val out = SparseMatrix.zeros(rows, cols)
      out := new ScalafishUpdater(Matrix.vStack(lSeq), Matrix.vStack(R), Matrix.vStack(data), rank)
      val approxFrobNorm = out.frobNorm2
      println("iter: %d, relerr: %.3f".format(iter, math.sqrt(approxFrobNorm / dataFrobNorm)))
    }
  }

  def updateWorkerData(data: Iterable[Matrix]): Future[Unit] = {
    val futures = data.zip(actors).map{
      case(mat, worker) => worker ? UpdateWorkerData(mat)
    }
    Future.sequence(futures).map { _ => () }
  }

  def updateWorkerR: Future[Unit] = {
    val futures = actors.map{
      worker => worker ? UpdateWorkerR(R)
    }
    Future.sequence(futures).map { _ => () }
  }

  def doWorkerUpdates: Future[Double] = {
    val futures = actors.map{ worker =>
      (worker ? DoUpdate).mapTo[UpdateResponse].map { res =>
        res.objSlice
      }
    }
    Future.reduce(futures)(_ + _)
  }

  def lookupL: Future[Seq[Matrix]] = {
    Future.sequence {
      actors.map { worker =>
        (worker ? GetCurrentL)
          .mapTo[CurrentL]
          .map { _.myL }
      }
    }
  }
}

class Worker(workerIndex: Int, cols: Int, rank: Int, slices: Int, mu: Double, alpha: Double) extends Actor {
  import Syntax._

  var R: IndexedSeq[DenseMatrix] = _
  var L: DenseMatrix = _
  var data: IndexedSeq[Matrix] = _
  var currentDelta: Matrix = _
  var iteration = 0

  val sliceSize = cols / slices

  override def preStart() = {
    R = Vector.fill(slices)(DenseMatrix.zeros(sliceSize, rank))
  }

  def receive = {
    case UpdateWorkerData(mat) => {
      L = DenseMatrix.rand(mat.rows, rank)
      // TODO keep a bunch of these and overwrite the one with the pattern of data(N)
      currentDelta = DenseMatrix.zeros(mat.rows, sliceSize)

      data = mat.colSlice(slices)

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
      def delta = new ScalafishUpdater(L, Rn, dataN, rank)
      val currentAlpha = (alpha / (1 + iteration)).toFloat

      currentDelta := 0.0f
      currentDelta := delta
      currentDelta *= currentAlpha

      L *= (1.0f - mu.toFloat * currentAlpha)
      L -= currentDelta * Rn

      currentDelta := 0.0f
      currentDelta := delta
      currentDelta *= currentAlpha

      Rn *= (1.0f - mu.toFloat * currentAlpha)
      Rn -= currentDelta.t * L

      // val dObj = R.view
      //   .zip(data)
      //   .map { case (rn, dn) =>
      //     Matrix.frobNorm2(currentDelta2)
      //   }
      //   .sum

      val curObj = 0.5 * (0 + mu.toFloat * (Matrix.frobNorm2(L) + Matrix.frobNorm2(Rn)))
      iteration += 1
      sender ! UpdateResponse(Rn, updateIndex, curObj)
    }
    case GetCurrentL => sender ! CurrentL(L)
  }
}
