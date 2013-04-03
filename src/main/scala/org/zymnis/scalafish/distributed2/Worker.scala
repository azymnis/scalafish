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

class Worker extends Actor {
  import Distributed2._
  implicit val rng = new java.util.Random(3)

  val left: Matrix = DenseMatrix.rand(ROWS / SUPERVISORS / WORKERS, FACTORRANK)
  val delta: IndexedSeq[Matrix] = SparseMatrix.zeros(ROWS/WORKERS/SUPERVISORS, COLS).colSlice(WORKERS*SUPERVISORS)
  var data: IndexedSeq[Matrix] = null
  var written: Boolean = false

  def calcObj(right: Matrix, data: Matrix, delta: Matrix, getObj: Boolean): Option[Double] = if (getObj) {
    val deltaUD = new ScalafishUpdater(left, right, data)
    delta := deltaUD
    Some(math.sqrt(delta.frobNorm2 / data.nonZeros))
  } else {
    None
  }

  def receive = {
    case InitializeData(worker, sm) =>
      println("initializing data for worker: " + worker.id)
      if (data == null) {
        data = sm.colSlice(SUPERVISORS * WORKERS)
      }
      sender ! Initialized(worker)

    case rs @ RunStep(step, part, worker, right, getObj) =>
      doStep(data(part.id), delta(part.id), right, MU, rs.alpha)
      // Send back the result
      // if (worker.id == 0) println("left: " + left)
      sender ! DoneStep(worker, step, part, right, calcObj(right, data(part.id), delta(part.id), getObj))

    case Write(part, w) =>
      if(!written) { w.write(left); written = true }
      sender ! Written(part)
  }

  def doStep(data: Matrix, delta: Matrix, right: Matrix, mu: Float, alpha: Float): Unit = {
      val deltaUD = new ScalafishUpdater(left, right, data)

      delta := deltaUD
      delta *= alpha

      left *= (1.0f - mu * alpha)
      left -= delta * right

      delta := deltaUD
      delta *= alpha

      right *= (1.0f - mu * alpha)
      right -= delta.t * left
  }
}
