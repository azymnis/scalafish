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

class Worker extends Actor with ActorLogging {
  import Distributed2._
  implicit val rng = new java.util.Random(3)

  val left: Matrix = DenseMatrix.rand(ROWS / SUPERVISORS / WORKERS, FACTORRANK)
  val delta: IndexedSeq[SparseMatrix] = SparseMatrix.zeros(ROWS/WORKERS/SUPERVISORS, COLS).colSlice(WORKERS*SUPERVISORS)
  var data: IndexedSeq[SparseMatrix] = null
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
      log.info("Initializing data for worker: " + worker.id)
      if (data == null) {
        // val sparseSm = SparseMatrix.zeros(sm.rows, sm.cols)
        // sparseSm := sm
        data = sm.colSlice(SUPERVISORS * WORKERS)
      }
      sender ! Initialized(worker)

    case rs @ RunStepLocal(step, part, worker, right, getObj) =>
      log.info("Worker received runstep command: " + step.id)
      log.info("partition %d, right matrix size: (%d, %d)".format(part.id, right.rows, right.cols))
      log.info("partition %d, left matrix size: (%d, %d)".format(part.id, left.rows, left.cols))
      log.info("partition %d, data matrix nonzeros: %d".format(part.id, data(part.id).nonZeros))

      doStep(data(part.id), delta(part.id), right, MU, rs.alpha)
      // Send back the result
      // if (worker.id == 0) println("left: " + left)
      sender ! DoneStepLocal(worker, step, part, right, calcObj(right, data(part.id), delta(part.id), getObj))

    case Write(part, w) =>
      if(!written) { w.write(left); written = true }
      sender ! Written(part)
  }

  def doStep(data: Matrix, delta: Matrix, right: Matrix, mu: Float, alpha: Float): Unit = {
    val deltaUD = new ScalafishUpdater(left, right, data)

    delta := deltaUD
    log.info("Done with delta update.")

    delta *= alpha
    log.info("Done with delta alpha multiplication.")

    left *= (1.0f - mu * alpha)
    log.info("Done with left scaling multiplication.")

    left -= delta * right

    log.info("Done with left update.")

    delta := deltaUD
    log.info("Done with delta update 2.")

    delta *= alpha
    log.info("Done with delta alpha 2 multiplication.")

    right *= (1.0f - mu * alpha)
    log.info("Done with right scaling multiplication.")

    right -= delta.t * left
    log.info("Done with right update.")
  }
}
