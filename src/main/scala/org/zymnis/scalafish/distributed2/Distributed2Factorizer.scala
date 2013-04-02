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

// Some constants for testing
object Distributed2 {
  val ROWS = 1000
  val COLS = 100
  val SUPERVISORS = 2
  val REALRANK = 4
  val FACTORRANK = REALRANK + 4
  val DENSITY = 0.1
  val MU = 1e-4f
  val ALPHA = 1e-2
  val ITERS = 400
  //val WORKERS = java.lang.Runtime.getRuntime.availableProcessors
  val WORKERS = 2

  require(ROWS % (SUPERVISORS * WORKERS) == 0, "Rows must be divisable by supervisors")
  require(COLS % (SUPERVISORS * WORKERS) == 0, "Cols must be divisable by supervisors")
}

case class Load(supervisor: SupervisorId, loader: MatrixLoader)
case class Loaded(supervisor: SupervisorId)
// TODO: RunStep and DoStep are basically the same
case class RunStep(step: StepId, part: PartitionId, worker: WorkerId, data: Matrix)
case class DoStep(worker: WorkerId, step: StepId, part: PartitionId, right: Matrix, mu: Float, alpha: Float)
case class DoneStep(worker: WorkerId, step: StepId, part: PartitionId, right: Matrix)
case class InitializeData(workerId: WorkerId, sparseMatrix: Map[Long, Float])
case class Initialized(worker: WorkerId)

class Master2 extends Actor {
  import Distributed2._

  sealed trait SupervisorState
  case class Initialized(index: SupervisorId) extends SupervisorState
  case class Loading(part: MatrixLoader, index: SupervisorId) extends SupervisorState
  case class HasLoaded(index: SupervisorId) extends SupervisorState
  case class Working(step: StepId, part: PartitionId, index: SupervisorId) extends SupervisorState

  val superMap: Map[SupervisorId, ActorRef] = (0 until SUPERVISORS).map { sid =>
    (SupervisorId(sid), context.actorOf(Props[Supervisor], name = "supervisor_" + sid))
  }.toMap

  var supervisors: IndexedSeq[SupervisorState] = (0 until SUPERVISORS).map { ind => Initialized(SupervisorId(ind)) }


  var partitionState: IndexedSeq[(StepId, SupervisorId)] = null

  val updateStrategy: UpdateStrategy = new CyclicUpdates(WORKERS, SUPERVISORS)

  var masterLoader: MatrixLoader = null
  var rightData: IndexedSeq[(StepId, Matrix)] = null

  def totalWorkers = SUPERVISORS * WORKERS

  // don't block forever waiting for messages
  context.setReceiveTimeout(120 seconds)

  def genPart: Matrix =
    DenseMatrix.rand(COLS/SUPERVISORS/ROWS, FACTORRANK)

  def receive = {
    case Load(_, loader) =>
      if(masterLoader == null) {
        masterLoader = loader
        supervisors = loader.rowPartition(supervisors.size)
          .zip(supervisors)
          .map {
            case (part, Initialized(sid)) =>
              val actor = superMap(sid)
              actor ! Load(sid, part)
              Loading(part, sid)
            case (part, _) => sys.error("unreachable")
          }
          rightData = (0 to totalWorkers).map { _ => (StepId(0), genPart) }
      }

    case Loaded(idx) =>
      supervisors = supervisors.updated(idx.id, HasLoaded(idx))
      if(supervisors.forall {
        case HasLoaded(_) => true
        case _ => false
      }) {
        // Everyone is loaded, now step:
        if(partitionState == null) startComputation
      }
    case DoneStep(worker, step, part, mat) =>
      finishStep(step, part, mat)

    case ReceiveTimeout => ()
      // Retry everyone who hasn't responded yet, they may have lost the message
      supervisors.collect { case Loading(part, idx) => superMap(idx) ! Load(idx, part) }
  }

  def startComputation: Unit = {
    val step0 = StepId(0)
    val fn = updateStrategy.route(step0)
    partitionState = (0 until totalWorkers).map { partid =>
      val part = PartitionId(partid)
      val (sid, wid) = fn(part)
      superMap(sid) ! RunStep(step0, part, wid, rightData(partid)._2)
      supervisors = supervisors.updated(sid.id, Working(step0, part, sid))
      (step0, sid)
    }

  }
  // Update state and return the next message pair to send
  def finishStep(step: StepId, partition: PartitionId, mat: Matrix): Unit = {
    // Copy this into memory
    val (currentStep, inmemoryMat) = rightData(partition.id)
    if(currentStep == step) {
      val newStep = currentStep.next
      // Copy, don't keep a reference to mat
      inmemoryMat := mat
      // Now send this matrix for it's next step:
      val (sup, work) = updateStrategy.route(newStep)(partition)
      val rs = RunStep(newStep, partition, work, mat)
      rightData = rightData.updated(partition.id, (newStep, inmemoryMat))
      superMap(sup) ! rs
    }
  }
}

/** One of these should be instantiated on each host. It creates a worker for each core
 * Holds all the shared memory for the workers
 */
class Supervisor extends Actor {
  import Distributed2._

  var supervisorIdx: SupervisorId = null
  var initialized = false

  var waitingMsg: Map[WorkerId, AnyRef] = Map.empty[WorkerId, AnyRef]

  val workers: IndexedSeq[ActorRef] = (0 until WORKERS).map { ind =>
    context.actorOf(Props[Worker], name = "worker_" + ind)
  }

  val timeToWait = 5.minutes

  implicit val timeout = Timeout(timeToWait)
  implicit val ec = ExecutionContext.defaultExecutionContext(context.system)

  def sendRightToMaster(step: StepId, id: PartitionId): Future[Boolean] = {
    null
  }

  def load(sIdx: SupervisorId, loader: MatrixLoader): Unit =
    if(!initialized) {
      initialized = true
      supervisorIdx = sIdx
      loader.rowPartition(WORKERS)
         .view
         .map { _.load }
         .map { Matrix.toMap(_) }
         .zip(workers)
         .zipWithIndex
         .foreach { case ((mapData, worker), idx) =>
           val wid = WorkerId(idx)
           val msg = InitializeData(wid, mapData)
           waitingMsg += wid -> msg
           worker ! msg
         }
      }

  // don't block forever waiting for messages
  context.setReceiveTimeout(120 seconds)

  def checkIfInited: Boolean = {
    waitingMsg.collect { case (wid: WorkerId, msg: InitializeData) => 1 }.sum match {
      case 0 =>
        //Everyone is initialized
        context.parent ! Loaded(supervisorIdx)
        true
      case _ => false
    }
  }

  def receive = {
    case Load(idx, loader) =>
      load(idx, loader)
      checkIfInited

    case Initialized(worker) =>
      waitingMsg.get(worker) match {
        case None => ()
        case Some(InitializeData(_,_)) => waitingMsg -= worker
      }
      checkIfInited

    case RunStep(step, part, worker, mat) =>
      workers(worker.id) ! DoStep(worker, step, part, mat, MU, (ALPHA/(step.id+1)).toFloat)
    case DoneStep(worker, step, part, mat) =>
      // TODO, cache? or will lost messages be rare enough?
      context.parent ! DoneStep(worker, step, part, mat)

    case ReceiveTimeout =>
      waitingMsg.collect { case (wid: WorkerId, msg: InitializeData) => workers(wid.id) ! msg }
  }
}

class Worker extends Actor {
  import Distributed2._

  val left: Matrix = DenseMatrix.rand(ROWS / SUPERVISORS / WORKERS, FACTORRANK)
  val delta: IndexedSeq[Matrix] = SparseMatrix.zeros(ROWS/WORKERS/SUPERVISORS, COLS).colSlice(WORKERS*SUPERVISORS)
  var data: IndexedSeq[Matrix] = null

  def receive = {
    case InitializeData(worker, sm) =>
      if (data == null) {
        data = SparseMatrix.from(ROWS/SUPERVISORS/WORKERS, COLS, sm).colSlice(SUPERVISORS * WORKERS)
      }
      sender ! Initialized(worker)

    case DoStep(worker, step, part, right, mu, alpha) =>
      doStep(data(part.id), delta(part.id), right, mu, alpha)
      // Send back the result
      sender ! DoneStep(worker, step, part, right)
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
