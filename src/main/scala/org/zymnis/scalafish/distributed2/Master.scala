package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.{Duration, Timeout}
import akka.util.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import java.net.InetSocketAddress
import scala.util.Random

import org.zymnis.scalafish.matrix._
import org.zymnis.scalafish.ScalafishUpdater

import Syntax._

class Master(nSupervisors: Int, nWorkers: Int) extends Actor {
  import Distributed2._
  implicit val rng = new java.util.Random(1)
  val supervisorAddresses = Seq[InetSocketAddress]()

  sealed trait SupervisorState
  case object Initialized extends SupervisorState
  case class Loading(loader: MatrixLoader) extends SupervisorState
  case object HasLoaded extends SupervisorState
  case class Working(step: StepId, part: PartitionId) extends SupervisorState

  val superMap: Map[SupervisorId, ActorRef] = (for (supervisorId <- (0 until nSupervisors)) yield {
    val addr = new InetSocketAddress("127.0.0.1", 2553 + supervisorId)
    val address = Address("akka", "SupervisorSystem", addr.getAddress.getHostAddress, addr.getPort)

    val supervisor = context.actorOf(
      Props[Supervisor].withDeploy(Deploy(scope = RemoteScope(address))),
      name = "supervisor_" + supervisorId)

    (SupervisorId(supervisorId), supervisor)
  }).toMap

  var supervisors: IndexedSeq[SupervisorState] = IndexedSeq.fill(nSupervisors)(Initialized)

  var partitionState: IndexedSeq[(StepId, SupervisorId)] = null

  val updateStrategy: UpdateStrategy = new CyclicUpdates(nWorkers, nSupervisors)

  var masterLoader: MatrixLoader = null
  var masterLeftWriter: MatrixWriter = null
  // TODO the master should write out R
  var masterRightWriter: MatrixWriter = null
  var rightData: IndexedSeq[(StepId, Matrix)] = null

  var writing: Boolean = false
  var writtenPartitions: Set[PartitionId] = Set[PartitionId]()

  def totalWorkers = nSupervisors * nWorkers

  // don't block forever waiting for messages
  context.setReceiveTimeout(120 seconds)

  def genPart: Matrix =
    DenseMatrix.rand(COLS/nSupervisors/nWorkers, FACTORRANK)

  def receive = {
    case Start(loader, lwriter, rwriter) =>
      if(masterLoader == null) {
        println("sending message to load data")
        masterLoader = loader
        masterLeftWriter = lwriter
        masterRightWriter = rwriter
        supervisors = loader.rowPartition(supervisors.size)
          .view
          .zip(supervisors)
          .zipWithIndex
          .map {
            case ((loadPart, Initialized), sidx) =>
              val sid = SupervisorId(sidx)
              val actor = superMap(sid)
              println("Found supervisor %d at %s.".format(sidx, actor))
              actor ! Load(sid, loadPart)
              Loading(loadPart)
            case (part, _) => sys.error("unreachable")
          }
          .toIndexedSeq

          rightData = (0 until totalWorkers).map { _ => (StepId(0), genPart) }
      }

    case Loaded(idx) =>
      supervisors = supervisors.updated(idx.id, HasLoaded)
      if(supervisors.forall {
        case HasLoaded => true
        case _ => false
      }) {
        // Everyone is loaded, now step:
        println("all supervisors initialized")
        if(partitionState == null) startComputation
      }
    case DoneStep(worker, step, part, mat, obj) =>
      // if (part.id == 0) println("right: " + rightData(0))
      obj.foreach { o => println("step %d, partition %d, OBJ: %4.3f".format(step.id, part.id, o)) }
      val wasFinished = allRsFinished
      finishStep(step, part, mat)
      checkTermination

    case Written(part) =>
      writtenPartitions += part
      checkTermination

    case ReceiveTimeout => ()
      // Retry everyone who hasn't responded yet, they may have lost the message
      supervisors.zipWithIndex.collect { case (Loading(lpart), idx) =>
        val sidx = SupervisorId(idx)
        superMap(sidx) ! Load(sidx, lpart)
      }
      checkTermination
  }

  def startComputation: Unit = {
    val step0 = StepId(0)
    val fn = updateStrategy.route(step0)
    partitionState = (0 until totalWorkers).map { partid =>
      val part = PartitionId(partid)
      val (sid, wid) = fn(part)
      superMap(sid) ! RunStep(step0, part, wid, rightData(partid)._2, true)
      supervisors = supervisors.updated(sid.id, Working(step0, part))
      (step0, sid)
    }
  }

  // Update state and return the next message pair to send
  def finishStep(step: StepId, partition: PartitionId, mat: Matrix): Unit = {
    // Copy this into memory
    val (currentStep, inmemoryMat) = rightData(partition.id)
    if (currentStep == step) {
      // println("step %d, partition %d".format(step.id, partition.id))
      val newStep = currentStep.next
      // Copy, don't keep a reference to mat
      inmemoryMat := mat
      // Now send this matrix for it's next step:
      val (sup, work) = updateStrategy.route(newStep)(partition)
      // val getObj = newStep.id % 20 == 0
      val getObj = true
      rightData = rightData.updated(partition.id, (newStep, inmemoryMat))
      if(currentStep.id < ITERS) {
        val rs = RunStep(newStep, partition, work, mat, getObj)
        supervisors = supervisors.updated(sup.id, Working(newStep, partition))
        partitionState = partitionState.updated(partition.id, (newStep, sup))
        superMap(sup) ! rs
      }
    }
  }

  def allRsFinished: Boolean = partitionState.forall { _._1.id >= ITERS }

  def checkTermination {
    if(writtenPartitions.size == totalWorkers) {
      context.stop(self)
      context.system.shutdown()
    }
    else if (allRsFinished) {
      masterLeftWriter.rowPartition(nSupervisors)
        .view
        .zip(superMap)
        .foreach { case (swriter, (sid, aref)) =>
          val baseId = PartitionId(sid.id * nWorkers)
          aref ! Write(baseId, swriter)
        }
    }
  }
}

object MasterApp {
  def apply(nSupervisors: Int, nWorkers: Int, host: String, port: Int) =
    new MasterApp(nSupervisors, nWorkers, Distributed2.getConfig(host, port))
}

class MasterApp(nSupervisors: Int, nWorkers: Int, config: Config) {
  val loader = HadoopMatrixLoader("/Users/argyris/Downloads/logodds", 10)
  val lwriter = new PrintWriter(-1, -1)
  val rwriter = new PrintWriter(-1, -1)

  val system = ActorSystem("MasterSystem", config)
  val master = system.actorOf(
    Props(new Master(nSupervisors, nWorkers)),
    name = "master")
  master ! Start(loader, lwriter, rwriter)
}
