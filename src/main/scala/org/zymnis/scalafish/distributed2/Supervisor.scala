package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.kernel.Bootable
import akka.util.{Duration, Timeout}
import akka.util.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import scala.util.Random

import org.zymnis.scalafish.matrix._
import org.zymnis.scalafish.ScalafishUpdater

import Syntax._

/** One of these should be instantiated on each host. It creates a worker for each core
 * Holds all the shared memory for the workers
 */
class Supervisor extends Actor {
  import Distributed2._
  implicit val rng = new java.util.Random(2)

  var supervisorIdx: SupervisorId = null
  var initialized = false

  var matrixPort = 0

  var waitingMsg: Map[WorkerId, AnyRef] = Map.empty[WorkerId, AnyRef]

  val workers: IndexedSeq[ActorRef] = (0 until WORKERS).map { ind =>
    context.actorOf(Props[Worker], name = "worker_" + ind)
  }

  val timeToWait = 5.minutes

  implicit val timeout = Timeout(timeToWait)
  implicit val ec = ExecutionContext.defaultExecutionContext(context.system)

  def load(sIdx: SupervisorId, loader: MatrixLoader): Unit =
    if(!initialized) {
      initialized = true
      supervisorIdx = sIdx
      loader.rowPartition(WORKERS)
         .view
         .map { _.load }
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
    case SetMatrixPort(port) => matrixPort = port

    case Load(idx, loader) =>
      println("Supervisor %s is loading.".format(self.path))
      load(idx, loader)
      checkIfInited

    case Initialized(worker) =>
      waitingMsg.get(worker) match {
        case None => ()
        case Some(InitializeData(_,_)) => waitingMsg -= worker
      }
      checkIfInited

    case rs @ RunStep(step, part, worker, mat, getObj) =>
      workers(worker.id) ! rs
    case ds: DoneStep =>
      // TODO, cache? or will lost messages be rare enough?
      context.parent ! ds

    case Write(basePart, mwriter) =>
      mwriter.rowPartition(WORKERS)
        .view
        .zip(workers)
        .zipWithIndex
        .foreach { case ((writer, worker), widx) =>
          val offset = basePart.id
          val part = PartitionId(offset + widx)
          worker ! Write(part, writer)
        }

    case w: Written => context.parent ! w
    case ReceiveTimeout =>
      waitingMsg.collect { case (wid: WorkerId, msg: InitializeData) => workers(wid.id) ! msg }
  }
}

object SupervisorApp {
  def apply(host: String, port: Int) = new SupervisorApp(Distributed2.getConfig(host, port))
}

class SupervisorApp(config: Config) extends Bootable {
  val system = ActorSystem("SupervisorSystem", config)

  def startup() {
  }

  def shutdown() {
    system.shutdown()
  }
}
