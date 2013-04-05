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

import java.net.InetSocketAddress

import Syntax._

import java.util.UUID

class SharedMatrices(localAdd: InetSocketAddress, rows: Int, cols: Int, count: Int)(implicit rng: java.util.Random)
  extends AbstractMatrixDataServer {
    override def port = localAdd.getPort

    override val matrices = SharedMemory((0 until count).map { i => (UUID.randomUUID, DenseMatrix.rand(rows, cols)) })

    def getRef(idx: Int): Option[MatrixRef] =
      matrices.get(idx).map { case (uuid, _) => MatrixRef(localAdd, uuid) }

    def take(idx: Int): Option[DenseMatrix] = matrices.take(idx).map { _._2 }
    def replace(idx: Int, dm: DenseMatrix): Option[MatrixRef] = {
      assert(dm.rows == rows, "Rows don't match")
      assert(dm.cols == cols, "Cols don't match")
      val newUuid = UUID.randomUUID
      if(matrices.put(idx, (newUuid, dm))) Some(MatrixRef(localAdd, newUuid)) else None
    }
}

class Master(nSupervisors: Int, nWorkers: Int, zkHost: String, zkPort: Int, zkPath: String, matrixAddress: InetSocketAddress) extends Actor {
  import Distributed2._
  implicit val rng = new java.util.Random(1)
  val supervisorAddresses = Seq[InetSocketAddress]()

  sealed trait SupervisorState
  case object Initialized extends SupervisorState
  case class Loading(loader: MatrixLoader) extends SupervisorState
  case object HasLoaded extends SupervisorState
  case class Working(step: StepId, part: PartitionId) extends SupervisorState

  var started: Boolean = false

  val discoActor = context.actorOf(Props(new DiscoveryActor()))

  var superMap: Map[SupervisorId, ActorRef] = Map.empty

  var supervisors: IndexedSeq[SupervisorState] = IndexedSeq.fill(nSupervisors)(Initialized)

  var partitionState: IndexedSeq[(StepId, SupervisorId)] = null

  val updateStrategy: UpdateStrategy = new CyclicUpdates(nWorkers, nSupervisors)

  var masterLoader: MatrixLoader = null
  var masterLeftWriter: MatrixWriter = null
  // TODO the master should write out R
  var masterRightWriter: MatrixWriter = null
  var rightData: SharedMatrices = null
  var rightStepMap: IndexedSeq[StepId] = null

  var writing: Boolean = false
  var writtenPartitions: Set[PartitionId] = Set[PartitionId]()

  def totalWorkers = nSupervisors * nWorkers

  // don't block forever waiting for messages
  context.setReceiveTimeout(120 seconds)

  def genPart: Matrix =
    DenseMatrix.rand(COLS/nSupervisors/nWorkers, FACTORRANK)

  def receive = {
    case Start(loader, lwriter, rwriter) =>
      masterLoader = loader
      masterLeftWriter = lwriter
      masterRightWriter = rwriter
      discoActor ! UseZookeeper(zkHost, zkPort, zkPath, nSupervisors + 1)

    case AnnounceSupervisors(found) =>
      superMap =
        (for {
          hp <- found if hp.shard != 0
        } yield {
          val supervisorId = hp.shard - 1
          val address = Address("akka", "SupervisorSystem", hp.host, hp.akkaPort)
          val matrixAddress = new InetSocketAddress(hp.host, hp.matrixPort)
          // TODO: store matrix address
          val supervisor = context.actorOf(
            Props[Supervisor].withDeploy(Deploy(scope = RemoteScope(address))),
            name = "supervisor_" + supervisorId)
          supervisor ! SetMatrixPort(matrixAddress)

          (SupervisorId(supervisorId), supervisor)
        }).toMap

      context.stop(discoActor)

      if(!started) {
        started = true
        println("sending message to load data")

        supervisors = masterLoader.rowPartition(supervisors.size)
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
          rightData = new SharedMatrices(matrixAddress, COLS/ totalWorkers, FACTORRANK, totalWorkers)
          rightData.start
          rightStepMap = (0 until totalWorkers).map { _ => StepId(0) }
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
      superMap(sid) ! RunStep(step0, part, wid, rightData.getRef(partid).get, true)
      supervisors = supervisors.updated(sid.id, Working(step0, part))
      (step0, sid)
    }
  }

  // Update state and return the next message pair to send
  def finishStep(step: StepId, partition: PartitionId, mat: MatrixRef): Unit = {
    val idx = partition.id
    val currentStep = rightStepMap(partition.id)
    if(currentStep != step) return
    rightData.take(idx).map { dm =>
      val newStep = currentStep.next
      val (sup, work) = updateStrategy.route(newStep)(partition)
      val getObj = newStep.id % 20 == 0
      // Now send this matrix for it's next step:
      rightStepMap = rightStepMap.updated(partition.id, newStep)
      val newRef = try {
        // Try our best to read
        if(!MatrixClient.read(mat.location, mat.uuid, dm)) println("Failed to read: " + mat)
        rightData.replace(idx, dm)
      }
      catch {
        case x:AnyRef =>
          println(x)
          rightData.replace(idx, dm)
      }
      if(currentStep.id < ITERS) {
        supervisors = supervisors.updated(sup.id, Working(newStep, partition))
        partitionState = partitionState.updated(partition.id, (newStep, sup))
        superMap(sup) ! RunStep(newStep, partition, work, newRef.get, getObj)
      }
    }
  }

  def allRsFinished: Boolean =
    Option(partitionState)
      .map { _.forall { _._1.id >= ITERS } }
      .getOrElse(false)

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
  def apply(dataPath: String, nSupervisors: Int, nWorkers: Int, host: String, port: Int, zkHost: String, zkPort: Int, zkPath: String, matAd: InetSocketAddress) =
    new MasterApp(dataPath, nSupervisors, nWorkers, Distributed2.getConfig(host, port), zkHost, zkPort, zkPath, matAd)
}

class MasterApp(dataPath: String, nSupervisors: Int, nWorkers: Int, config: Config, zkHost: String, zkPort: Int, zkPath: String, matAd: InetSocketAddress) {
  val loader = UnshardedHadoopMatrixLoader(dataPath)
  val lwriter = new PrintWriter(-1, -1)
  val rwriter = new PrintWriter(-1, -1)

  val system = ActorSystem("MasterSystem", config)
  val master = system.actorOf(
    Props(new Master(nSupervisors, nWorkers, zkHost, zkPort, zkPath, matAd)),
    name = "master")
  master ! Start(loader, lwriter, rwriter)
}
