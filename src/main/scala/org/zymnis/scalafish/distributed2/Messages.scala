package org.zymnis.scalafish.distributed2

import org.zymnis.scalafish.matrix.{Matrix, DenseMatrix}

import java.io.Serializable
import java.net.InetSocketAddress
import java.util.UUID

/*
 * Basic types to wrap ints so we don't get confused
 */
case class StepId(id: Int) extends Serializable {
  def next: StepId = StepId(id + 1)
}
case class WorkerId(id: Int) extends Serializable
case class SupervisorId(id: Int) extends Serializable
case class PartitionId(id: Int) extends Serializable

/*
 * Messages sent between Master <-> Supervisor <-> Worker
 * We use this marker trait to register a serializer with Akka
 */
trait Message
case class Start(loader: MatrixLoader, lwriter: MatrixWriter, rwriter: MatrixWriter) extends Message
case class Load(supervisor: SupervisorId, loader: MatrixLoader) extends Message
case class Loaded(supervisor: SupervisorId) extends Message
case class MatrixRef(location: InetSocketAddress, uuid: UUID)
case class RunStep(step: StepId, part: PartitionId, worker: WorkerId, ref: MatrixRef, getObj: Boolean) extends Message {
  def alpha: Float = (Distributed2.ALPHA / (step.id + 1)).toFloat
}
case class DoneStep(worker: WorkerId, step: StepId, part: PartitionId, right: MatrixRef, objOpt:
Option[Double]) extends Message
case class InitializeData(workerId: WorkerId, sparseMatrix: Matrix) extends Message
case class Initialized(worker: WorkerId) extends Message
case class Write(part: PartitionId, writer: MatrixWriter) extends Message
case class Written(part: PartitionId) extends Message
case class SetMatrixPort(addr: InetSocketAddress) extends Message

// These messages should never be serialized (only supervisor <=> worker)
case class RunStepLocal(step: StepId, part: PartitionId, worker: WorkerId, ref: DenseMatrix, getObj: Boolean) {
  def alpha: Float = (Distributed2.ALPHA / (step.id + 1)).toFloat
}
case class DoneStepLocal(worker: WorkerId, step: StepId, part: PartitionId, right: DenseMatrix, objOpt: Option[Double])
