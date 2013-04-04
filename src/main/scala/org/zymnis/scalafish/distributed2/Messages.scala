package org.zymnis.scalafish.distributed2

import org.zymnis.scalafish.matrix.Matrix

import java.io.Serializable

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
 */
case class Start(loader: MatrixLoader, lwriter: MatrixWriter, rwriter: MatrixWriter)
  extends Serializable
case class Load(supervisor: SupervisorId, loader: MatrixLoader) extends Serializable
case class Loaded(supervisor: SupervisorId) extends Serializable
case class RunStep(step: StepId, part: PartitionId, worker: WorkerId, data: Matrix, getObj: Boolean) extends Serializable {
  def alpha: Float = (Distributed2.ALPHA / (step.id + 1)).toFloat
}
case class DoneStep(worker: WorkerId, step: StepId, part: PartitionId, right: Matrix, objOpt:
Option[Double]) extends Serializable
case class InitializeData(workerId: WorkerId, sparseMatrix: Matrix) extends Serializable
case class Initialized(worker: WorkerId) extends Serializable
case class Write(part: PartitionId, writer: MatrixWriter) extends Serializable
case class Written(part: PartitionId) extends Serializable

