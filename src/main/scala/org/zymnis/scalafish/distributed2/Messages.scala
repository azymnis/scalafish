package org.zymnis.scalafish.distributed2

import org.zymnis.scalafish.matrix.Matrix

/*
 * Basic types to wrap ints so we don't get confused
 */
case class StepId(id: Int) {
  def next: StepId = StepId(id + 1)
}
case class WorkerId(id: Int)
case class SupervisorId(id: Int)
case class PartitionId(id: Int)

/*
 * Messages sent between Master <-> Supervisor <-> Worker
 */
case class Start(loader: MatrixLoader, lwriter: MatrixWriter, rwriter: MatrixWriter)
case class Load(supervisor: SupervisorId, loader: MatrixLoader)
case class Loaded(supervisor: SupervisorId)
case class RunStep(step: StepId, part: PartitionId, worker: WorkerId, data: Matrix, getObj: Boolean) {
  def alpha: Float = (Distributed2.ALPHA / (step.id + 1)).toFloat
}
case class DoneStep(worker: WorkerId, step: StepId, part: PartitionId, right: Matrix, objOpt: Option[Double])
case class InitializeData(workerId: WorkerId, sparseMatrix: Matrix)
case class Initialized(worker: WorkerId)
case class Write(part: PartitionId, writer: MatrixWriter)
case class Written(part: PartitionId)

