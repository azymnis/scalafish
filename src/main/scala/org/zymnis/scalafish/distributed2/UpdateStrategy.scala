package org.zymnis.scalafish.distributed2

case class StepId(id: Int) {
  def next: StepId = StepId(id + 1)
}
case class WorkerId(id: Int)
case class SupervisorId(id: Int)
case class PartitionId(id: Int)

trait UpdateStrategy {
  def workersPerSupervisor: Int
  def supervisors: Int
  def partitions: Int

  def partition(step: StepId): (SupervisorId, WorkerId) => PartitionId
  // Inverse of partition
  def route(step: StepId): PartitionId => (SupervisorId, WorkerId)

  def totalWorkers = workersPerSupervisor * supervisors
}

class CyclicUpdates(override val workersPerSupervisor: Int,
    override val supervisors: Int) extends UpdateStrategy {

  def partitions = totalWorkers

  def partition(step: StepId) = { (s: SupervisorId, w: WorkerId) =>
    val totalWorker = workersPerSupervisor * s.id + w.id
    PartitionId(mod(step.id + totalWorker, partitions))
  }
  def mod(n: Int, modulus: Int): Int = {
    val m = n % modulus
    if(m < 0) m + modulus else m
  }
  def route(step: StepId) = { (p: PartitionId) =>
    val totalWorker = mod(p.id - step.id, partitions)
    (SupervisorId(totalWorker/workersPerSupervisor), WorkerId(totalWorker % workersPerSupervisor))
  }
}
