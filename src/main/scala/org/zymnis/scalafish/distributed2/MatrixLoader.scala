package org.zymnis.scalafish.distributed2

import org.zymnis.scalafish.matrix._

trait MatrixLoader {
  def rows: Int
  def cols: Int
  def rowPartition(parts: Int): IndexedSeq[MatrixLoader]
  def load: Matrix
}

object MatrixLoader {
  def from(m: Matrix): MatrixLoader = new MatrixLoader {
    def rows = m.rows
    def cols = m.cols
    def rowPartition(parts: Int) = m.rowSlice(parts).map { from(_) }
    def load = m
  }
}

class TestLoader {
  import Distributed2._
  val real = DenseMatrix.randLowRank(ROWS, COLS, REALRANK)
  val load = SparseMatrix.sample(DENSITY, real)

  def rows = ROWS
  def cols = COLS
  def rowPartition(parts: Int) = load.rowSlice(parts).map { MatrixLoader.from(_) }
}


