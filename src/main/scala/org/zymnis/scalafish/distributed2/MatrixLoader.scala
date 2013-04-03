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

class TestLoader extends MatrixLoader {
  import Distributed2._
  val real = DenseMatrix.randLowRank(ROWS, COLS, REALRANK)
  val load = SparseMatrix.sample(DENSITY, real)

  def rows = ROWS
  def cols = COLS
  def rowPartition(parts: Int) = load.rowSlice(parts).map { MatrixLoader.from(_) }
}

trait MatrixWriter {
  def rows: Int
  def cols: Int
  /**
   * This produces an IndexedSeq exactly parts in length,
   * calling
   * {{{
   *
   * matrix.rowSlice(parts)
   *   .zip(writer.rowPartition(parts))
   *   .foreach { case (m,mw) => mw.write(m) }
   *
   * }}}
   * should be the same as writer.write(matrix)
   */
  def rowPartition(parts: Int): IndexedSeq[MatrixWriter]
  def write(m: Matrix): Unit
}

class PrintWriter(override val rows: Int, override val cols: Int) extends MatrixWriter {
  def write(m: Matrix) = {
    require(m.rows == rows, "Rows don't match %d != %d".format(m.rows, rows))
    require(m.cols == cols, "Cols don't match %d != %d".format(m.cols, cols))
    println(m)
  }
  def rowPartition(parts: Int) = sys.error("Not yet implemented")
}
