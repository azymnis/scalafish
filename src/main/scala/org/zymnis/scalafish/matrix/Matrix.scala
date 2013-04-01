package org.zymnis.scalafish.matrix

import scala.math.Equiv

import scala.annotation.tailrec

/** Addresses the specific needs we have for matrices
 * Specifically optimized for avoiding allocations, and the (Int,Int) => Float case
 * And mutability to minimize data copying
 * TODO: make this sealed for matching
 */
trait Matrix extends Shaped { self =>

  def size: Long = rows.toLong * cols.toLong

  def indexer: Indexer
  def apply(rowcol: Long): Float = {
    val row = indexer.row(rowcol)
    val col = indexer.col(rowcol)
    apply(row, col)
  }

  def apply(row: Int, col: Int): Float = {
    val idx = indexer.rowCol(row, col)
    apply(idx)
  }

  def update(row: Int, col: Int, f: Float): Unit = {
    val idx = indexer.rowCol(row, col)
    update(idx, f)
  }

  def update(rowcol: Long, f: Float): Unit = {
    val row = indexer.row(rowcol)
    val col = indexer.col(rowcol)
    update(row, col, f)
  }

  def :=(updater: MatrixUpdater): this.type = {
    updater.update(self)
    self
  }

  // Returns a view with min inclusive, but max exclusive bounds
  def blockView(rowMin: Int, colMin: Int, rowMax: Int, colMax: Int): Matrix = new Matrix {
    def rows = rowMax - rowMin
    def cols = colMax - colMin
    val indexer = Indexer.shifted(self.indexer, rowMin, colMin)
    override def apply(row: Int, col: Int) = self.apply(row + rowMin, col + colMin)
    override def update(row: Int, col: Int, f: Float) = self.update(row + rowMin, col + colMin, f)
  }

  def allIndices: LongIterator = new LongIterator {
    var rowIdx = 0
    var colIdx = -1
    def hasNext: Boolean = {
      colIdx += 1
      if (colIdx == cols) { colIdx = 0; rowIdx += 1 }
      if (rowIdx == rows ) {
        // Done
        false
      }
      else {
        true
      }
    }
    def next = self.indexer.rowCol(rowIdx, colIdx)
  }

  // By default, it does this in the dense way: look at all row/cols
  def denseIndices: LongIterator =
    allIndices.filter(new LongPredicate { def apply(l: Long) = self(l) != 0.0f })

  def rowSlice(numSlices: Int): IndexedSeq[Matrix] = {
    require(numSlices > 0, "numSlices must be positive")
    val rowsPerSlice = rows / numSlices
    (0 until numSlices).flatMap { s =>
      val rowStart = rowsPerSlice * s
      val rowEnd = if (s == (numSlices - 1)) {
        rows
      } else {
        rowsPerSlice * (s + 1)
      }

      if (rowEnd > rowStart) {
        Some(self.blockView(rowStart, 0, rowEnd, cols))
      } else {
        None
      }
    }
  }

  // Transpose view (DOES NOT COPY)
  def t: Matrix = new Matrix {
    def rows = self.cols
    def cols = self.rows
    def indexer = self.indexer.transpose
    override def apply(row: Int, col: Int) = self.apply(col, row)
    override def update(row: Int, col: Int, f: Float) = self.update(col, row, f)
    // Transpose of a transpose is identity:
    override def t = self
  }

  // Super inefficient, just for debug
  override def toString = {
    val strb = new StringBuilder()
    strb.append("Matrix(" + rows + " x " + cols + "):")
    var row = 0
    var col = 0
    while(row < rows) {
      col = 0
      strb.append("\n")
      while(col < cols) {
        strb.append("%.2f ".format(apply(row, col)))
        col += 1
      }
      row += 1
    }
    strb.append("\n")
    strb.toString
  }
}

object Matrix {
  def frobNorm2(m: Matrix): Double = {
    m match {
      case dm: DenseMatrix => DenseMatrix.frobNorm2(dm)
      case sm: SparseMatrix => sm.frobNorm2
    }
  }

  implicit val defaultEquiv = equiv(1e-6)
  // TODO this doesn't need to allocate
  def equiv(frobEps: Double): Equiv[Matrix] = Equiv.fromFunction[Matrix] { (m1, m2) =>
    import Syntax._
    (m1.rows == m2.rows) && (m2.cols == m2.cols) && {
      val temp = DenseMatrix.zeros(m1.rows, m1.cols)
      temp := m1 - m2
      val fnorm = frobNorm2(temp)
      fnorm < frobEps
    }
  }
}
