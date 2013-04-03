package org.zymnis.scalafish.matrix

import scala.annotation.tailrec
import scala.math.Equiv

import java.io.{ File, PrintWriter }

/** Addresses the specific needs we have for matrices
 * Specifically optimized for avoiding allocations, and the (Int,Int) => Float case
 * And mutability to minimize data copying
 * TODO: make this sealed for matching
 */
trait Matrix extends Shaped { self =>

  def size: Long = rows.toLong * cols.toLong

  def indexer: Indexer

  def writeToFile(fileName: String) {
    val p = new java.io.PrintWriter(fileName)
    try {
      Matrix.toMap(self).foreach { case (idx, value) =>
        val row = indexer.row(idx)
        val col = indexer.col(idx)
        val line = "%d\t%d\t%4.5f".format(row, col, value)
        p.println(line)
      }
    } finally {
      p.close()
    }
  }

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
    val indexer = Indexer.rowMajor(cols)
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

  def nonZeros: Long = {
    val iter = denseIndices
    var nnz = 0L
    while (iter.hasNext) {
      iter.next
      nnz += 1
    }
    nnz
  }

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

  def colSlice(numSlices: Int): IndexedSeq[Matrix] =
    self.t.rowSlice(numSlices).map { _.t }

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

  def frobNorm2: Double = {
    val it = denseIndices
    var result = 0.0
    while(it.hasNext) {
      val idx = it.next
      val valueD = apply(idx).toDouble
      result += valueD * valueD
    }
    result
  }
}

object RowMajorMatrix {
  def zeros(rows: Int, cols: Int): RowMajorMatrix = new RowMajorMatrix(cols, Array.fill(rows * cols)(0.0f))
}

class RowMajorMatrix(override val cols: Int, val items: Array[Float]) extends Matrix {
  assert((items.size % cols) == 0, "size not divisible by cols")

  val rows = items.size / cols
  val indexer = Indexer.rowMajor(cols)
  override def apply(rowCol: Long): Float = items(rowCol.toInt)
  override def update(rowCol: Long, f: Float): Unit = { items(rowCol.toInt) = f }
}

object Matrix {
  def frobNorm2(m: Matrix): Double = m.frobNorm2

  // Make a matrix out of an array on the heap. Safe for small matrices that are passed over RPC
  def rowMajor(cols: Int, items: Array[Float]): RowMajorMatrix = new RowMajorMatrix(cols, items)

  // TODO: use (Array[Long], Array[Float])
  def toMap(m: Matrix): Map[Long, Float] = {
    val iter = m.denseIndices
    var mapV = Map.empty[Long, Float]
    while(iter.hasNext) {
      val idx = iter.next
      mapV += (idx -> m(idx))
    }
    mapV
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

  def vStack(blocks: Seq[Matrix]): Matrix = new Matrix {
    val cols = blocks.head.cols
    require(blocks.forall { _.cols == cols },
      "all blocks must have same number of columns")
    val rows = blocks.map { _.rows }.sum
    val indexer = Indexer.rowMajor(cols)
    val sizeMap = blocks
      .view
      .map { _.size }
      .scanLeft(0L) { _ + _ }
      .zip(blocks)
      .toList // could be sortedmap

    override def apply(ind: Long) = {
      val (cnt, mat) = sizeMap.takeWhile { _._1 <= ind }.last
      mat.apply(ind - cnt)
    }

    override def update(ind: Long, value: Float) = {
      val (cnt, mat) = sizeMap.takeWhile { _._1 <= ind }.last
      mat.update(ind - cnt, value)
    }
  }
}
