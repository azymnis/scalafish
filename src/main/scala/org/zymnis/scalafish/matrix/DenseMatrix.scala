package org.zymnis.scalafish.matrix

import java.nio.ByteBuffer

class DenseMatrix private (override val rows: Int, override val cols: Int, override val indexer: Indexer) extends Matrix {
  assert(rows > 0, "rows must be > 0")
  assert(cols > 0, "rows must be > 0")

  import DenseMatrix.BYTES_PER_FLOAT
  val MAX_BYTE_BUFFER = Int.MaxValue
  val TOTAL_SIZE = size * BYTES_PER_FLOAT

  val (blocks, rows_per_block, rows_in_last_block) = {
    if(TOTAL_SIZE <= MAX_BYTE_BUFFER) {
      // Everything fits in one
      (1, rows, rows)
    }
    else {
      /*
       * Max k such that k * cols <= MAX_BYTE_BUFFER
       * k = MAX_BYTE_BUFFER / cols
       */
      val rows_per_block = (MAX_BYTE_BUFFER / cols) / BYTES_PER_FLOAT
      val blocks = rows / rows_per_block + (if((rows % rows_per_block) == 0) 0 else 1)
      val last_block = rows - (blocks - 1) * rows_per_block
      (blocks, rows_per_block, last_block)
    }
  }
  private val data = {
    val ary = new Array[ByteBuffer](blocks)
    var idx = 0
    while(idx < blocks) {
      val rowsInThisBlock = if(idx == blocks - 1) rows_in_last_block else rows_per_block
      ary(idx) = ByteBuffer.allocateDirect(rowsInThisBlock * cols * BYTES_PER_FLOAT)
      idx += 1
    }
    ary
  }

  override def apply(row: Int, col: Int): Float = {
    val block = row / rows_per_block
    val blockrow = row % rows_per_block
    data(block).getFloat(indexer.rowCol(blockrow, col).toInt * BYTES_PER_FLOAT)
  }

  def fill(f: Float): this.type = {
    (0 until blocks).foreach { b =>
      val bb = data(b)
      val rowsInThisBlock = if(b == blocks - 1) rows_in_last_block else rows_per_block
      val floats = rowsInThisBlock * cols
      var idx = 0
      while(idx < floats) {
        bb.putFloat(idx * BYTES_PER_FLOAT, f)
        idx += 1
      }
    }
    this
  }

  override def update(row: Int, col: Int, f: Float): Unit = {
    val block = row / rows_per_block
    val blockrow = row % rows_per_block
    data(block).putFloat(indexer.rowCol(blockrow, col).toInt * BYTES_PER_FLOAT, f)
  }
}

object DenseMatrix {
  val BYTES_PER_FLOAT = 4
  def zeros(rows: Int, cols: Int): DenseMatrix =
    new DenseMatrix(rows, cols, Indexer.rowMajor(cols))

  def apply(su: ShapedUpdater): DenseMatrix = {
    val z = zeros(su.rows, su.cols)
    z := su
    z
  }

  def rand(rows: Int, cols: Int)(implicit rng: java.util.Random): DenseMatrix = {
    val mat = zeros(rows, cols)
    var rowIdx = 0
    var colIdx = 0
    while(colIdx < cols) {
      rowIdx = 0
      while(rowIdx < rows) {
        mat.update(rowIdx, colIdx, rng.nextFloat)
        rowIdx += 1
      }
      colIdx += 1
    }
    mat
  }

  def randLowRank(rows: Int, cols: Int, rank: Int)(implicit rng: java.util.Random): DenseMatrix = {
    import Syntax._

    require(rank < rows, "Rank should be less than the number of rows")
    require(rank < rows, "Rank should be less than the number of cols")
    val leftM = rand(rows, rank)
    val rightM = rand(cols, rank)
    val result = zeros(rows, cols)
    result := leftM * rightM.t
    result
  }

  def frobNorm2(dm: DenseMatrix): Double = dm.frobNorm2
}
