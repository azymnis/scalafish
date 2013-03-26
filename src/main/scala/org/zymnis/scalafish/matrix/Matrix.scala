package org.zymnis.scalafish.matrix

/** Addresses the specific needs we have for matrices
 * Specifically optimized for avoiding allocations, and the (Int,Int) => Float case
 * And mutability to minimize data copying
 * TODO: make this sealed for matching
 */
trait Matrix { self =>
  def rows: Int
  def cols: Int

  def size: Long = rows.toLong * cols.toLong

  def indexer: Indexer
  def apply(row: Int, col: Int): Float

  def update(row: Int, col: Int, f: Float): Unit

  def :=(updater: MatrixUpdater): this.type = {
    updater.update(self)
    self
  }

  // Transpose view (DOES NOT COPY)
  def t: Matrix = new Matrix {
    def rows = self.cols
    def cols = self.rows
    def indexer = self.indexer.transpose
    def apply(row: Int, col: Int) = self.apply(col, row)
    def update(row: Int, col: Int, f: Float) = self.update(col, row, f)
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
}
