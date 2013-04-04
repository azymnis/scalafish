package org.zymnis.scalafish.matrix

import it.unimi.dsi.fastutil.longs.{Long2FloatOpenHashMap => Long2FloatMap}

class SparseMatrix private (override val rows: Int,
    override val cols: Int,
    override val indexer: Indexer,
    hashMap: Long2FloatMap) extends Matrix { self =>

  hashMap.defaultReturnValue(0.0f)

  def contains(row: Int, col: Int): Boolean = {
    val idx = indexer.rowCol(row, col)
    hashMap.containsKey(idx)
  }
  override def apply(idx: Long): Float = hashMap.get(idx)

  // Returns a view with min inclusive, but max exclusive bounds
  override def blockView(rowMin: Int, colMin: Int, rowMax: Int, colMax: Int): Matrix = {
    val thisRows = rowMax - rowMin
    val thisCols = colMax - colMin
    val thisSize = thisRows.toLong * thisCols.toLong
    if (thisSize < hashMap.size) {
      //Better off doing the dense version:
      super.blockView(rowMin, colMin, rowMax, colMax)
    }
    else new Matrix {
      override val rows = thisRows
      override val cols = thisCols
      val indexer = Indexer.rowMajor(cols)
      override def apply(row: Int, col: Int) = self.apply(row + rowMin, col + colMin)
      override def update(row: Int, col: Int, f: Float) = self.update(row + rowMin, col + colMin, f)
      override def denseIndices = self.denseIndices.filter(
        new LongPredicate { def apply(l: Long) = {
          val r = self.indexer.row(l) - rowMin
          val c = self.indexer.col(l) - colMin
          (0 <= r && r < rows) && (0 <= c && c < cols)
        }
      })
    }
  }

  override def nonZeros: Long = hashMap.size.toLong

  override def update(idx: Long, f: Float) {
    if( f != 0.0 ) {
      hashMap.put(idx, f)
    }
    else {
      hashMap.remove(idx)
    }
  }

  override def update(row: Int, col: Int, f: Float) {
    val idx = indexer.rowCol(row, col)
    update(idx, f)
  }

  override def denseIndices: LongIterator = new LongIterator {
    val lit = hashMap.keySet.iterator
    def hasNext = lit.hasNext
    def next = lit.nextLong
  }
}

object SparseMatrix {
  def apply(su: ShapedUpdater): SparseMatrix = {
    val z = zeros(su.rows, su.cols)
    z := su
    z
  }

  def zeros(rows: Int, cols: Int): SparseMatrix =
    new SparseMatrix(rows, cols,  Indexer.rowMajor(cols), new Long2FloatMap)

  // TODO: Inefficient, fix
  def from(rows: Int, cols: Int, rep: Map[Long, Float]): SparseMatrix = {
    val out = zeros(rows, cols)
    val iter = rep.keysIterator
    while (iter.hasNext) {
      val key = iter.next
      out.update(key, rep(key))
    }
    out
  }

  // Reads matrix from a TSV file with three columns
  def fromFile(rows: Int, cols: Int, fileName: String): SparseMatrix = {
    val out = zeros(rows, cols)
    scala.io.Source.fromFile(fileName).getLines.foreach { line =>
      val data = line.split("\t")
      if (data.size == 3) {
        val row = data(0).toInt
        val col = data(1).toInt
        val value = data(2).toFloat
        out.update(row, col, value)
      }
    }
    out
  }

  def from(rows: Int, cols: Int, row: Iterable[Int], col: Iterable[Int], vs: Array[Float]): SparseMatrix = {
    val idxer = Indexer.rowMajor(cols)
    val indices = row.view.zip(col).map { case (row, col) => idxer.rowCol(row, col) }.toArray
    val ht = new Long2FloatMap(indices, vs)
    new SparseMatrix(rows, cols, idxer, ht)
  }

  def patternOf(sm: SparseMatrix): SparseMatrix = {
    val pattern = new SparseMatrix(sm.rows, sm.cols, sm.indexer, new Long2FloatMap)
    val iter = sm.denseIndices
    while(iter.hasNext) {
      pattern.update(iter.next, 1.0f)
    }
    pattern
  }

  // TODO: this can be made O(p * m.size) but is m.size now
  def sample(prob: Double, m: Matrix)(implicit rng: java.util.Random): SparseMatrix = {
    val s = zeros(m.rows, m.cols)
    val size = m.size
    var idx = 0L
    while (idx < size) {
      if(prob > rng.nextDouble) {
        val (r,c) = (m.indexer.row(idx), m.indexer.col(idx))
        s.update(r, c, m(r, c))
      }
      idx += 1L
    }
    s
  }
}
