package org.zymnis.scalafish.matrix

import it.unimi.dsi.fastutil.longs.{Long2FloatOpenHashMap => Long2FloatMap}

class SparseMatrix private (override val rows: Int,
    override val cols: Int,
    override val indexer: Indexer,
    hashMap: Long2FloatMap) extends Matrix {

  hashMap.defaultReturnValue(0.0f)

  def contains(row: Int, col: Int): Boolean = {
    val idx = indexer.rowCol(row, col)
    hashMap.containsKey(idx)
  }
  def apply(idx: Long): Float = hashMap.get(idx)
  def apply(row: Int, col: Int) = {
    val idx = indexer.rowCol(row, col)
    hashMap.get(idx)
  }

  def update(idx: Long, f: Float) {
    if( f != 0.0 ) {
      hashMap.put(idx, f)
    }
    else {
      hashMap.remove(idx)
    }
  }

  def update(row: Int, col: Int, f: Float) {
    val idx = indexer.rowCol(row, col)
    update(idx, f)
  }

  def denseIndices: LongIterator = new LongIterator {
    val lit = hashMap.keySet.iterator
    def hasNext = lit.hasNext
    def next = lit.nextLong
  }

  def frobNorm2: Double = {
    val it = denseIndices
    var result = 0.0
    while(it.hasNext) {
      val idx = it.next
      val valueD = hashMap.get(idx).toDouble
      result += valueD * valueD
    }
    result
  }
}

object SparseMatrix {
  def zeros(rows: Int, cols: Int): SparseMatrix = new SparseMatrix(rows, cols,  Indexer.rowMajor(cols), new Long2FloatMap)
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
  def sample(prob: Double, m: Matrix): SparseMatrix = {
    val s = zeros(m.rows, m.cols)
    val rng = new java.util.Random
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
