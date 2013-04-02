package org.zymnis.scalafish

import org.zymnis.scalafish.matrix._

class ScalafishUpdater(left: Matrix, right: Matrix, data: Matrix, rank: Int) extends MatrixUpdater {
  def update(oldDelta: Matrix) = {
    val iter = data.denseIndices
    while(iter.hasNext) {
      val idx = iter.next
      val row = data.indexer.row(idx)
      val col = data.indexer.col(idx)
      // going to set: oldDelta(row, col) = L(row,_) * R(col,_) - data(row, col)
      var rankIdx = 0
      var sum = 0.0
      while(rankIdx < rank) {
        sum += left(row, rankIdx) * right(col, rankIdx)
        rankIdx += 1
      }
      val newV = (sum - data(idx)).toFloat
      oldDelta.update(row, col, newV)
    }
  }
}
