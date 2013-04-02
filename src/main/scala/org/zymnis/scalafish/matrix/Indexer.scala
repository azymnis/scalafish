package org.zymnis.scalafish.matrix

import scala.annotation.tailrec

// Index a matrix by a Long
trait Indexer { self =>
  def rowCol(row: Int, col: Int): Long
  def row(rowcol: Long): Int
  def col(rowcol: Long): Int
  def transposeRowCol(rowColIdx: Long): Long = {
    val originalRow = row(rowColIdx)
    val originalCol = col(rowColIdx)
    rowCol(originalCol, originalRow)
  }

  def transpose: Indexer = new Indexer {
    def rowCol(row: Int, col: Int) = self.rowCol(col, row)
    def row(rowcol: Long) = self.col(rowcol)
    def col(rowcol: Long) = self.row(rowcol)
    override def transpose = self
  }
}

object Indexer {
  def rowMajor(cols: Int): Indexer = new Indexer {
    require(cols > 0, "rowMajor needs positive cols")
    val colsLong = cols.toLong
    def rowCol(row: Int, col: Int) = col.toLong + colsLong * row.toLong
    def row(rowcol: Long) = (rowcol / colsLong).toInt
    def col(rowcol: Long) = (rowcol % colsLong).toInt
  }
  def colMajor(rows: Int): Indexer = rowMajor(rows).transpose
  // def vStack(indexers: Seq[(Indexer, Int)], cols: Int) = new Indexer {
  //   def rowCol(row: Int, col: Int) = {
  //     require(col < cols, "col must be less than block num cols")
  //     @tailrec
  //     def rowColInner(rem: Seq[(Indexer, Int)], thisRow: Int): Long = {
  //       val diff = thisRow - rem.head._2
  //       if (diff < 0) {
  //         rem.head._1.rowCol(thisRow, col)
  //       } else {
  //         rowColInner(rem.tail, diff)
  //       }
  //     }
  //     rowColInner(indexers, row)
  //   }

  //   def row(rowcol: Long) = {
  //     @tailrec
  //     def rowColInner(rem: Seq[(Indexer, Int)], thisRC: Long, accRows: Int = 0): Int = {
  //       val thisRows = rem.head._2
  //       val diff = thisRC - thisRows.toLong * cols
  //       if (diff < 0) {
  //         println("accRows: %d, thisRC: %d".format(accRows, thisRC))
  //         rem.head._1.row(thisRC) + accRows
  //       } else {
  //         rowColInner(rem.tail, diff, accRows + thisRows)
  //       }
  //     }
  //     rowColInner(indexers, rowcol)
  //   }

  //   def col(rowcol: Long) = {
  //     @tailrec
  //     def rowColInner(rem: Seq[(Indexer, Int)], thisRC: Long): Int = {
  //       val diff = thisRC - rem.head._2.toLong * cols
  //       if (diff < 0) {
  //         rem.head._1.col(thisRC)
  //       } else {
  //         rowColInner(rem.tail, diff)
  //       }
  //     }
  //     rowColInner(indexers, rowcol)
  //   }
  // }
}

trait LongPredicate {
  def apply(l: Long): Boolean
}

// Used to iterator over indexed (row, col) pairs
trait LongIterator { self =>
  def hasNext: Boolean
  def next: Long
  // Some combinators:
  def filter(lp: LongPredicate): LongIterator = new LongIterator {
    var advanced = false
    var nextLong: Long = 0L

    @tailrec
    final def advance: Boolean =
      if(self.hasNext) {
        nextLong = self.next
        if (lp(nextLong)) {
          advanced = true
          true
        }
        else
          advance
      }
      else {
        false
      }

    override def hasNext = {
      if (!advanced) { advanced = advance }
      advanced
    }
    override def next = {
      advanced = false
      nextLong
    }
  }
}
