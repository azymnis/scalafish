package org.zymnis.scalafish.matrix

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
    val colsLong = cols.toLong
    def rowCol(row: Int, col: Int) = col.toLong + colsLong * row.toLong
    def row(rowcol: Long) = (rowcol / colsLong).toInt
    def col(rowcol: Long) = (rowcol % colsLong).toInt
  }
  def colMajor(rows: Int): Indexer = rowMajor(rows).transpose
  def shifted(indexer: Indexer, rows: Int, cols: Int) = new Indexer {
    def rowCol(row: Int, col: Int) = indexer.rowCol(row + rows, col + cols)
    def row(rowcol: Long) = indexer.row(rowcol) - rows
    def col(rowcol: Long) = indexer.col(rowcol) - cols
  }
}

// Used to iterator over indexed (row, col) pairs
trait LongIterator { self =>
  def hasNext: Boolean
  def next: Long
}
