package org.zymnis.scalafish.serialization

import org.zymnis.scalafish.matrix.{RowMajorMatrix, Matrix, SparseMatrix}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

class MatrixSerializer extends Serializer[Matrix] {
  override def read(kryo: Kryo, input: Input, typ: Class[Matrix]): Matrix =
    if(input.readBoolean) readDense(input) else readSparse(input)

  def readDense(in: Input): Matrix = {
    val (rows, cols) = (in.readInt, in.readInt)
    val obj = RowMajorMatrix.zeros(rows, cols)
    var rowIdx = 0
    var colIdx = 0
    while(rowIdx < rows) {
      colIdx = 0
      while(colIdx < cols) {
        obj.update(rowIdx, colIdx, in.readFloat)
        colIdx += 1
      }
      rowIdx += 1
    }
    obj
  }
  def readSparse(in: Input): Matrix = {
    val (rows, cols) = (in.readInt, in.readInt)
    val sm = SparseMatrix.zeros(rows, cols)
    val denseVals = in.readLong
    var cnt = 0
    while(cnt < denseVals) {
      val (row, col, vfloat) = (in.readInt, in.readInt, in.readFloat)
      sm.update(row, col, vfloat)
      cnt += 1
    }
    sm
  }

  override def write(kryo: Kryo, output: Output, obj: Matrix) {
    val nonz = obj.nonZeros
    val size = obj.size
    // Dense storage takes size * 4 bytes
    // sparse storage takes nonz * 12 bytes (int,int,float)
    if(nonz * 3 < size) writeSparse(nonz, output, obj) else writeDense(output, obj)
  }

  def writeDense(out: Output, obj: Matrix) {
    out.writeBoolean(true)
    val (rows, cols) = (obj.rows, obj.cols)
    out.writeInt(rows)
    out.writeInt(cols)
    var rowIdx = 0
    var colIdx = 0
    while(rowIdx < rows) {
      colIdx = 0
      while(colIdx < cols) {
        out.writeFloat(obj(rowIdx, colIdx))
        colIdx += 1
      }
      rowIdx += 1
    }
  }
  def writeSparse(nonz: Long, out: Output, obj: Matrix) {
    out.writeBoolean(false)
    val (rows, cols) = (obj.rows, obj.cols)
    out.writeInt(rows)
    out.writeInt(cols)
    out.writeLong(nonz)
    val iter = obj.denseIndices
    while(iter.hasNext) {
      val idx = iter.next
      out.writeInt(obj.indexer.row(idx))
      out.writeInt(obj.indexer.col(idx))
      out.writeFloat(obj(idx))
    }
  }
}
