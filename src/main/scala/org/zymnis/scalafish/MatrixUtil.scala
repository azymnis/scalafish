package org.zymnis.scalafish

import breeze.linalg._

import scala.util.Random

object MatrixUtil {
  def randLowRank(rows: Int, cols: Int, rank: Int, noise: Double) = {
    val L = DenseMatrix.rand(rows, rank)
    val R = DenseMatrix.rand(cols, rank)
    L * R.t + DenseMatrix.rand(rows, cols) * noise
  }

  def randSparseMatrix(rows: Int, cols: Int, rank: Int, p: Double, noise: Double): (DenseMatrix[Double], CSCMatrix[Double]) = {
    val realMat = randLowRank(rows, cols, rank, noise)
    val out = CSCMatrix.zeros[Double](rows, cols)
    val rand = new Random
    realMat.keysIterator.foreach{ case(r, c) =>
      if(rand.nextDouble < p)
        out(r, c) = realMat(r, c)
    }
    (realMat, out)
  }

  def randSparseSliced(rows: Int, cols: Int, rank: Int, p: Double, noise: Double, slices: Int): (DenseMatrix[Double], Iterable[CSCMatrix[Double]]) = {
    val realMat = randLowRank(rows, cols, rank, noise)
    val rand = new Random
    val sliceSize = rows / slices

    val matIter = (0 to slices).map{ i =>
      val range = (i * sliceSize) to ((i + 1) * sliceSize - 1)
      val out = CSCMatrix.zeros[Double](sliceSize, cols)
      realMat(range, ::).keysIterator.foreach{ case(r, c) =>
        if(rand.nextDouble < p)
          out(r, c) = realMat(r, c)
      }
      out
    }
    (realMat, matIter)
  }

  def frobNorm(m: DenseMatrix[Double]): Double =
    m mapValues { math.pow(_, 2) } sum
}
