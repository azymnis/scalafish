package org.zymnis.scalafish

import breeze.linalg._

import scala.util.Random

object DistributedMatrixFactorizer {
  // def randSparseMatrix(rows: Int, cols: Int, rank: Int, p: Double, noise: Double, numPieces: Int): (DenseMatrix[Double], Array[CSCMatrix[Double]]) = {
  //   val L = DenseMatrix.rand(rows, rank)
  //   val R = DenseMatrix.rand(cols, rank)
  //   val realMat = L * R.t + DenseMatrix.rand(rows, cols) * noise
  //   val out = CSCMatrix.zeros[Double](rows, cols)
  //   val rand = new Random
  //   realMat.keysIterator.foreach{ case(r, c) =>
  //     if(rand.nextDouble < p)
  //       out(r, c) = realMat(r, c)
  //   }
  //   (realMat, out)
  // }
}
