package org.zymnis.scalafish

import breeze.linalg._

import scala.util.Random

object SimpleMatrixFactorizer extends App {
  val (real, data) = MatrixUtil.randSparseMatrix(200, 200, 10, 0.10, 0.001)
  val mf = new SimpleMatrixFactorizer(data, 15, 1e-3, 1e-2)
  (0 to 20).foreach{ i =>
    val obj = mf.currentObjective
    val relerr = math.sqrt(MatrixUtil.frobNorm(mf.currentGuess - real)) / math.sqrt(MatrixUtil.frobNorm(real))
    println("iteration: " + i + ", obj: " + obj + ", relerr: " + relerr)
    mf.update
  }
}

class SimpleMatrixFactorizer(data: CSCMatrix[Double], rank: Int,
    mu: Double, alpha: Double) {

  val L = DenseMatrix.rand(data.rows, rank)
  val R = DenseMatrix.rand(data.cols, rank)

  var iteration = 1

  // Get the sparsity pattern in a separate matrix
  val pat = DenseMatrix.zeros[Double](data.rows, data.cols)
  data.activeKeysIterator.foreach{ case(r, c) => pat(r, c) = 1.0 }

  def currentObjective: Double =
    MatrixUtil.frobNorm(currentDelta) + (mu / 2) * (MatrixUtil.frobNorm(L) + MatrixUtil.frobNorm(R))

  def currentDelta = pat :* (L*R.t - data)

  def update {
    val newAlpha = alpha / iteration
    L := L - (L * (mu / 2) + currentDelta * R ) * newAlpha
    R := R - (R * (mu / 2) + currentDelta.t * L ) * newAlpha
    iteration += 1
  }

  def currentGuess = L*R.t
}
