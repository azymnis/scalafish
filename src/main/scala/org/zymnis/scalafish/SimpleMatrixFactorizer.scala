package org.zymnis.scalafish

import breeze.linalg._

import scala.util.Random

object SimpleMatrixFactorizer {
  def main(args: Array[String]) {
    val (real, data) = randSparseMatrix(1000, 200, 10, 0.20, 0.001)
    val mf = new SimpleMatrixFactorizer(data, 10, 1e-3, 1e-3)
    (0 to 50).foreach{ i =>
      val obj = mf.currentObjective
      val relerr = math.sqrt(mf.frobNorm(mf.currentGuess - real)) / math.sqrt(mf.frobNorm(real))
      println("iteration: " + i + ", obj: " + obj + ", relerr: " + relerr)
      mf.update
    }
  }

  def randSparseMatrix(rows: Int, cols: Int, rank: Int, p: Double, noise: Double): (DenseMatrix[Double], CSCMatrix[Double]) = {
    val L = DenseMatrix.rand(rows, rank)
    val R = DenseMatrix.rand(cols, rank)
    val realMat = L * R.t + DenseMatrix.rand(rows, cols) * noise
    val out = CSCMatrix.zeros[Double](rows, cols)
    val rand = new Random
    realMat.keysIterator.foreach{ case(r, c) =>
      if(rand.nextDouble < p)
        out(r, c) = realMat(r, c)
    }
    (realMat, out)
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
    frobNorm(currentDelta) + (mu / 2) * (frobNorm(L) + frobNorm(R))

  def currentDelta = pat :* (L*R.t - data)

  def update {
    val newAlpha = alpha / iteration
    L := L - (L * (mu / 2) + currentDelta * R ) * newAlpha
    R := R - (R * (mu / 2) + currentDelta.t * L ) * newAlpha
    iteration += 1
  }

  def currentGuess = L*R.t

  def frobNorm(m: DenseMatrix[Double]): Double =
    m mapValues { math.pow(_, 2) } sum
}
