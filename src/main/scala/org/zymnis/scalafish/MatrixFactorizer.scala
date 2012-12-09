package org.zymnis.scalafish

import breeze.linalg._

import scala.util.Random

object MatrixFactorizer {
  def main(args: Array[String]) {
    val (real, data) = randSparseMatrix(200, 200, 20, 0.05, 0.001)
    val mf = new MatrixFactorizer(data, 20)
    (0 to 50).foreach{ i =>
      val obj = mf.currentObjective
      val relerr = mf.frobNorm(mf.currentGuess - real) / mf.frobNorm(real)
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

  val defaultParams = FactorizerParams(1e-6, 1e-3)
}

class MatrixFactorizer(data: CSCMatrix[Double], rank: Int,
    params: FactorizerParams = MatrixFactorizer.defaultParams) {

  val L = DenseMatrix.rand(data.rows, rank)
  val R = DenseMatrix.rand(data.cols, rank)

  var iteration = 1

  // Get the sparsity pattern in a separate matrix
  val pat = DenseMatrix.zeros[Double](data.rows, data.cols)
  data.activeKeysIterator.foreach{ case(r, c) => pat(r, c) = 1.0 }

  def currentObjective: Double =
    frobNorm(currentDelta) + (params.mu / 2) * (frobNorm(L) + frobNorm(R))

  def currentDelta = pat :* (L*R.t - data)

  def update {
    val newAlpha = params.alpha / iteration
    L := L - (L * (params.mu / 2) + currentDelta * R ) * newAlpha
    R := R - (R * (params.mu / 2) + currentDelta.t * L ) * newAlpha
    iteration += 1
  }

  def currentGuess = L*R.t

  def frobNorm(m: DenseMatrix[Double]): Double =
    m mapValues { math.pow(_, 2) } sum
}

case class FactorizerParams(mu: Double, alpha: Double)
