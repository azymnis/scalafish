package org.zymnis.scalafish

import org.zymnis.scalafish.matrix._

import Syntax._ // Matrix syntax

import scala.util.Random


object SimpleMatrixFactorizer extends App {
  val (rows, cols, rank) = (200, 100, 10)
  val real = DenseMatrix.randLowRank(rows, cols, rank)
  val data = SparseMatrix.sample(0.1, real)
  val approx = DenseMatrix.zeros(rows, cols) // probably don't materialize this in the real deal

  val mf = new SimpleMatrixFactorizer(data, 15, 1e-3f, 1e-2f)
  (0 to 20).foreach{ i =>
    val obj = mf.currentObjective
    approx := mf.currentGuess
    approx := approx - real
    val relerr = math.sqrt(Matrix.frobNorm2(approx)) / math.sqrt(Matrix.frobNorm2(real))
    println("iteration: " + i + ", obj: " + obj + ", relerr: " + relerr)
    mf.update
  }
  println(approx)
  println(mf.L)
}

class SimpleMatrixFactorizer(data: SparseMatrix, rank: Int, mu: Float, alpha: Float) {

  val L = DenseMatrix.rand(data.rows, rank)
  val R = DenseMatrix.rand(data.cols, rank)

  var iteration = 1

  private var currentDelta = SparseMatrix.zeros(data.rows, data.cols)

  var currentObjective: Double = computeObjective

  def computeObjective: Double =
    0.5 * (Matrix.frobNorm2(currentDelta) + mu * (Matrix.frobNorm2(L) + Matrix.frobNorm2(R)))

  def delta: MatrixUpdater = new MatrixUpdater {
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
          sum += L(row, rankIdx) * R(col, rankIdx)
          rankIdx += 1
        }
        oldDelta.update(row, col, (sum - data(idx)).toFloat)
      }
    }
  }

  def update {
    val newAlpha = alpha / iteration

    currentDelta := delta
    currentDelta *= newAlpha

    L *= (1.0f - mu * newAlpha)
    L += (currentDelta * R)

    currentDelta := delta
    // Slightly out of date by the end, but cheaper
    currentObjective = computeObjective
    currentDelta *= newAlpha

    R *= (1.0f - mu * newAlpha)
    R += (currentDelta.t * L)

    iteration += 1
  }

  def currentGuess: MatrixUpdater = L * R.t
}
