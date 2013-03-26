package org.zymnis.scalafish.matrix

import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Gen._
import org.scalacheck.Prop.forAll

import Syntax._

object MatrixProperties extends Properties("Matrix") {

  val FNORM_EPS = 1e-6

  def denseRandGen(rows: Int, cols: Int): Gen[Matrix] =
    Gen(_ => Some(DenseMatrix.rand(rows, cols)))

  def sparseRandGen(rows: Int, cols: Int, p: Double): Gen[Matrix] =
    denseRandGen(rows, cols).map { SparseMatrix.sample(p, _) }

  def denseWithNegs(rows: Int, cols: Int): Gen[Matrix] =
    Gen(_ => {
      val ones = DenseMatrix.zeros(rows, cols)
      ones := 1f
      val rand = DenseMatrix.rand(rows, cols)
      rand *= 2f
      rand := rand - ones
      Some(rand)
    })

  def sparseWithNegs(rows: Int, cols: Int, p: Double): Gen[Matrix] =
    denseWithNegs(rows, cols).map { SparseMatrix.sample(p, _) }

  def dense(rows: Int, cols: Int) = Gen.oneOf(denseRandGen(rows, cols), denseWithNegs(rows, cols))

  def sparse(rows: Int, cols: Int, p: Double) =
    Gen.oneOf(sparseRandGen(rows, cols, p), sparseWithNegs(rows, cols, p))

  def denseOrSparse(rows: Int, cols: Int, p: Double): Gen[Matrix] =
    Gen.oneOf(dense(rows, cols), sparse(rows, cols, p))

  def fnormIsZero(fn: Double): Boolean =
    fn >= 0.0 && fn <= FNORM_EPS

  // for nxk matrices (to guarantee dimensions match): A B^T == (B A^T)^T
  def transposeLaw(cons: (Int,Int) => Matrix)(implicit dma: Arbitrary[Matrix]) = forAll { (a: Matrix, b: Matrix) =>
    val c = cons(a.rows, a.rows)
    val d = cons(a.rows, a.rows)
    val e = cons(a.rows, a.rows)
    c := a * b.t
    d := b * a.t
    // First way to compute
    e := c - d.t
    val fnorm1 = Matrix.frobNorm2(e)
    // Inline
    c -= d.t
    val fnorm2 = Matrix.frobNorm2(c)
    fnormIsZero(fnorm1) && fnormIsZero(fnorm2)
  }

  property("TransposeLaw 3x5 into Dense") = {
    implicit val arb = Arbitrary(denseOrSparse(3, 5, 0.3))
    transposeLaw(DenseMatrix.zeros(_,_)) &&
    transposeLaw(SparseMatrix.zeros(_,_))
  }
  property("TransposeLaw 10x10") = {
    implicit val arb = Arbitrary(denseOrSparse(10, 10, 0.1))
    transposeLaw(DenseMatrix.zeros(_,_)) &&
    transposeLaw(SparseMatrix.zeros(_,_))
  }
  property("TransposeLaw 1x1") = {
    implicit val arb = Arbitrary(denseOrSparse(1, 1, 1.0))
    transposeLaw(DenseMatrix.zeros(_,_)) &&
    transposeLaw(SparseMatrix.zeros(_,_))
  }


}
