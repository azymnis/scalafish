package org.zymnis.scalafish.matrix

import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Gen._
import org.scalacheck.Prop.forAll

import Syntax._

object MatrixProperties extends Properties("Matrix") {

  val FNORM_EPS = 1e-5

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

  trait MatrixCons {
    def apply(rows: Int, cols: Int): Matrix
  }

  def denseCons: Gen[MatrixCons] =
    Gen(_ => {
      Some(new MatrixCons {
        def apply(rows: Int, cols: Int) = DenseMatrix.zeros(rows, cols)
      })
    })

  def sparseCons: Gen[MatrixCons] =
    Gen(_ => Some(new MatrixCons {
        def apply(rows: Int, cols: Int) = SparseMatrix.zeros(rows, cols)
      })
    )

  val eitherCons = Gen.oneOf(denseCons, sparseCons)
  implicit val eitherArb: Arbitrary[MatrixCons] = Arbitrary(eitherCons)

  def newMatrix(rows: Int, cols: Int)(implicit cons: Arbitrary[MatrixCons]): Matrix = {
    cons.arbitrary.sample.get(rows, cols)
  }

  def fnormIsZero(fn: Double): Boolean =
    fn >= 0.0 && fn <= FNORM_EPS

  def isZero(m: Matrix): Boolean =
    fnormIsZero(Matrix.frobNorm2(m))

  // Expensive, but easy to check
  def row(m: Matrix, row: Int): IndexedSeq[Float] =
    (0 until m.cols).map { c => m(row, c) }

  def col(m: Matrix, col: Int): IndexedSeq[Float] =
    (0 until m.rows).map { r => m(r, col) }

  def dot(v1: IndexedSeq[Float], v2: IndexedSeq[Float]): Float =
    v1.view.zip(v2).map { case (l,r) => l * r }.sum

  def prod(set: Matrix, m1: Matrix, m2: Matrix): Unit =
    (0 until m1.rows).foreach { r =>
      (0 until m2.cols).foreach { c =>
        val thisD = dot(row(m1, r), col(m2, c))
        set.update(r, c, thisD)
      }
    }

  // for nxk matrices (to guarantee dimensions match): A B^T == (B A^T)^T
  def transposeLaw(rows: Int, cols: Int)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    implicit val arb = Arbitrary(denseOrSparse(rows, cols, density))
    forAll { (a: Matrix, b: Matrix) =>
      val c = newMatrix(rows, rows)
      val d = newMatrix(rows, rows)
      val e = newMatrix(rows, rows)
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
  }

  property("TransposeLaw 3x5 into Dense") = transposeLaw(3, 5)
  property("TransposeLaw 10x10") = transposeLaw(10, 10)
  property("TransposeLaw 10x1") = transposeLaw(10, 1)
  property("TransposeLaw 1x10") = transposeLaw(1, 10)
  property("TransposeLaw 1x1") = transposeLaw(1, 1)

  def productLaw(rows: Int, cols: Int)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    implicit val arb = Arbitrary(denseOrSparse(rows, cols, density))

    forAll { (a: Matrix, b: Matrix) =>
      val temp1 = newMatrix(rows, rows)
      val temp2 = newMatrix(rows, rows)
      temp1 := a * b.t
      prod(temp2, a, b.t)
      temp1 -= temp2
      isZero(temp1)
    }
  }

  property("Product 10x100") = productLaw(10, 100)
  property("Product 10x10") = productLaw(10, 10)
  property("Product 10x1") = productLaw(10, 1)
  property("Product 1x1") = productLaw(1, 1)

  // (A + B) + C == A + (B + C)
  def additionLaws(rows: Int, cols: Int)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    implicit val arb = Arbitrary(denseOrSparse(rows, cols, density))

    val eq = Equiv[Matrix].equiv _

    forAll { (a: Matrix, b: Matrix, c: Matrix) =>
      val temp1 = newMatrix(rows, cols)
      val temp2 = newMatrix(rows, cols)
      temp1 := (a + b) + c
      temp2 := a + (b + c)
      eq(temp1, temp2)
    }
  }

  property("Monoid + 10x100") = additionLaws(10, 100)
  property("Monoid + 10x10") = additionLaws(10, 10)
  property("Monoid + 10x1") = additionLaws(10, 1)
  property("Monoid + 1x1") = additionLaws(1, 1)

  // (x * M)(i,j) == x * (M(i,j))
  def scalarLaws(rows: Int, cols: Int)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    implicit val arb = Arbitrary(denseOrSparse(rows, cols, density))
    implicit val smallFloats = Arbitrary(Gen.choose(-100.0f, 100.0f))
    val eq = Equiv[Matrix].equiv _

    forAll { (m: Matrix, f: Float) =>
      val temp = newMatrix(rows, cols)
      val temp2 = newMatrix(rows, cols)
      temp := m // copy
      temp *= f // Update in place
      temp2 := m * f // Don't change m
      eq(temp, temp2) && {
        (0 until rows).forall { r =>
          (0 until cols).forall { c =>
            val diff = scala.math.abs(temp(r, c) - (m(r, c) * f))
            diff < FNORM_EPS
          }
        }
      }
    }
  }

  property("Scalars work 10x100") = scalarLaws(10, 100)
  property("Scalars work 10x10") = scalarLaws(10, 10)
  property("Scalars work 10x1") = scalarLaws(10, 1)
  property("Scalars work 1x1") = scalarLaws(1, 1)

  // Norm laws:
  property("FrobNormLaws for constant DenseMatrix") =
    forAll { (rowsBig: Int, colsBig: Int) =>
      // Don't generate too large of a float:
      val constant = (10.0 * scala.math.random).toFloat
      val rows = (rowsBig % 500) + 501 // Make sure we are between 1 and 1000
      val cols = (colsBig % 500) + 501
      val sz = rows * cols
      val mat = DenseMatrix.zeros(rows, cols)
      mat := constant
      val diff = scala.math.abs(Matrix.frobNorm2(mat)/sz - (constant * constant))
      diff < FNORM_EPS
    }

  property("FrobNormLaws for constant SparseMatrix") =
    forAll { (nonZeros: List[(Int,Int)]) =>
      def pos(x: Int) =
        if (x >= 0) x
        else if (x == Int.MinValue) Int.MaxValue // -min == min
        else -x
      val nonEmpty = ((0,0) :: (nonZeros.map { case (r,c) => (pos(r), pos(c)) })).distinct

      val rows = nonEmpty.view.map { _._1 }.max + 1
      val cols = nonEmpty.view.map { _._2 }.max + 1

      // Don't generate too large of a float:
      val constant = (10.0 * scala.math.random).toFloat
      val sz = nonEmpty.size
      val mat = SparseMatrix.zeros(rows, cols)
      nonEmpty.foreach { case (r,c) => mat.update(r, c, constant) }

      val diff = scala.math.abs(Matrix.frobNorm2(mat)/sz - (constant * constant))
      diff < FNORM_EPS
    }

  // MatrixUpdaters work as expected:
  // "C += A * B is C = C + (A * B)") = forAll (a:
  def incrementProductLaw(rows: Int, cols: Int, inc: Boolean = true)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    implicit val arb = Arbitrary(denseOrSparse(rows, cols, density))

    val eq = Equiv[Matrix].equiv _

    forAll { (a: Matrix, b: Matrix) =>
      val c = newMatrix(rows, rows)
      c := DenseMatrix.rand(rows, rows)
      val c2 = newMatrix(rows, rows)
      val temp = newMatrix(rows, rows)
      temp := a * b.t
      if(inc) {
        c2 := c + temp
        // Different way
        c += a * b.t
      }
      else {
        c2 := c - temp
        // Different way
        c -= a * b.t
      }
      eq(c, c2)
    }
  }
  property("C += A*B is the same as C = C + (A*B)") = incrementProductLaw(2,3)
  property("C += A*B is the same as C = C + (A*B)") = incrementProductLaw(2,30)
  property("C += A*B is the same as C = C + (A*B)") = incrementProductLaw(20,3)
  property("C += A*B is the same as C = C + (A*B)") = incrementProductLaw(1,1)

  property("C -= A*B is the same as C = C - (A*B)") = incrementProductLaw(2,3,false)
  property("C -= A*B is the same as C = C - (A*B)") = incrementProductLaw(2,30,false)
  property("C -= A*B is the same as C = C - (A*B)") = incrementProductLaw(20,3,false)
  property("C -= A*B is the same as C = C - (A*B)") = incrementProductLaw(1,1,false)

  def transposeProductLaw(size: Int)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    val (rows, cols) = (size, size)
    implicit val arb = Arbitrary(denseOrSparse(rows * 2, cols * 2, density))

    val eq = Equiv[Matrix].equiv _

    forAll { mat: Matrix =>
      def fourSplit(m: Matrix) = {
        val m1 = m.blockView(0, 0, rows, cols)
        val m2 = m.blockView(0, cols, rows, 2 * cols)
        val m3 = m.blockView(rows, 0, 2 * rows, cols)
        val m4 = m.blockView(rows, cols, 2 * rows, 2 * cols)
        (m1, m2, m3, m4)
      }

      val res1 = DenseMatrix.zeros(2 * rows, 2 * rows)
      val res2 = DenseMatrix.zeros(2 * rows, 2 * rows)

      res1 := mat * mat.t

      val (a, b, c, d) = fourSplit(mat)
      val (a2, b2, c2, d2) = fourSplit(res2)
      a2 := a * a.t
      a2 += b * b.t
      b2 := a * c.t
      b2 += b * d.t
      c2 := c * a.t
      c2 += d * b.t
      d2 := c * c.t
      d2 += d * d.t

      eq(res1, res2)
    }
  }

  property("Transpose product is block equivalent 10x10") = transposeProductLaw(10)
  property("Transpose product is block equivalent 1x1") = transposeProductLaw(1)

  def sliceProperty(size: Int, numSlices: Int)(implicit cons: Arbitrary[MatrixCons]) = {
    val density = scala.math.random
    val (rows, cols) = (size, size)
    implicit val arb = Arbitrary(denseOrSparse(rows, cols, density))

    val eq = Equiv[Matrix].equiv _

    forAll { mat: Matrix =>
      val zeros = DenseMatrix.zeros(rows, cols)
      val matSlices = mat.rowSlice(numSlices)
      val zerosSlices = zeros.rowSlice(numSlices)

      matSlices.zip(zerosSlices).foreach { case (mSlice, zSlice) =>
        zSlice := mSlice
      }

      eq(mat, zeros)
    }
  }

  property("Set by slice is equivalent to regular set on 10x10, 3 slices") = sliceProperty(10, 3)
  property("Set by slice is equivalent to regular set on 8x8, 4 slices") = sliceProperty(8, 4)
  property("Set by slice is equivalent to regular set on 1x1, 3 slices") = sliceProperty(1, 3)
}
