package org.zymnis.scalafish.matrix

trait Shaped {
  def rows: Int
  def cols: Int
}

trait MatrixUpdater {
  def update(m: Matrix): Unit
}

trait ShapedUpdater extends MatrixUpdater with Shaped

object MatrixUpdater {
  def plus(m: Matrix): ShapedUpdater = new ShapedUpdater {
    def rows = m.rows
    def cols = m.cols
    def update(that: Matrix) = {
      require(m.rows == that.rows, "Rows do not match")
      require(m.cols == that.cols, "Cols do not match")
      var idxI = 0
      var idxJ = 0
      while(idxI < m.rows) {
        idxJ = 0
        while(idxJ < m.cols) {
          that.update(idxI, idxJ, m(idxI, idxJ) + that(idxI, idxJ))
          idxJ += 1
        }
        idxI += 1
      }
    }
  }

  def diff(m1: Matrix, m2: Matrix): ShapedUpdater = new ShapedUpdater {
    def rows = m1.rows
    def cols = m1.cols
    // TODO perhaps optimize this for cache locality, assume m1 is rowMajor for now
    def update(result: Matrix) = {
      require(m1.rows == m2.rows, "Rows do not match")
      require(m1.cols == m2.cols, "Cols do not match")

      require(result.rows == m2.rows, "Result rows do not match: " +
        (result.rows, m2.rows))
      require(result.cols == m2.cols, "Result cols do not match" +
        (result.cols, m2.cols))
      var idxI = 0
      var idxJ = 0
      while(idxI < result.rows) {
        idxJ = 0
        while(idxJ < result.cols) {
          result.update(idxI, idxJ, m1(idxI, idxJ) - m2(idxI, idxJ))
          idxJ += 1
        }
        idxI += 1
      }
    }
  }

  def rand: MatrixUpdater = new MatrixUpdater {
    def update(m: Matrix) {
      val cols = m.cols
      val rows = m.rows
      var rowIdx = 0
      var colIdx = 0
      val rng = new java.util.Random
      while(colIdx < cols) {
        rowIdx = 0
        while(rowIdx < rows) {
          m.update(rowIdx, colIdx, rng.nextFloat)
          rowIdx += 1
        }
        colIdx += 1
      }
    }
  }

  // Macros would be huge here:
  def scale(scalar: Float): MatrixUpdater = new MatrixUpdater {
    def update(m: Matrix) {
      m match {
        case dm: DenseMatrix => denseUpdate(dm)
        case sm: SparseMatrix => sparseUpdate(sm)
      }
    }
    def sparseUpdate(s: SparseMatrix) {
      val iter = s.denseIndices
      while(iter.hasNext) {
        val idx = iter.next
        s.update(idx, s(idx) * scalar)
      }
    }
    def denseUpdate(m: DenseMatrix) {
      var rowIdx = 0
      while(rowIdx < m.rows) {
        var colIdx = 0
        while(colIdx < m.cols) {
          val old = m(rowIdx, colIdx)
          m.update(rowIdx, colIdx, old * scalar)
          colIdx += 1
        }
        rowIdx += 1
      }
    }
  }

  // Macros would be huge here:
  def scaled(oldM: Matrix, scalar: Float): ShapedUpdater = new ShapedUpdater {
    def rows = oldM.rows
    def cols = oldM.cols

    def update(m: Matrix) {
      var rowIdx = 0
      while(rowIdx < m.rows) {
        var colIdx = 0
        while(colIdx < m.cols) {
          val old = oldM(rowIdx, colIdx) * scalar
          m.update(rowIdx, colIdx, old)
          colIdx += 1
        }
        rowIdx += 1
      }
    }
  }

  // Macros would be huge here:
  val negate: MatrixUpdater = new MatrixUpdater {
    def update(m: Matrix) {
      var rowIdx = 0
      while(rowIdx < m.rows) {
        var colIdx = 0
        while(colIdx < m.cols) {
          val old = m(rowIdx, colIdx)
          m.update(rowIdx, colIdx, -old)
          colIdx += 1
        }
        rowIdx += 1
      }
    }
  }
}

class SumMatrixUpdater(ms: IndexedSeq[Matrix]) extends ShapedUpdater {
  def rows = ms(0).rows
  def cols = ms(0).cols
  // TODO perhaps optimize this for cache locality, assume m1 is rowMajor for now
  def update(result: Matrix) = {
    val rows = ms.view.map { _.rows }.reduce { (l,r) =>
      require(l == r, "Rows do not match")
      l
    }
    val cols = ms.view.map { _.cols }.reduce { (l,r) =>
      require(l == r, "Cols do not match")
      l
    }

    require(result.rows == rows, "Result rows do not match: " +
      (result.rows, rows))
    require(result.cols == cols, "Result cols do not match" +
      (result.cols, cols))

    var idxI = 0
    var idxJ = 0
    val matrices = ms.size

    while(idxI < rows) {
      idxJ = 0
      while(idxJ < cols) {
        var midx = 0
        var sum = 0.0f
        while(midx < matrices) {
          sum += ms(midx)(idxI, idxJ)
          midx += 1
        }
        result.update(idxI, idxJ, sum)
        idxJ += 1
      }
      idxI += 1
    }
  }

  def +(that: Matrix): SumMatrixUpdater =
    new SumMatrixUpdater(ms :+ that)
}

// Sets result = coeff * result + pcoeff * (m1 * m2)
case class ProductUpdater(m1: Matrix, m2: Matrix, coeff: Double = 0.0, pcoeff: Double = 1.0)
  extends ShapedUpdater {
    def rows = m1.rows
    def cols = m2.cols
    // TODO perhaps optimize this for cache locality, assume m1 is rowMajor for now
    def update(result: Matrix) = {
      require(result.rows == m1.rows, "Rows do not match: %d, %d".format(result.rows, m1.rows))
      require(result.cols == m2.cols, "Cols do not match: %d, %d".format(result.cols, m2.cols))
      require(m1.cols == m2.rows, "Inner dimension mismatch")
      // R_{ij} = \sum_k A_ik B_kj
      var idxI = 0
      var idxJ = 0
      var idxK = 0
      while(idxI < result.rows) {
        idxJ = 0
        while(idxJ < result.cols) {
          idxK = 0
          var sum = 0.0
          while(idxK < m1.cols) {
            sum += m1(idxI, idxK) * m2(idxK, idxJ)
            idxK += 1
          }
          val newV = (coeff * result(idxI, idxJ) + pcoeff * sum).toFloat
          result.update(idxI, idxJ, newV)
          idxJ += 1
        }
        idxI += 1
      }
    }
  }

