package org.zymnis.scalafish.matrix

trait MatrixUpdater {
  def update(m: Matrix): Unit
}

object MatrixUpdater {
  def product(m1: Matrix, m2: Matrix): MatrixUpdater = new MatrixUpdater {
    // TODO perhaps optimize this for cache locality, assume m1 is rowMajor for now
    def update(result: Matrix) = {
      require(result.rows == m1.rows, "Rows do not match")
      require(result.cols == m2.cols, "Cols do not match")
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
          result.update(idxI, idxJ, sum.toFloat)
          idxJ += 1
        }
        idxI += 1
      }
    }
  }

  def plus(m: Matrix): MatrixUpdater = new MatrixUpdater {
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

  def sum(m1: Matrix, m2: Matrix): MatrixUpdater = new MatrixUpdater {
    // TODO perhaps optimize this for cache locality, assume m1 is rowMajor for now
    def update(result: Matrix) = {
      require(m1.rows == m2.rows, "Rows do not match")
      require(m1.cols == m2.cols, "Cols do not match")
      require(result.rows == m2.rows, "Result rows do not match")
      require(result.cols == m2.cols, "Result rows do not match")

      var idxI = 0
      var idxJ = 0
      while(idxI < result.rows) {
        idxJ = 0
        while(idxJ < result.cols) {
          result.update(idxI, idxJ, m1(idxI, idxJ) + m2(idxI, idxJ))
          idxJ += 1
        }
        idxI += 1
      }
    }
  }

  def diff(m1: Matrix, m2: Matrix): MatrixUpdater = new MatrixUpdater {
    // TODO perhaps optimize this for cache locality, assume m1 is rowMajor for now
    def update(result: Matrix) = {
      require(m1.rows == m2.rows, "Rows do not match")
      require(m1.cols == m2.cols, "Cols do not match")
      require(result.rows == m2.rows, "Result rows do not match")
      require(result.cols == m2.cols, "Result rows do not match")
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

  // Macros would be huge here:
  def scale(scalar: Float): MatrixUpdater = new MatrixUpdater {
    def update(m: Matrix) {
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