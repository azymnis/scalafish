package org.zymnis.scalafish.matrix

class MatrixOps(mat: Matrix) {
  import MatrixUpdater._
  // Not safe to do L *= L, or any derived matrix
  // TODO: should have some checks on that
  def *(that: Matrix): MatrixUpdater = product(mat, that)

  def +(that: Matrix): SumMatrixUpdater =
    new SumMatrixUpdater(Vector(mat, that))

  def +(that: MatrixUpdater): MatrixUpdater = new MatrixUpdater {
    def update(newM: Matrix) {
      that.update(newM)
      // Now do the plus:
      plus(mat).update(newM)
    }
  }

  def +=(that: Matrix): Matrix = mat := plus(that)
  def +=(that: MatrixUpdater): Matrix = {
    //mat = mat + that
    val newUpdater = this + that
    newUpdater.update(mat)
    mat
  }

  def -(that: Matrix): MatrixUpdater = diff(mat, that)
  def -=(that: Matrix): Matrix = mat := diff(mat, that)

  def *(that: Float): MatrixUpdater = scaled(mat, that)
  def *=(that: Float): Matrix = mat := scale(that)
  def *:(that: Float): MatrixUpdater = scaled(mat, that)
}

object Syntax {
  implicit def toOps(mat: Matrix): MatrixOps = new MatrixOps(mat)
  implicit def setter(m: Matrix): MatrixUpdater = new MatrixUpdater {
    def update(that: Matrix) = {
      require(m.rows == that.rows, "Rows do not match")
      require(m.cols == that.cols, "Cols do not match")
      var idxI = 0
      var idxJ = 0
      while(idxI < m.rows) {
        idxJ = 0
        while(idxJ < m.cols) {
          that.update(idxI, idxJ, m(idxI, idxJ))
          idxJ += 1
        }
        idxI += 1
      }
    }
  }
  implicit def setter(f: Float): MatrixUpdater = new MatrixUpdater {
    def update(m: Matrix) = {
      m match {
        case sm: SparseMatrix => throw new Exception("Cannot set sparse matrix to constant")
        case dm: DenseMatrix => dm.fill(f)
      }
    }
  }
}

