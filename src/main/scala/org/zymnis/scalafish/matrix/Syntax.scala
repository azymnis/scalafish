package org.zymnis.scalafish.matrix

class MatrixOps(mat: Matrix) {
  import MatrixUpdater._
  // Not safe to do L *= L, or any derived matrix
  // TODO: should have some checks on that
  def *(that: Matrix): MatrixUpdater = product(mat, that)

  def +(that: Matrix): MatrixUpdater = sum(mat, that)
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

  def *(that: Float): MatrixUpdater = scale(that)
  def *=(that: Float): Matrix = mat := scale(that)
  def *:(that: Float): MatrixUpdater = scale(that)
}

object Syntax {
  implicit def toOps(mat: Matrix): MatrixOps = new MatrixOps(mat)
  implicit def setter(f: Float): MatrixUpdater = new MatrixUpdater {
    def update(m: Matrix) = {
      m match {
        case sm: SparseMatrix => throw new Exception("Cannot set sparse matrix to constant")
        case dm: DenseMatrix => dm.fill(f)
      }
    }
  }
}

