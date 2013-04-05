package org.zymnis.scalafish.distributed2

import java.util.concurrent.atomic.AtomicReferenceArray

import scala.annotation.tailrec

object SharedMemory {
  def apply[A <: AnyRef](items: Iterable[A]): SharedMemory[A] = {
    val sm = new SharedMemory[A](items.size)
    val success = items.view.zipWithIndex.forall { case (it, idx) => sm.put(idx, it) }
    assert(success, "Could not initialize")
    sm
  }
}

class SharedMemory[A <: AnyRef](val size: Int) {
  private val data = new AtomicReferenceArray[A](size)

  def take(idx: Int): Option[A] = Option(data.getAndSet(idx, null.asInstanceOf[A]))
  def get(idx: Int): Option[A] = Option(data.get(idx))
  def swap(idx: Int, item: A): Option[A] =
    Option(data.getAndSet(idx, item))

  def findTake(fn: A => Boolean): Option[(Int, A)] = {
    @tailrec
    def findRec(idx: Int = 0): Option[(Int, A)] = {
      if(idx == size) None
      else {
        val a = data.get(idx)
        if(a != null && fn(a)) {
          take(idx) match {
            case Some(taken) => Some((idx, taken))
            case None => findRec(idx + 1)
          }
        }
        else {
          findRec(idx + 1)
        }
      }
    }
    findRec()
  }

  def effect[S](idx: Int)(fn: (A) => (S,A)): Option[S] = {
    take(idx).map { a =>
      val (s, newA) = fn(a);
      put(idx, newA)
      s
    }
  }

  def put(idx: Int, item: A): Boolean =
    data.compareAndSet(idx, null.asInstanceOf[A], item)
}
