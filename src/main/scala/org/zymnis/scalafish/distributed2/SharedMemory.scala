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

  def effect[S](idx: Int)(fn: (A) => (S,A)): Option[S] = {
    take(idx).map { a =>
      val (s, newA) = fn(a)
      // Must succeed
      put(idx, newA)
      s
    }
  }

  def put(idx: Int, item: A): Boolean =
    data.compareAndSet(idx, null.asInstanceOf[A], item)
}
