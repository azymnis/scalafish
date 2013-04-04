package org.zymnis.scalafish.serialization

/*******************************************************************************
 * Copyright 2012 Roman Levenstein
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit

object ObjectPool {
  def apply[T](number: Int)(byName : => T): ObjectPool[T] = new ObjectPool[T](number) {
    def newInstance = byName
  }
}

// Support pooling of objects. Useful if you want to reduce
// the GC overhead and memory pressure.
abstract class ObjectPool[T](number: Int) {
  def newInstance: T

  private val size = new AtomicInteger(0)
  private val pool = new ArrayBlockingQueue[T](number)

  def fetch(): T = {
    val item = pool.poll
    if(item == null) createOrBlock else item
  }

  def release(o: T): Unit = {
    pool.offer(o)
  }

  def add(o: T):Unit ={
    pool.add(o)
  }

  private def createOrBlock: T = {
    size.get match {
      case e: Int if e == number => block
      case _ => create
    }
  }

  private def create: T = {
    size.incrementAndGet match {
      case e: Int if e > number => size.decrementAndGet; fetch()
      case e: Int => newInstance
    }
  }

  private def block: T = {
    val timeout = 5000
    pool.poll(timeout, TimeUnit.MILLISECONDS) match {
      case o: T => o
      case _ => throw new Exception("Couldn't acquire object in %d milliseconds.".format(timeout))
    }
  }
}
