package org.zymnis.scalafish.serialization

import akka.actor.{ExtendedActorSystem, ActorRef}
import akka.serialization.Serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.objenesis.strategy.StdInstantiatorStrategy

import com.twitter.chill.{
  KryoSerializer => ChillSerializers,
  KryoBase => ChillKryo,
  KryoImplicits
}

import org.zymnis.scalafish.matrix._

import KryoImplicits._

/**
 * By default, needs a constructor that takes exactly one actor system
 * Kryo is not thread-safe so we use an object pool to avoid over allocating
 */
class KryoAkkaPooled(system: ExtendedActorSystem) extends Serializer {

  val MAX_KRYOS = 32
  val DEFAULT_BUFFER = 128 * 1024
  val MAX_BUFFER = Int.MaxValue // akka will stop before this
  val REGISTRATION_REQUIRED = false // Try to serialize anything

  class KryoBuffer(k: Kryo) {
    val out = new Output(DEFAULT_BUFFER, MAX_BUFFER)
    def serialize(obj: AnyRef): Array[Byte] = {
      k.writeClassAndObject(out, obj)
      val bytes = out.toBytes
      out.clear
      bytes
    }
    def deserialize(bytes: Array[Byte]): AnyRef =
      k.readClassAndObject(new Input(bytes)).asInstanceOf[AnyRef]
  }

  private val pool = ObjectPool[KryoBuffer](MAX_KRYOS) {
    val k = new ChillKryo
    ChillSerializers.registerAll(k)
    // Add ActorRef serialization:
    k.setRegistrationRequired(REGISTRATION_REQUIRED)
    k.setInstantiatorStrategy(new StdInstantiatorStrategy)
    k.forSubclass[ActorRef](new ActorRefSerializer(system))
    k.forSubclass[Matrix](new MatrixSerializer)
    k.registerClasses(Seq(classOf[DenseMatrix], classOf[Matrix], classOf[SparseMatrix], classOf[RowMajorMatrix]))
    new KryoBuffer(k)
  }

  def includeManifest: Boolean = false
  def identifier = 8675309
  def toBinary(obj: AnyRef): Array[Byte] = {
    val ser = pool.fetch
    val bytes = ser.serialize(obj)
    pool.release(ser)
    bytes
  }
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val ser = pool.fetch
    val obj = ser.deserialize(bytes)
    pool.release(ser)
    obj
  }
}

/*
// test

import org.zymnis.scalafish.serialization.KryoAkkaPooled
import org.zymnis.scalafish.matrix._

val kryo = new KryoAkkaPooled(null)

val sp = kryo.toBinary(SparseMatrix.zeros(10,2))
kryo.fromBinary(sp, None)

import  org.zymnis.scalafish.distributed2._
Load(SupervisorId(1), new TestLoader)
*/
