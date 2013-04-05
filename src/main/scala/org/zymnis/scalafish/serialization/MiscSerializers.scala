package org.zymnis.scalafish.serialization

import org.zymnis.scalafish.matrix.{RowMajorMatrix, Matrix, SparseMatrix}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

import java.net.InetSocketAddress
import java.util.UUID

class InetSocketAddressSerializer extends Serializer[InetSocketAddress] {
  def read(kryo: Kryo, input: Input, typ: Class[InetSocketAddress]): InetSocketAddress = {
    val host = input.readString
    val port = input.readInt(true)
    new InetSocketAddress(host, port)
  }

  def write(kryo: Kryo, out: Output, obj: InetSocketAddress) {
    out.writeString(obj.getHostName)
    out.writeInt(obj.getPort, true)
  }
}

class UUIDSerializer extends Serializer[UUID] {
  def write(k: Kryo, output: Output, uuid: UUID) {
    output.writeLong(uuid.getMostSignificantBits(), false)
    output.writeLong(uuid.getLeastSignificantBits(), false)
  }

  def read(kryo: Kryo, input: Input, cl: Class[UUID]): UUID =
    new UUID(input.readLong(false), input.readLong(false))
}
