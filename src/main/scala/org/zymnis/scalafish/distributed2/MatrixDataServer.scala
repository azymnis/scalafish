package org.zymnis.scalafish.distributed2

import org.zymnis.scalafish.matrix.DenseMatrix
import java.nio.channels.{
  ServerSocketChannel => ServChan,
  SocketChannel => SocketChan,
  ClosedByInterruptException,
  ClosedChannelException,
  AsynchronousCloseException
}
import java.nio.ByteBuffer
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

import scala.annotation.tailrec

import java.util.UUID

/**
 * Serves blocks of data
 */
abstract class AbstractMatrixDataServer {
  def port: Int
  def matrices: SharedMemory[(UUID, DenseMatrix)]

  private val processThread = new AtomicReference[(ServChan, java.lang.Thread)]()
  private val running = new AtomicBoolean(false)


  def start: Boolean = {
    if(!running.getAndSet(true)) {
      // time to start
      val serverChan = ServChan.open
      serverChan.socket.bind(new InetSocketAddress(port))
      val thisThread = new java.lang.Thread(new java.lang.Runnable {
        def run = process(serverChan)
      })
      processThread.getAndSet((serverChan, thisThread))
      thisThread.start
      true
    }
    else {
      false
    }
  }
  def stop: Boolean = {
    if(running.getAndSet(false)) {
      // stop the channel
      processThread.getAndSet(null)._1.close
      true
    }
    else false
  }

  @tailrec
  private def process(chan: ServChan): Unit = {
    val req = try {
      chan.accept
    }
    catch {
      case closed@(_:ClosedByInterruptException |
       _: ClosedChannelException |
       _: AsynchronousCloseException) => { stop; return () }
    }
    val uuidReq = readUUID(req)
    matrices.findTake { _._1 == uuidReq }
      .map { case (idx, uuidmat) =>
        try {
          writeMatrix(uuidmat._2, req)
        }
        finally {
          // Put it back
          matrices.put(idx, uuidmat)
        }
      }
      .orElse {
        println("Unknown UUID: req %s".format(uuidReq))
        None
      }
    req.close
    // Loop
    if(running.get) process(chan) else chan.close
  }
  def readUUID(socket: SocketChan): UUID = {
    val uuidBuf = ByteBuffer.allocate(16)
    SocketUtil.readFully(socket, uuidBuf)
    new UUID(uuidBuf.getLong, uuidBuf.getLong)
  }

  def writeMatrix(dm: DenseMatrix, socket: SocketChan): Unit =
    dm.data.foreach { bb => SocketUtil.writeFully(bb, socket) }
}

object SocketUtil {
  @tailrec
  final def readFully(sock: SocketChan, bb: ByteBuffer): Boolean = {
    val dup = bb.duplicate
    val cnt = sock.read(dup)
    if(cnt < 0) false
    else if(dup.remaining > 0) readFully(sock, dup)
    else true
  }
  @tailrec
  final def writeFully(bb: ByteBuffer, sock: SocketChan): Unit = {
    val dup = bb.duplicate
    sock.write(dup)
    if(dup.remaining > 0) writeFully(dup, sock) else ()
  }
}

class MatrixDataServer(override val port: Int, override val matrices: SharedMemory[(UUID, DenseMatrix)])
  extends AbstractMatrixDataServer

object MatrixClient {
  def read(server: InetSocketAddress, matrixId: UUID, into: DenseMatrix): Boolean = {
    val chan = SocketChan.open(server)
    // Write the UUID:
    writeUUID(matrixId, chan)
    val result = readMatrix(chan, into)
    chan.close
    result
  }
  def writeUUID(id: UUID, socket: SocketChan): Unit = {
    val uuidBuf = ByteBuffer.allocate(16)
    val dup = uuidBuf.duplicate
    dup.putLong(id.getMostSignificantBits)
    dup.putLong(id.getLeastSignificantBits)
    SocketUtil.writeFully(uuidBuf, socket)
  }
  def readMatrix(socket: SocketChan, dm: DenseMatrix): Boolean =
    dm.data.forall { bb => SocketUtil.readFully(socket, bb) }
}

/*
import org.zymnis.scalafish.distributed2._
import org.zymnis.scalafish.matrix._

def genMat = (java.util.UUID.randomUUID, DenseMatrix.rand(10,10)(new java.util.Random))

val shm = new SharedMemory[(java.util.UUID, DenseMatrix)](4)
shm.put(0, genMat)
shm.put(1, genMat)
val port = 10003
val srv = new MatrixDataServer(port, shm)
srv.start
val uuidmat = shm.effect(0) { uuid => println(uuid); (uuid, uuid) }
val dst = DenseMatrix.zeros(10,10)
MatrixClient.read(new java.net.InetSocketAddress("127.0.0.1", port), uuidmat.get._1, dst)
srv.stop
*/
