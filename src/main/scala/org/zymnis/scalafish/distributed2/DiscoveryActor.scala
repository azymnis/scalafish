package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.{Duration, Timeout}
import akka.util.duration._
import akka.actor.ReceiveTimeout

import org.json.simple.{ JSONObject, JSONValue }

import org.zymnis.scalafish.zookeeper._

import java.lang.{ Long => JLong }

sealed trait DiscoveryMessage extends Message
case class UseZookeeper(host: String, port: Int, path: String, count: Int) extends DiscoveryMessage
case object FindSupervisors extends DiscoveryMessage
case class ReceiveChildren(children: List[String]) extends DiscoveryMessage
case class ReceiveData(data: String) extends DiscoveryMessage

case class AnnounceSupervisors(found: List[HostPorts]) extends Message

case class HostPorts(shard: Int, host: String, akkaPort: Int, matrixPort: Int)

sealed trait DiscoveryState { self =>
  def handle(msg: DiscoveryMessage, ref: ActorRef, system: ActorContext): DiscoveryState = {
    msg match {
      case UseZookeeper(host, port, path, count) => self match {
        case bself: Born =>
          DiscoInit(bself.caller,
            system.actorOf(Props(new Client(host, port))), ref, path, Nil, count)
        case _ => println("Unexpected message: " + msg + " in state: " + self); self
      }
      case FindSupervisors => self match {
        case dself: DiscoInit => dself.getChildren
        case _ => println("Unexpected message: " + msg + " in state: " + self); self
      }
      case ReceiveChildren(children) => self match {
        case dself: DiscoInit => dself.receiveChildren(children)
        case _ => println("Unexpected message: " + msg + " in state: " + self); self
      }
      case ReceiveData(data) => self match {
        case dself: DiscoInit => dself.receiveData(data)
        case _ => println("Unexpected message: " + msg + " in state: " + self); self
      }
    }
  }

}

case class Born(caller: ActorRef) extends DiscoveryState

case class DiscoInit(caller: ActorRef, zk: ActorRef, master: ActorRef, path: String, refs: List[HostPorts], count: Int) extends DiscoveryState {
  implicit val ref = caller

  def getChildren: DiscoveryState = {
    announceIfDone { _ =>
      zk ! GetChildren(Path(path))
      this
    }
  }

  def receiveChildren(children: List[String]): DiscoveryState = {
    announceIfDone { _ =>
      if(children.size == count) {
        children.foreach { c =>
          zk ! GetData(Path(path + "/" + c))
        }
      }
      this
    }
  }

  def receiveData(data: String): DiscoveryState = {
    println(data)
    val jobj = JSONValue.parse(data).asInstanceOf[JSONObject]
    val obj = jobj.get("additionalEndpoints").asInstanceOf[JSONObject]
    val host = obj.get("akka").asInstanceOf[JSONObject].get("host").asInstanceOf[String]
    val akkaPort = obj.get("akka").asInstanceOf[JSONObject].get("port").asInstanceOf[JLong].toInt
    val matrixPort = obj.get("matrix").asInstanceOf[JSONObject].get("port").asInstanceOf[JLong].toInt
    val shard = jobj.get("shard").asInstanceOf[JLong].toInt
    val hp = HostPorts(shard, host, akkaPort, matrixPort)
    val out = copy(refs = hp :: refs)
    out.announceIfDone { _ => out }
  }

  def announceIfDone[T <: DiscoveryState](f: Unit => T) = {
    if (refs.size == count) {
      println("announcing done")
      master ! AnnounceSupervisors(refs)
      zk ! Close
      Done
    } else {
      f.apply()
    }
  }

}

case object Done extends DiscoveryState

class DiscoveryActor extends Actor {
  context.setReceiveTimeout(2 seconds)

  var state: DiscoveryState = Born(self)

  def receive = {
    case msg: DiscoveryMessage => state = state.handle(msg, sender, context)
    case GetChildrenResult(_, children, _) => {
      self ! ReceiveChildren(children.map { _.get })
    }
    case gr @ GetResult(_, data, _) => {
      self ! ReceiveData(new String(data))
    }
    case ReceiveTimeout => self ! FindSupervisors
  }
}

object DiscoverTest extends App {
  val system = ActorSystem("Disco")
  val client = system.actorOf(Props(new DiscoTestActor()))
  client ! UseZookeeper(args(0), args(1).toInt, args(2), args(3).toInt)
  client ! FindSupervisors
}

class DiscoTestActor extends Actor {
  def receive = {
    case msg @ UseZookeeper(host, port, path, count) => {
      val da = context.actorOf(Props(new DiscoveryActor()))
      da ! msg
      da ! FindSupervisors
    }
    case AnnounceSupervisors(found) => {
      println(found)
      context.stop(self)
      context.system.shutdown()
    }
  }
}
