package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.{Duration, Timeout}
import akka.util.duration._
import akka.actor.ReceiveTimeout

import org.zymnis.scalafish.zookeeper._

sealed trait DiscoveryMessage extends Message
case class UseZookeeper(host: String, port: Int, path: String, count: Int) extends DiscoveryMessage
case object FindSupervisors extends DiscoveryMessage
case class ReceiveChildren(children: List[String]) extends DiscoveryMessage
case class ReceiveData(data: String) extends DiscoveryMessage

case class AnnounceSupervisors(found: List[String]) extends Message

sealed trait DiscoveryState { self =>
  def handle(msg: DiscoveryMessage, ref: ActorRef, system: ActorContext): DiscoveryState = {
    msg match {
      case UseZookeeper(host, port, path, count) =>
        DiscoInit(self.asInstanceOf[Born].caller, system.actorOf(Props(new Client(host, port))), ref, path, Nil, count)
      case FindSupervisors => self.asInstanceOf[DiscoInit].getChildren
      case ReceiveChildren(children) => self.asInstanceOf[DiscoInit].receiveChildren(children)
      case ReceiveData(data) => self.asInstanceOf[DiscoInit].receiveData(data)
    }
  }

}

case class Born(caller: ActorRef) extends DiscoveryState

case class DiscoInit(caller: ActorRef, zk: ActorRef, master: ActorRef, path: String, refs: List[String], count: Int) extends DiscoveryState {
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
    val out = copy(refs = data :: refs)
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
  client ! UseZookeeper("127.0.0.1", 2181, "/newTest", 3)
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
