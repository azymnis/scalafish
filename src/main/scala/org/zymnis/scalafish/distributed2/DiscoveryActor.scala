package org.zymnis.scalafish.distributed2

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.{Duration, Timeout}
import akka.util.duration._

import org.zymnis.scalafish.zookeeper.Client

import java.util.UUID

sealed trait DiscoveryMessage extends Message
case class UseZookeeper(host: String, port: Int) extends DiscoveryMessage
case class FindSupervisors(req: UUID, count: Int) extends DiscoveryMessage
case class AnnounceSupervisors(req: UUID, found: Set[ActorRef]) extends DiscoveryMessage

sealed trait DiscoveryState

case object Born extends DiscoveryState {
  def initalize(zk: UseZookeeper, system: ActorSystem): DiscoInit =
    DiscoInit(system.actorOf(Props(new Client(zk.host, zk.port))), Map.empty)
}

case class DiscoInit(zk: ActorRef, reqs: Map[FindSupervisors, Set[ActorRef]]) extends DiscoveryState {
  def find(find: FindSupervisors): DiscoInit = {
    null
  }
}

class DiscoveryActor extends Actor {
  def receive = {
    case _ => ()
  }
}
