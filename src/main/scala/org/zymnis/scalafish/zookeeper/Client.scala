package org.zymnis.scalafish.zookeeper

import akka.actor._
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import org.apache.zookeeper.{
  ZooKeeper,
  Watcher,
  WatchedEvent,
  AsyncCallback,
  CreateMode => ZKCreateMode,
  ZooDefs
}

import org.apache.zookeeper.KeeperException.Code

import org.apache.zookeeper.data.Stat

import java.{util => ju}
import scala.collection.JavaConverters._

case class Path(get: String)

sealed trait CreateMode
case object Ephemeral extends CreateMode
case object EphemeralSeq extends CreateMode
case object Persistent extends CreateMode
case object PersistentSeq extends CreateMode

object CreateMode {
  def apply(cm: CreateMode): ZKCreateMode = {
    cm match {
      case Ephemeral => ZKCreateMode.EPHEMERAL
      case EphemeralSeq => ZKCreateMode.EPHEMERAL_SEQUENTIAL
      case Persistent => ZKCreateMode.PERSISTENT
      case PersistentSeq => ZKCreateMode.PERSISTENT_SEQUENTIAL
    }
  }
}

sealed trait Command
case class Exists(path: Path) extends Command
case class CreateOpen(path: Path, data: Array[Byte], mode: CreateMode) extends Command
case class Delete(path: Path, version: Int = -1) extends Command
case class GetData(path: Path) extends Command
case class SetData(path: Path, data: Array[Byte], version: Int = -1) extends Command
case class GetChildren(path: Path) extends Command
case object Close extends Command

sealed trait Result
case class CreateResult(requestedPath: Path, actualPath: Path) extends Result
case class DeleteResult(path: Path) extends Result
case class StatResult(path: Path, stat: Stat) extends Result
case class SetDataResult(path: Path, stat: Stat) extends Result
case class GetResult(path: Path, data: Array[Byte], stat: Stat) extends Result
case class GetChildrenResult(path: Path, children: List[Path], stat: Stat) extends Result
case class Error(reasonCode: Int, request: Command) extends Result

class TestClient(client: ActorRef) extends Actor {

  def receive = {
    case r: Result => println(r)
    case x: Command =>
      println("command: " + x.toString)
      client ! x
  }
}

/*

import org.zymnis.scalafish.zookeeper._

import akka.actor._

val system = ActorSystem("ZK")
val client = system.actorOf(Props(new Client("127.0.0.1:10000", 5000)))
val test = system.actorOf(Props(new TestClient(client)))

test ! GetData(Path("/twitter/service/adey/staging/adreview_server/member_0000000088"))

*/
class Client(connect: String, timeout: Int) extends Actor {

  val zk = new ZooKeeper(connect, timeout, nullWatcher)

  val nullWatcher = new Watcher {
    def process(ev: WatchedEvent) { }
  }

  def receive = {
    case cr@CreateOpen(path, data, mode) =>
      val cm = CreateMode(mode)
      val acl = ZooDefs.Ids.OPEN_ACL_UNSAFE
      zk.create(path.get, data, acl, cm, new AsyncCallback.StringCallback {
        def processResult(reasonCode: Int, p: String, ctx: Object, name: String) {
          val safeSender = ctx.asInstanceOf[ActorRef]
          Code.get(reasonCode) match {
            case Code.OK =>
              println("Ok")
              safeSender ! CreateResult(path, Path(name))
            case _ =>
              println("Other: " + reasonCode)
              safeSender ! Error(reasonCode, cr)
          }
        }
      }, sender)

    case del@Delete(path, ver) =>
      zk.delete(path.get, ver, new AsyncCallback.VoidCallback {
        def processResult(reasonCode: Int, p: String, ctx: Object) {
          val safeSender = ctx.asInstanceOf[ActorRef]
          Code.get(reasonCode) match {
            case Code.OK =>
              println("Ok")
              safeSender ! DeleteResult(path)
            case _ =>
              println("Other: " + reasonCode)
              safeSender ! Error(reasonCode, del)
          }
        }
      }, sender)

    case ex@Exists(path) =>
      zk.exists(path.get, false, new AsyncCallback.StatCallback {
        def processResult(reasonCode: Int, p: String, ctx: Object, stat: Stat) {
          println(path + Code.get(reasonCode).toString + stat)
          val safeSender = ctx.asInstanceOf[ActorRef]
          Code.get(reasonCode) match {
            case Code.OK =>
              println("Ok")
              safeSender ! StatResult(path, stat)
            case _ =>
              println("Other: " + reasonCode)
              safeSender ! Error(reasonCode, ex)
          }
        }
      }, sender)

    case gd@GetData(p) =>
      println(sender)
      zk.getData(p.get, false, new AsyncCallback.DataCallback {
        def processResult(reasonCode: Int, path: String, ctx: Object, data: Array[Byte], stat: Stat) {
          println(path + Code.get(reasonCode).toString + stat)
          val safeSender = ctx.asInstanceOf[ActorRef]
          Code.get(reasonCode) match {
            case Code.OK => safeSender ! GetResult(p, data, stat)
            case _ => safeSender ! Error(reasonCode, gd)
          }
        }
      }, sender)

    case sd@SetData(p, d, version) =>
      zk.setData(p.get, d, version, new AsyncCallback.StatCallback {
        def processResult(reasonCode: Int, path: String, ctx: Object, stat: Stat) {
          val safeSender = ctx.asInstanceOf[ActorRef]
          println(path + Code.get(reasonCode).toString + stat)
          Code.get(reasonCode) match {
            case Code.OK => safeSender ! SetDataResult(p, stat)
            case _ => safeSender ! Error(reasonCode, sd)
          }
        }
      }, sender)

    case gc@GetChildren(p) =>
      zk.getChildren(p.get, false, new AsyncCallback.Children2Callback {
        def processResult(reasonCode: Int, path: String, ctx: Object, children: ju.List[String], stat: Stat) {
          val safeSender = ctx.asInstanceOf[ActorRef]
          println(path + Code.get(reasonCode).toString + stat)
          Code.get(reasonCode) match {
            case Code.OK => {
              println(safeSender)
              safeSender ! GetChildrenResult(p, children.asScala.map { Path(_) }.toList, stat)
            }
            case _ => {
              safeSender ! Error(reasonCode, gc)
            }
          }
        }
      }, sender)

    case Close => zk.close
  }
}
