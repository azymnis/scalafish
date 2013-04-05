package org.zymnis.scalafish.job

import com.backtype.cascading.tap.PailTap
import com.backtype.hadoop.pail.{Pail, PailStructure}
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tuple.Fields
import cascading.tap.{ Tap, SinkMode }
import cascading.tap.hadoop.{ TemplateTap, Hfs }
import com.twitter.bijection.Injection
import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import java.util.{ List => JList }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import scala.collection.JavaConverters._

class TemplateSource(parent: FileSource, sinkTemplate: String) extends FileSource {
  override def localPath = parent.localPath
  override def hdfsPaths = parent.hdfsPaths
  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    mode match {
      // TODO support strict in Local
      case Local(_) => super.createTap(readOrWrite)
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => createHdfsReadTap(hdfsMode)
        case Write =>  {
          val parentTap = parent.createTap(readOrWrite).asInstanceOf[Hfs]
          println("This is the path that's fucking us: " + parentTap.getPath)
          new TemplateTap(parentTap, sinkTemplate).asInstanceOf[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]]
        }
      }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }
}
