package org.zymnis.scalafish.job

import com.backtype.hadoop.pail.{PailStructure, Pail}
import com.twitter.scalding._
import commons.source.{CodecPailStructure, PailSource}
import com.twitter.bijection.{Bijection, Bufferable, Injection}
import cascading.flow.hadoop.HadoopFlowProcess
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList,  UUID }
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConverters._

class ScalafishPailStructure extends PailStructure[SparseElement] {
  val injection: Injection[SparseElement, Array[Byte]] =
    SparseElement.toTuple.andThen(Bufferable.injectionOf[(Int, Int, Float)])

  var shards: Int = 10

  def setShards(i: Int) = { this.shards = i; this }

  override def isValidTarget(paths: String*): Boolean = paths.length >= 1
  override def getTarget(obj: SparseElement): JList[String] =
    List((obj.row % shards).toString).asJava
  override def serialize(obj: SparseElement): Array[Byte] = injection.apply(obj)
  override def deserialize(bytes: Array[Byte]): SparseElement = injection.invert(bytes).get
  override val getType = classOf[SparseElement]
}
