package org.zymnis.scalafish.job

import com.backtype.hadoop.pail.{PailStructure, Pail}
import com.twitter.scalding._
import com.twitter.scalding.commons.source.VersionedKeyValSource
import commons.source.{CodecPailStructure, PailSource}
import com.twitter.bijection.{Bijection, Bufferable, Injection}
import cascading.flow.hadoop.HadoopFlowProcess
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList,  UUID }
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConverters._
import scala.util.MurmurHash

object PrepJob {
  implicit lazy val setInjection: Injection[Set[Int], Array[Byte]] = Bufferable.injectionOf[Set[Int]]
}

class PrepJob(args: Args) extends Job(args) {
  type Row = Int
  type HashedColumn = Int
  type HashedRow = Int
  type Column = Int
  type Value = Float

  import PrepJob.setInjection
  import TDsl._

  val matrixInput = args("input")
  val columnMod = args("total-columns").toInt
  val rowMod = args("total-rows").toInt
  val outputPath = args("output")
  val mappingPath = args("mapping-path")
  val supervisors = args("supervisors").toInt
  val workers = args("workers").toInt
  val rowShards = supervisors * workers

  lazy val hasher = new MurmurHash[Column](123456789)

  // Row, Column, Value
  val source = TypedTsv[(Row, Column, Value)](matrixInput)
    .map { case (col, row, value) => (row, col, value) } // REMOVE, we only have this for our test matrix.

  def mod(a: Int, modulus: Int): Int = {
    val ret = a % modulus
    if (ret < 0)
      ret + modulus
    else
      ret
  }

  def hashMod(i: Int, modulus: Int) = {
    hasher.reset
    hasher(i)
    mod(hasher.hash, modulus)
  }

  val processed: TypedPipe[(HashedRow, HashedColumn, Row, Column, Value)] =
    source.map { case (row, col, value) =>
        (hashMod(row, rowMod), hashMod(col, columnMod), row, col, value)
    }

  val rowMapping: TypedPipe[(HashedRow, Set[Row])] =
    processed.map { case (hashedRow, _, row, _, _) => (hashedRow, row) }
      .group.withReducers(20).toSet
      .write(VersionedKeyValSource[HashedRow, Set[Row]](mappingPath + "/row"))

  val colMapping: TypedPipe[(HashedColumn, Set[Column])] =
    processed.map { case (_, hashedCol, _, col, _) => (hashedCol, col) }
      .group.withReducers(20).toSet
      .write(VersionedKeyValSource[HashedColumn, Set[Column]](mappingPath + "/col"))

  // Write out the final data to a pail at outputPath.
  val data =
    processed.map { case (hashedRow, hashedCol, row, col, value) =>
        ((hashedRow, hashedCol), value)
    }.group.withReducers(20).sum
      .map { case ((row, col), value) => (row, col, value) }

  for {
    sup <- (0 until supervisors)
    work <- (0 until workers)
  } {
    data
      .filter { _._1 % (supervisors * workers) == sup * work }
      .groupAll
      .values
      .write(TypedTsv[(Int, Int, Float)](outputPath + "/" + sup + "/" + work))
  }

  // Path for writing metadata for transfer.
  val tempPath = "/tmp/scalafish/" + UUID.randomUUID()
  val fields: cascading.tuple.Fields = ('row)
  val tempSource = SequenceFile(tempPath, fields)

  val numRows = source.map { _._1 }.groupAll.max.map { _._2 }
    .toPipe(fields)
    .write(tempSource)

  override def next = {
    val conf = new JobConf
    config.foreach { case (k, v) => conf.set(k.toString, v.toString) }
    val process = new HadoopFlowProcess(conf)
    val iter = process.openTapForRead(tempSource.createTap(Read))
    val coords: Option[Map[String, Int]] =
      iter.asScala.toSeq.headOption.map { tuple =>
        Map(
          "row" -> rowMod,
          "col" -> columnMod,
          "shards" -> rowShards
        )
      }
    iter.close()
    HDFSMetadata.put(conf, outputPath, coords) // place coords in metadata
    val fs = FileSystem.get(conf)
    fs.delete(new Path(tempPath), true) // delete temp files
    None
  }
}
