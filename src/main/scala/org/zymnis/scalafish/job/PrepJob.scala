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

object SparseElement {
  implicit val toTuple: Bijection[SparseElement, (Int, Int, Float)] =
    Bijection.build[SparseElement, (Int, Int, Float)]
    { case SparseElement(row, col, value) => (row, col, value) }
    { case (row, col, value) => SparseElement(row, col, value) }

  implicit val kOrd: Ordering[SparseElement] =
    Ordering.by[SparseElement, (Int, Int, Float)](toTuple)
}

case class SparseElement(row: Int, col: Int, value: Float)

object PrepJob {
  implicit val tripleInjection: Injection[SparseElement, Array[Byte]] =
    SparseElement.toTuple.andThen(Bufferable.injectionOf[(Int, Int, Float)])

  def shardFn(shards: Int) = { elem: SparseElement => List((elem.row % shards).toString) }
  def sink(path: String, shards: Int): Source with Mappable[SparseElement] =
    PailSource.sink(path, shardFn(shards))
}

class PrepJob(args: Args) extends Job(args) {
  import TDsl._

  val matrixInput = args("input")
  val outputPath = args("output")
  val numShards = args("shards").toInt

  val grouped = TypedTsv[(Int, Int, Float)](matrixInput).groupAll

  // SparseElement, (originalRowID, compressedIndex)
  val sortedByRow: TypedPipe[(SparseElement, (Int, Int))] =
    grouped.sortBy(_._1)
        .scanLeft((SparseElement(0, 0, 0), (0, 0))) {
      case ((_, (prev, idx)), (row, col, value)) =>
        val newIdx = if (row != prev) (idx + 1) else idx
        (SparseElement(row, col, value), (row, newIdx))
    }.toTypedPipe.map(_._2)

  // SparseElement, (originalColID, compressedColIndex)
  val sortedByCol: TypedPipe[(SparseElement, (Int, Int))] =
    grouped.sortBy(_._2)
        .scanLeft((SparseElement(0, 0, 0), (0, 0))) {
      case ((_, (prev, idx)), (row, col, value)) =>
        val newIdx = if (col != prev) (idx + 1) else idx
        (SparseElement(row, col, value), (col, newIdx))
    }.toTypedPipe
        .map(_._2)

  // SparseElement,
  // ((originalRowID, compressedRowIndex), (originalColID, compressedColIndex))
  val groupedElements: TypedPipe[(SparseElement, ((Int, Int), (Int, Int)))] =
    sortedByRow.group.withReducers(20)
        .join(sortedByCol.group)
        .toTypedPipe

  // TODO: Unique these somehow.

  // ((compressedRowIndex, compressedColIndex), (originalRowIndex, originalColIndex))
  val rowMapping: TypedPipe[(Int, Int)] =
    groupedElements.map { case (elem, ((row, compressedRow), (col, compressedCol))) =>
      (compressedRow, row)
    }.write(TypedTsv[(Int, Int)](args("row-mapping")))

  val columnMapping: TypedPipe[(Int, Int)] =
    groupedElements.map { case (elem, ((row, compressedRow), (col, compressedCol))) =>
      (compressedCol, col)
    }.write(TypedTsv[(Int, Int)](args("column-mapping")))

  val output =
    groupedElements.map { case (elem, ((_, compressedRow), (_, compressedCol))) =>
      SparseElement(compressedRow, compressedCol, elem.value)
    }.write(PrepJob.sink(outputPath, numShards))

  val tempPath = "/tmp/scalafish/" + UUID.randomUUID()
  val fields: cascading.tuple.Fields = ('maxRow, 'maxCol)
  val tempSource = SequenceFile(tempPath, fields)

  groupedElements.map { case (elem, ((row, compressedRow), (col, compressedCol))) =>
    (compressedRow, compressedCol) }
      .groupAll
      .max
      .map { _._2 }
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
          "row" -> tuple.getInteger("maxRow"),
          "col" -> tuple.getInteger("maxCol"),
          "shards" -> numShards
        )
      }
    iter.close()
    HDFSMetadata.put(conf, outputPath, coords) // place coords in metadata
    val fs = FileSystem.get(conf)
    fs.delete(new Path(tempPath), true) // delete temp files
    None
  }
}
