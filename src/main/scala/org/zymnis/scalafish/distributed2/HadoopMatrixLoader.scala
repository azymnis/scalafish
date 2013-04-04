/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.zymnis.scalafish.distributed2

import com.backtype.hadoop.pail.{ PailStructure, Pail }
import com.twitter.bijection.{AbstractInjection, Bijection, Bufferable, Injection}
import com.twitter.chill.InjectiveSerializer
import com.esotericsoftware.kryo.{ Serializer => KSerializer }
import cascading.flow.hadoop.HadoopFlowProcess
import org.apache.hadoop.mapred.JobConf
import java.util.{ List => JList,  UUID }
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConverters._
import org.zymnis.scalafish.matrix._
import org.zymnis.scalafish.job._

import com.twitter.bijection.Conversion.asMethod
import scala.util.control.Exception.allCatch

object HadoopMatrixLoader {
  implicit val toTriple: Injection[HadoopMatrixLoader, (String, Int, Int)] =
    new AbstractInjection[HadoopMatrixLoader, (String, Int, Int)] {
      def apply(loader: HadoopMatrixLoader) = (loader.pail.getRoot, loader.rows, loader.cols)
      def invert(triple: (String, Int, Int)) = {
        val (root, rows, cols) = triple
        scala.util.control.Exception.allCatch.opt {
          val pail = Pail.create(root, new ScalafishPailStructure(), false)
            .asInstanceOf[Pail[SparseElement]]
          new HadoopMatrixLoader(pail, rows, cols)
        }
      }
  }

  implicit val toBytes: Injection[HadoopMatrixLoader, Array[Byte]] =
    toTriple.andThen(Bufferable.injectionOf[(String, Int, Int)])

  val kryoSerializer: KSerializer[HadoopMatrixLoader] = InjectiveSerializer.asKryo

  def apply(rootPath: String, shards: Int) = {
    val structure = new ScalafishPailStructure().setShards(shards)
    val pail: Pail[SparseElement] =
      Pail.create(rootPath, structure, false).asInstanceOf[Pail[SparseElement]]

    val conf = new JobConf
    val process = new HadoopFlowProcess(new JobConf)
    val coords = HDFSMetadata.get[Map[String, Int]](conf, rootPath)
      .getOrElse(sys.error("No metadata available!"))

    assert(coords.contains("row"))
    assert(coords.contains("col"))
    new HadoopMatrixLoader(pail, coords("row"), coords("col"))
  }
}

class HadoopMatrixLoader(val pail: Pail[SparseElement], val rows: Int, val cols: Int) extends MatrixLoader {
  val iter: Iterator[SparseElement] =
    pail.iterator().asScala.asInstanceOf[Iterator[SparseElement]]

  override def rowPartition(parts: Int): IndexedSeq[MatrixLoader] = {
    // assert(parts == shards, "can't partition!")
    (0 to parts).map { i =>
      new HadoopMatrixLoader(pail.getSubPail(i.toString), rows / parts, cols)
    }.toIndexedSeq
  }

  override def load: Matrix = {
    val ret = SparseMatrix.zeros(rows, cols)
    iter.foreach { elem: SparseElement =>
      ret.update(elem.row, elem.col, elem.value)
    }
    ret
  }
}
