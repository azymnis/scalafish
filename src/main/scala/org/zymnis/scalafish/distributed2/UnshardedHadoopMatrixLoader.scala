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
import com.twitter.scalding._
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

object UnshardedHadoopMatrixLoader {
  def apply(rootPath: String) = {
    // TODO: Set up namenode auth.
    val conf = new JobConf
    conf.addResource("/etc/hadoop/hadoop-conf-dw-smf1")
    val process = new HadoopFlowProcess(conf)
    val coords = HDFSMetadata.get[Map[String, Int]](conf, rootPath)
      .getOrElse(sys.error("No metadata available!"))

    assert(coords.contains("row"))
    assert(coords.contains("col"))
    new UnshardedHadoopMatrixLoader(rootPath, conf, coords("row"), coords("col"), Some(_))
  }
}

/**
  * Pred should return true if this particular loader is responsible
  * for that shard in the matrix, false otherwise.
  */
class UnshardedHadoopMatrixLoader(val path: String, conf: JobConf, val rows: Int, val cols: Int,
  pred: Int => Option[Int]) extends MatrixLoader {
  import Dsl._

  override def rowPartition(parts: Int): IndexedSeq[MatrixLoader] = {
    (0 to parts).map { i =>
      val newMaxRows = rows / parts
      new UnshardedHadoopMatrixLoader(path, conf, newMaxRows, cols, { idx =>
        val inclusiveLowerBound = i * newMaxRows
        val exclusiveUpperBound = (i + 1) * newMaxRows
        if ((idx >= inclusiveLowerBound) && (idx < exclusiveUpperBound))
          Some(idx - inclusiveLowerBound)
        else
          None
      })
    }.toIndexedSeq
  }

  override def load: Matrix = {
    implicit val mode = Hdfs(true, conf)
    val ret = SparseMatrix.zeros(rows, cols)
    Tsv(path).readAtSubmitter[(Int, Int, Float)]
      .filter { case (row, col, value) => pred(row).isDefined }
      .foreach { case (row, col, value) =>
        ret.update(row, col, value)
    }
    ret
  }
}
