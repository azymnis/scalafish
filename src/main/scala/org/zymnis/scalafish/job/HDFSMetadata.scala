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

package org.zymnis.scalafish.job

import com.twitter.bijection.json.{ JsonInjection, JsonNodeInjection }
import java.io.{ DataOutputStream, DataInputStream }
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object HDFSMetadata {
  val METADATA_FILE = "_meta.json"

  def getPath(rootPath: String): Path = new Path(rootPath, METADATA_FILE)

  def get[T: JsonNodeInjection](conf: Configuration, path: String): Option[T] = {
    val p = getPath(path)
    val fs = FileSystem.get(conf)
    val is = new DataInputStream(fs.open(p))
    val ret = JsonInjection.fromString[T](WritableUtils.readString(is))
    is.close()
    ret
  }

  def put[T: JsonNodeInjection](conf: Configuration, path: String, obj: Option[T]) {
    obj.map { JsonInjection.toString[T].apply(_) }
        .foreach { meta =>
      val fs = FileSystem.get(conf)
      val metaPath = getPath(path)
      val os = new DataOutputStream(fs.create(metaPath))
      WritableUtils.writeString(os, meta)
      os.close()
    }
  }
}
