package org.zymnis.scalafish.zookeeper

import org.json.simple.JSONValue

// Use java.lang.Number for numbers, java.util.List for lists, java.util.Map for objects
object JsonState {
  def apply(x: AnyRef): Array[Byte] =
    JSONValue.toJSONString(x).getBytes

  def apply(bytes: Array[Byte]): AnyRef =
    JSONValue.parse(new String(bytes, "UTF-8"))
}
