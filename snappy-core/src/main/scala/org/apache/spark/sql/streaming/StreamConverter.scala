/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.streaming

import java.io.InputStream

trait StreamConverter extends Serializable {
  def convert(inputStream: InputStream): Iterator[Any]

  def getTargetType: scala.Predef.Class[_]
}

class MyStreamConverter extends StreamConverter with Serializable {
  override def convert(inputStream: java.io.InputStream): Iterator[Any] = {
    scala.io.Source.fromInputStream(inputStream, "UTF-8")
  }

  override def getTargetType: scala.Predef.Class[_] = classOf[String]
}