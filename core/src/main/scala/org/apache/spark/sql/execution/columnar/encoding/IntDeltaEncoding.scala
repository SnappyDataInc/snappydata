/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution.columnar.encoding

import org.apache.spark.sql.types.{DataType, IntegerType}

final class IntDeltaEncoding extends IntDeltaEncodingBase with NotNullColumn

final class IntDeltaEncodingNullable
    extends IntDeltaEncodingBase with NullableColumn

abstract class IntDeltaEncodingBase extends UncompressedBase {

  private[this] var prev = 0

  override final def typeId: Int = 4

  override final def supports(dataType: DataType): Boolean =
    dataType == IntegerType

  override final def nextInt(bytes: Array[Byte]): Unit = {
    val delta = bytes(cursor)
    cursor += 1
    if (delta > Byte.MinValue) {
      prev += delta
    } else {
      prev = super.readInt(bytes)
      cursor += 4
    }
  }

  override final def readInt(bytes: Array[Byte]): Int =
    prev
}
