/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution

import java.util.{Iterator => JIterator}

import scala.reflect.ClassTag

import org.apache.spark.unsafe.types.UTF8String

/**
 * A fast hash set implementation for non-null data. This hash set supports
 * insertions and updates, but not deletions. It is much faster than Java's
 * standard HashSet while using much less memory overhead.
 * <p>
 * A special feature of this set is that it allows using the key objects
 * for storing additional data too and allows update of the same by the new
 * passed in key when it matches existing key in the map. Hence it can be
 * used as a more efficient map that uses a single object for both key and
 * value parts (and user's key object can be coded to be so).
 * <p>
 * Adapted from Spark's OpenHashSet implementation. It deliberately uses
 * java interfaces to keep byte code overheads minimal.
 */
final class ObjectHashSet[T <: AnyRef : ClassTag](initialCapacity: Int,
    loadFactor: Double, numColumns: Int)
    extends java.lang.Iterable[T] with Serializable {

  private[this] var _capacity = nextPowerOf2(initialCapacity)
  private[this] var _size = 0
  private[this] var _growThreshold = (loadFactor * _capacity).toInt

  private[this] var _mask = _capacity - 1
  private[this] var _data: Array[T] = newArray(_capacity)
  private[this] var _keyIsUnique: Boolean = true
  private[this] var _minValues: Array[Long] = _
  private[this] var _maxValues: Array[Long] = _

  private[this] def newArray(capacity: Int): Array[T] =
    implicitly[ClassTag[T]].newArray(capacity)

  def size: Int = _size

  def mask: Int = _mask

  def data: Array[T] = _data

  def keyIsUnique: Boolean = _keyIsUnique

  def addString(key: UTF8String, default: UTF8String => T): T = {
    val hash = key.hashCode()
    val data = _data
    val mask = _mask
    var pos = hash & mask
    var delta = 1
    while (true) {
      val mapKey = data(pos)
      if (mapKey ne null) {
        if (mapKey.asInstanceOf[StringKey].s.equals(key)) {
          // update
          return mapKey
        } else {
          // quadratic probing with position increase by 1, 2, 3, ...
          pos = (pos + delta) & mask
          delta += 1
        }
      } else {
        val entry = default(key)
        // insert into the map and rehash if required
        data(pos) = entry
        handleNewInsert()
        return entry
      }
    }
    throw new AssertionError("not expected to reach")
  }

  def addLong(key: Long, default: Long => T): T = {
    val hash = HashingUtil.hashLong(key)
    val data = _data
    val mask = _mask
    var pos = hash & mask
    var delta = 1
    while (true) {
      val mapKey = data(pos)
      if (mapKey ne null) {
        if (mapKey.asInstanceOf[LongKey].l == key) {
          // update
          return mapKey
        } else {
          // quadratic probing with position increase by 1, 2, 3, ...
          pos = (pos + delta) & mask
          delta += 1
        }
      } else {
        val entry = default(key)
        // insert into the map and rehash if required
        data(pos) = entry.asInstanceOf[T]
        handleNewInsert()
        return entry
      }
    }
    throw new AssertionError("not expected to reach")
  }

  def setKeyIsUnique(unique: Boolean): Unit = {
    if (_keyIsUnique != unique) _keyIsUnique = unique
  }

  private def initArray(values: Array[Long], init: Long): Unit = {
    var index = 0
    val len = values.length
    while (index < len) {
      values(index) = init
      index += 1
    }
  }

  def updateLimits(v: Long, ordinal: Int): Unit = {
    if (_minValues == null) {
      _minValues = new Array[Long](numColumns)
      initArray(_minValues, Long.MaxValue)
    }
    val currentMin = _minValues(ordinal)
    if (v < currentMin) {
      _minValues(ordinal) = v
    }
    if (_maxValues == null) {
      _maxValues = new Array[Long](numColumns)
      initArray(_maxValues, Long.MinValue)
    }
    val currentMax = _maxValues(ordinal)
    if (v > currentMax) {
      _maxValues(ordinal) = v
    }
  }

  def getMinValue(ordinal: Int): Long = _minValues match {
    case null => Long.MaxValue // no value in map so skip everything
    case minValues => minValues(ordinal)
  }

  def getMaxValue(ordinal: Int): Long = _maxValues match {
    case null => Long.MinValue // no value in map so skip everything
    case maxValues => maxValues(ordinal)
  }

  override def iterator: JIterator[T] = new JIterator[T] {

    private[this] var _pos = -1

    override def hasNext: Boolean =
      throw new UnsupportedOperationException("not expected to be invoked")

    override def remove(): Unit =
      throw new UnsupportedOperationException("not expected to be invoked")

    override def next(): T = {
      val data = _data
      val size = data.length
      var pos = _pos + 1
      while (pos < size) {
        val d = data(pos)
        if (d ne null) {
          _pos = pos
          return d
        }
        pos += 1
      }
      _pos = size
      null.asInstanceOf[T]
    }
  }

  def handleNewInsert(): Boolean = {
    _size += 1
    // check and trigger a rehash if load factor exceeded
    if (_size <= _growThreshold) {
      false
    } else {
      rehash()
      true
    }
  }

  /**
   * Double the table's size and re-hash everything.
   * Caller must check for overloaded set before triggering a rehash.
   */
  private def rehash(): Unit = {
    val capacity = _capacity
    val data = _data

    val newCapacity = checkCapacity(capacity << 1)
    val newData = newArray(newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      val d = data(oldPos)
      if (d ne null) {
        var newPos = d.hashCode() & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert.
        while (keepGoing) {
          if (newData(newPos) eq null) {
            // Inserting the key at newPos
            newData(newPos) = d
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }

    _capacity = newCapacity
    _data = newData
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }

  private def checkCapacity(capacity: Int): Int = {
    val maxCapacity = 1 << 30
    if (capacity > 0 && capacity <= maxCapacity) {
      capacity
    } else {
      throw new IllegalStateException(
        s"Can't contain more than ${(loadFactor * maxCapacity).toInt} elements")
    }
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    checkCapacity(if (highBit == n) n else highBit << 1)
  }
}

abstract class StringKey(val s: UTF8String) {

  override lazy val hashCode: Int = s.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case o: StringKey => s == o.s
    case _ => false
  }
}

abstract class LongKey(val l: Long) {

  override def hashCode(): Int = HashingUtil.hashLong(l)

  override def equals(obj: Any): Boolean = obj match {
    case o: LongKey => l == o.l
    case _ => false
  }
}
