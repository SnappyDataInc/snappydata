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

import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.store.ManagedDirectBufferAllocator
import com.gemstone.gemfire.internal.shared.{BufferAllocator, HeapBufferAllocator}
import io.snappydata.util.StringUtils

import org.apache.spark.memory.MemoryManagerCallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.UnsafeRow.calculateBitSetWidthInBytes
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding.checkBufferSize
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Base class for encoding and decoding in columnar form. Memory layout of
 * the bytes for a set of column values is:
 * {{{
 *    .----------------------- Encoding scheme (4 bytes)
 *   |    .------------------- Null bitset size as number of longs N (4 bytes)
 *   |   |
 *   |   |   .---------------- Null bitset longs (8 x N bytes,
 *   |   |   |                                    empty if null count is zero)
 *   |   |   |     .---------- Encoded non-null elements
 *   V   V   V     V
 *   +---+---+-----+---------+
 *   |   |   | ... | ... ... |
 *   +---+---+-----+---------+
 *    \-----/ \-------------/
 *     header      body
 * }}}
 */
trait ColumnEncoding {

  def typeId: Int

  def supports(dataType: DataType): Boolean
}

// TODO: SW: check perf after removing the columnBytes argument to decoders
// if its same, then remove since it will help free up many registers
abstract class ColumnDecoder extends ColumnEncoding {

  protected final var baseCursor: Long = _

  /**
   * Normally not used by decoder but supplied by caller to the methods
   * but can be used if required but needs to be set by caller explicitly.
   */
  final var currentCursor: Long = _

  protected[sql] def hasNulls: Boolean

  protected[sql] def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long

  protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long

  final def initialize(buffer: ByteBuffer, field: StructField): Long = {
    val allocator = ColumnEncoding.getAllocator(buffer)
    initialize(allocator.baseObject(buffer), allocator.baseOffset(buffer) +
        buffer.position(), field)
  }

  /**
   * Delta encoder/decoder depend on initialize being final and invoking
   * initializeCursor and initializeNulls as below.
   */
  final def initialize(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    initializeCursor(columnBytes, initializeNulls(columnBytes, cursor, field), field)
  }

  private[sql] def initializeNullsBeforeFinish(
      columnBytes: AnyRef, cursor: Long, numNullBytes: Int): Unit = {}

  def mutated(ordinal: Int): Int = 1

  /**
   * Returns 1 to indicate that column value was not-null,
   * 0 to indicate that it was null and -1 to indicate that
   * <code>wasNull()</code> needs to be invoked after the
   * appropriate read method.
   */
  def isNull(columnBytes: AnyRef, ordinal: Int, mutated: Int): Int

  /** Absolute ordinal null check for random access. */
  def isNullAt(columnBytes: AnyRef, position: Int): Boolean =
    throw new UnsupportedOperationException(s"isNullAt for $toString")

  def nextBoolean(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextBoolean for $toString")

  /** Random access to the encoded data. */
  def absoluteBoolean(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteBoolean for $toString")

  def readBoolean(columnBytes: AnyRef, cursor: Long, mutated: Int): Boolean =
    throw new UnsupportedOperationException(s"readBoolean for $toString")

  def nextByte(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextByte for $toString")

  def absoluteByte(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteByte for $toString")

  def readByte(columnBytes: AnyRef, cursor: Long, mutated: Int): Byte =
    throw new UnsupportedOperationException(s"readByte for $toString")

  def nextShort(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextShort for $toString")

  def absoluteShort(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteShort for $toString")

  def readShort(columnBytes: AnyRef, cursor: Long, mutated: Int): Short =
    throw new UnsupportedOperationException(s"readShort for $toString")

  def nextInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextInt for $toString")

  def absoluteInt(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteInt for $toString")

  def readInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    throw new UnsupportedOperationException(s"readInt for $toString")

  def nextLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextLong for $toString")

  def absoluteLong(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteLong for $toString")

  def readLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"readLong for $toString")

  def nextFloat(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextFloat for $toString")

  def absoluteFloat(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteFloat for $toString")

  def readFloat(columnBytes: AnyRef, cursor: Long, mutated: Int): Float =
    throw new UnsupportedOperationException(s"readFloat for $toString")

  def nextDouble(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextDouble for $toString")

  def absoluteDouble(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteDouble for $toString")

  def readDouble(columnBytes: AnyRef, cursor: Long, mutated: Int): Double =
    throw new UnsupportedOperationException(s"readDouble for $toString")

  def nextLongDecimal(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextLongDecimal for $toString")

  def absoluteLongDecimal(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteLongDecimal for $toString")

  def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long, mutated: Int): Decimal =
    throw new UnsupportedOperationException(s"readLongDecimal for $toString")

  def nextDecimal(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextDecimal for $toString")

  def absoluteDecimal(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteDecimal for $toString")

  def readDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long, mutated: Int): Decimal =
    throw new UnsupportedOperationException(s"readDecimal for $toString")

  def nextUTF8String(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextUTF8String for $toString")

  def absoluteUTF8String(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteUTF8String for $toString")

  def readUTF8String(columnBytes: AnyRef, cursor: Long, mutated: Int): UTF8String =
    throw new UnsupportedOperationException(s"readUTF8String for $toString")

  def getStringDictionary: Array[UTF8String] = null

  def readDictionaryIndex(columnBytes: AnyRef, cursor: Long, mutated: Int): Int = -1

  def nextDate(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    nextInt(columnBytes, cursor, mutated)

  def absoluteDate(columnBytes: AnyRef, position: Int): Long =
    absoluteInt(columnBytes, position)

  def readDate(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    readInt(columnBytes, cursor, mutated)

  def nextTimestamp(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    nextLong(columnBytes, cursor, mutated)

  def absoluteTimestamp(columnBytes: AnyRef, position: Int, mutated: Int): Long =
    absoluteLong(columnBytes, position)

  def readTimestamp(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    readLong(columnBytes, cursor, mutated)

  def nextInterval(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextInterval for $toString")

  def absoluteInterval(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteInterval for $toString")

  def readInterval(columnBytes: AnyRef, cursor: Long, mutated: Int): CalendarInterval =
    throw new UnsupportedOperationException(s"readInterval for $toString")

  def nextBinary(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextBinary for $toString")

  def absoluteBinary(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteBinary for $toString")

  def readBinary(columnBytes: AnyRef, cursor: Long, mutated: Int): Array[Byte] =
    throw new UnsupportedOperationException(s"readBinary for $toString")

  def nextArray(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextArray for $toString")

  def absoluteArray(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteArray for $toString")

  def readArray(columnBytes: AnyRef, cursor: Long, mutated: Int): ArrayData =
    throw new UnsupportedOperationException(s"readArray for $toString")

  def nextMap(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextMap for $toString")

  def absoluteMap(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteMap for $toString")

  def readMap(columnBytes: AnyRef, cursor: Long, mutated: Int): MapData =
    throw new UnsupportedOperationException(s"readMap for $toString")

  def nextStruct(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    throw new UnsupportedOperationException(s"nextStruct for $toString")

  def absoluteStruct(columnBytes: AnyRef, position: Int): Long =
    throw new UnsupportedOperationException(s"absoluteStruct for $toString")

  def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long, mutated: Int): InternalRow =
    throw new UnsupportedOperationException(s"readStruct for $toString")

  /**
   * Only to be used for implementations (ResultSet adapter) that need to check
   * for null after having invoked the appropriate read method.
   * The <code>isNull</code> method should return -1 for such implementations.
   */
  def wasNull(): Boolean = false

  /**
   * Get the number of null values till given 0-based position (exclusive)
   * for random access.
   */
  protected def numNullsUntilPosition(columnBytes: AnyRef, position: Int): Int =
    throw new UnsupportedOperationException(s"numNullsUntilPosition for $toString")
}

trait ColumnEncoder extends ColumnEncoding {

  protected final var allocator: BufferAllocator = _
  private final var finalAllocator: BufferAllocator = _
  protected[sql] final var columnData: ByteBuffer = _
  protected[sql] final var columnBeginPosition: Long = _
  protected[sql] final var columnEndPosition: Long = _
  protected[sql] final var columnBytes: AnyRef = _
  protected[sql] final var reuseUsedSize: Int = _
  protected final var forComplexType: Boolean = _

  protected final var _lowerLong: Long = _
  protected final var _upperLong: Long = _
  protected final var _lowerDouble: Double = _
  protected final var _upperDouble: Double = _
  protected final var _lowerStr: UTF8String = _
  protected final var _upperStr: UTF8String = _
  protected final var _lowerDecimal: Decimal = _
  protected final var _upperDecimal: Decimal = _

  /**
   * Get the allocator for the final data to be sent for storage.
   * It is on-heap for now in embedded mode while off-heap for
   * connector mode to minimize copying in both cases.
   * This should be changed to use the matching allocator as per the
   * storage being used by column store in embedded mode.
   */
  protected final def storageAllocator: BufferAllocator = {
    if (finalAllocator ne null) finalAllocator
    else {
      finalAllocator = GemFireCacheImpl.getCurrentBufferAllocator
      finalAllocator
    }
  }

  protected final def isAllocatorFinal: Boolean =
    allocator.getClass eq storageAllocator.getClass

  protected final def setAllocator(allocator: BufferAllocator): Unit = {
    if (this.allocator ne allocator) {
      this.allocator = allocator
      this.finalAllocator = null
    }
  }

  def sizeInBytes(cursor: Long): Long = cursor - columnBeginPosition

  def defaultSize(dataType: DataType): Int = dataType match {
    case CalendarIntervalType => 12 // uses 12 and not 16 bytes
    case _ => dataType.defaultSize
  }

  def initSizeInBytes(dataType: DataType, initSize: Long, defSize: Int): Long = {
    initSize * defSize
  }

  protected[sql] def initializeNulls(initSize: Int): Int

  final def initialize(field: StructField, initSize: Int,
      withHeader: Boolean): Long = {
    initialize(field, initSize, withHeader,
      GemFireCacheImpl.getCurrentBufferAllocator)
  }

  protected def initializeLimits(): Unit = {
    if (forComplexType) {
      // set limits for complex types that will never be hit
      _lowerLong = Long.MinValue
      _upperLong = Long.MaxValue
      _lowerDouble = Double.MinValue
      _upperDouble = Double.MaxValue
    } else {
      // for other cases set limits that will be hit on first attempt
      _lowerLong = Long.MaxValue
      _upperLong = Long.MinValue
      _lowerDouble = Double.MaxValue
      _upperDouble = Double.MinValue
    }
    _lowerStr = null
    _upperStr = null
    _lowerDecimal = null
    _upperDecimal = null
  }

  final def initialize(field: StructField, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long =
    initialize(Utils.getSQLDataType(field.dataType), field.nullable,
      initSize, withHeader, allocator)

  /**
   * Initialize this ColumnEncoder.
   *
   * @param dataType   DataType of the field to be written
   * @param nullable   True if the field is nullable, false otherwise
   * @param initSize   Initial estimated number of elements to be written
   * @param withHeader True if header is to be written to data (typeId etc)
   * @param allocator  the [[BufferAllocator]] to use for the data
   * @return initial position of the cursor that caller must use to write
   */
  def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    setAllocator(allocator)
    val defSize = defaultSize(dataType)

    this.forComplexType = dataType match {
      case _: ArrayType | _: MapType | _: StructType => true
      case _ => false
    }

    val numNullWords = initializeNulls(initSize)
    val numNullBytes = numNullWords << 3

    // initialize the lower and upper limits
    if (withHeader) initializeLimits()
    else if (numNullWords != 0) assert(assertion = false,
      s"Unexpected nulls=$numNullWords for withHeader=false")

    var baseSize = numNullBytes.toLong
    if (withHeader) {
      baseSize += 8L /* typeId + nullsSize */
    }
    if ((columnData eq null) || (columnData.limit() < (baseSize + defSize))) {
      var initByteSize = 0L
      if (reuseUsedSize > baseSize) {
        initByteSize = reuseUsedSize
      } else {
        initByteSize = initSizeInBytes(dataType, initSize, defSize) + baseSize
      }
      setSource(allocator.allocate(checkBufferSize(initByteSize),
        ColumnEncoding.BUFFER_OWNER), releaseOld = true)
    } else {
      // for primitive types optimistically trim to exact size
      dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType |
             DateType | TimestampType | FloatType | DoubleType
          if reuseUsedSize > 0 && isAllocatorFinal &&
              reuseUsedSize != columnData.limit() =>
          setSource(allocator.allocate(reuseUsedSize,
            ColumnEncoding.BUFFER_OWNER), releaseOld = true)

        case _ => // continue to use the previous columnData
      }
    }
    reuseUsedSize = 0
    if (withHeader) {
      var cursor = ensureCapacity(columnBeginPosition, 8 + numNullBytes)
      // typeId followed by nulls bitset size and space for values
      ColumnEncoding.writeInt(columnBytes, cursor, typeId)
      cursor += 4
      // write the number of null words
      ColumnEncoding.writeInt(columnBytes, cursor, numNullBytes)
      cursor + 4L + numNullBytes
    } else columnBeginPosition
  }

  final def baseOffset: Long = columnBeginPosition

  final def offset(cursor: Long): Long = cursor - columnBeginPosition

  final def buffer: AnyRef = columnBytes

  /**
   * Write any internal structures (e.g. dictionary) of the encoder that would
   * normally be written by [[finish]] after the header and null bit mask.
   */
  def writeInternals(columnBytes: AnyRef, cursor: Long): Long = cursor

  /**
   * Get a decoder for currently written data before [[finish]] has been invoked.
   * The decoder is required to be already initialized and caller should be able
   * to invoke "absolute*" methods on it.
   */
  private[sql] def decoderBeforeFinish: ColumnDecoder

  /**
   * Initialize the position skipping header on currently written data
   * for a decoder returned by [[decoderBeforeFinish]].
   */
  protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long

  protected[sql] final def setSource(buffer: ByteBuffer,
      releaseOld: Boolean): Unit = {
    if (buffer ne columnData) {
      if (releaseOld && (columnData ne null)) {
        allocator.release(columnData)
      }
      columnData = buffer
      columnBytes = allocator.baseObject(buffer)
      columnBeginPosition = allocator.baseOffset(buffer)
    }
    columnEndPosition = columnBeginPosition + buffer.limit()
  }

  protected[sql] final def clearSource(newSize: Int, releaseData: Boolean): Unit = {
    if (columnData ne null) {
      if (releaseData) {
        allocator.release(columnData)
      }
      columnData = null
      columnBytes = null
      columnBeginPosition = 0
      columnEndPosition = 0
    }
    reuseUsedSize = newSize
  }

  protected final def copyTo(dest: ByteBuffer, srcOffset: Int,
      endOffset: Int): Unit = {
    val src = columnData
    // buffer to buffer copy after position reset for source
    val position = src.position()
    val limit = src.limit()

    if (position != srcOffset) src.position(srcOffset)
    if (limit != endOffset) src.limit(endOffset)

    dest.put(src)

    // move back position and limit to original values
    src.position(position)
    src.limit(limit)
  }

  /** Expand the underlying bytes if required and return the new cursor */
  protected final def expand(cursor: Long, required: Int): Long = {
    val numWritten = cursor - columnBeginPosition
    setSource(allocator.expand(columnData, required,
      ColumnEncoding.BUFFER_OWNER), releaseOld = false)
    columnBeginPosition + numWritten
  }

  final def ensureCapacity(cursor: Long, required: Int): Long = {
    if ((cursor + required) <= columnEndPosition) cursor
    else expand(cursor, required)
  }

  final def lowerLong: Long = _lowerLong

  final def upperLong: Long = _upperLong

  final def lowerDouble: Double = _lowerDouble

  final def upperDouble: Double = _upperDouble

  final def lowerString: UTF8String = _lowerStr

  final def upperString: UTF8String = _upperStr

  final def lowerDecimal: Decimal = _lowerDecimal

  final def upperDecimal: Decimal = _upperDecimal

  protected final def updateLongStats(value: Long): Unit = {
    val lower = _lowerLong
    if (value < lower) {
      _lowerLong = value
      // check for first write case
      if (lower == Long.MaxValue) _upperLong = value
    } else if (value > _upperLong) {
      _upperLong = value
    }
  }

  protected final def updateDoubleStats(value: Double): Unit = {
    val lower = _lowerDouble
    if (value < lower) {
      // check for first write case
      if (lower == Double.MaxValue) _upperDouble = value
      _lowerDouble = value
    } else if (value > _upperDouble) {
      _upperDouble = value
    }
  }

  protected final def updateStringStats(value: UTF8String): Unit = {
    if (value ne null) {
      val lower = _lowerStr
      // check for first write case
      if (lower eq null) {
        if (!forComplexType) {
          val valueClone = StringUtils.cloneIfRequired(value)
          _lowerStr = valueClone
          _upperStr = valueClone
        }
      } else if (value.compare(lower) < 0) {
        _lowerStr = StringUtils.cloneIfRequired(value)
      } else if (value.compare(_upperStr) > 0) {
        _upperStr = StringUtils.cloneIfRequired(value)
      }
    }
  }

  protected final def updateDecimalStats(value: Decimal): Unit = {
    if (value ne null) {
      val lower = _lowerDecimal
      // check for first write case
      if (lower eq null) {
        if (!forComplexType) {
          _lowerDecimal = value
          _upperDecimal = value
        }
      } else if (value.compare(lower) < 0) {
        _lowerDecimal = value
      } else if (value.compare(_upperDecimal) > 0) {
        _upperDecimal = value
      }
    }
  }

  def nullCount: Int

  def isNullable: Boolean

  def writeIsNull(ordinal: Int): Unit

  def writeBoolean(cursor: Long, value: Boolean): Long =
    throw new UnsupportedOperationException(s"writeBoolean for $toString")

  def writeByte(cursor: Long, value: Byte): Long =
    throw new UnsupportedOperationException(s"writeByte for $toString")

  def writeShort(cursor: Long, value: Short): Long =
    throw new UnsupportedOperationException(s"writeShort for $toString")

  def writeInt(cursor: Long, value: Int): Long =
    throw new UnsupportedOperationException(s"writeInt for $toString")

  def writeLong(cursor: Long, value: Long): Long =
    throw new UnsupportedOperationException(s"writeLong for $toString")

  def writeFloat(cursor: Long, value: Float): Long =
    throw new UnsupportedOperationException(s"writeFloat for $toString")

  def writeDouble(cursor: Long, value: Double): Long =
    throw new UnsupportedOperationException(s"writeDouble for $toString")

  def writeLongDecimal(cursor: Long, value: Decimal,
      ordinal: Int, precision: Int, scale: Int): Long =
    throw new UnsupportedOperationException(s"writeLongDecimal for $toString")

  def writeDecimal(cursor: Long, value: Decimal,
      ordinal: Int, precision: Int, scale: Int): Long =
    throw new UnsupportedOperationException(s"writeDecimal for $toString")

  def writeDate(cursor: Long, value: Int): Long =
    writeInt(cursor, value)

  def writeTimestamp(cursor: Long, value: Long): Long =
    writeLong(cursor, value)

  def writeInterval(cursor: Long, value: CalendarInterval): Long =
    throw new UnsupportedOperationException(s"writeInterval for $toString")

  def writeUTF8String(cursor: Long, value: UTF8String): Long =
    throw new UnsupportedOperationException(s"writeUTF8String for $toString")

  def writeBinary(cursor: Long, value: Array[Byte]): Long =
    throw new UnsupportedOperationException(s"writeBinary for $toString")

  def writeBooleanUnchecked(cursor: Long, value: Boolean): Long =
    throw new UnsupportedOperationException(s"writeBooleanUnchecked for $toString")

  def writeByteUnchecked(cursor: Long, value: Byte): Long =
    throw new UnsupportedOperationException(s"writeByteUnchecked for $toString")

  def writeShortUnchecked(cursor: Long, value: Short): Long =
    throw new UnsupportedOperationException(s"writeShortUnchecked for $toString")

  def writeIntUnchecked(cursor: Long, value: Int): Long =
    throw new UnsupportedOperationException(s"writeIntUnchecked for $toString")

  def writeLongUnchecked(cursor: Long, value: Long): Long =
    throw new UnsupportedOperationException(s"writeLongUnchecked for $toString")

  def writeFloatUnchecked(cursor: Long, value: Float): Long =
    throw new UnsupportedOperationException(s"writeFloatUnchecked for $toString")

  def writeDoubleUnchecked(cursor: Long, value: Double): Long =
    throw new UnsupportedOperationException(s"writeDoubleUnchecked for $toString")

  def writeUnsafeData(cursor: Long, baseObject: AnyRef, baseOffset: Long,
      numBytes: Int): Long =
    throw new UnsupportedOperationException(s"writeUnsafeData for $toString")

  // Helper methods for writing complex types and elements inside them.

  /**
   * Temporary offset results to be read by generated code immediately
   * after initializeComplexType, so not an issue for nested types.
   */
  protected final var baseTypeOffset: Long = _
  protected final var baseDataOffset: Long = _

  @inline final def setOffsetAndSize(cursor: Long, fieldOffset: Long,
      baseOffset: Long, size: Int): Unit = {
    val relativeOffset = cursor - columnBeginPosition - baseOffset
    val offsetAndSize = (relativeOffset << 32L) | size.toLong
    Platform.putLong(columnBytes, columnBeginPosition + fieldOffset,
      offsetAndSize)
  }

  final def getBaseTypeOffset: Long = baseTypeOffset

  final def getBaseDataOffset: Long = baseDataOffset

  /**
   * Complex types are written similar to UnsafeRows while respecting platform
   * endianness (format is always little endian) so appropriate for storage.
   * Also have other minor differences related to size writing and interval
   * type handling. General layout looks like below:
   * {{{
   *   .--------------------------- Optional total size including itself (4 bytes)
   *   |   .----------------------- Optional number of elements (4 bytes)
   *   |   |   .------------------- Null bitset longs (8 x (N / 8) bytes)
   *   |   |   |
   *   |   |   |     .------------- Offsets+Sizes of elements (8 x N bytes)
   *   |   |   |     |     .------- Variable length elements
   *   V   V   V     V     V
   *   +---+---+-----+-------------+
   *   |   |   | ... | ... ... ... |
   *   +---+---+-----+-------------+
   *    \-----/ \-----------------/
   *     header      body
   * }}}
   * The above generic layout is used for ARRAY and STRUCT types.
   *
   * The total size of the data is written for top-level complex types. Nested
   * complex objects write their sizes in the "Offsets+Sizes" portion in the
   * respective parent object.
   *
   * ARRAY types also write the number of elements in the array in the header
   * while STRUCT types skip it since it is fixed in the meta-data.
   *
   * The null bitset follows the header. To keep the reads aligned at 8 byte
   * boundaries while preserving space, the implementation will combine the
   * header and the null bitset portion, then pad them together at 8 byte
   * boundary (in particular it will consider header as some additional empty
   * fields in the null bitset itself).
   *
   * After this follows the "Offsets+Sizes" which keeps the offset and size
   * for variable length elements. Fixed length elements less than 8 bytes
   * in size are written directly in the offset+size portion. Variable length
   * elements have their offsets (from start of this array) and sizes encoded
   * in this portion as a long (4 bytes for each of offset and size). Fixed
   * width elements that are greater than 8 bytes are encoded like variable
   * length elements. [[CalendarInterval]] is the only type currently that
   * is of that nature whose "months" portion is encoded into the size
   * while the "microseconds" portion is written into variable length part.
   *
   * MAP types are written as an ARRAY of keys followed by ARRAY of values
   * like in Spark. To keep things simpler both ARRAYs always have the
   * optional size header at their respective starts which together determine
   * the total size of the encoded MAP object. For nested MAP types, the
   * total size is skipped from the "Offsets+Sizes" portion and only
   * the offset is written (which is the start of key ARRAY).
   */
  final def initializeComplexType(cursor: Long, numElements: Int,
      skipBytes: Int, writeNumElements: Boolean): Long = {
    val numNullBytes = calculateBitSetWidthInBytes(
      numElements + (skipBytes << 3))
    // space for nulls and offsets at the start
    val fixedWidth = numNullBytes + (numElements << 3)
    var position = cursor
    if (position + fixedWidth > columnEndPosition) {
      position = expand(position, fixedWidth)
    }
    val baseTypeOffset = offset(position).toInt
    // initialize the null bytes to zeros
    allocator.clearBuffer(columnData, baseTypeOffset, numNullBytes)
    this.baseTypeOffset = baseTypeOffset
    this.baseDataOffset = baseTypeOffset + numNullBytes
    if (writeNumElements) {
      writeIntUnchecked(position + skipBytes - 4, numElements)
    }
    position + fixedWidth
  }

  private final def writeStructData(cursor: Long, value: AnyRef, size: Int,
      valueOffset: Long, fieldOffset: Long, baseOffset: Long): Long = {
    val alignedSize = ((size + 7) >>> 3) << 3
    // Write the bytes to the variable length portion.
    var position = cursor
    if (position + alignedSize > columnEndPosition) {
      position = expand(position, alignedSize)
    }
    setOffsetAndSize(position, fieldOffset, baseOffset, size)
    Platform.copyMemory(value, valueOffset, columnBytes, position, size)
    position + alignedSize
  }

  final def writeStructUTF8String(cursor: Long, value: UTF8String,
      fieldOffset: Long, baseOffset: Long): Long = {
    writeStructData(cursor, value.getBaseObject, value.numBytes(),
      value.getBaseOffset, fieldOffset, baseOffset)
  }

  final def writeStructBinary(cursor: Long, value: Array[Byte],
      fieldOffset: Long, baseOffset: Long): Long = {
    writeStructData(cursor, value, value.length, Platform.BYTE_ARRAY_OFFSET,
      fieldOffset, baseOffset)
  }

  final def writeStructDecimal(cursor: Long, value: Decimal,
      fieldOffset: Long, baseOffset: Long): Long = {
    // assume precision and scale are matching and ensured by caller
    val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
    writeStructData(cursor, bytes, bytes.length, Platform.BYTE_ARRAY_OFFSET,
      fieldOffset, baseOffset)
  }

  final def writeStructInterval(cursor: Long, value: CalendarInterval,
      fieldOffset: Long, baseOffset: Long): Long = {
    var position = cursor
    if (position + 8 > columnEndPosition) {
      position = expand(position, 8)
    }
    // write months in the size field itself instead of using separate bytes
    setOffsetAndSize(position, fieldOffset, baseOffset, value.months)
    Platform.putLong(columnBytes, position, value.microseconds)
    position + 8
  }

  /** flush any pending data when [[finish]] is not being invoked explicitly */
  def flushWithoutFinish(cursor: Long): Long = cursor

  /**
   * Finish encoding the current column and return the data as a ByteBuffer.
   * The encoder can be reused for new column data of same type again.
   */
  def finish(cursor: Long): ByteBuffer

  /**
   * The final size of the encoder column (excluding header and nulls) which should match
   * that occupied after [[finish]] but without writing anything.
   */
  def encodedSize(cursor: Long, dataBeginPosition: Long): Long =
    throw new UnsupportedOperationException(s"encodedSize for $toString")

  /**
   * Close and relinquish all resources of this encoder.
   * The encoder may no longer be usable after this call.
   */
  def close(): Unit = {
    clearSource(newSize = 0, releaseData = true)
  }

  protected[sql] def getNumNullWords: Int

  protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long

  protected final def releaseForReuse(newSize: Int): Unit = {
    columnData.rewind()
    reuseUsedSize = newSize
  }
}

object ColumnEncoding {

  private[columnar] val DICTIONARY_TYPE_ID = 2

  private[columnar] val BIG_DICTIONARY_TYPE_ID = 3

  private[columnar] val BUFFER_OWNER = "ENCODER"

  private[columnar] val BITS_PER_LONG = 64

  private[columnar] val MAX_BITMASK = 1L << 63

  /** maximum number of null words that can be allowed to go waste in storage */
  private[columnar] val MAX_WASTED_WORDS_FOR_NULLS = 8

  val littleEndian: Boolean = ByteOrder.nativeOrder == ByteOrder.LITTLE_ENDIAN

  val allDecoders: Array[(DataType, Boolean) => ColumnDecoder] = Array(
    createUncompressedDecoder,
    createRunLengthDecoder,
    createDictionaryDecoder,
    createBigDictionaryDecoder,
    createBooleanBitSetDecoder,
    createIntDeltaDecoder,
    createLongDeltaDecoder
  )

  final def checkBufferSize(size: Long): Int = {
    if (size >= 0 && size < Int.MaxValue) size.toInt
    else {
      throw new ArrayIndexOutOfBoundsException(
        s"Invalid size/index = $size. Max allowed = ${Int.MaxValue - 1}.")
    }
  }

  def getAllocator(buffer: ByteBuffer): BufferAllocator =
    if (buffer.isDirect) ManagedDirectBufferAllocator.instance()
    else HeapBufferAllocator.instance()

  def getColumnDecoder(buffer: ByteBuffer, field: StructField): ColumnDecoder = {
    val allocator = getAllocator(buffer)
    getColumnDecoder(allocator.baseObject(buffer), allocator.baseOffset(buffer) +
        buffer.position(), field)
  }

  def getColumnDecoderAndBuffer(buffer: ByteBuffer,
      field: StructField): (ColumnDecoder, AnyRef, Long) = {
    val allocator = getAllocator(buffer)
    val columnBytes = allocator.baseObject(buffer)
    val baseOffset = allocator.baseOffset(buffer) + buffer.position()
    (getColumnDecoder(columnBytes, baseOffset, field), columnBytes, baseOffset)
  }

  final def getColumnDecoder(columnBytes: AnyRef, offset: Long,
      field: StructField): ColumnDecoder = {
    // typeId at the start followed by null bit set values
    var cursor = offset
    val typeId = readInt(columnBytes, cursor)
    cursor += 4
    val dataType = Utils.getSQLDataType(field.dataType)
    if (typeId >= allDecoders.length) {
      val bytesStr = columnBytes match {
        case null => ""
        case bytes: Array[Byte] => s" bytes: ${bytes.toSeq}"
        case _ => ""
      }
      throw new IllegalStateException(s"Unknown encoding typeId = $typeId " +
          s"for $dataType($field)$bytesStr")
    }

    val numNullWords = readInt(columnBytes, cursor)
    val decoder = allDecoders(typeId)(dataType,
      // use NotNull version if field is marked so or no nulls in the batch
      field.nullable && numNullWords > 0)
    if (decoder.typeId != typeId) {
      throw new IllegalStateException(s"typeId for $decoder = " +
          s"${decoder.typeId} does not match $typeId in global registration")
    }
    if (!decoder.supports(dataType)) {
      throw new IllegalStateException("Encoder bug? Unsupported type " +
          s"$dataType for $decoder")
    }
    decoder
  }

  def getColumnEncoder(field: StructField): ColumnEncoder =
    getColumnEncoder(Utils.getSQLDataType(field.dataType), field.nullable)

  def getColumnEncoder(dataType: DataType, nullable: Boolean): ColumnEncoder = {
    // TODO: SW: add RunLength by default (others on explicit
    //    compression level with LZ4/LZF for binary/complex data)
    dataType match {
      case StringType => createDictionaryEncoder(StringType, nullable)
      case BooleanType => createBooleanBitSetEncoder(BooleanType, nullable)
      case _ => createUncompressedEncoder(dataType, nullable)
    }
  }

  private[columnar] def createUncompressedDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder =
    if (nullable) new UncompressedDecoderNullable else new UncompressedDecoder

  private[columnar] def createRunLengthDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | DateType | LongType | TimestampType | StringType =>
      if (nullable) new RunLengthDecoderNullable else new RunLengthDecoder
    case _ => throw new UnsupportedOperationException(
      s"RunLengthDecoder not supported for $dataType")
  }

  private[columnar] def createDictionaryDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryDecoderNullable
      else new DictionaryDecoder
    case _ => throw new UnsupportedOperationException(
      s"DictionaryDecoder not supported for $dataType")
  }

  private[columnar] def createBigDictionaryDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new BigDictionaryDecoderNullable
      else new BigDictionaryDecoder
    case _ => throw new UnsupportedOperationException(
      s"BigDictionaryDecoder not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case BooleanType =>
      if (nullable) new BooleanBitSetDecoderNullable
      else new BooleanBitSetDecoder
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetDecoder not supported for $dataType")
  }

  private[columnar] def createIntDeltaDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case IntegerType | DateType =>
      if (nullable) new IntDeltaDecoderNullable else new IntDeltaDecoder
    case _ => throw new UnsupportedOperationException(
      s"IntDeltaDecoder not supported for $dataType")
  }

  private[columnar] def createLongDeltaDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case LongType | TimestampType =>
      if (nullable) new LongDeltaDecoderNullable else new LongDeltaDecoder
    case _ => throw new UnsupportedOperationException(
      s"LongDeltaDecoder not supported for $dataType")
  }

  private[columnar] def createUncompressedEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder =
    if (nullable) new UncompressedEncoderNullable else new UncompressedEncoder

  private[columnar] def createDictionaryEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryEncoderNullable else new DictionaryEncoder
    case _ => throw new UnsupportedOperationException(
      s"DictionaryEncoder not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder = dataType match {
    case BooleanType => if (nullable) new BooleanBitSetEncoderNullable
    else new BooleanBitSetEncoder
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetEncoder not supported for $dataType")
  }

  @inline final def readShort(columnBytes: AnyRef,
      cursor: Long): Short = if (littleEndian) {
    Platform.getShort(columnBytes, cursor)
  } else {
    java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
  }

  @inline final def readInt(columnBytes: AnyRef,
      cursor: Long): Int = if (littleEndian) {
    Platform.getInt(columnBytes, cursor)
  } else {
    java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
  }

  @inline final def readLong(columnBytes: AnyRef,
      cursor: Long): Long = if (littleEndian) {
    Platform.getLong(columnBytes, cursor)
  } else {
    java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
  }

  @inline final def readIntBigEndian(columnBytes: AnyRef, cursor: Long): Int = {
    if (ColumnEncoding.littleEndian) Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
    else Platform.getInt(columnBytes, cursor)
  }

  @inline final def readLongBigEndian(columnBytes: AnyRef, cursor: Long): Long = {
    if (ColumnEncoding.littleEndian) {
      java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
    } else Platform.getLong(columnBytes, cursor)
  }

  @inline final def readFloat(columnBytes: AnyRef,
      cursor: Long): Float = if (littleEndian) {
    Platform.getFloat(columnBytes, cursor)
  } else {
    java.lang.Float.intBitsToFloat(java.lang.Integer.reverseBytes(
      Platform.getInt(columnBytes, cursor)))
  }

  @inline final def readDouble(columnBytes: AnyRef,
      cursor: Long): Double = if (littleEndian) {
    Platform.getDouble(columnBytes, cursor)
  } else {
    java.lang.Double.longBitsToDouble(java.lang.Long.reverseBytes(
      Platform.getLong(columnBytes, cursor)))
  }

  @inline final def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String = {
    val size = readInt(columnBytes, cursor)
    UTF8String.fromAddress(columnBytes, cursor + 4, size)
  }

  @inline final def writeShort(columnBytes: AnyRef,
      cursor: Long, value: Short): Unit = if (littleEndian) {
    Platform.putShort(columnBytes, cursor, value)
  } else {
    Platform.putShort(columnBytes, cursor, java.lang.Short.reverseBytes(value))
  }

  @inline final def writeInt(columnBytes: AnyRef,
      cursor: Long, value: Int): Unit = if (littleEndian) {
    Platform.putInt(columnBytes, cursor, value)
  } else {
    Platform.putInt(columnBytes, cursor, java.lang.Integer.reverseBytes(value))
  }

  @inline final def writeLong(columnBytes: AnyRef,
      cursor: Long, value: Long): Unit = if (littleEndian) {
    Platform.putLong(columnBytes, cursor, value)
  } else {
    Platform.putLong(columnBytes, cursor, java.lang.Long.reverseBytes(value))
  }

  final def writeUTF8String(columnBytes: AnyRef,
      cursor: Long, base: AnyRef, offset: Long, size: Int): Long = {
    ColumnEncoding.writeInt(columnBytes, cursor, size)
    val position = cursor + 4
    Platform.copyMemory(base, offset, columnBytes, position, size)
    position + size
  }
}

case class ColumnStatsSchema(fieldName: String,
    dataType: DataType) {
  val upperBound: AttributeReference = AttributeReference(
    fieldName + ".upperBound", dataType)()
  val lowerBound: AttributeReference = AttributeReference(
    fieldName + ".lowerBound", dataType)()
  val nullCount: AttributeReference = AttributeReference(
    fieldName + ".nullCount", IntegerType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount)

  assert(schema.length == ColumnStatsSchema.NUM_STATS_PER_COLUMN)
}

object ColumnStatsSchema {
  val NUM_STATS_PER_COLUMN = 3
  val COUNT_INDEX_IN_SCHEMA = 0

  val COUNT_ATTRIBUTE: AttributeReference = AttributeReference(
    "batchCount", IntegerType, nullable = false)()
}

trait NotNullDecoder extends ColumnDecoder {

  override protected[sql] final def hasNulls: Boolean = false

  protected[sql] def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = {
    val numNullWords = ColumnEncoding.readInt(columnBytes, cursor + 4)
    if (numNullWords != 0) {
      throw new IllegalStateException(
        s"Nulls bitset of size $numNullWords found in NOT NULL column $field")
    }
    cursor + 8 // skip typeId and nullValuesSize
  }

  override final def isNull(columnBytes: AnyRef, ordinal: Int, mutated: Int): Int = 0

  override def isNullAt(columnBytes: AnyRef, position: Int): Boolean = false

  override protected def numNullsUntilPosition(columnBytes: AnyRef,
      position: Int): Int = 0
}

trait NullableDecoder extends ColumnDecoder {

  protected final var nullOffset: Long = _
  protected final var numNullBytes: Int = _
  // intialize to -1 so that nextNullOrdinal + 1 starts at 0
  protected final var nextNullOrdinal: Int = -1

  override protected[sql] final def hasNulls: Boolean = true

  private final def updateNextNullOrdinal(columnBytes: AnyRef) {
    nextNullOrdinal = BitSet.nextSetBit(columnBytes, nullOffset,
      nextNullOrdinal + 1, numNullBytes)
  }

  protected[sql] def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = {
    // skip typeId
    var position = cursor + 4
    numNullBytes = ColumnEncoding.readInt(columnBytes, position)
    assert(numNullBytes > 0,
      s"Expected valid null values but got length = $numNullBytes")
    position += 4
    nullOffset = position
    // skip null bit set
    position += numNullBytes
    updateNextNullOrdinal(columnBytes)
    position
  }

  override private[sql] def initializeNullsBeforeFinish(
      columnBytes: AnyRef, cursor: Long, numNullBytes: Int): Unit = {
    this.numNullBytes = numNullBytes
    this.nullOffset = cursor
    updateNextNullOrdinal(columnBytes)
  }

  override final def isNull(columnBytes: AnyRef, ordinal: Int, mutated: Int): Int = {
    if (ordinal != nextNullOrdinal) 0
    else {
      updateNextNullOrdinal(columnBytes)
      1
    }
  }

  override final def isNullAt(columnBytes: AnyRef, position: Int): Boolean = {
    BitSet.isSet(columnBytes, nullOffset, position, numNullBytes)
  }

  /**
   * Get the number of null values till given 0-based position (exclusive)
   * for random access.
   */
  override final protected def numNullsUntilPosition(columnBytes: AnyRef,
      position: Int): Int = {
    val numNullBytes = this.numNullBytes
    if (numNullBytes == 0) return 0

    // TODO: SW: check logic again and write unit tests
    val posNumBytes = position >>> 3
    val numBytesToCheck = math.min(numNullBytes, posNumBytes)
    var i = 0
    var numNulls = 0
    while (i < numBytesToCheck) {
      // ignoring endian-ness when getting the full count
      val word = Platform.getLong(columnBytes, nullOffset + i)
      if (word != 0L) numNulls += java.lang.Long.bitCount(word)
      i += 8
    }
    // last word may remain where position may be in the middle of the word
    if (numBytesToCheck == posNumBytes) {
      // mod 64
      val pos = position & 0x3f
      if (pos != 0) {
        val word = ColumnEncoding.readLong(columnBytes, nullOffset + i)
        if (word != 0) {
          // mask the bits after or at position
          numNulls += java.lang.Long.bitCount(word & ((1L << pos.toLong) - 1L))
        }
      }
    }
    numNulls
  }
}

trait NotNullEncoder extends ColumnEncoder {

  override protected[sql] def initializeNulls(initSize: Int): Int = 0

  override protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long =
    columnBeginPosition + 8L // skip typeId and nulls size

  override def nullCount: Int = 0

  override def isNullable: Boolean = false

  override def writeIsNull(ordinal: Int): Unit =
    throw new UnsupportedOperationException(s"writeIsNull for $toString")

  override protected[sql] def getNumNullWords: Int = 0

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = cursor

  override def finish(cursor: Long): ByteBuffer = {
    val newSize = checkBufferSize(cursor - columnBeginPosition)
    // check if need to shrink byte array since it is stored as is in region
    // avoid copying only if final shape of object in region is same
    // else copy is required in any case and columnData can be reused
    if (cursor == columnEndPosition && isAllocatorFinal) {
      val columnData = this.columnData
      clearSource(newSize, releaseData = false)
      // mark its allocation for storage
      if (columnData.isDirect) {
        MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
          columnData, allowNonAllocator = false)
      }
      columnData
    } else {
      // copy to exact size
      val newColumnData = storageAllocator.allocateForStorage(newSize)
      copyTo(newColumnData, srcOffset = 0, newSize)
      newColumnData.rewind()
      // reuse this columnData in next round if possible
      releaseForReuse(newSize)
      newColumnData
    }
  }

  override def encodedSize(cursor: Long, dataBeginPosition: Long): Long =
    cursor - dataBeginPosition
}

trait NullableEncoder extends NotNullEncoder {

  protected final var maxNulls: Long = _
  protected final var nullWords: Array[Long] = _
  protected final var initialNumWords: Int = _

  override protected[sql] def getNumNullWords: Int = {
    val nullWords = this.nullWords
    var numWords = nullWords.length
    while (numWords > 0 && nullWords(numWords - 1) == 0L) numWords -= 1
    numWords
  }

  override protected[sql] def initializeNulls(initSize: Int): Int = {
    if (nullWords eq null) {
      val numWords = math.max(1, calculateBitSetWidthInBytes(initSize) >>> 3)
      maxNulls = numWords.toLong << 6L
      nullWords = new Array[Long](numWords)
      initialNumWords = numWords
      numWords
    } else {
      val numWords = nullWords.length
      maxNulls = numWords.toLong << 6L
      initialNumWords = numWords
      // clear the words
      var i = 0
      while (i < numWords) {
        if (nullWords(i) != 0L) nullWords(i) = 0L
        i += 1
      }
      numWords
    }
  }

  override protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long = {
    // initialize the NullableDecoder
    decoder.initializeNullsBeforeFinish(nullWords, Platform.LONG_ARRAY_OFFSET,
      getNumNullWords << 3)
    columnBeginPosition + (initialNumWords << 3) + 8L // skip typeId and nulls size
  }

  override def nullCount: Int = {
    var sum = 0
    var i = 0
    val numWords = getNumNullWords
    while (i < numWords) {
      sum += java.lang.Long.bitCount(nullWords(i))
      i += 1
    }
    sum
  }

  override def isNullable: Boolean = true

  override def writeIsNull(ordinal: Int): Unit = {
    if (ordinal < maxNulls) {
      BitSet.set(nullWords, Platform.LONG_ARRAY_OFFSET, ordinal)
    } else {
      // expand
      val oldNulls = nullWords
      val oldLen = getNumNullWords
      // ensure that ordinal fits (SNAP-1760)
      val newLen = math.max(oldNulls.length << 1, (ordinal >> 6) + 1)
      nullWords = new Array[Long](newLen)
      maxNulls = newLen.toLong << 6L
      if (oldLen > 0) System.arraycopy(oldNulls, 0, nullWords, 0, oldLen)
      BitSet.set(nullWords, Platform.LONG_ARRAY_OFFSET, ordinal)
    }
  }

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = {
    var position = cursor
    var index = 0
    while (index < numWords) {
      ColumnEncoding.writeLong(columnBytes, position, nullWords(index))
      position += 8
      index += 1
    }
    position
  }

  private def allowWastedWords(cursor: Long, numWords: Int): Boolean = {
    initialNumWords > numWords && numWords > 0 &&
        (initialNumWords - numWords) < ColumnEncoding.MAX_WASTED_WORDS_FOR_NULLS &&
        cursor == columnEndPosition
  }

  override def finish(cursor: Long): ByteBuffer = {
    // trim trailing empty words
    val numWords = getNumNullWords
    // check if the number of words to be written matches the space that
    // was left at initialization; as an optimization allow for larger
    // space left at initialization when one full data copy can be avoided
    val baseOffset = columnBeginPosition
    if (initialNumWords == numWords) {
      writeNulls(columnBytes, baseOffset + 8, numWords)
      super.finish(cursor)
    } else if (allowWastedWords(cursor, numWords)) {
      // write till initialNumWords and not just numWords to clear any
      // trailing empty bytes (required since ColumnData can be reused)
      writeNulls(columnBytes, baseOffset + 8, initialNumWords)
      super.finish(cursor)
    } else {
      // make space (or shrink) for writing nulls at the start
      val numNullBytes = numWords << 3
      val initialNullBytes = initialNumWords << 3
      val oldSize = cursor - baseOffset
      val newSize = checkBufferSize(oldSize + numNullBytes - initialNullBytes)
      val storageAllocator = this.storageAllocator
      val newColumnData = storageAllocator.allocateForStorage(newSize)

      // first copy the rest of the bytes skipping header and nulls
      val srcOffset = 8 + initialNullBytes
      val destOffset = 8 + numNullBytes
      newColumnData.position(destOffset)
      copyTo(newColumnData, srcOffset, oldSize.toInt)
      newColumnData.rewind()

      // reuse this columnData in next round if possible but
      // skip if there was a large wastage in this round
      if (math.abs(initialNumWords - numWords) < ColumnEncoding.MAX_WASTED_WORDS_FOR_NULLS) {
        releaseForReuse(newSize)
      } else {
        clearSource(newSize, releaseData = true)
      }

      // now write the header including nulls
      val newColumnBytes = storageAllocator.baseObject(newColumnData)
      var position = storageAllocator.baseOffset(newColumnData)
      ColumnEncoding.writeInt(newColumnBytes, position, typeId)
      position += 4
      ColumnEncoding.writeInt(newColumnBytes, position, numNullBytes)
      position += 4
      // write the null words
      writeNulls(newColumnBytes, position, numWords)
      newColumnData
    }
  }
}
