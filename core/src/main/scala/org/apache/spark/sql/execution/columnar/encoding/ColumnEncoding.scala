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

import java.lang.reflect.Field
import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import io.snappydata.util.StringUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.UnsafeRow.calculateBitSetWidthInBytes
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding.checkBufferSize
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.bitset.BitSetMethods
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.BitSet

/**
 * Base class for encoding and decoding in columnar form. Memory layout of
 * the bytes for a set of column values is:
 * {{{
 *   .----------------------- Encoding scheme (4 bytes)
 *   |   .------------------- Null bitset size as number of longs N (4 bytes)
 *   |   |   .--------------- Null bitset longs (8 x N bytes,
 *   |   |   |                                   empty if null count is zero)
 *   |   |   |     .--------- Encoded non-null elements
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

  protected def hasNulls: Boolean

  protected def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long

  protected def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long

  def initialize(buffer: ByteBuffer, field: StructField): Long = {
    val allocator = ColumnEncoding.getAllocator(buffer)
    initialize(allocator.baseObject(buffer), allocator.baseOffset(buffer) +
        buffer.position(), field)
  }

  def initialize(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    initializeCursor(columnBytes,
      initializeNulls(columnBytes, cursor, field), field)
  }

  /**
   * Returns 1 to indicate that column value was not-null,
   * 0 to indicate that it was null and -1 to indicate that
   * <code>wasNull()</code> needs to be invoked after the
   * appropriate read method.
   */
  def notNull(columnBytes: AnyRef, ordinal: Int): Int

  def nextBoolean(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextBoolean for $toString")

  def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    throw new UnsupportedOperationException(s"readBoolean for $toString")

  def nextByte(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextByte for $toString")

  def readByte(columnBytes: AnyRef, cursor: Long): Byte =
    throw new UnsupportedOperationException(s"readByte for $toString")

  def nextShort(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextShort for $toString")

  def readShort(columnBytes: AnyRef, cursor: Long): Short =
    throw new UnsupportedOperationException(s"readShort for $toString")

  def nextInt(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextInt for $toString")

  def readInt(columnBytes: AnyRef, cursor: Long): Int =
    throw new UnsupportedOperationException(s"readInt for $toString")

  def nextLong(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextLong for $toString")

  def readLong(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"readLong for $toString")

  def nextFloat(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextFloat for $toString")

  def readFloat(columnBytes: AnyRef, cursor: Long): Float =
    throw new UnsupportedOperationException(s"readFloat for $toString")

  def nextDouble(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextDouble for $toString")

  def readDouble(columnBytes: AnyRef, cursor: Long): Double =
    throw new UnsupportedOperationException(s"readDouble for $toString")

  def nextLongDecimal(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextLongDecimal for $toString")

  def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal =
    throw new UnsupportedOperationException(s"readLongDecimal for $toString")

  def nextDecimal(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextDecimal for $toString")

  def readDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal =
    throw new UnsupportedOperationException(s"readDecimal for $toString")

  def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextUTF8String for $toString")

  def readUTF8String(columnBytes: AnyRef, cursor: Long): UTF8String =
    throw new UnsupportedOperationException(s"readUTF8String for $toString")

  def getStringDictionary: Array[UTF8String] = null

  def readDictionaryIndex(columnBytes: AnyRef, cursor: Long): Int = -1

  def readDate(columnBytes: AnyRef, cursor: Long): Int =
    readInt(columnBytes, cursor)

  def readTimestamp(columnBytes: AnyRef, cursor: Long): Long =
    readLong(columnBytes, cursor)

  def nextInterval(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextInterval for $toString")

  def readInterval(columnBytes: AnyRef, cursor: Long): CalendarInterval =
    throw new UnsupportedOperationException(s"readInterval for $toString")

  def nextBinary(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextBinary for $toString")

  def readBinary(columnBytes: AnyRef, cursor: Long): Array[Byte] =
    throw new UnsupportedOperationException(s"readBinary for $toString")

  def nextArray(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextArray for $toString")

  def readArray(columnBytes: AnyRef, cursor: Long): ArrayData =
    throw new UnsupportedOperationException(s"readArray for $toString")

  def nextMap(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextMap for $toString")

  def readMap(columnBytes: AnyRef, cursor: Long): MapData =
    throw new UnsupportedOperationException(s"readMap for $toString")

  def nextStruct(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextStruct for $toString")

  def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long): InternalRow =
    throw new UnsupportedOperationException(s"readStruct for $toString")

  /**
   * Only to be used for implementations (ResultSet adapter) that need to check
   * for null after having invoked the appropriate read method.
   * The <code>notNull</code> method should return -1 for such implementations.
   */
  def wasNull(): Boolean = false
}

trait ColumnEncoder extends ColumnEncoding {

  protected final var allocator: ColumnAllocator = _
  private final var finalAllocator: ColumnAllocator = _
  protected final var columnData: ByteBuffer = _
  protected final var columnBeginPosition: Long = _
  protected final var columnEndPosition: Long = _
  protected final var columnBytes: AnyRef = _
  protected final var reuseUsedSize: Int = _
  protected final var forComplexType: Boolean = _

  protected final var _lowerLong: Long = _
  protected final var _upperLong: Long = _
  protected final var _lowerDouble: Double = _
  protected final var _upperDouble: Double = _
  protected final var _lowerStr: UTF8String = _
  protected final var _upperStr: UTF8String = _
  protected final var _lowerDecimal: Decimal = _
  protected final var _upperDecimal: Decimal = _
  protected final var _count: Int = 0

  /**
   * Get the allocator for the final data to be sent for storage.
   * It is on-heap for now in embedded mode while off-heap for
   * connector mode to minimize copying in both cases.
   * This should be changed to use the matching allocator as per the
   * storage being used by column store in embedded mode.
   */
  protected final def storageAllocator: ColumnAllocator = {
    if (finalAllocator ne null) finalAllocator
    else {
      finalAllocator =
          if (GemFireCacheImpl.getInstance ne null) HeapBufferAllocator
          else allocator
      finalAllocator
    }
  }

  protected final def isAllocatorFinal: Boolean =
    allocator.getClass eq storageAllocator.getClass

  protected def setAllocator(allocator: ColumnAllocator): Unit = {
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

  protected def initializeNulls(initSize: Int): Int

  final def initialize(field: StructField, initSize: Int,
      withHeader: Boolean): Long = {
    initialize(field, initSize, withHeader, DirectBufferAllocator)
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
    _count = 0
  }

  def initialize(field: StructField, initSize: Int,
      withHeader: Boolean, allocator: ColumnAllocator): Long = {
    setAllocator(allocator)
    val dataType = Utils.getSQLDataType(field.dataType)
    val defSize = defaultSize(dataType)

    this.forComplexType = dataType match {
      case _: ArrayType | _: MapType | _: StructType => true
      case _ => false
    }

    // initialize the lower and upper limits
    if (withHeader) initializeLimits()

    val numNullWords = initializeNulls(initSize)
    val numNullBytes = numNullWords.toLong << 3L
    if (withHeader) initializeLimits()
    else if (numNullWords != 0) assert(assertion = false,
      s"Unexpected nulls=$numNullWords for withHeader=false")

    if (columnData eq null) {
      var initByteSize: Long = 0L
      if (reuseUsedSize > 0) {
        initByteSize = reuseUsedSize
      } else {
        initByteSize = defSize.toLong * initSize + numNullBytes
        if (withHeader) {
          initByteSize += 8L /* typeId + nullsSize */
        }
      }
      setSource(allocator.allocate(checkBufferSize(initByteSize)),
        releaseOld = true)
    } else {
      // for primitive types optimistically trim to exact size
      dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType |
             DateType | TimestampType | FloatType | DoubleType
          if reuseUsedSize > 0 && isAllocatorFinal &&
              reuseUsedSize != columnData.limit() =>
          setSource(allocator.allocate(reuseUsedSize), releaseOld = true)

        case _ => // continue to use the previous columnData
      }
    }
    reuseUsedSize = 0
    if (withHeader) {
      var cursor = columnBeginPosition
      // typeId followed by nulls bitset size and space for values
      ColumnEncoding.writeInt(columnBytes, cursor, typeId)
      cursor += 4
      // write the number of null words
      ColumnEncoding.writeInt(columnBytes, cursor, numNullWords)
      cursor + 4L + numNullBytes
    } else columnBeginPosition
  }

  final def baseOffset: Long = columnBeginPosition

  final def offset(cursor: Long): Long = cursor - columnBeginPosition

  final def buffer: AnyRef = columnBytes

  protected final def setSource(buffer: ByteBuffer,
      releaseOld: Boolean): Unit = {
    if (buffer ne columnData) {
      if ((columnData ne null) && releaseOld) {
        allocator.release(columnData)
      }
      columnData = buffer
      columnBytes = allocator.baseObject(buffer)
      columnBeginPosition = allocator.baseOffset(buffer)
      columnEndPosition = columnBeginPosition + buffer.limit()
    }
  }

  protected final def clearSource(newSize: Int, releaseData: Boolean): Unit = {
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
    if (limit > endOffset) src.limit(endOffset)

    dest.put(src)

    // move back position and limit to original values
    src.position(position)
    src.limit(limit)
  }

  /** Expand the underlying bytes if required and return the new cursor */
  protected final def expand(cursor: Long, required: Int): Long = {
    val numWritten = cursor - columnBeginPosition
    setSource(allocator.expand(columnData, cursor,
      columnBeginPosition, required), releaseOld = false)
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

  final def count: Int = _count

  protected final def updateLongStats(value: Long): Unit = {
    val lower = _lowerLong
    if (value < lower) {
      _lowerLong = value
      // check for first write case
      if (lower == Long.MaxValue) _upperLong = value
    } else if (value > _upperLong) {
      _upperLong = value
    }
    updateCount()
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
    updateCount()
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
    updateCount()
  }

  @inline final def updateCount(): Unit = {
    _count += 1
  }

  def nullCount: Int

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

  /**
   * Finish encoding the current column and return the data as a ByteBuffer.
   * The encoder can be reused for new column data of same type again.
   */
  def finish(cursor: Long): ByteBuffer

  /**
   * Close and relinquish all resources of this encoder.
   * The encoder may no longer be usable after this call.
   */
  def close(): Unit = {
    clearSource(newSize = 0, releaseData = true)
  }

  protected def getNumNullWords: Int

  protected def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long

  protected final def releaseForReuse(newSize: Int): Unit = {
    columnData.clear()
    reuseUsedSize = newSize
  }
}

object ColumnEncoding {

  private[columnar] val bitSetWords: Field = {
    val f = classOf[BitSet].getDeclaredField("words")
    f.setAccessible(true)
    f
  }

  private[columnar] val BITS_PER_LONG = 64

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

  def getAllocator(buffer: ByteBuffer): ColumnAllocator =
    if (buffer.isDirect) DirectBufferAllocator else HeapBufferAllocator

  def getColumnDecoder(buffer: ByteBuffer, field: StructField): ColumnDecoder = {
    val allocator = getAllocator(buffer)
    getColumnDecoder(allocator.baseObject(buffer), allocator.baseOffset(buffer) +
        buffer.position(), field)
  }

  def getColumnDecoder(columnBytes: AnyRef, offset: Long,
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

  def getColumnEncoder(field: StructField): ColumnEncoder = {
    // TODO: SW: Only uncompressed + dictionary encoding for a start.
    // Need to add RunLength and BooleanBitSet by default (others on explicit
    //    compression level with LZ4/LZF for binary/complex data)
    Utils.getSQLDataType(field.dataType) match {
      case StringType => createDictionaryEncoder(StringType, field.nullable)
      case dataType => createUncompressedEncoder(dataType, field.nullable)
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
  val count: AttributeReference = AttributeReference(
    fieldName + ".count", IntegerType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount, count)

  assert(schema.length == ColumnStatsSchema.NUM_STATS_PER_COLUMN)
}

object ColumnStatsSchema {
  val NUM_STATS_PER_COLUMN = 4
  val COUNT_INDEX_IN_SCHEMA = 3
}

trait NotNullDecoder extends ColumnDecoder {

  override protected final def hasNulls: Boolean = false

  protected def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = {
    val numNullWords = ColumnEncoding.readInt(columnBytes, cursor + 4)
    if (numNullWords != 0) {
      throw new IllegalStateException(
        s"Nulls bitset of size $numNullWords found in NOT NULL column $field")
    }
    cursor + 8 // skip typeId and nullValuesSize
  }

  override final def notNull(columnBytes: AnyRef, ordinal: Int): Int = 1
}

trait NullableDecoder extends ColumnDecoder {

  protected final var nullOffset: Long = _
  protected final var numNullWords: Int = _
  // intialize to -1 so that nextNullOrdinal + 1 starts at 0
  protected final var nextNullOrdinal: Int = -1

  override protected final def hasNulls: Boolean = true

  private final def updateNextNullOrdinal(columnBytes: AnyRef) {
    nextNullOrdinal = BitSetMethods.nextSetBit(columnBytes, nullOffset,
      nextNullOrdinal + 1, numNullWords)
  }

  protected def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = {
    var position = cursor + 4
    // skip typeId
    numNullWords = ColumnEncoding.readInt(columnBytes, position)
    assert(numNullWords > 0,
      s"Expected valid null values but got length = $numNullWords")
    position += 4
    nullOffset = position
    // skip null bit set
    position += (numNullWords << 3)
    updateNextNullOrdinal(columnBytes)
    position
  }

  override final def notNull(columnBytes: AnyRef, ordinal: Int): Int = {
    if (ordinal != nextNullOrdinal) 1
    else {
      updateNextNullOrdinal(columnBytes)
      0
    }
  }
}

trait NotNullEncoder extends ColumnEncoder {

  override protected def initializeNulls(initSize: Int): Int = 0

  override def nullCount: Int = 0

  override def writeIsNull(ordinal: Int): Unit =
    throw new UnsupportedOperationException(s"writeIsNull for $toString")

  override protected def getNumNullWords: Int = 0

  override protected def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = cursor

  override def finish(cursor: Long): ByteBuffer = {
    val newSize = checkBufferSize(cursor - columnBeginPosition)
    // check if need to shrink byte array since it is stored as is in region
    // avoid copying only if final shape of object in region is same
    // else copy is required in any case and columnData can be reused
    if (cursor == columnEndPosition && isAllocatorFinal) {
      val columnData = this.columnData
      clearSource(newSize, releaseData = false)
      columnData
    } else {
      // copy to exact size
      val newColumnData = storageAllocator.allocate(newSize)
      copyTo(newColumnData, srcOffset = 0, newSize)
      newColumnData.rewind()
      // reuse this columnData in next round if possible
      releaseForReuse(newSize)
      newColumnData
    }
  }
}

trait NullableEncoder extends NotNullEncoder {

  protected final var maxNulls: Long = _
  protected final var nullWords: Array[Long] = _
  protected final var initialNumWords: Int = _

  override protected def getNumNullWords: Int = {
    val nullWords = this.nullWords
    var numWords = nullWords.length
    while (numWords > 0 && nullWords(numWords - 1) == 0L) numWords -= 1
    numWords
  }

  override protected def initializeNulls(initSize: Int): Int = {
    if (nullWords eq null) {
      val numWords = calculateBitSetWidthInBytes(initSize) >>> 3
      maxNulls = numWords.toLong << 6L
      nullWords = new Array[Long](numWords)
      initialNumWords = numWords
      numWords
    } else {
      // trim trailing empty words
      val numWords = getNumNullWords
      initialNumWords = numWords
      // clear rest of the words
      var i = 0
      while (i < numWords) {
        if (nullWords(i) != 0L) nullWords(i) = 0L
        i += 1
      }
      numWords
    }
  }

  override def nullCount: Int = {
    var sum = 0
    var i = 0
    val numWords = nullWords.length
    while (i < numWords) {
      sum += java.lang.Long.bitCount(nullWords(i))
      i += 1
    }
    sum
  }

  override def writeIsNull(ordinal: Int): Unit = {
    if (ordinal < maxNulls) {
      BitSetMethods.set(nullWords, Platform.LONG_ARRAY_OFFSET, ordinal)
    } else {
      // expand
      val oldNulls = nullWords
      val oldLen = oldNulls.length
      val newLen = oldLen << 1
      nullWords = new Array[Long](newLen)
      maxNulls = newLen << 6L
      System.arraycopy(oldNulls, 0, nullWords, 0, oldLen)
      BitSetMethods.set(nullWords, Platform.LONG_ARRAY_OFFSET, ordinal)
    }
  }

  override protected def writeNulls(columnBytes: AnyRef, cursor: Long,
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

  override def finish(cursor: Long): ByteBuffer = {
    // trim trailing empty words
    val numWords = getNumNullWords
    // maximum number of null words that can be allowed to go waste in storage
    val maxWastedWords = 50
    // check if the number of words to be written matches the space that
    // was left at initialization; as an optimization allow for larger
    // space left at initialization when one full data copy can be avoided
    val baseOffset = columnBeginPosition
    if (initialNumWords == numWords) {
      writeNulls(columnBytes, baseOffset + 8, numWords)
      super.finish(cursor)
    } else if (initialNumWords > numWords && numWords > 0 &&
        (initialNumWords - numWords) < maxWastedWords &&
        cursor == columnEndPosition) {
      // write till initialNumWords and not just numWords to clear any
      // trailing empty bytes (required since ColumnData can be reused)
      writeNulls(columnBytes, baseOffset + 8, initialNumWords)
      super.finish(cursor)
    } else {
      // make space (or shrink) for writing nulls at the start
      val numNullBytes = numWords << 3
      val initialNullBytes = initialNumWords << 3
      val oldSize = cursor - baseOffset
      val newSize = math.min(Int.MaxValue - 1,
        oldSize + numNullBytes - initialNullBytes).toInt
      val storageAllocator = this.storageAllocator
      val newColumnData = storageAllocator.allocate(newSize)

      // first copy the rest of the bytes skipping header and nulls
      val srcOffset = 8 + initialNullBytes
      val destOffset = 8 + numNullBytes
      newColumnData.position(destOffset)
      copyTo(newColumnData, srcOffset, oldSize.toInt)
      newColumnData.rewind()

      // reuse this columnData in next round if possible but
      // skip if there was a large wastage in this round
      if (math.abs(initialNumWords - numWords) < maxWastedWords) {
        releaseForReuse(newSize)
      } else {
        clearSource(newSize, releaseData = true)
      }

      // now write the header including nulls
      val newColumnBytes = storageAllocator.baseObject(newColumnData)
      var position = storageAllocator.baseOffset(newColumnData)
      ColumnEncoding.writeInt(newColumnBytes, position, typeId)
      position += 4
      ColumnEncoding.writeInt(newColumnBytes, position, numWords)
      position += 4
      // write the null words
      writeNulls(newColumnBytes, position, numWords)
      newColumnData
    }
  }
}
