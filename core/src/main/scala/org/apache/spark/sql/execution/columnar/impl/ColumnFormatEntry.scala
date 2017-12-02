/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.columnar.impl

import java.io.{DataInput, DataOutput}
import java.nio.{ByteBuffer, ByteOrder}
import javax.annotation.concurrent.GuardedBy

import com.gemstone.gemfire.cache.{DiskAccessException, EntryDestroyedException, EntryOperation, Region, RegionDestroyedException}
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.gemstone.gemfire.internal.cache.partitioned.PREntriesIterator
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView
import com.gemstone.gemfire.internal.cache.store.SerializedDiskBuffer
import com.gemstone.gemfire.internal.shared._
import com.gemstone.gemfire.internal.shared.unsafe.{DirectBufferAllocator, UnsafeHolder}
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer.REFERENCE_SIZE
import com.gemstone.gemfire.internal.{ByteBufferDataInput, DSCODE, DataSerializableFixedID, HeapDataOutputStream}
import com.pivotal.gemfirexd.internal.engine.store.{GemFireContainer, RegionKey}
import com.pivotal.gemfirexd.internal.engine.{GfxdDataSerializable, GfxdSerializable, Misc}
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLInteger, SQLLongint}
import com.pivotal.gemfirexd.internal.impl.sql.compile.TableName
import com.pivotal.gemfirexd.internal.snappy.ColumnBatchKey
import io.snappydata.Constant

import org.apache.spark.memory.MemoryManagerCallback
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeleteDelta, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry.alignedSize
import org.apache.spark.sql.store.CompressionCodecId
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Utility methods for column format storage keys and values.
 */
object ColumnFormatEntry {

  def registerTypes(): Unit = {
    // register the column key and value types
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnFormatKey])
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnFormatValue])
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnDelta])
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnDeleteDelta])
  }

  /**
   * max number of consecutive compressions after which buffer will be
   * replaced with compressed one in memory
   */
  private[columnar] val MAX_CONSECUTIVE_COMPRESSIONS = 2

  private[columnar] def alignedSize(size: Int) = ((size + 7) >>> 3) << 3

  val STATROW_COL_INDEX: Int = -1

  val DELTA_STATROW_COL_INDEX: Int = -2

  val DELETE_MASK_COL_INDEX: Int = -3
}

/**
 * Key object in the column store.
 */
final class ColumnFormatKey(private[columnar] var uuid: Long,
    private[columnar] var partitionId: Int,
    private[columnar] var columnIndex: Int)
    extends GfxdDataSerializable with ColumnBatchKey with RegionKey with Serializable {

  // to be used only by deserialization
  def this() = this(-1L, -1, -1)

  override def getNumColumnsInTable(columnTableName: String): Int = {
    val bufferTable = ColumnFormatRelation.getTableName(columnTableName)
    val bufferRegion = Misc.getRegionForTable(bufferTable, true)
    bufferRegion.getUserAttribute.asInstanceOf[GemFireContainer].getNumColumns - 1
  }

  override def getColumnBatchRowCount(itr: PREntriesIterator[_],
      re: AbstractRegionEntry, numColumnsInTable: Int): Int = {
    val numColumns = numColumnsInTable * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
    val currentBucketRegion = itr.getHostedBucketRegion
    if (columnIndex == ColumnFormatEntry.STATROW_COL_INDEX &&
        !re.isDestroyedOrRemoved) {
      val value = re.getValue(currentBucketRegion)
          .asInstanceOf[ColumnFormatValue].getValueRetain(decompress = true, compress = false)
      if (value ne null) {
        val buffer = value.getBuffer
        val baseRowCount = try {
          if (buffer.remaining() > 0) {
            val unsafeRow = Utils.toUnsafeRow(buffer, numColumns)
            unsafeRow.getInt(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)
          } else 0
        } finally {
          value.release()
        }
        // decrement the deleted row count
        val deleteKey = withColumnIndex(ColumnFormatEntry.DELETE_MASK_COL_INDEX)
        val delete = currentBucketRegion.get(deleteKey, null,
          false /* generateCallbacks */ , true /* disableCopyOnRead */ ,
          false /* preferCD */ , null, null, null, null, false /* returnTombstones */ ,
          false /* allowReadFromHDFS */).asInstanceOf[ColumnFormatValue]
            .getValueRetain(decompress = true, compress = false)
        if (delete eq null) baseRowCount
        else {
          val deleteBuffer = delete.getBufferRetain
          try {
            if (deleteBuffer.remaining() > 0) {
              val allocator = ColumnEncoding.getAllocator(deleteBuffer)
              baseRowCount - ColumnEncoding.readInt(allocator.baseObject(deleteBuffer),
                allocator.baseOffset(deleteBuffer) + deleteBuffer.position() + 8)
            } else baseRowCount
          } finally {
            delete.release()
          }
        }
      } else 0
    } else 0
  }

  def getColumnIndex: Int = columnIndex

  private[columnar] def withColumnIndex(columnIndex: Int): ColumnFormatKey =
    new ColumnFormatKey(uuid, partitionId, columnIndex)

  override def hashCode(): Int = Murmur3_x86_32.hashInt(
    ClientResolverUtils.addLongToHashOpt(uuid, columnIndex), partitionId)

  override def equals(obj: Any): Boolean = obj match {
    case k: ColumnFormatKey => uuid == k.uuid &&
        partitionId == k.partitionId && columnIndex == k.columnIndex
    case _ => false
  }

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_KEY

  override def toData(out: DataOutput): Unit = {
    out.writeLong(uuid)
    out.writeInt(partitionId)
    out.writeInt(columnIndex)
  }

  override def fromData(in: DataInput): Unit = {
    uuid = in.readLong()
    partitionId = in.readInt()
    columnIndex = in.readInt()
  }

  override def getSizeInBytes: Int = {
    alignedSize(Sizeable.PER_OBJECT_OVERHEAD +
        8 /* uuid */ + 4 /* columnIndex */ + 4 /* partitionId */)
  }

  override def nCols(): Int = 3

  override def getKeyColumn(index: Int): DataValueDescriptor = index match {
    case 0 => new SQLLongint(uuid)
    case 1 => new SQLInteger(partitionId)
    case 2 => new SQLInteger(columnIndex)
  }

  override def getKeyColumns(keys: Array[DataValueDescriptor]): Unit = {
    keys(0) = new SQLLongint(uuid)
    keys(1) = new SQLInteger(partitionId)
    keys(2) = new SQLInteger(columnIndex)
  }

  override def getKeyColumns(keys: Array[AnyRef]): Unit = {
    keys(0) = Long.box(uuid)
    keys(1) = Int.box(partitionId)
    keys(2) = Int.box(columnIndex)
  }

  override def setRegionContext(region: LocalRegion): Unit = {}

  override def beforeSerializationWithValue(
      valueIsToken: Boolean): KeyWithRegionContext = this

  override def afterDeserializationWithValue(v: AnyRef): Unit = {}

  override def toString: String =
    s"ColumnKey(columnIndex=$columnIndex,partitionId=$partitionId,uuid=$uuid)"
}

/**
 * Partition resolver for the column store.
 */
final class ColumnPartitionResolver(tableName: TableName)
    extends InternalPartitionResolver[ColumnFormatKey, ColumnFormatValue] {

  private val regionPath = tableName.getFullTableNameAsRegionPath

  private lazy val region = Misc.getRegionByPath(regionPath)
      .asInstanceOf[PartitionedRegion]
  private lazy val rootMasterRegion = ColocationHelper.getLeaderRegionName(region)

  override def getName: String = "ColumnPartitionResolver"

  override def getRoutingObject(opDetails: EntryOperation[ColumnFormatKey,
      ColumnFormatValue]): AnyRef = Int.box(opDetails.getKey.partitionId)

  override def getRoutingObject(key: AnyRef, value: AnyRef,
      callbackArg: AnyRef, region: Region[_, _]): AnyRef = {
    Int.box(key.asInstanceOf[ColumnFormatKey].partitionId)
  }

  override val getPartitioningColumns: Array[String] = Array("PARTITIONID")

  override def getPartitioningColumnsCount: Int = 1

  override def getMasterTable(rootMaster: Boolean): String = {
    val master = if (rootMaster) rootMasterRegion else region.getColocatedWithRegion
    if (master ne null) master.getFullPath else null
  }

  override def getDDLString: String =
    s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'"

  override def close(): Unit = {}
}

/**
 * Value object in the column store simply encapsulates binary data as a
 * ByteBuffer. This can be either a direct buffer or a heap buffer depending
 * on the system off-heap configuration. The reason for a separate type is to
 * easily store data off-heap without any major changes to engine otherwise as
 * well as efficiently serialize/deserialize them directly to Oplog/socket.
 *
 * This class extends [[SerializedDiskBuffer]] to avoid a copy when
 * reading/writing from Oplog. Consequently it writes the serialization header
 * itself (typeID + classID + size) into stream as would be written by
 * DataSerializer.writeObject. This helps it avoid additional byte writes when
 * transferring data to the channels.
 */
class ColumnFormatValue extends SerializedDiskBuffer
    with GfxdSerializable with Sizeable {

  @volatile
  @transient protected var columnBuffer = DiskEntry.Helper.NULL_BUFFER

  @GuardedBy("this")
  @transient protected[columnar] var compressionCodecId: Byte = Constant.DEFAULT_CODECID.id.toByte
  /**
   * This keeps track of whether the buffer is compressed or not.
   * In addition it keeps a count of how many times compression was done on
   * the buffer without intervening decompression, and if it exceeds
   * [[ColumnFormatEntry.MAX_CONSECUTIVE_COMPRESSIONS]] and no one is using
   * the decompressed buffer, then replace columnBuffer with compressed version.
   *
   * A negative value indicates that the buffer is not compressible (too small
   * or not enough compression can be achieved), a zero indicates a compressed
   * buffer while a positive count indicates a decompressed buffer and number
   * of times compression was done.
   */
  @GuardedBy("this")
  @transient protected var decompressionState: Byte = -1
  @GuardedBy("this")
  @transient protected var fromDisk: Boolean = false
  @GuardedBy("this")
  @transient protected var diskId: DiskId = _
  @GuardedBy("this")
  @transient protected var regionContext: RegionEntryContext = _

  def this(buffer: ByteBuffer, codecId: Int, isCompressed: Boolean,
      changeOwnerToStorage: Boolean = true) = {
    this()
    setBuffer(buffer, codecId, isCompressed, changeOwnerToStorage)
  }

  def setBuffer(buffer: ByteBuffer, codecId: Int,
      isCompressed: Boolean, changeOwnerToStorage: Boolean = true): Unit = synchronized {
    val columnBuffer = if (changeOwnerToStorage) {
      transferToStorage(buffer, GemFireCacheImpl.getCurrentBufferAllocator)
    } else buffer
    // reference count is required to be 1 at this point
    if (refCount != 1) {
      throw new IllegalStateException(s"Unexpected refCount=$refCount")
    }
    this.columnBuffer = columnBuffer.order(ByteOrder.LITTLE_ENDIAN)
    this.compressionCodecId = codecId.toByte
    this.decompressionState = if (isCompressed) 0 else 1
  }

  private def transferToStorage(buffer: ByteBuffer, allocator: BufferAllocator): ByteBuffer = {
    val newBuffer = allocator.transfer(buffer, DirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
    if (allocator.isManagedDirect) {
      MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
        newBuffer, allowNonAllocator = true)
    }
    newBuffer
  }

  @GuardedBy("this")
  protected final def isCompressed: Boolean = decompressionState == 0

  override final def copyToHeap(owner: String): Unit = synchronized {
    columnBuffer = HeapBufferAllocator.instance().transfer(columnBuffer, owner)
  }

  @inline protected final def duplicateBuffer(buffer: ByteBuffer): ByteBuffer = {
    // slice buffer for non-zero position so callers don't have to deal with it
    if (buffer.position() == 0) buffer.duplicate() else buffer.slice()
  }

  /**
   * Callers of this method should have a corresponding release method
   * for eager release to work else off-heap object may keep around
   * occupying system RAM until the next GC cycle. Callers may decide
   * whether to keep the [[release]] method in a finally block to ensure
   * its invocation, or do it only in normal paths because JVM reference
   * collector will eventually clean it in any case.
   *
   * Calls to this specific class are guaranteed to always return buffers
   * which have position as zero so callers can make simplifying assumptions
   * about the same.
   */
  override final def getBufferRetain: ByteBuffer = {
    val thisValue = getValueRetain(decompress = false, compress = false)
    assert(thisValue == this)
    thisValue.getBuffer
  }

  /**
   * Return the data as a ByteBuffer. Should be invoked only after a [[retain]]
   * or [[getValueRetain]] call.
   */
  override final def getBuffer: ByteBuffer = duplicateBuffer(columnBuffer)

  override def getValueRetain(decompress: Boolean, compress: Boolean): ColumnFormatValue = {
    if (decompress && compress) {
      throw new IllegalArgumentException("both decompress and compress true")
    }
    var diskId: DiskId = null
    var regionContext: RegionEntryContext = null
    synchronized {
      val buffer = this.columnBuffer
      if ((buffer ne DiskEntry.Helper.NULL_BUFFER) && incrementReference()) {
        return transformValue(buffer, decompress, compress)
      }
      diskId = this.diskId
      regionContext = this.regionContext
    }
    // try to read using DiskId
    if (diskId ne null) {
      val dr = regionContext match {
        case r: LocalRegion => r.getDiskRegionView
        case _ => regionContext.asInstanceOf[DiskRegionView]
      }
      dr.acquireReadLock()
      try diskId.synchronized {
        synchronized {
          if ((columnBuffer ne DiskEntry.Helper.NULL_BUFFER) && incrementReference()) {
            return transformValue(columnBuffer, decompress, compress)
          }
          DiskEntry.Helper.getValueOnDiskNoLock(diskId, dr) match {
            case v: ColumnFormatValue =>
              // transfer the buffer from the temporary ColumnFormatValue
              columnBuffer = v.columnBuffer
              decompressionState = v.decompressionState
              fromDisk = true
              // restart reference count from 1
              val refCount = this.refCount
              this.refCount = if (refCount <= 0) 1 else refCount + 1
              return transformValue(columnBuffer, decompress, compress)

            case null | _: Token => // return empty buffer
            case o => throw new IllegalStateException(
              s"unexpected value in column store $o")
          }
        }
      } catch {
        case _: EntryDestroyedException | _: DiskAccessException |
             _: RegionDestroyedException =>
        // These exception types mean that value has disappeared from disk
        // due to compaction or bucket has moved so return empty value.
        // RegionDestroyedException is also ignored since background
        // processors like gateway event processors will not expect it.
      } finally {
        dr.releaseReadLock()
      }
    }
    this
  }

  private def transformValue(buffer: ByteBuffer, decompress: Boolean,
      compress: Boolean): ColumnFormatValue = {
    if (decompress) {
      if (this.decompressionState != 0) {
        if (this.decompressionState > 1) {
          this.decompressionState = 1
        }
        this
      } else {
        // decompress and replace buffer if possible
        val isDirect = buffer.isDirect
        // replace underlying if either no other thread is holding a reference
        // or if this is a heap buffer
        val allocator = GemFireCacheImpl.getCurrentBufferAllocator
        val decompressed = Utils.codecDecompress(buffer, allocator)
        val newValue = copy(decompressed, isCompressed = false, changeOwnerToStorage = false)
        if (!isDirect || this.refCount == 1) {
          val updateStats = (regionContext ne null) && !fromDisk
          if (updateStats && !isDirect) {
            StoreCallbacksImpl.acquireStorageMemory(regionContext.getFullPath,
              decompressed.capacity() - buffer.capacity(), buffer = null,
              offHeap = false, shouldEvict = true)
          }
          this.columnBuffer = transferToStorage(decompressed, allocator)
          this.decompressionState = 1
          if (isDirect) {
            UnsafeHolder.releaseDirectBuffer(buffer)
          }
          // update the statistics
          if (updateStats) {
            regionContext.updateMemoryStats(this, newValue)
          }
          this
        } else newValue
      }
    } else if (compress) {
      // a negative value indicates that the buffer is not compressible (either too
      // small or minimum compression ratio is not achieved)
      if (this.decompressionState <= 0) this
      else {
        // compress buffer if required
        if (compressionCodecId != CompressionCodecId.None.id) {
          val isDirect = buffer.isDirect
          // check if stored buffer should also be replaced (if no other thread
          // is holding a reference or this is a heap buffer)
          val maxCompressionsExceeded = this.decompressionState >
              ColumnFormatEntry.MAX_CONSECUTIVE_COMPRESSIONS
          val allocator = GemFireCacheImpl.getCurrentBufferAllocator
          val compressed = Utils.codecCompress(compressionCodecId, buffer,
            buffer.remaining(), allocator)
          if (compressed ne buffer) {
            val newValue = copy(compressed, isCompressed = true, changeOwnerToStorage = false)
            if (maxCompressionsExceeded && (!isDirect || this.refCount == 1)) {
              // trim to size if there is wasted space
              val size = compressed.limit()
              val newBuffer = if (compressed.capacity() >= size + 32) {
                val trimmed = allocator.allocateForStorage(size).order(ByteOrder.LITTLE_ENDIAN)
                trimmed.put(compressed)
                if (isDirect) {
                  UnsafeHolder.releaseDirectBuffer(compressed)
                }
                trimmed
              } else transferToStorage(compressed, allocator)
              this.columnBuffer = newBuffer
              this.decompressionState = 0
              if (isDirect) {
                UnsafeHolder.releaseDirectBuffer(buffer)
              }
              // update the statistics
              if ((regionContext ne null) && !fromDisk) {
                if (!isDirect) {
                  StoreCallbacksImpl.releaseStorageMemory(regionContext.getFullPath,
                    buffer.capacity() - newBuffer.capacity(), offHeap = false)
                }
                regionContext.updateMemoryStats(this, newValue)
              }
              this
            } else {
              if (!maxCompressionsExceeded) {
                this.decompressionState = (this.decompressionState + 1).toByte
              }
              newValue
            }
          } else {
            // mark that buffer is not compressible to avoid more attempts
            this.decompressionState = -1
            this
          }
        } else this
      }
    } else this
  }

  // always true because compressed/uncompressed transitions need proper locking
  override final def needsRelease: Boolean = true

  override protected def releaseBuffer(): Unit = {
    // Remove the buffer at this point. Any further reads will need to be
    // done either using DiskId, or will return empty if no DiskId is available
    val buffer = this.columnBuffer
    if (buffer.isDirect) {
      this.columnBuffer = DiskEntry.Helper.NULL_BUFFER
      this.decompressionState = -1
      this.fromDisk = false
      DirectBufferAllocator.instance().release(buffer)
    }
  }

  protected def copy(buffer: ByteBuffer, isCompressed: Boolean,
      changeOwnerToStorage: Boolean): ColumnFormatValue = synchronized {
    new ColumnFormatValue(buffer, compressionCodecId, isCompressed, changeOwnerToStorage)
  }

  override final def setDiskLocation(id: DiskId,
      context: RegionEntryContext): Unit = synchronized {
    if (id ne null) {
      this.diskId = id
      // set/update diskRegion only if incoming value has been provided
      if (context ne null) {
        this.regionContext = context
        this.compressionCodecId = CompressionCodecId.fromName(
          context.getColumnCompressionCodec).id.toByte
      }
    } else {
      this.diskId = null
      this.regionContext = null
    }
  }

  override final def write(channel: OutputStreamChannel): Unit = {
    // write the pre-serialized buffer as is
    // Oplog layer will get compressed form by calling getValueRetain(false, true)
    val buffer = getBufferRetain
    try {
      // first write the serialization header
      // write the typeId + classId and size
      channel.write(DSCODE.DS_FIXED_ID_BYTE)
      channel.write(DataSerializableFixedID.GFXD_TYPE)
      channel.write(getGfxdID)
      channel.write(0.toByte) // padding
      channel.writeInt(buffer.limit())

      // no need to change position back since this is a duplicate ByteBuffer
      write(channel, buffer)
    } finally {
      release()
    }
  }

  override final def writeSerializationHeader(src: ByteBuffer,
      writeBuf: ByteBuffer): Boolean = {
    if (writeBuf.remaining() >= 8) {
      writeBuf.put(DSCODE.DS_FIXED_ID_BYTE)
      writeBuf.put(DataSerializableFixedID.GFXD_TYPE)
      writeBuf.put(getGfxdID)
      writeBuf.put(0.toByte) // padding
      if (writeBuf.order() eq ByteOrder.BIG_ENDIAN) {
        writeBuf.putInt(src.remaining())
      } else {
        writeBuf.putInt(Integer.reverseBytes(src.remaining()))
      }
      true
    } else false
  }

  override final def channelSize(): Int = 8 /* header */ + columnBuffer.remaining()

  override final def size(): Int = columnBuffer.remaining()

  override final def getDSFID: Int = DataSerializableFixedID.GFXD_TYPE

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_VALUE

  override def getSerializationVersions: Array[Version] = null

  override def toData(out: DataOutput): Unit = {
    // avoid forced compression for localhost connection but still send
    // compressed if already compressed to avoid potentially unnecessary work
    var outputStreamChannel: OutputStreamChannel = null
    val writeValue = out match {
      case channel: OutputStreamChannel =>
        outputStreamChannel = channel
        getValueRetain(decompress = false, !channel.isSocketToSameHost)
      case _ => getValueRetain(decompress = false, compress = true)
    }
    val buffer = writeValue.getBuffer
    try {
      val numBytes = buffer.limit()
      out.writeByte(0) // padding for 8-byte alignment
      out.writeInt(numBytes)
      if (numBytes > 0) {
        if (outputStreamChannel ne null) {
          write(outputStreamChannel, buffer)
        } else out match {
          case hdos: HeapDataOutputStream =>
            hdos.write(buffer)

          case _ =>
            val allocator = ColumnEncoding.getAllocator(buffer)
            out.write(allocator.toBytes(buffer))
        }
      }
    } finally {
      writeValue.release()
    }
  }

  override def fromData(in: DataInput): Unit = {
    // skip padding
    in.readByte()
    val numBytes = in.readInt()
    if (numBytes > 0) {
      val allocator = GemFireCacheImpl.getCurrentBufferAllocator
      var buffer = in match {
        case din: ByteBufferDataInput =>
          // just transfer the internal buffer; higher layer (e.g. BytesAndBits)
          // will take care not to release this buffer (if direct);
          // buffer is already positioned at start of data
          val buffer = allocator.transfer(din.getInternalBuffer,
            DirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
          if (buffer.isDirect) {
            MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
              buffer, allowNonAllocator = true)
          }
          buffer

        case channel: InputStreamChannel =>
          val buffer = allocator.allocateForStorage(numBytes)
          var numTries = 0
          do {
            if (channel.read(buffer) == 0) {
              // wait for a bit after some retries (no timeout)
              numTries += 1
              ClientSharedUtils.parkThreadForAsyncOperationIfRequired(channel, 0L, numTries)
            }
          } while (buffer.hasRemaining)
          // move to the start of data
          buffer.rewind()
          buffer

        case _ =>
          // order is BIG_ENDIAN by default
          val bytes = new Array[Byte](numBytes)
          in.readFully(bytes, 0, numBytes)
          allocator.fromBytesToStorage(bytes, 0, numBytes)
      }
      buffer = buffer.order(ByteOrder.LITTLE_ENDIAN)
      val codecId = -buffer.getInt(buffer.position())
      val isCompressed = codecId > 0
      // owner is already marked for storage
      // if not compressed set the default codecId while the actual one will be
      // set when the value is placed in region (in setDiskLocation) that will
      // be used in further toData calls if requuired
      setBuffer(buffer, if (isCompressed) codecId else Constant.DEFAULT_CODECID.id,
        isCompressed, changeOwnerToStorage = false)
    } else {
      this.columnBuffer = DiskEntry.Helper.NULL_BUFFER
      this.decompressionState = -1
      this.fromDisk = false
    }
  }

  override def getSizeInBytes: Int = {
    // Cannot use ReflectionObjectSizer to get estimate especially for direct
    // buffer which has a reference queue all of which gets counted incorrectly.
    // Returns instantaneous size by design and not synchronized
    // (or retain/release) with capacity being valid even after releaseBuffer.
    val buffer = columnBuffer
    if (buffer.isDirect) {
      val freeMemorySize = Sizeable.PER_OBJECT_OVERHEAD + 8
      /* address */
      val cleanerSize = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 7
      /* next, prev, thunk in Cleaner, 4 in Reference */
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 4 /* hb, att, cleaner, fd */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8
      /* address */
      val size = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 3 /* BB, DiskId, DiskRegion */
      alignedSize(size) + alignedSize(bbSize) +
          alignedSize(cleanerSize) + alignedSize(freeMemorySize)
    } else {
      val hbSize = Sizeable.PER_OBJECT_OVERHEAD + 4 /* length */ +
          buffer.capacity()
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* hb */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8
      /* unused address */
      val size = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 3 /* BB, DiskId, DiskRegion */
      alignedSize(size) + alignedSize(bbSize) + alignedSize(hbSize)
    }
  }

  override def getOffHeapSizeInBytes: Int = {
    // Returns instantaneous size by design and not synchronized
    // (or retain/release) with capacity being valid even after releaseBuffer.
    val buffer = columnBuffer
    if (buffer.isDirect) {
      buffer.capacity() + DirectBufferAllocator.DIRECT_OBJECT_OVERHEAD
    } else 0
  }

  override def toString: String = {
    val buffer = getBuffer
    s"ColumnValue[size=${buffer.remaining()} $buffer diskId=$diskId context=$regionContext]"
  }
}
