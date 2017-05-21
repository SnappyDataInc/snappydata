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
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer
import java.sql.{Blob, Connection, PreparedStatement, ResultSet, Statement}

import com.gemstone.gemfire.internal.cache.{BucketRegion, LocalRegion, NonLocalRegionEntry}
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder
import com.pivotal.gemfirexd.internal.engine.store.{CompactCompositeKey, CompactCompositeRegionKey, GemFireContainer}
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, RowLocation, SQLInteger}
import io.snappydata.thrift.common.BufferedBlob
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import org.apache.spark.sql.execution.PartitionedPhysicalScan
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.{Logging, TaskContext}

import scala.language.implicitConversions

abstract class ResultSetIterator[A](conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends Iterator[A] with Logging {

  protected[this] final var doMove = true

  protected[this] final var hasNextValue: Boolean = rs ne null

  if (context ne null) {
    context.addTaskCompletionListener { _ => {
      logDebug("closed connection for task from listener " + context.partitionId())
      close()
    }
    }
  }

  override final def hasNext: Boolean = {
    var success = false
    try {
      if (doMove && hasNextValue) {
        success = rs.next()
        doMove = false
        success
      } else {
        success = hasNextValue
        success
      }
    } finally {
      if (!success) {
        hasNextValue = false
        //logDebug("closed connection for task due to failure")
        //close()
      }
    }
  }

  override final def next(): A = {
    if (doMove) {
      hasNext
      doMove = true
      if (!hasNextValue) return null.asInstanceOf[A]
    }
    val result = getCurrentValue
    doMove = true
    result
  }

  protected def getCurrentValue: A

  def close() {
    //if (!hasNextValue) return
    try {
      // GfxdConnectionWrapper.restoreContextStack(stmt, rs)
      // rs.lightWeightClose()
      rs.close()
    } catch {
      case e: Exception => logWarning("Exception closing resultSet", e)
    }
    try {
      stmt.close()
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      conn.commit()
      conn.close()
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    hasNextValue = false
  }
}

case class ColumnBatch(numRows: Int, buffers: Array[ByteBuffer],
    statsData: Array[Byte])

object ColumnBatchIterator {

  def apply(container: GemFireContainer,
      bucketIds: java.util.Set[Integer]): ColumnBatchIterator = {
    new ColumnBatchIterator(container, bucketIds, null)

  }

  def apply(batch: ColumnBatch): ColumnBatchIterator = {
    new ColumnBatchIterator(null, null, batch)
  }
}

final class ColumnBatchIterator(container: GemFireContainer,
    bucketIds: java.util.Set[Integer], val batch: ColumnBatch)
    extends PRValuesIterator[Array[Byte]](container, bucketIds) {

  if (container ne null){
    assert(!container.isOffHeap,
      s"Unexpected byte[][] iterator call for off-heap $container")
  }

  protected var currentVal: Array[Byte] = _
  var currentKeyUUID: DataValueDescriptor = _
  var currentKeyPartitionId: DataValueDescriptor = _
  var currentBucketRegion: BucketRegion = _
  val baseRegion: LocalRegion = if (container ne null) container.getRegion else null
  var batchProcessed = false

  def getColumnLob(bufferPosition: Int): ByteBuffer = {
    if (container ne null) {
      val key = new CompactCompositeRegionKey(Array(
        currentKeyUUID, currentKeyPartitionId, new SQLInteger(bufferPosition)),
        container.getExtraTableInfo())
      val rl = if (currentBucketRegion != null) currentBucketRegion.get(key)
      else baseRegion.get(key)
      val value = rl.asInstanceOf[Array[Array[Byte]]]
      val rf = container.getRowFormatter(value(0))
      ByteBuffer.wrap(rf.getLob(value, PartitionedPhysicalScan.CT_BLOB_POSITION))
    } else {
      batch.buffers(bufferPosition - 1)
    }
  }

  override protected def moveNext(): Unit = {
    while ((container ne null) && itr.hasNext) {
      val rl = itr.next().asInstanceOf[RowLocation]
      if(!rl.isDestroyedOrRemoved) {
        currentBucketRegion = itr.getHostedBucketRegion
        // get the stat row region entries only. region entries for individual columns
        // will be fetched on demand
        if ((currentBucketRegion ne null) || rl.isInstanceOf[NonLocalRegionEntry]) {
          val key = rl.getKeyCopy.asInstanceOf[CompactCompositeKey]
          if (key.getKeyColumn(2).getInt ==
            JDBCSourceAsStore.STATROW_COL_INDEX) {
            val v = if (currentBucketRegion != null) currentBucketRegion.get(key)
            else baseRegion.get(key)
            if (v ne null) {
              val value = v.asInstanceOf[Array[Array[Byte]]]
              currentKeyUUID = key.getKeyColumn(0)
              currentKeyPartitionId = key.getKeyColumn(1)
              val rowFormatter = container.getRowFormatter(value(0))
              currentVal = rowFormatter.getLob(value, PartitionedPhysicalScan.CT_BLOB_POSITION)
              return
            }
          }
        }
      }
    }
    if ((container eq null) && !batchProcessed) {
      currentVal = batch.statsData
      batchProcessed = true
      return
    }
    hasNextValue = false
  }
}

final class ColumnBatchIteratorOnRS(conn: Connection,
    requiredColumns: Array[String],
    stmt: Statement, rs: ResultSet,
    context: TaskContext,
    fetchColQuery: String)
    extends ResultSetIterator[Array[Byte]](conn, stmt, rs, context) {
  var currentUUID: String = _
  val ps: PreparedStatement = conn.prepareStatement(fetchColQuery)
  var colBuffers: Option[Int2ObjectOpenHashMap[(ByteBuffer, Blob)]] = None

  def getColumnLob(bufferPosition: Int): ByteBuffer = {
    colBuffers match {
      case Some(map) => map.get(bufferPosition)._1
      case None =>
        for (i <- requiredColumns.indices) {
          ps.setString(i + 1, currentUUID)
        }
        val colIter = ps.executeQuery()
        val bufferMap = new Int2ObjectOpenHashMap[(ByteBuffer, Blob)]()
        var index = 1
        while (colIter.next()) {
          val colBlob = colIter.getBlob(1)
          val colBuffer = colBlob match {
            case blob: BufferedBlob => blob.getAsBuffer
            case blob => ByteBuffer.wrap(blob.getBytes(
              1, blob.length().asInstanceOf[Int]))
          }
          bufferMap.put(index, (colBuffer, colBlob))
          index = index + 1
        }
        colBuffers = Some(bufferMap)

        bufferMap.get(bufferPosition)._1
    }
  }

  override protected def getCurrentValue: Array[Byte] = {
    currentUUID = rs.getString(2)
    colBuffers match {
      case Some(buffers) =>
        val values = buffers.values().iterator()
        while (values.hasNext) {
          val (buffer, blob) = values.next()
          blob.free()
          // release previous set of buffers immediately
          UnsafeHolder.releaseIfDirectBuffer(buffer)
        }
      case None =>
    }
    colBuffers = None
    val statsData = rs.getBlob(1)
    val statsBytes = statsData.getBytes(1, statsData.length().asInstanceOf[Int])
    statsData.free()
    statsBytes
  }

  override def close(): Unit = {
    colBuffers match {
      case Some(buffers) =>
        val values = buffers.values().iterator()
        while (values.hasNext) {
          val (buffer, blob) = values.next()
          try {
            blob.free()
          } catch {
            case e: Exception => logWarning("Exception clearing Blob", e)
          }
          // release last set of buffers immediately
          UnsafeHolder.releaseIfDirectBuffer(buffer)
        }
      case None =>
    }
    super.close()
  }
}

object JDBCSourceAsStore {
  val STATROW_COL_INDEX: Int = -1
}
