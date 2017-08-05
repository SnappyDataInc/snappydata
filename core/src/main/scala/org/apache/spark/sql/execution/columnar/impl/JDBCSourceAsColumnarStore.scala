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
package org.apache.spark.sql.execution.columnar.impl

import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, Statement}
import java.util.UUID

import scala.annotation.meta.param
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, LocalRegion, PartitionedRegion, TXManagerImpl}
import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder
import com.pivotal.gemfirexd.internal.engine.{GfxdConstants, Misc}
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnectionContext
import io.snappydata.impl.SparkShellRDDHelper
import io.snappydata.thrift.internal.ClientBlob

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{ConnectionPropertiesSerializer, StructTypeSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{DynamicReplacableConstant, ParamLiteral}
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteDelta
import org.apache.spark.sql.execution.row.{ResultSetTraversal, RowFormatScanRDD, RowInsertExec}
import org.apache.spark.sql.execution.{BufferedRowIterator, ConnectionPool, RDDKryo, WholeStageCodegenExec}
import org.apache.spark.sql.hive.ConnectorCatalog
import org.apache.spark.sql.sources.{ConnectionProperties, Filter}
import org.apache.spark.sql.store.{CodeGeneration, StoreUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyContext, SnappySession, SparkSession, ThinClientConnectorMode}
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, Partition, TaskContext}

/**
 * Column Store implementation for GemFireXD.
 */
class JDBCSourceAsColumnarStore(private var _connProperties: ConnectionProperties,
    var numPartitions: Int, private var _tableName: String, var schema: StructType)
    extends ExternalStore with KryoSerializable with Logging {

  self =>

  @transient protected lazy val rand = new XORShiftRandom

  override final def tableName: String = _tableName

  override final def connProperties: ConnectionProperties = _connProperties

  private lazy val connectionType = ExternalStoreUtils.getConnectionType(
    connProperties.dialect)

  override def write(kryo: Kryo, output: Output): Unit = {
    ConnectionPropertiesSerializer.write(kryo, output, _connProperties)
    output.writeInt(numPartitions)
    output.writeString(_tableName)
    StructTypeSerializer.write(kryo, output, schema)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    _connProperties = ConnectionPropertiesSerializer.read(kryo, input)
    numPartitions = input.readInt()
    _tableName = input.readString()
    schema = StructTypeSerializer.read(kryo, input, c = null)
  }

  override def storeColumnBatch(tableName: String, batch: ColumnBatch,
      partitionId: Int, batchId: Option[String], maxDeltaRows: Int)
      (implicit conn: Option[Connection] = None): Unit = {
    doInsertOrPut(tableName, batch, batchId, getPartitionID(tableName, partitionId),
      maxDeltaRows)(conn)
  }

  // begin should decide the connection which will be used by insert/commit/rollback
  def beginTx(): Array[_ <: Object] = {
    implicit val conn = self.getConnection(tableName, onExecutor = true)

    assert(!conn.isClosed)
    tryExecute(tableName, closeOnSuccessOrFailure = false, onExecutor = true) {
      (conn: Connection) => {
        connectionType match {
          case ConnectionType.Embedded =>
            val txMgr = Misc.getGemFireCache.getCacheTransactionManager
            if (TXManagerImpl.snapshotTxState.get() == null && (txMgr.getTXState == null)) {
              txMgr.begin(com.gemstone.gemfire.cache.IsolationLevel.SNAPSHOT, null)
              Array(conn, txMgr.getTransactionId.stringFormat())
            } else {
              Array(conn, null)
            }
          case _ =>
            val txId = SparkShellRDDHelper.snapshotTxIdForWrite.get
            if (txId == null) {
              logDebug("Going to start the transaction on server ")
              val startAndGetSnapshotTXId = conn.prepareCall(s"call sys.START_SNAPSHOT_TXID (?)")
              startAndGetSnapshotTXId.registerOutParameter(1, java.sql.Types.VARCHAR)
              startAndGetSnapshotTXId.execute()
              val txid: String = startAndGetSnapshotTXId.getString(1)
              startAndGetSnapshotTXId.close()
              SparkShellRDDHelper.snapshotTxIdForWrite.set(txid)
              logDebug(s"The snapshot tx id is $txid and tablename is $tableName")
              Array(conn, txid)
            } else {
              // it should always be not null.
              if (!txId.equals("null")) {
                val statement = conn.createStatement()
                statement.execute(
                  s"call sys.USE_SNAPSHOT_TXID('$txId')")
              }
              Array(conn, null)
            }
        }
      }
    } (implicitly, Some(conn))
  }

  def commitTx(txId: String)(implicit conn: Option[Connection]): Unit = {
    // noinspection RedundantDefaultArgument
    tryExecute(tableName, closeOnSuccessOrFailure = true, onExecutor = true) {
      (conn: Connection) => {
        connectionType match {
          case ConnectionType.Embedded =>
            // if(SparkShellRDDHelper.snapshotTxIdForRead.get)
            Misc.getGemFireCache.getCacheTransactionManager.commit()
          case _ =>
            logDebug(s"Going to commit $txId the transaction on server ")
            val ps = conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
            ps.setString(1, if (txId == null) "null" else txId)
            ps.executeUpdate()
            logDebug(s"The txid being committed is $txId")
            ps.close()
            SparkShellRDDHelper.snapshotTxIdForWrite.set(null)
            logDebug(s"Committed $txId the transaction on server ")
        }
      }
    }(implicitly, conn)
  }


  def rollbackTx(txId: String)(implicit conn: Option[Connection]): Unit = {
    // noinspection RedundantDefaultArgument
    tryExecute(tableName, closeOnSuccessOrFailure = true, onExecutor = true) {
      (conn: Connection) => {
        connectionType match {
          case ConnectionType.Embedded =>
            Misc.getGemFireCache.getCacheTransactionManager.rollback()
          case _ =>
            logDebug(s"Going to rollback $txId the transaction on server ")
            val ps = conn.prepareStatement(s"call sys.ROLLBACK_SNAPSHOT_TXID(?)")
            ps.setString(1, if (txId == null) "null" else txId)
            ps.executeUpdate()
            logDebug(s"The txid being rolledback is $txId")
            ps.close()
            SparkShellRDDHelper.snapshotTxIdForWrite.set(null)
            logDebug(s"Rolled back $txId the transaction on server ")
        }
      }
    }(implicitly, conn)
  }

  override def storeDelete(tableName: String, buffer: ByteBuffer,
      statsData: Array[Byte], partitionId: Int, batchId: String): Unit = {
    val allocator = GemFireCacheImpl.getCurrentBufferAllocator
    val statsBuffer = createStatsBuffer(statsData, allocator)
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable[ColumnFormatKey,
            ColumnFormatValue](tableName, true)
        val keyValues = new java.util.HashMap[ColumnFormatKey, ColumnFormatValue](2)
        var key = new ColumnFormatKey(partitionId,
          ColumnFormatEntry.DELETE_MASK_COL_INDEX, batchId)
        val value = new ColumnDeleteDelta(buffer)
        keyValues.put(key, value)

        // add the stats row
        key = new ColumnFormatKey(partitionId,
          ColumnFormatEntry.DELTA_STATROW_COL_INDEX, batchId)
        val statsValue = new ColumnDelta(statsBuffer)
        keyValues.put(key, statsValue)

        region.putAll(keyValues)

      case _ =>
        // TODO: SW: temporarily made close=true for delete
        // noinspection RedundantDefaultArgument
        tryExecute(tableName, closeOnSuccessOrFailure = true, onExecutor = true) { connection =>
          val deleteStr = getRowInsertOrPutStr(tableName, isPut = true)
          val stmt = connection.prepareStatement(deleteStr)
          var blob: ClientBlob = null
          try {
            stmt.setString(1, batchId)
            stmt.setInt(2, partitionId)
            stmt.setInt(3, ColumnFormatEntry.DELETE_MASK_COL_INDEX)
            blob = new ClientBlob(buffer, true)
            stmt.setBlob(4, blob)
            stmt.addBatch()

            // add the stats row
            stmt.setString(1, batchId)
            stmt.setInt(2, partitionId)
            stmt.setInt(3, ColumnFormatEntry.DELTA_STATROW_COL_INDEX)
            blob = new ClientBlob(statsBuffer, true)
            stmt.setBlob(4, blob)

            stmt.executeBatch()
            stmt.close()
          } finally {
            // free the blob
            if (blob != null) {
              try {
                blob.free()
              } catch {
                case NonFatal(_) => // ignore
              }
            }
          }
        }
    }
  }

  def closeConnection(c: Option[Connection]): Unit = {
    c match {
      case Some(conn) if !conn.isClosed =>
        connectionType match {
          case ConnectionType.Embedded => // Do nothing
          case _ =>
            // it should always be not null.
            // get clears the state from connection
            // the tx would have been committed earlier
            // or it will be committed later
            val txId = SparkShellRDDHelper.snapshotTxIdForWrite.get
            if (txId != null && !txId.equals("null")) {
              val statement = conn.createStatement()
              statement.execute(
                s"call sys.GET_SNAPSHOT_TXID('$txId')")
            }
            // conn commit removed txState from the conn context.
            conn.commit()
            conn.close()
        }
      case _ => // Do nothing
    }
  }

  private def createStatsBuffer(statsData: Array[Byte],
      allocator: BufferAllocator): ByteBuffer = {
    // need to create a copy since underlying Array[Byte] can be re-used
    val statsLen = statsData.length
    val statsBuffer = allocator.allocateForStorage(statsLen)
    statsBuffer.put(statsData, 0, statsLen)
    statsBuffer.rewind()
    statsBuffer
  }

  /**
   * Insert the base entry and n column entries in Snappy. Insert the base entry
   * in the end to ensure that the partial inserts of a cached batch are ignored
   * during iteration. We are not cleaning up the partial inserts of cached
   * batches for now.
   */
  private def doSnappyInsertOrPut(tableName: String, region: LocalRegion, batch: ColumnBatch,
      batchId: Option[String], partitionId: Int, maxDeltaRows: Int): Unit = {
    val deltaUpdate = batch.deltaIndexes ne null
    val statRowIndex = if (deltaUpdate) ColumnFormatEntry.DELTA_STATROW_COL_INDEX
    else ColumnFormatEntry.STATROW_COL_INDEX
    var index = 1
    val uuid = batchId.getOrElse(UUID.randomUUID().toString)
    try {
      // add key-values pairs for each column
      val keyValues = new java.util.HashMap[ColumnFormatKey, ColumnFormatValue](
        batch.buffers.length + 1)
      batch.buffers.foreach { buffer =>
        val columnIndex = if (deltaUpdate) batch.deltaIndexes(index - 1) else index
        val key = new ColumnFormatKey(partitionId, columnIndex, uuid)
        val value = if (deltaUpdate) new ColumnDelta(buffer) else new ColumnFormatValue(buffer)
        keyValues.put(key, value)
        index += 1
      }
      // add the stats row
      val key = new ColumnFormatKey(partitionId, statRowIndex, uuid)
      val allocator = Misc.getGemFireCache.getBufferAllocator
      val statsBuffer = createStatsBuffer(batch.statsData, allocator)
      val value = if (deltaUpdate) new ColumnDelta(statsBuffer)
      else new ColumnFormatValue(statsBuffer)
      keyValues.put(key, value)

      // do a putAll of the key-value map
      region.putAll(keyValues)
    } catch {
      // TODO: test this code
      // should be rolled back. so no need to delete.
      case NonFatal(e) =>
        // delete the base entry first
        val key = new ColumnFormatKey(partitionId, statRowIndex, uuid)
        try {
          region.destroy(key)
        } catch {
          case NonFatal(_) => // ignore
        }
        // delete the column entries
        batch.buffers.indices.foreach { index =>
          val columnIndex = if (deltaUpdate) batch.deltaIndexes(index) else index + 1
          val key = new ColumnFormatKey(partitionId, columnIndex, uuid)
          try {
            region.destroy(key)
          } catch {
            case NonFatal(_) => // ignore
          }
        }
        throw e
    }
  }

  /**
   * Insert the base entry and n column entries in Snappy. Insert the base entry
   * in the end to ensure that the partial inserts of a cached batch are ignored
   * during iteration. We are not cleaning up the partial inserts of cached
   * batches for now.
   */
  private def doGFXDInsertOrPut(tableName: String, batch: ColumnBatch,
      batchId: Option[String], partitionId: Int,
      maxDeltaRows: Int): (Connection => Any) = {
    {
      (connection: Connection) => {
        val deltaUpdate = batch.deltaIndexes ne null
        // we are using the same connection on which tx was started.
        val rowInsertStr = getRowInsertOrPutStr(tableName, deltaUpdate)
        val stmt = connection.prepareStatement(rowInsertStr)
        val statRowIndex = if (deltaUpdate) ColumnFormatEntry.DELTA_STATROW_COL_INDEX
        else ColumnFormatEntry.STATROW_COL_INDEX
        var index = 1
        val uuid = batchId.getOrElse(UUID.randomUUID().toString)
        var blobs: Array[ClientBlob] = null
        try {
          // add the columns
          blobs = batch.buffers.map(buffer => {
            val columnIndex = if (deltaUpdate) batch.deltaIndexes(index - 1) else index
            stmt.setString(1, uuid)
            stmt.setInt(2, partitionId)
            stmt.setInt(3, columnIndex)
            val blob = new ClientBlob(buffer, true)
            stmt.setBlob(4, blob)
            index += 1
            stmt.addBatch()
            blob
          })
          // add the stat row
          stmt.setString(1, uuid)
          stmt.setInt(2, partitionId)
          stmt.setInt(3, statRowIndex)
          val allocator = GemFireCacheImpl.getCurrentBufferAllocator
          val statsBuffer = createStatsBuffer(batch.statsData, allocator)
          stmt.setBlob(4, new ClientBlob(statsBuffer, true))
          stmt.addBatch()

          stmt.executeBatch()
          stmt.close()
        } catch {
          // TODO: test this code
          // TODO:Suranjan This code is no needed now.
          // Need to rollback.
          // we can't do this for only this cachedbatch
          // all the inserts in this partitionId will be rolled back.
          case NonFatal(e) =>
            val deletestmt = connection.prepareStatement(
              s"delete from $tableName where partitionId = $partitionId " +
                  s" and uuid = ? and columnIndex = ? ")
            // delete the base entry
            try {
              deletestmt.setString(1, uuid)
              deletestmt.setInt(2, statRowIndex)
              deletestmt.executeUpdate()
            } catch {
              case NonFatal(_) => // Do nothing
            }

            for (idx <- 1 to batch.buffers.length) {
              try {
                val columnIndex = if (deltaUpdate) batch.deltaIndexes(idx - 1) else idx
                deletestmt.setString(1, uuid)
                deletestmt.setInt(2, columnIndex)
                deletestmt.executeUpdate()
              } catch {
                case NonFatal(_) => // Do nothing
              }
            }
            deletestmt.close()
            throw e
        } finally {
          // free the blobs
          if (blobs != null) {
            for (blob <- blobs) {
              try {
                blob.free()
              } catch {
                case NonFatal(_) => // ignore
              }
            }
          }
        }
      }
    }
  }

  protected def getRowInsertOrPutStr(tableName: String, isPut: Boolean): String = {
    if (isPut) s"put into $tableName values (?, ?, ?, ?)"
    else s"insert into $tableName values (?, ?, ?, ?)"
  }

  override def getConnection(id: String, onExecutor: Boolean): Connection = {
    connectionType match {
      case ConnectionType.Embedded =>
        val currentCM = ContextService.getFactory.getCurrentContextManager
        if (currentCM ne null) {
          val conn = EmbedConnectionContext.getEmbedConnection(currentCM)
          if (conn ne null) return conn
        }
      case _ => // get pooled connection
    }

    val connProps = if (onExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
  }

  override def getConnectedExternalStore(table: String,
      onExecutor: Boolean): ConnectedExternalStore =
    new JDBCSourceAsColumnarStore(connProperties, numPartitions, tableName, schema)
        with ConnectedExternalStore {
      @transient protected[this] override val connectedInstance: Connection =
        self.getConnection(table, onExecutor)
    }

  override def getColumnBatchRDD(tableName: String,
      rowBuffer: String,
      requiredColumns: Array[String],
      prunePartitions: => Int,
      session: SparkSession,
      schema: StructType): RDD[Any] = {
    val snappySession = session.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD(snappySession,
          tableName, prunePartitions, this)
      case _ =>
        // remove the url property from poolProps since that will be
        // partition-specific
        val poolProps = connProperties.poolProps -
            (if (connProperties.hikariCP) "jdbcUrl" else "url")

        val (parts, embdClusterRelDestroyVersion) =
          SnappyContext.getClusterMode(session.sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
            val catalog = snappySession.sessionCatalog.asInstanceOf[ConnectorCatalog]
            val relInfo = catalog.getCachedRelationInfo(catalog.newQualifiedTableName(rowBuffer))
            (relInfo.partitions, relInfo.embdClusterRelDestroyVersion)
          case _ =>
            (Array.empty[Partition], -1)
        }

        new SmartConnectorColumnRDD(snappySession,
          tableName, requiredColumns, ConnectionProperties(connProperties.url,
            connProperties.driver, connProperties.dialect, poolProps,
            connProperties.connProps, connProperties.executorConnProps,
            connProperties.hikariCP), schema, this, parts, embdClusterRelDestroyVersion)
    }
  }

  private def doInsertOrPut(columnTableName: String, batch: ColumnBatch, batchId: Option[String],
      partitionId: Int, maxDeltaRows: Int) (implicit conn: Option[Connection] = None): Unit = {
    // split the batch and put into row buffer if it is small
    if (maxDeltaRows > 0 && batch.numRows < math.max(maxDeltaRows / 10,
      GfxdConstants.SNAPPY_MIN_COLUMN_DELTA_ROWS)) {
      // TODO: SW: temporarily made close=true for updates
      // noinspection RedundantDefaultArgument
      tryExecute(tableName, closeOnSuccessOrFailure = batch.deltaIndexes ne null,
        onExecutor = true)(doInsertOrPutImpl(tableName, batch, batchId, partitionId,
        maxDeltaRows))(implicitly, conn)
    } else {
      connectionType match {
        case ConnectionType.Embedded =>
          val region = Misc.getRegionForTable(columnTableName, true).asInstanceOf[PartitionedRegion]
          val batchID = Some(batchId.getOrElse(region.newJavaUUID().toString))
          doSnappyInsertOrPut(columnTableName, region, batch, batchID, partitionId, maxDeltaRows)

        case _ =>
          // TODO: SW: temporarily made close=true for updates
          // noinspection RedundantDefaultArgument
          tryExecute(tableName, closeOnSuccessOrFailure = batch.deltaIndexes ne null,
            onExecutor = true)(doGFXDInsertOrPut(columnTableName, batch, batchId, partitionId,
            maxDeltaRows))(implicitly, conn)
      }
    }
  }

  private def doInsertOrPutImpl(columnTableName: String, batch: ColumnBatch,
      batchId: Option[String], partitionId: Int,
      maxDeltaRows: Int): (Connection => Any) = {
    (connection: Connection) => {
      val gen = CodeGeneration.compileCode(
        tableName + ".COLUMN_TABLE.DECOMPRESS", schema.fields, () => {
          val schemaAttrs = schema.toAttributes
          val tableScan = ColumnTableScan(schemaAttrs, dataRDD = null,
            otherRDDs = Seq.empty, numBuckets = -1,
            partitionColumns = Seq.empty, partitionColumnAliases = Seq.empty,
            baseRelation = null, schema, allFilters = Seq.empty, schemaAttrs,
            caseSensitive = true)
          val insertPlan = RowInsertExec(tableScan, putInto = true,
            Seq.empty, Seq.empty, -1, schema, None, onExecutor = true,
            tableName, connProperties)
          // now generate the code with the help of WholeStageCodegenExec
          // this is only used for local code generation while its RDD
          // semantics and related methods are all ignored
          val (ctx, code) = ExternalStoreUtils.codeGenOnExecutor(
            WholeStageCodegenExec(insertPlan), insertPlan)
          val references = ctx.references
          // also push the index of connection reference at the end which
          // will be used below to update connection before execution
          references += insertPlan.connRef
          (code, references.toArray)
        })
      val refs = gen._2.clone()
      // set the connection object for current execution
      val connectionRef = refs(refs.length - 1).asInstanceOf[Int]
      refs(connectionRef) = connection
      // no harm in passing a references array with extra element at end
      val iter = gen._1.generate(refs).asInstanceOf[BufferedRowIterator]
      // put the single ColumnBatch in the iterator read by generated code
      iter.init(partitionId, Array(Iterator[Any](new ResultSetTraversal(
        conn = null, stmt = null, rs = null, context = null),
        ColumnBatchIterator(batch)).asInstanceOf[Iterator[InternalRow]]))
      // ignore the result which is the update count
      while (iter.hasNext) {
        iter.next()
      }
      // release the batch buffers
      batch.buffers.foreach(UnsafeHolder.releaseIfDirectBuffer)
    }
  }

  // use the same saved connection for all operation
  private def getPartitionID(tableName: String, partitionId: Int = -1): Int = {
    if (partitionId >= 0) return partitionId
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable(tableName, true).asInstanceOf[LocalRegion]
        region match {
          case pr: PartitionedRegion =>
            if (partitionId == -1) {
              val primaryBucketIds = pr.getDataStore.
                  getAllLocalPrimaryBucketIdArray
              // TODO: do load-balancing among partitions instead
              // of random selection
              val numPrimaries = primaryBucketIds.size()
              // if no local primary bucket, then select some other
              if (numPrimaries > 0) {
                primaryBucketIds.getQuick(rand.nextInt(numPrimaries))
              } else {
                rand.nextInt(pr.getTotalNumberOfBuckets)
              }
            } else {
              partitionId
            }
          case _ => partitionId
        }
      // TODO: SW: for split mode, get connection to one of the
      // local servers and a bucket ID for only one of those
      case _ => if (partitionId < 0) rand.nextInt(numPartitions) else partitionId
    }
  }
}


final class ColumnarStorePartitionedRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    @(transient @param) partitionPruner: => Int,
    @transient private val store: JDBCSourceAsColumnarStore)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  private[this] var allPartitions: Array[Partition] = _
  private val evaluatePartitions: () => Array[Partition] = () => {
    val region = Misc.getRegionForTable(tableName, true)
    partitionPruner match {
      case -1 if allPartitions != null =>
        allPartitions
      case -1 =>
        allPartitions = session.sessionState.getTablePartitions(
          region.asInstanceOf[PartitionedRegion])
        allPartitions
      case bucketId: Int =>
        if (java.lang.Boolean.getBoolean("DISABLE_PARTITION_PRUNING")) {
          allPartitions = session.sessionState.getTablePartitions(
            region.asInstanceOf[PartitionedRegion])
          allPartitions
        } else {
          val pr = region.asInstanceOf[PartitionedRegion]
          val distMembers = StoreUtils.getBucketOwnersForRead(bucketId, pr)
          val prefNodes = distMembers.collect {
            case m if SnappyContext.containsBlockId(m.toString) =>
              Utils.getHostExecutorId(SnappyContext.getBlockId(
                m.toString).get.blockId)
          }
          Array(new MultiBucketExecutorPartition(0, ArrayBuffer(bucketId),
            pr.getTotalNumberOfBuckets, prefNodes.toSeq))
        }
    }
  }

  override def compute(part: Partition, context: TaskContext): Iterator[Any] = {

    Option(context).foreach(_.addTaskCompletionListener(_ => {
      val tx = TXManagerImpl.snapshotTxState.get()
      if (tx != null /* && !(tx.asInstanceOf[TXStateProxy]).isClosed() */ ) {
        val cache = Misc.getGemFireCacheNoThrow
        if (cache ne null) {
          val txMgr = cache.getCacheTransactionManager
          txMgr.masqueradeAs(tx)
          txMgr.commit()
        }
      }
    }))

    // TODO: maybe we can start tx here
    // We can start different tx in each executor for first phase,
    // till global snapshot is available.
    // check if the tx is already running.
    // val txMgr = GemFireCacheImpl.getExisting.getCacheTransactionManager
    // txMgr.begin()
    // who will call commit.
    // val txId = txMgr.getTransactionId
    val bucketIds = part match {
      case p: MultiBucketExecutorPartition => p.buckets
      case _ => java.util.Collections.singleton(Int.box(part.index))
    }
    // val container = GemFireXDUtils.getGemFireContainer(tableName, true)
    // ColumnBatchIterator(container, bucketIds)
    val r = Misc.getRegionForTable(tableName, true).asInstanceOf[LocalRegion]
    ColumnBatchIterator(r, bucketIds, context)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiBucketExecutorPartition].hostExecutorIds
  }

  override protected def getPartitions: Array[Partition] = {
    evaluatePartitions()
  }

  def getPartitionEvaluator: () => Array[Partition] = evaluatePartitions

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(tableName)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tableName = input.readString()
  }
}

final class SmartConnectorColumnRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    private var requiredColumns: Array[String],
    private var connProperties: ConnectionProperties,
    private val schema: StructType,
    @transient private val store: ExternalStore,
    val parts: Array[Partition],
    val relDestroyVersion: Int = -1)
    extends RDDKryo[Any](session.sparkContext, Nil)
        with KryoSerializable {

  override def compute(split: Partition,
      context: TaskContext): Iterator[ByteBuffer] = {
    val helper = new SparkShellRDDHelper
    val conn: Connection = helper.getConnection(connProperties, split)
    val partitionId = split.index
    val (fetchStatsQuery, fetchColQuery) = helper.getSQLStatement(tableName,
      partitionId, requiredColumns.map(_.replace(store.columnPrefix, "")), schema)
    // fetch the stats
    val (statement, rs, txId) = helper.executeQuery(conn, tableName, split,
      fetchStatsQuery, relDestroyVersion)
    val itr = new ColumnBatchIteratorOnRS(conn, requiredColumns, statement, rs,
      context, partitionId, fetchColQuery)

    if (context ne null) {
      context.addTaskCompletionListener { _ =>
        logDebug(s"The txid going to be committed is $txId " + tableName)

        // if ((txId ne null) && !txId.equals("null")) {
          val ps = conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
          ps.setString(1, if (txId == null) "null" else txId)
          ps.executeUpdate()
          logDebug(s"The txid being committed is $txId")
          ps.close()
          SparkShellRDDHelper.snapshotTxIdForRead.set(null)
        // }
      }
    }
    itr
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String])
  }

  override def getPartitions: Array[Partition] = {
    if (parts != null && parts.length > 0) {
      return parts
    }
    SparkShellRDDHelper.getPartitions(tableName)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)

    output.writeString(tableName)
    output.writeVarInt(requiredColumns.length, true)
    for (column <- requiredColumns) {
      output.writeString(column)
    }
    ConnectionPropertiesSerializer.write(kryo, output, connProperties)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)

    tableName = input.readString()
    val numColumns = input.readVarInt(true)
    requiredColumns = Array.fill(numColumns)(input.readString())
    connProperties = ConnectionPropertiesSerializer.read(kryo, input)

  }
}

class SmartConnectorRowRDD(_session: SnappySession,
    _tableName: String,
    _isPartitioned: Boolean,
    _columns: Array[String],
    _connProperties: ConnectionProperties,
    _filters: Array[Filter] = Array.empty[Filter],
    _partEval: () => Array[Partition] = () => Array.empty[Partition],
    _relDestroyVersion: Int = -1,
    _commitTx: Boolean)
    extends RowFormatScanRDD(_session, _tableName, _isPartitioned, _columns,
      pushProjections = true, useResultSet = true, _connProperties,
    _filters, _partEval, _commitTx) {


  override def commitTxBeforeTaskCompletion(conn: Option[Connection],
      context: TaskContext): Unit = {
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => {
      val txId = SparkShellRDDHelper.snapshotTxIdForRead.get
      logDebug(s"The txid going to be committed is $txId " + tableName)
      // if ((txId ne null) && !txId.equals("null")) {
        val ps = conn.get.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
        ps.setString(1, if (txId == null) "null" else txId)
        ps.executeUpdate()
        logDebug(s"The txid being committed is $txId")
        ps.close()
        SparkShellRDDHelper.snapshotTxIdForRead.set(null)
      // }
    }))
  }

  override def computeResultSet(
      thePart: Partition): (Connection, Statement, ResultSet) = {
    val helper = new SparkShellRDDHelper
    val conn: Connection = helper.getConnection(
      connProperties, thePart)
    if (isPartitioned) {
      val ps = conn.prepareStatement(
        s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?, ${_relDestroyVersion})")
      ps.setString(1, tableName)
      val partition = thePart.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
      val bucketString = partition.buckets.mkString(",")
      ps.setString(2, bucketString)
      ps.executeUpdate()
      ps.close()
    }
    val sqlText = s"SELECT $columnList FROM $tableName$filterWhereClause"

    val args = filterWhereArgs
    val stmt = conn.prepareStatement(sqlText)
    if (args ne null) {
      ExternalStoreUtils.setStatementParameters(stmt, args.map {
        case pl: ParamLiteral => pl.convertedLiteral
        case l : DynamicReplacableConstant => l.convertedLiteral
        case v => v
      })
    }
    val fetchSize = connProperties.executorConnProps.getProperty("fetchSize")
    if (fetchSize ne null) {
      stmt.setFetchSize(fetchSize.toInt)
    }

    val txId = SparkShellRDDHelper.snapshotTxIdForRead.get
    if (txId != null) {
      if (!txId.equals("null")) {
        val statement = conn.createStatement()
        statement.execute(
          s"call sys.USE_SNAPSHOT_TXID('$txId')")
      }
    }

    val rs = stmt.executeQuery()

    // get the txid which was used to take the snapshot.
    if (!_commitTx) {
      val getSnapshotTXId = conn.prepareCall(s"call sys.GET_SNAPSHOT_TXID (?)")
      getSnapshotTXId.registerOutParameter(1, java.sql.Types.VARCHAR)
      getSnapshotTXId.execute()
      val txid: String = getSnapshotTXId.getString(1)
      getSnapshotTXId.close()
      SparkShellRDDHelper.snapshotTxIdForRead.set(txid)
      logDebug(s"The snapshot tx id is $txid and tablename is $tableName")
    }
    logDebug(s"The previous snapshot tx id is $txId and tablename is $tableName")
    (conn, stmt, rs)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String])
  }

  override def getPartitions: Array[Partition] = {
    // use incoming partitions if provided (e.g. for collocated tables)
    val parts = partitionEvaluator()
    if (parts != null && parts.length > 0) {
      return parts
    }
    val conn = ExternalStoreUtils.getConnection(tableName, connProperties,
      forExecutor = true)
    try {
      SparkShellRDDHelper.getPartitions(tableName)
    } finally {
      conn.commit()
      conn.close()
    }
  }

  def getSQLStatement(resolvedTableName: String,
      requiredColumns: Array[String], partitionId: Int): String = {
    "select " + requiredColumns.mkString(", ") + " from " + resolvedTableName
  }
}
