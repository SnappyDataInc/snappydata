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

import java.lang
import java.util.{Collections, UUID}

import com.gemstone.gemfire.internal.cache.{BucketRegion, ExternalTableMetaData, LocalRegion}
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, StoreCallbacks, UMMMemoryTracker}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer}
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import com.pivotal.gemfirexd.tools.sizer.GemFireXDInstrumentation
import io.snappydata.Constant
import org.apache.spark.memory.{MemoryManagerCallback, MemoryMode}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.{ColumnBatchCreator, ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.hive.{ExternalTableType, SnappyStoreHiveCatalog}
import org.apache.spark.sql.store.{StoreHashFunction, StoreUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SnappyContext, SnappySession, SplitClusterMode}
import org.apache.spark.storage.TestBlockId
import org.apache.spark.{Logging, SparkEnv, SparkException}

import scala.collection.JavaConverters._

object StoreCallbacksImpl extends StoreCallbacks with Logging with Serializable {

  private val partitioner = new StoreHashFunction

  val sizer: GemFireXDInstrumentation = GemFireXDInstrumentation.getInstance



  override def createColumnBatch(region: BucketRegion, batchID: UUID,
      bucketID: Int): java.util.Set[AnyRef] = {
    val pr = region.getPartitionedRegion
    val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
    val catalogEntry: ExternalTableMetaData = container.fetchHiveMetaData(false)

    if (catalogEntry != null) {
      // LCC should be available assuming insert is already being done
      // via a proper connection
      var conn: EmbedConnection = null
      var contextSet: Boolean = false
      try {
        var lcc: LanguageConnectionContext = Misc.getLanguageConnectionContext
        if (lcc == null) {
          conn = GemFireXDUtils.getTSSConnection(true, true, false)
          conn.getTR.setupContextStack()
          contextSet = true
          lcc = conn.getLanguageConnectionContext
          if (lcc == null) {
            Misc.getGemFireCache.getCancelCriterion.checkCancelInProgress(null)
          }
        }
        val row: AbstractCompactExecRow = container.newTemplateRow()
            .asInstanceOf[AbstractCompactExecRow]
        lcc.setExecuteLocally(Collections.singleton(bucketID), pr, false, null)
        try {
          val sc = lcc.getTransactionExecute.openScan(
            container.getId.getContainerId, false, 0,
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_NOLOCK /* not used */ ,
            null, null, 0, null, null, 0, null)

          val dependents = if (catalogEntry.dependents != null) {
            val tables = Misc.getMemStore.getAllContainers.asScala.
                map(x => (x.getSchemaName + "." + x.getTableName, x.fetchHiveMetaData(false)))
            catalogEntry.dependents.toSeq.map(x => tables.find(x == _._1).get._2)
          } else {
            Seq.empty
          }

          val tableName = container.getQualifiedTableName
          // add weightage column for sample tables if required
          var schema = catalogEntry.schema.asInstanceOf[StructType]
          if (catalogEntry.tableType == ExternalTableType.Sample.toString &&
              schema(schema.length - 1).name != Utils.WEIGHTAGE_COLUMN_NAME) {
            schema = schema.add(Utils.WEIGHTAGE_COLUMN_NAME,
              LongType, nullable = false)
          }
          val batchCreator = new ColumnBatchCreator(pr,
            ColumnFormatRelation.columnBatchTableName(tableName), schema,
            catalogEntry.externalStore.asInstanceOf[ExternalStore],
            catalogEntry.compressionCodec)
          batchCreator.createAndStoreBatch(sc, row,
            batchID, bucketID, dependents)
        } finally {
          lcc.setExecuteLocally(null, null, false, null)
        }
      } catch {
        case e: Throwable => throw e
      } finally {
        if (contextSet) {
          conn.getTR.restoreContextStack()
        }
      }
    } else {
      java.util.Collections.emptySet[AnyRef]()
    }
  }

  def getInternalTableSchemas: java.util.List[String] = {
    val schemas = new java.util.ArrayList[String](2)
    schemas.add(SnappyStoreHiveCatalog.HIVE_METASTORE)
    schemas.add(Constant.INTERNAL_SCHEMA_NAME)
    schemas
  }

  override def getHashCodeSnappy(dvd: scala.Any, numPartitions: Int): Int = {
    partitioner.computeHash(dvd, numPartitions)
  }

  override def getHashCodeSnappy(dvds: scala.Array[Object],
      numPartitions: Int): Int = {
    partitioner.computeHash(dvds, numPartitions)
  }

  override def columnBatchTableName(table: String): String = {
    ColumnFormatRelation.columnBatchTableName(table)
  }

  override def snappyInternalSchemaName(): String = {
    io.snappydata.Constant.INTERNAL_SCHEMA_NAME
  }

  override def cleanUpCachedObjects(table: String,
      sentFromExternalCluster: lang.Boolean): Unit = {
    if (sentFromExternalCluster) {
      // cleanup invoked on embedded mode nodes
      // from external cluster (in split mode) driver
      ExternalStoreUtils.removeCachedObjects(table)
      // clean up cached hive relations on lead node
      if (GemFireXDUtils.getGfxdAdvisor.getMyProfile.hasSparkURL) {
        SnappyStoreHiveCatalog.registerRelationDestroy()
      }
    } else {
      // clean up invoked on external cluster driver (in split mode)
      // from embedded mode lead
      val sc = SnappyContext.globalSparkContext
      val mode = SnappyContext.getClusterMode(sc)
      mode match {
        case SplitClusterMode(_, _) =>
          StoreUtils.removeCachedObjects(
            SnappySession.getOrCreate(sc).sqlContext, table,
            registerDestroy = true)

        case _ =>
          throw new SparkException("Clean up expected to be invoked on" +
              " external cluster driver. Current cluster mode is " + mode)
      }
    }
  }

  override def registerRelationDestroyForHiveStore(): Unit = {
    SnappyStoreHiveCatalog.registerRelationDestroy()
  }

  override def getLastIndexOfRow(o: Object): Int = {
    val r = o.asInstanceOf[Row]
    if (r != null) {
      r.getInt(r.length - 1)
    } else {
      -1
    }
  }

  override def acquireStorageMemory(objectName: String, numBytes: Long,
      buffer: UMMMemoryTracker, shouldEvict: Boolean): Boolean = {
    val blockId = TestBlockId(s"SNAPPY_STORAGE_BLOCK_ID_$objectName")
    MemoryManagerCallback.memoryManager.acquireStorageMemoryForObject(objectName,
      blockId, numBytes, MemoryMode.ON_HEAP, buffer, shouldEvict)
  }

  override def releaseStorageMemory(objectName: String, numBytes: Long): Unit = {
    MemoryManagerCallback.memoryManager.
      releaseStorageMemoryForObject(objectName, numBytes, MemoryMode.ON_HEAP)
  }

  override def dropStorageMemory(objectName: String, ignoreBytes: Long): Unit =
    MemoryManagerCallback.memoryManager.
      dropStorageMemoryForObject(objectName, MemoryMode.ON_HEAP, ignoreBytes)

  override def resetMemoryManager(): Unit = MemoryManagerCallback.resetMemoryManager()

  override def isSnappyStore: Boolean = true

  override def getRegionOverhead(region: LocalRegion): Long = {
    region.estimateMemoryOverhead(sizer)
  }

  override def getNumBytesForEviction: Long = {
    SparkEnv.get.memoryManager.maxOnHeapStorageMemory
  }

  override def getStoragePoolUsedMemory: Long =
    MemoryManagerCallback.memoryManager.getStoragePoolMemoryUsed()

  override def getStoragePoolSize: Long =
    MemoryManagerCallback.memoryManager.getStoragePoolSize

  override def getExecutionPoolUsedMemory: Long
  = MemoryManagerCallback.memoryManager.getExecutionPoolUsedMemory

  override def getExecutionPoolSize: Long =
    MemoryManagerCallback.memoryManager.getExecutionPoolSize
}

trait StoreCallback extends Serializable {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}
