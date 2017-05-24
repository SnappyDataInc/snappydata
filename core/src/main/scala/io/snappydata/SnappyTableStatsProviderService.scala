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

package io.snappydata

import scala.collection.mutable
import scala.language.implicitConversions
import scala.collection.JavaConverters._

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.DataPolicy
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext
import com.gemstone.gemfire.internal.cache.{PartitionedRegion, LocalRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdMessage, GfxdListResultCollector}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.{CompactCompositeKey, GemFireContainer}
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyRegionStatsCollectorFunction, SnappyRegionStatsCollectorResult, SnappyIndexStats, SnappyRegionStats}
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation
import io.snappydata.Constant._

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.columnar.{JDBCSourceAsStore, JDBCAppendableRelation}
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.sql.{SnappyContext, ThinClientConnectorMode}
import org.apache.spark.unsafe.Platform

/*
* Object that encapsulates the actual stats provider service. Stats provider service
* will either be SnappyEmbeddedTableStatsProviderService or SnappyThinConnectorTableStatsProvider
 */
object SnappyTableStatsProviderService {
  // var that points to the actual stats provider service
  private var statsProviderService: TableStatsProviderService = null

  def start(sc: SparkContext): Unit = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        throw new IllegalStateException("Not expected to be called for ThinClientConnectorMode")
      case _ =>
        statsProviderService = SnappyEmbeddedTableStatsProviderService
    }
    statsProviderService.start(sc)
  }

  def start(sc: SparkContext, url: String): Unit = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        statsProviderService = SnappyThinConnectorTableStatsProvider
      case _ =>
        throw new IllegalStateException("This is expected to be called for ThinClientConnectorMode only")
    }
    statsProviderService.start(sc, url)
  }

  def stop(): Unit = {
    statsProviderService.stop()
  }

  def getService: TableStatsProviderService = {
    if (statsProviderService == null) {
      throw new IllegalStateException("SnappyTableStatsProviderService not started")
    }
    statsProviderService
  }

  var suspendCacheInvalidation = false
}

object SnappyEmbeddedTableStatsProviderService extends TableStatsProviderService {

  def start(sc: SparkContext): Unit = {
    val delay = sc.getConf.getLong(Constant.SPARK_SNAPPY_PREFIX +
        "calcTableSizeInterval", DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL)
    doRun = true
    Misc.getGemFireCache.getCCPTimer.schedule(
      new SystemTimer.SystemTimerTask {
        private val logger: LogWriterI18n = Misc.getGemFireCache.getLoggerI18n

        override def run2(): Unit = {
          try {
            if (doRun) {
              aggregateStats()
            }
          } catch {
            case _: CancelException => // ignore
            case e: Exception => if (!e.getMessage.contains(
              "com.gemstone.gemfire.cache.CacheClosedException")) {
              logger.warning(e)
            } else {
              logger.error(e)
            }
          }
        }

        override def getLoggerI18n: LogWriterI18n = {
          logger
        }
      },
      delay, delay)
  }

  override def start(sc: SparkContext, url: String): Unit = {
    throw new IllegalStateException("This is expected to be called for " +
        "ThinClientConnectorMode only")
  }

  override def fillAggregatedMemberStatsOnDemand(): Unit = {

    val existingMembers = membersInfo.keys.toArray
    val collector = new GfxdListResultCollector(null, true)
    val msg = new MemberStatisticsMessage(collector)

    msg.executeFunction()

    val memStats = collector.getResult

    val itr = memStats.iterator()

    val members = mutable.Map.empty[String, mutable.Map[String, Any]]
    while (itr.hasNext) {
      val o = itr.next().asInstanceOf[ListResultCollectorValue]
      val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]
      val map = mutable.HashMap.empty[String, Any]
      val keyItr = memMap.keySet().iterator()

      while (keyItr.hasNext) {
        val key = keyItr.next()
        map.put(key, memMap.get(key))
      }
      map.put("status", "Running")

      val dssUUID = memMap.get("diskStoreUUID").asInstanceOf[java.util.UUID]
      if (dssUUID != null) {
        members.put(dssUUID.toString, map)
      } else {
        members.put(memMap.get("id").asInstanceOf[String], map)
      }
    }
    membersInfo ++= members
    // mark members no longer running as stopped
    existingMembers.filterNot(members.contains).foreach(m =>
      membersInfo(m).put("status", "Stopped"))
  }

  override def getStatsFromAllServers: (Seq[SnappyRegionStats], Seq[SnappyIndexStats]) = {
    var result = new java.util.ArrayList[SnappyRegionStatsCollectorResult]().asScala
    val dataServers = GfxdMessage.getAllDataStores
    try {
      if (dataServers != null && dataServers.size() > 0) {
        result = FunctionService.onMembers(dataServers)
            .withCollector(new GfxdListResultCollector())
            .execute(SnappyRegionStatsCollectorFunction.ID).getResult().
            asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
            .asScala
      }
    }
    catch {
      case e: Exception => log.warn(e.getMessage, e)
    }
    (result.flatMap(_.getRegionStats.asScala), result.flatMap(_.getIndexStats.asScala))
  }

  def publishColumnTableRowCountStats(): Unit = {
    def asSerializable[C](c: C) = c.asInstanceOf[C with Serializable]

    val regions = asSerializable(Misc.getGemFireCache.getApplicationRegions.asScala)
    for (region: LocalRegion <- regions) {
      if (region.getDataPolicy == DataPolicy.PARTITION ||
          region.getDataPolicy == DataPolicy.PERSISTENT_PARTITION) {
        val table = Misc.getFullTableNameFromRegionPath(region.getFullPath)
        val pr = region.asInstanceOf[PartitionedRegion]
        val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
        if (table.startsWith(Constant.INTERNAL_SCHEMA_NAME) &&
            table.endsWith(Constant.SHADOW_TABLE_SUFFIX)) {
          if (container != null) {
            val bufferTable = JDBCAppendableRelation.getTableName(table)
            val numColumnsInBaseTbl = (Misc.getMemStore.getAllContainers.asScala.find(c => {
              c.getQualifiedTableName.equalsIgnoreCase(bufferTable)}).get.getNumColumns) - 1

            val numColumnsInStatBlob = numColumnsInBaseTbl * ColumnStatsSchema.NUM_STATS_PER_COLUMN
            val itr = pr.localEntriesIterator(null.asInstanceOf[InternalRegionFunctionContext],
              true, false, true, null).asInstanceOf[PartitionedRegion#PRLocalScanIterator]
            // Resetting PR Numrows in cached batch as this will be calculated every time.
            // TODO: Decrement count using deleted rows bitset in case of deletes in columntable
            var rowsInColumnBatch: Long = 0
            while (itr.hasNext) {
              val rl = itr.next().asInstanceOf[RowLocation]
              val key = rl.getKeyCopy.asInstanceOf[CompactCompositeKey]
              val x = key.getKeyColumn(2).getInt
              val currentBucketRegion = itr.getHostedBucketRegion
              if (x == JDBCSourceAsStore.STATROW_COL_INDEX) {
                currentBucketRegion.get(key) match {
                  case currentVal: Array[Array[Byte]] =>
                    val rowFormatter = container.
                        getRowFormatter(currentVal(0))
                    val statBytes = rowFormatter.getLob(currentVal, 4)
                    val unsafeRow = new UnsafeRow(numColumnsInStatBlob)
                    unsafeRow.pointTo(statBytes, Platform.BYTE_ARRAY_OFFSET,
                      statBytes.length)
                    rowsInColumnBatch += unsafeRow.getInt(
                      ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)
                  case _ =>
                }
              }
            }
            pr.getPrStats.setPRNumRowsInColumnBatches(rowsInColumnBatch)
          }
        }
      }
    }
  }

}
