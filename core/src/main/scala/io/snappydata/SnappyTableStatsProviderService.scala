/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.util.concurrent.TimeUnit
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.{AbstractRegionEntry, LocalRegion, PartitionedRegion, RegionEntry}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdListResultCollector, GfxdMessage}
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.ui._
import io.prometheus.client.Gauge
import io.snappydata.Constant._
import io.snappydata.collection.ObjectObjectHashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatKey, ColumnFormatRelation, ColumnFormatValue, RemoteEntriesIterator}
import org.apache.spark.sql.{SnappyContext, ThinClientConnectorMode}

/*
* Object that encapsulates the actual stats provider service. Stats provider service
* will either be SnappyEmbeddedTableStatsProviderService or SnappyThinConnectorTableStatsProvider
 */
object SnappyTableStatsProviderService {
  // var that points to the actual stats provider service
  private var statsProviderService: TableStatsProviderService = _

  def start(sc: SparkContext, url: String): Unit = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        statsProviderService = SnappyThinConnectorTableStatsProvider
      case _ =>
        statsProviderService = SnappyEmbeddedTableStatsProviderService
    }
    statsProviderService.start(sc, url)
  }

  def stop(): Unit = {
    val service = statsProviderService
    if (service ne null) {
      service.stop()
    }
  }

  def getService: TableStatsProviderService = {
    val service = statsProviderService
    if (service eq null) {
      throw new IllegalStateException("SnappyTableStatsProviderService not started")
    }
    service
  }

  var suspendCacheInvalidation = false
}

object SnappyEmbeddedTableStatsProviderService extends TableStatsProviderService {

  override def start(sc: SparkContext, url: String): Unit = {
    if (!doRun) {
      this.synchronized {
        if (!doRun) {
          val delay = sc.getConf.getLong(PROPERTY_PREFIX +
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
                  case e: Exception => if (e.getMessage != null && !e.getMessage.contains(
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
      }
    }
  }

  override def fillAggregatedMemberStatsOnDemand(): Unit = {

    try {
      val existingMembers = membersInfo.keys.toArray
      val collector = new GfxdListResultCollector(null, true)
      val msg = new MemberStatisticsMessage(collector)

      msg.executeFunction()

      val memStats = collector.getResult

      val itr = memStats.iterator()

      val members = ObjectObjectHashMap.withExpectedSize[String,
          MemberStatistics](8)
      while (itr.hasNext) {
        val o = itr.next().asInstanceOf[ListResultCollectorValue]
        val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]

        val dssUUID = memMap.get("diskStoreUUID").asInstanceOf[java.util.UUID]
        val id = memMap.get("id").toString

        var memberStats: MemberStatistics = {
          if (dssUUID != null && membersInfo.contains(dssUUID.toString)) {
            membersInfo(dssUUID.toString)
          } else if (membersInfo.contains(id)) {
            membersInfo(id)
          } else {
            null
          }
        }

        if (memberStats == null) {
          memberStats = new MemberStatistics(memMap)
          // Initialize prometheus metrics for member
          var shortDir = memMap.get("userDir").toString.substring(
            memMap.get("userDir").toString.lastIndexOf(System.getProperty("file.separator")) + 1)
          shortDir = shortDir.replaceAll("-", "_")
          val cpuUsageGauge: Gauge = Gauge.build()
              .name("snappydata_member_" + shortDir + "_cpu_usage")
              //.name("snappydata_member_cpu_usage").labelNames("shortDir")
              .help("SnappyData Member [" + shortDir + "] CPU usage")
              .register(p_collectorRegistry)
          val cpuUsageTimer: Gauge.Timer = cpuUsageGauge.startTimer()

          if (dssUUID != null) {
            members.put(dssUUID.toString, memberStats)
            p_memberCpuUsageGaugeMap.put(dssUUID.toString, cpuUsageGauge)
            p_memberCpuUsageTimerMap.put(dssUUID.toString, cpuUsageTimer)
          }
          // Set prometheus cpu metrics for member
          try {
            println("----------First-------- Adding value " + memMap.get("cpuActive").toString
                + " to cpu gauge for member "+memMap.get("id"))
            cpuUsageGauge.set(memMap.get("cpuActive").toString.toInt)
          } catch {
            case e: Exception => logInfo("Exception Occurred while setting CPU metrics for member: "
                + memMap.get("id") + " : "+ e.getMessage)
          } finally {
            cpuUsageTimer.setDuration()
          }
        } else {
          memberStats.updateMemberStatistics(memMap)
          if (dssUUID != null) {
            members.put(dssUUID.toString, memberStats)
            // Set prometheus cpu metrics for member
            val cpuUsageGauge: Gauge = p_memberCpuUsageGaugeMap.get(dssUUID.toString).get
            val cpuUsageTimer: Gauge.Timer = p_memberCpuUsageTimerMap.get(dssUUID.toString).get
            try {
              println("------------------ Adding value " + memMap.get("cpuActive").toString
                  + " to cpu gauge for member "+memMap.get("id"))
              cpuUsageGauge.set(memMap.get("cpuActive").toString.toInt)
            } catch {
              case e: Exception => logInfo("Exception Occurred while setting CPU metrics for member: "
                  + memMap.get("id") + " : "+ e.getMessage)
            } finally {
              cpuUsageTimer.setDuration()
            }
          }
        }

        memberStats.setStatus("Running")
      }
      membersInfo ++= members.asScala
      // mark members no longer running as stopped
      existingMembers.filterNot(members.containsKey).foreach(m =>
        membersInfo(m).setStatus("Stopped"))

      // update cluster level stats
      ClusterStatistics.getInstance().updateClusterStatistics(membersInfo.asJava)

    } catch {
      case NonFatal(e) => logWarning(e.getMessage, e)
    } finally {
      // push to job snappymetrics
      p_pushGateway.push(p_collectorRegistry, "snappymetrics")
    }
  }

  override def getStatsFromAllServers(sc: Option[SparkContext] = None): (Seq[SnappyRegionStats],
      Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = {
    var result = new java.util.ArrayList[SnappyRegionStatsCollectorResult]().asScala
    val dataServers = GfxdMessage.getAllDataStores
    var resultObtained: Boolean = false
    try {
      if (dataServers != null && dataServers.size() > 0) {
        result = FunctionService.onMembers(dataServers)
            // .withCollector(new GfxdListResultCollector())
            .execute(SnappyRegionStatsCollectorFunction.ID).getResult(5, TimeUnit.SECONDS).
            asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
            .asScala
        resultObtained = true
      }
    }
    catch {
      case NonFatal(e) => {
        log.warn("Exception occurred while collecting Table Statistics: " + e.getMessage)
        log.debug(e.getMessage, e)
      }
    }

    val hiveTables = Misc.getMemStore.getExternalCatalog.getHiveTables(true).asScala

    val externalTables: mutable.Buffer[SnappyExternalTableStats] = {
      try {
        // External Tables
        hiveTables.collect {
          case table if table.tableType.equalsIgnoreCase("EXTERNAL") => {
            new SnappyExternalTableStats(table.entityName, table.tableType, table.shortProvider,
              table.externalStore, table.dataSourcePath, table.driverClass)
          }
        }
      }
      catch {
        case NonFatal(e) => {
          log.warn("Exception occurred while collecting External Table Statistics: " + e.getMessage)
          log.debug(e.getMessage, e)
          mutable.Buffer.empty[SnappyExternalTableStats]
        }
      }
    }

    if (resultObtained) {
      // Return updated tableSizeInfo
      // Map to hold hive table type against table names as keys
      val tableTypesMap: mutable.HashMap[String, String] = mutable.HashMap.empty[String, String]
      hiveTables.foreach(ht => {
        val key = ht.schema.toString.concat("." + ht.entityName)
        tableTypesMap.put(key.toUpperCase, ht.tableType)
      })

      val regionStats = result.flatMap(_.getRegionStats.asScala).map(rs => {
        val tableName = rs.getTableName
        if (tableTypesMap.contains(tableName.toUpperCase)
            && tableTypesMap.get(tableName.toUpperCase).get.equalsIgnoreCase("COLUMN")) {
          rs.setColumnTable(true)
        } else {
          rs.setColumnTable(false)
        }
        rs
      })

      // Return updated details
      (regionStats,
          result.flatMap(_.getIndexStats.asScala),
          externalTables)
    } else {
      // Return last successfully updated tableSizeInfo
      (tableSizeInfo.values.toSeq,
          result.flatMap(_.getIndexStats.asScala),
          externalTables)
    }
  }

  type PRIterator = PartitionedRegion#PRLocalScanIterator

  /**
   * Allows pulling stats rows efficiently if required. For the corner case
   * of bucket moving away while iterating other buckets.
   */
  private val createRemoteIterator = new BiFunction[java.lang.Integer, PRIterator,
      java.util.Iterator[RegionEntry]] {
    override def apply(bucketId: Integer,
        iter: PRIterator): java.util.Iterator[RegionEntry] = {
      new RemoteEntriesIterator(bucketId, Array.emptyIntArray,
        iter.getPartitionedRegion, null)
    }
  }

  def publishColumnTableRowCountStats(): Unit = {
    def asSerializable[C](c: C) = c.asInstanceOf[C with Serializable]

    val regions = asSerializable(Misc.getGemFireCache.getApplicationRegions.asScala)
    for (region: LocalRegion <- regions) {
      if (region.getDataPolicy.withPartitioning()) {
        val table = Misc.getFullTableNameFromRegionPath(region.getFullPath)
        val pr = region.asInstanceOf[PartitionedRegion]
        val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
        if (ColumnFormatRelation.isColumnTable(table) &&
            pr.getLocalMaxMemory > 0) {
          var numColumnsInTable = -1
          // Resetting PR numRows in cached batch as this will be calculated every time.
          var rowsInColumnBatch = 0L
          var offHeapSize = 0L
          if (container ne null) {
            // TODO: this should use a transactional iterator to get a consistent
            // snapshot (also pass the same transaction to getNumColumnsInTable
            //   for reading value and delete count)
            val itr = new pr.PRLocalScanIterator(false /* primaryOnly */ , null /* no TX */ ,
              null /* not required since includeValues is false */ ,
              createRemoteIterator, false /* forUpdate */ , false /* includeValues */)
            // using direct region operations
            while (itr.hasNext) {
              val re = itr.next().asInstanceOf[AbstractRegionEntry]
              val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
              val bucketRegion = itr.getHostedBucketRegion
              if (bucketRegion.getBucketAdvisor.isPrimary) {
                if (numColumnsInTable < 0) {
                  numColumnsInTable = key.getNumColumnsInTable(table)
                }
                rowsInColumnBatch += key.getColumnBatchRowCount(bucketRegion, re,
                  numColumnsInTable)
              }
              re._getValue() match {
                case v: ColumnFormatValue => offHeapSize += v.getOffHeapSizeInBytes
                case _ =>
              }
            }
          }
          val stats = pr.getPrStats
          stats.setPRNumRowsInColumnBatches(rowsInColumnBatch)
          stats.setOffHeapSizeInBytes(offHeapSize)
        }
      }
    }
  }
}
