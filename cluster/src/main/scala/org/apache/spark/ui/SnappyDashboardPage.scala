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

package org.apache.spark.ui

import java.text.SimpleDateFormat
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.Node

import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyRegionStats}
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.internal.Logging

private[ui] class SnappyDashboardPage (parent: SnappyDashboardTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {

    val pageHeaderText: String = SnappyDashboardPage.pageHeaderText

    // Generate Pages HTML
    val pageTitleNode = createPageTitleNode(pageHeaderText)

    val clusterStatsDetails = {
      val clustersStatsTitle = createTitleNode(SnappyDashboardPage.clusterStatsTitle,
                                 SnappyDashboardPage.clusterStatsTitleTooltip)
      val clusterDetails = clusterStats

      clustersStatsTitle ++ clusterDetails
    }

    val membersStatsDetails = {
      val membersStatsTitle = createTitleNode(SnappyDashboardPage.membersStatsTitle,
                                SnappyDashboardPage.membersStatsTitleTooltip)
      val membersStatsTable = memberStats

      membersStatsTitle ++ membersStatsTable
    }

    val tablesStatsDetails = {
      val tablesStatsTitle = createTitleNode(SnappyDashboardPage.tablesStatsTitle,
                                SnappyDashboardPage.tablesStatsTitleTooltip)
      val tablesStatsTable = tableStats

      tablesStatsTitle ++ tablesStatsTable
    }

    val extTablesStatsDetails = {
      val extTablesStatsTitle = createTitleNode(SnappyDashboardPage.extTablesStatsTitle,
        SnappyDashboardPage.extTablesStatsTitleTooltip)
      val extTablesStatsTable = extTableStats

      extTablesStatsTitle ++ extTablesStatsTable
    }

    val jsScripts = <script src={
                              UIUtils.prependBaseUri("/static/snappydata/snappy-dashboard.js")
                            }></script>

    val pageContent = jsScripts ++ pageTitleNode ++ clusterStatsDetails ++ membersStatsDetails ++
                      tablesStatsDetails ++ extTablesStatsDetails

    UIUtils.headerSparkPage(pageHeaderText, pageContent, parent, Some(500),
      useDataTables = true, isSnappyPage = true)

  }

  private def createPageTitleNode(title: String): Seq[Node] = {
    <div id="AutoUpdateErrorMsgContainer">
      <div id="AutoUpdateErrorMsg">
      </div>
    </div>
    <div id="CPUCoresContainer" style="position: absolute; width: 100%;">
      <div id="CPUCoresDetails">
        <div id="TotalCoresHolder">
          <span style="padding-left: 5px;"> Total CPU Cores: </span>
          <span id="totalCores"> </span>
        </div>
      </div>
    </div>
    <div class="row-fluid">
      <div class="span12">
        <h3 class="page-title-node-h3">
          {title}
        </h3>
      </div>
    </div>
  }

  private def createTitleNode(title: String, tooltip: String): Seq[Node] = {
    <div class="row-fluid">
      <div class="span12">
        <h4 class="title-node-h4" data-toggle="tooltip" data-placement="top" title={tooltip}>
          {title}
        </h4>
      </div>
    </div>
  }

  private def clusterStats(): Seq[Node] = {
    <div class="container-fluid" style="text-align: center;">
      <div id="cpuUsageContainer" class="graph-container">
      </div>
      <div id="heapUsageContainer" class="graph-container">
      </div>
      <div id="offheapUsageContainer" class="graph-container">
      </div>
      <div id="diskSpaceUsageContainer" class="graph-container">
      </div>
    </div>
  }

  private def memberStats(): Seq[Node] = {
    <div class="container-fluid">
      <table id="memberStatsGrid" class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th class="table-th-col-heading" style="width: 60px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("statusTooltip")
                    }>
                {SnappyDashboardPage.memberStatsColumn("status")}
              </span>
            </th>
            <th class="table-th-col-heading">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("descriptionTooltip")
                    }>
                {SnappyDashboardPage.memberStatsColumn("description")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 100px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("memberTypeTooltip")
                    }>
                {SnappyDashboardPage.memberStatsColumn("memberType")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("cpuUsageTooltip")
                    }>
                {SnappyDashboardPage.memberStatsColumn("cpuUsage")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("memoryUsageTooltip")
                    }>
                {SnappyDashboardPage.memberStatsColumn("memoryUsage")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("heapMemoryTooltip")
                    }>
                Heap Memory<br/>(Used / Total)
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.memberStatsColumn("offHeapMemoryTooltip")
                    }>
                Off-Heap Memory<br/>(Used / Total)
              </span>
            </th>
          </tr>
        </thead>
      </table>
    </div>
  }

  private def tableStats(): Seq[Node] = {
    <div class="container-fluid">
      <table id="tableStatsGrid" class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th class="table-th-col-heading">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("nameTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("name")}
              </span>
            </th>
            <th class="table-th-col-heading">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("storageModelTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("storageModel")}
              </span>
            </th>
            <th class="table-th-col-heading">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("distributionTypeTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("distributionType")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("rowCountTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("rowCount")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("sizeInMemoryTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("sizeInMemory")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("totalSizeTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("totalSize")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width: 200px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.tableStatsColumn("bucketCountTooltip")
                    }>
                {SnappyDashboardPage.tableStatsColumn("bucketCount")}
              </span>
            </th>
          </tr>
        </thead>
      </table>
    </div>
  }

  private def extTableStats(): Seq[Node] = {

    <div class="container-fluid">
      <table id="extTableStatsGrid" class="table table-bordered table-condensed table-striped">
        <thead>
          <tr>
            <th class="table-th-col-heading" style="width:300px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.extTableStatsColumn("nameTooltip")
                    }>
                {SnappyDashboardPage.extTableStatsColumn("name")}
              </span>
            </th>
            <th class="table-th-col-heading" style="width:300px;">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.extTableStatsColumn("providerTooltip")
                    }>
                {SnappyDashboardPage.extTableStatsColumn("provider")}
              </span>
            </th>
            <th class="table-th-col-heading">
              <span data-toggle="tooltip" title=""
                    data-original-title={
                      SnappyDashboardPage.extTableStatsColumn("externalSourceTooltip")
                    }>
                {SnappyDashboardPage.extTableStatsColumn("externalSource")}
              </span>
            </th>
          </tr>
        </thead>
      </table>
    </div>
  }

}

object SnappyDashboardPage {
  val pageHeaderText = "SnappyData Dashboard"

  object Status {
    val normal = "Normal"
    val warning = "Warning"
    val error = "Error"
    val severe = "Severe"
  }

  val ValueNotApplicable = "N/A"

  val clusterStatsTitle = "Cluster"
  val clusterStatsTitleTooltip = "SnappyData Clusters Summary"
  val clusterStats = scala.collection.mutable.HashMap.empty[String, Any]
  clusterStats += ("status" -> "Cluster Status")
  clusterStats += ("members" -> "Members")
  clusterStats += ("servers" -> "Data Servers")
  clusterStats += ("leads" -> "Leads")
  clusterStats += ("locators" -> "Locators")
  clusterStats += ("clients" -> "Connections")
  clusterStats += ("tables" -> "Tables")
  clusterStats += ("cpuUsage" -> "CPU Usage")
  clusterStats += ("cpuUsageTooltip" -> "Clusters CPU Usage")
  clusterStats += ("memoryUsage" -> "Memory Usage")
  clusterStats += ("memoryUsageTooltip" -> "Clusters Total Memory Usage")
  clusterStats += ("heapUsage" -> "Heap Usage")
  clusterStats += ("heapUsageTooltip" -> "Clusters Total Heap Usage")
  clusterStats += ("offHeapUsage" -> "Off-Heap Usage")
  clusterStats += ("offHeapUsageTooltip" -> "Clusters Total Off-Heap Usage")
  clusterStats += ("jvmHeapUsage" -> "JVM Heap Usage")
  clusterStats += ("jvmHeapUsageTooltip" -> "Clusters Total JVM Heap Usage")

  val membersStatsTitle = "Members"
  val membersStatsTitleTooltip = "SnappyData Members Summary"
  val memberStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  memberStatsColumn += ("status" -> "Status")
  memberStatsColumn += ("statusTooltip" -> "Members Status")
  memberStatsColumn += ("id" -> "Id")
  memberStatsColumn += ("idTooltip" -> "Members unique Identifier")
  memberStatsColumn += ("name" -> "Name")
  memberStatsColumn += ("nameTooltip" -> "Members Name")
  memberStatsColumn += ("nameOrId" -> "Member")
  memberStatsColumn += ("nameOrIdTooltip" -> "Members Name/Id")
  memberStatsColumn += ("description" -> "Member")
  memberStatsColumn += ("descriptionTooltip" -> "Members Description")
  memberStatsColumn += ("host" -> "Host")
  memberStatsColumn += ("hostTooltip" -> "Physical machine on which member is running")
  memberStatsColumn += ("cpuUsage" -> "CPU Usage")
  memberStatsColumn += ("cpuUsageTooltip" -> "CPU used by Member Host")
  memberStatsColumn += ("memoryUsage" -> "Memory Usage")
  memberStatsColumn += ("memoryUsageTooltip" -> "Memory(Heap + Off-Heap) used by Member")
  memberStatsColumn += ("usedMemory" -> "Used Memory")
  memberStatsColumn += ("usedMemoryTooltip" -> "Used Memory")
  memberStatsColumn += ("totalMemory" -> "Total Memory")
  memberStatsColumn += ("totalMemoryTooltip" -> "Total Memory")
  memberStatsColumn += ("clients" -> "Connections")
  memberStatsColumn += ("clientsTooltip" -> "Number of JDBC connections to Member")
  memberStatsColumn += ("memberType" -> "Type")
  memberStatsColumn += ("memberTypeTooltip" -> "Member is Lead / Locator / Data Server")
  memberStatsColumn += ("lead" -> "Lead")
  memberStatsColumn += ("leadTooltip" -> "Member is Lead")
  memberStatsColumn += ("locator" -> "Locator")
  memberStatsColumn += ("locatorTooltip" -> "Member is Locator")
  memberStatsColumn += ("server" -> "Server")
  memberStatsColumn += ("serverTooltip" -> "Member is Server")
  memberStatsColumn += ("storageMemoryUsed" -> "StorageUsed")
  memberStatsColumn += ("storageMemoryToolTip" -> "Total storage pool memory used")
  memberStatsColumn += ("storageMemoryPoolSize" -> "StoragePoolSize")
  memberStatsColumn += ("storageMemorySizeToolTip" -> "Max storage pool memory size")
  memberStatsColumn += ("executionMemoryUsed" -> "ExecutionUsed")
  memberStatsColumn += ("executionMemoryToolTip" -> "Total execution pool memory used")
  memberStatsColumn += ("executionMemoryPoolSize" -> "ExecutionPoolSize")
  memberStatsColumn += ("executionMemorySizeToolTip" -> "Max execution pool memory size")
  memberStatsColumn += ("heapMemory" -> "Heap Memory (Used / Total)")
  memberStatsColumn += ("heapMemoryTooltip" -> "Members used and total Heap Memory")
  memberStatsColumn += ("offHeapMemory" -> "Off-Heap Memory (Used / Total)")
  memberStatsColumn += ("offHeapMemoryTooltip" -> "Members used and total Off Heap Memory")
  memberStatsColumn += ("jvmHeapMemory" -> "JVM Heap (Used / Total)")
  memberStatsColumn += ("jvmHeapMemoryTooltip" -> "Members used and total JVM Heap")

  val tablesStatsTitle = "Tables"
  val tablesStatsTitleTooltip = "SnappyData Tables Summary"
  val tableStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  tableStatsColumn += ("id" -> "Id")
  tableStatsColumn += ("idTooltip" -> "Tables unique Identifier")
  tableStatsColumn += ("name" -> "Name")
  tableStatsColumn += ("nameTooltip" -> "Tables Name")
  tableStatsColumn += ("storageModel" -> "Storage Model")
  tableStatsColumn += ("storageModelTooltip" -> "Storage Model is either COLUMN or ROW ")
  tableStatsColumn += ("distributionType" -> "Distribution Type")
  tableStatsColumn += ("distributionTypeTooltip" ->
      "Distribution Type is either PARTITIONED or REPLICATED table ")
  tableStatsColumn += ("rowCount" -> "Row Count")
  tableStatsColumn += ("rowCountTooltip" -> "Total Rows in Table")
  tableStatsColumn += ("sizeInMemory" -> "Memory Size")
  tableStatsColumn += ("sizeInMemoryTooltip" -> "Tables Size in Memory")
  tableStatsColumn += ("totalSize" -> "Total Size")
  tableStatsColumn += ("totalSizeTooltip" ->
      "Tables Total Size (In Memory size + Disk Overflow Size)")
  tableStatsColumn += ("bucketCount" -> "Buckets")
  tableStatsColumn += ("bucketCountTooltip" -> "Number of Buckets in Table")

  val extTablesStatsTitle = "External Tables"
  val extTablesStatsTitleTooltip = "SnappyData ExternalTables Summary"
  val extTableStatsColumn = scala.collection.mutable.HashMap.empty[String, String]
  extTableStatsColumn += ("id" -> "Id")
  extTableStatsColumn += ("idTooltip" -> "External Tables unique Identifier")
  extTableStatsColumn += ("name" -> "Name")
  extTableStatsColumn += ("nameTooltip" -> "External Tables Name")
  extTableStatsColumn += ("type" -> "Type")
  extTableStatsColumn += ("typeTooltip" -> "External Tables Type")
  extTableStatsColumn += ("provider" -> "Provider")
  extTableStatsColumn += ("providerTooltip" -> "External Tables Provider")
  extTableStatsColumn += ("externalSource" -> "Source")
  extTableStatsColumn += ("externalSourceTooltip" -> "External Source of Tables ")

}
