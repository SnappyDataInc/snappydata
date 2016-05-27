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
package io.snappydata.gemxd

import java.lang.Long
import java.util

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor
import com.pivotal.gemfirexd.internal.snappy.{CallbackFactoryProvider, ClusterCallbacks, LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl

import org.apache.spark.Logging
import org.apache.spark.scheduler.cluster.SnappyClusterManager

/**
 * Callbacks that are sent by GemXD to Snappy for cluster management
 */
object ClusterCallbacksImpl extends ClusterCallbacks with Logging {

  CallbackFactoryProvider.setClusterCallbacks(this)

  private[snappydata] def initialize(): Unit = {
    // nothing to be done; singleton constructor does all
  }

  override def getLeaderGroup: util.HashSet[String] = {
    val leaderServerGroup = new util.HashSet[String]
    leaderServerGroup.add(LeadImpl.LEADER_SERVERGROUP)
    leaderServerGroup
  }

  override def launchExecutor(driverUrl: String,
      driverDM: InternalDistributedMember): Unit = {
    val url = if (driverUrl == null || driverUrl == "") {
      logInfo(s"call to launchExecutor but driverUrl is invalid. $driverUrl")
      None
    }
    else {
      Some(driverUrl)
    }
    logInfo(s"invoking startOrTransmute with. $url")
    ExecutorInitiator.startOrTransmuteExecutor(url, driverDM)
  }

  override def getDriverURL: String = {
    SnappyClusterManager.cm.map(_.schedulerBackend) match {
      case Some(x) =>
        logInfo(s"returning driverUrl=${x.driverUrl}")
        x.driverUrl
      case None =>
        null
    }
  }

  override def stopExecutor(): Unit = {
    ExecutorInitiator.stop()
  }

  override def getSQLExecute(sql: String, ctx: LeadNodeExecutionContext,
      v: Version): SparkSQLExecute = new SparkSQLExecuteImpl(sql, ctx, v)

  override def readDVDArray(dvds: Array[DataValueDescriptor],
      types: Array[Int], in: ByteArrayDataInput, numEightColGroups: Int,
      numPartialCols: Int): Unit = {
    SparkSQLExecuteImpl.readDVDArray(dvds, types, in, numEightColGroups,
      numPartialCols)
  }

  override def clearSnappyContextForConnection(
      connectionId: java.lang.Long): Unit = {
    SnappyContextPerConnection.removeSnappyContext(connectionId)
  }
}
