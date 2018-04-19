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
package io.snappydata

import java.io.File

import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl
import org.apache.spark.executor.SnappyExecutor
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.ui.SnappyDashboardTab
import org.apache.spark.util.{SnappyUtils, Utils}

object ToolsCallbackImpl extends ToolsCallback {

  override def updateUI(sc: SparkContext): Unit = {
    SnappyUtils.getSparkUI(sc).foreach(new SnappyDashboardTab(_))
  }

  override def removeAddedJar(sc: SparkContext, jarName: String): Unit =
    sc.removeAddedJar(jarName)

  /**
    * Callback to spark Utils to fetch file
    */
  override def doFetchFile(
      url: String,
      targetDir: File,
      filename: String): File = {
    SnappyUtils.doFetchFile(url, targetDir, filename)
  }

  override def setSessionDependencies(sparkContext: SparkContext, appName: String,
      classLoader: ClassLoader): Unit = {
    SnappyUtils.setSessionDependencies(sparkContext, appName, classLoader)
  }

  override def addURIs(jars: Array[String]): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    val loader = lead.urlclassloader
    jars.foreach(j => {
      val url = new File(SparkFiles.getRootDirectory(), j).toURI.toURL
      loader.addURL(url)
    })
  }

  override def addURIsToExecutorClassLoader(jars: Array[String]): Unit = {
    if (ExecutorInitiator.snappyExecBackend != null) {
      val snappyexecutor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
      snappyexecutor.updateMainLoader(jars)
    }
  }
}
