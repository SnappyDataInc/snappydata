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
package io.snappydata.tools

import java.util.Properties

import com.gemstone.gemfire.cache.Cache
import com.gemstone.gemfire.internal.cache.CacheServerLauncher
import com.pivotal.gemfirexd.{FabricServer, FabricService}
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.impl.LeadImpl
import io.snappydata.{Lead, LocalizedMessages, ServiceManager}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Extending server launcher to init Jobserver as part of lead
  * node startup. This node won't start DRDA network server.
  */
class LeaderLauncher(baseName: String) extends GfxdServerLauncher(baseName) {

  val genericLogger = LoggerFactory.getLogger(getClass)

  @throws[Exception]
  override protected def getFabricServiceInstance: FabricService = ServiceManager.getLeadInstance

  def initStartupArgs(args: ArrayBuffer[String]): Array[String] = {

    if (args.length == 0) {
      usage()
      System.exit(1)
    }

    def changeOrAppend(attr: String, value: String, overwrite: Boolean = false) = {
      args.indexWhere(_.indexOf(attr) > 0) match {
        case -1 => args += s"""-${attr}=${value}"""
        case idx if overwrite => args(idx) = args(idx).takeWhile(_ != '=') + s"""=${value}"""
        case idx => args(idx) = args(idx) ++ s""",${value}"""
      }
    }


    args(0).equalsIgnoreCase("start") match {
      case true =>
        changeOrAppend(GfxdServerLauncher.RUN_NETSERVER, "false", true)
      case _ =>
    }

    args.toArray[String]
  }

  override protected def usage(): Unit = {
    val script: String = LocalizedMessages.res.getTextMessage("SD_LEAD_SCRIPT")
    val name: String = LocalizedMessages.res.getTextMessage("SD_LEAD_NAME")
    val extraHelp = LocalizedResource.getMessage("FS_EXTRA_HELP", LocalizedMessages.
        res.getTextMessage("FS_PRODUCT"))
    val usageOutput: String = LocalizedResource.getMessage("SERVER_HELP",
      script, name, LocalizedResource.getMessage("FS_ADDRESS_ARG"), extraHelp)
    printUsage(usageOutput, SanityManager.DEFAULT_MAX_OUT_LINES)
  }

  override protected def run(args: Array[String]): Unit = {
    super.run(initStartupArgs(ArrayBuffer(args: _*)))
  }

  override protected def setDefaultHeapSize(): Boolean = false

  @throws[Exception]
  override protected def startServerVM(props: Properties) : Unit = {
    val leadImpl = getFabricServiceInstance.asInstanceOf[LeadImpl]
    leadImpl.notifyOnStatusChange(writeStatusOnChange)
    leadImpl.start(props)
    this.bootProps = props
  }

  @throws[Exception]
  override protected def startAdditionalServices(cache: Cache,
      options: java.util.Map[String, Object], props: Properties): Unit = {
    // don't call super.startAdditionalServices.
    // We don't want to init net-server in leader.

    // disabling net server startup etc.

  }

  override protected def checkStatusForWait(status: CacheServerLauncher.Status): Boolean = {
    (status.state == CacheServerLauncher.STARTING ||
        status.state == CacheServerLauncher.WAITING)
  }

  def writeStatusOnChange(newState: State): Unit = {

    newState match {
      case State.STANDBY =>
        status = CacheServerLauncher.createStatus(this.baseName,
          CacheServerLauncher.STANDBY, getProcessId)
        writeStatus(status)
        genericLogger.info("lead node standby status written.")

      case State.STARTING =>
        status = CacheServerLauncher.createStatus(this.baseName,
              CacheServerLauncher.STARTING, getProcessId)
        writeStatus(status)
        genericLogger.info("Lead Node starting status written.")

      case State.RUNNING =>
        status = CacheServerLauncher.createStatus(this.baseName,
              CacheServerLauncher.RUNNING, getProcessId)
        writeStatus(status)
        genericLogger.info("Lead Node running status written.")
      case _ =>
        return

    }

  }

  override protected def getBaseName(name: String) = "snappyleader"

} // end of class

object LeaderLauncher {

  def main(args: Array[String]): Unit = {
    val launcher = new LeaderLauncher("SnappyData Leader")
    launcher.run(args)
  }

}
