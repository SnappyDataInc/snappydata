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
package io.snappydata


import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soubhikc on 11/11/15.
  */
trait ToolsCallback {

  def invokeLeadStartAddonService(sc: SparkContext)
  def invokeStartFabricServer(sc: SparkContext, hostData: Boolean)
  def invokeStopFabricServer(sc: SparkContext)
  def getLocatorJDBCURL(sc:SparkContext):String
}
