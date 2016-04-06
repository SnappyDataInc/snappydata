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

/**
 * Constant names suggested per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 *
 * we decided to use upper case with underscore word separator.
 */
object Constant {

  val DEFAULT_EMBEDDED_URL = "jdbc:snappydata:"

  val SNAPPY_URL_PREFIX = "snappydata://"

  val JDBC_URL_PREFIX = "snappydata://"

  val JDBC_EMBEDDED_DRIVER = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"

  val JDBC_CLIENT_DRIVER = "com.pivotal.gemfirexd.jdbc.ClientDriver"

  val PROPERTY_PREFIX = "snappydata."

  val STORE_PROPERTY_PREFIX = s"${PROPERTY_PREFIX}store."

  private[snappydata] val JOBSERVER_PROPERTY_PREFIX = "jobserver."

  val DEFAULT_SCHEMA = "APP"

  val DEFAULT_CONFIDENCE: Double = 0.95

  val COLUMN_MIN_BATCH_SIZE: Int = 200

  val DEFAULT_USE_HIKARICP = false

  // Interval in ms  to run the SnappyAnalyticsService
  val DEFAULT_ANALYTICS_SERVICE_INTERVAL: Long = 5000

  // Internal Column table store schema
  final val INTERNAL_SCHEMA_NAME = "SNAPPYSYS_INTERNAL"

  // Internal Column table store suffix
  final val SHADOW_TABLE_SUFFIX = "_COLUMN_STORE_"
}

/**
 * Property names should be as per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 * i.e. upper camel case.
 */
object Property {

  val locators = s"${Constant.STORE_PROPERTY_PREFIX}locators"

  val mcastPort = s"${Constant.STORE_PROPERTY_PREFIX}mcast-port"

  val jobserverEnabled = s"${Constant.JOBSERVER_PROPERTY_PREFIX}enabled"

  val jobserverConfigFile = s"${Constant.JOBSERVER_PROPERTY_PREFIX}configFile"

  val embedded = s"${Constant.PROPERTY_PREFIX}embedded"

  val metastoreDBURL = s"${Constant.PROPERTY_PREFIX}metastore-db-url"

  val metastoreDriver = s"${Constant.PROPERTY_PREFIX}metastore-db-driver"
}
