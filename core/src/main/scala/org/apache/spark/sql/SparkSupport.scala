/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl

import org.apache.spark.util.Utils
import org.apache.spark.{SparkContext, SparkException}

/**
 * Helper trait for easy access to [[SparkInternals]] using the "internals" method.
 */
trait SparkSupport {
  protected final def internals: SparkInternals = SparkSupport.internals()
}

/**
 * Load appropriate Spark version support as per the current Spark version.
 */
object SparkSupport {

  /**
   * The default Spark version for which core will be built and must exactly match
   * the version of the embedded SnappyData Spark since this will be used on executors.
   */
  final val DEFAULT_VERSION = "2.1.1"

  @volatile private[this] var internalImpl: SparkInternals = _

  private val INTERNAL_PACKAGE = "org.apache.spark.sql.internal"

  /**
   * Get the appropriate [[SparkInternals]] for current SparkContext version.
   */
  def internals(context: SparkContext = null): SparkInternals = {
    val impl = internalImpl
    if (impl ne null) internalImpl
    else synchronized {
      val impl = internalImpl
      if (impl ne null) internalImpl
      else {
        val sparkVersion =
          if (context eq null) {
            // check for embedded product
            if (GemFireCacheImpl.getInstance() ne null) DEFAULT_VERSION
            else SnappyContext.globalSparkContext match {
              case null => throw new SparkException("No SparkContext")
              case ctx => ctx.version
            }
          } else context.version
        val implClass = sparkVersion match {
          // list all the supported versions below; all implementations are required to
          // have a public constructor having current SparkContext as the one argument
          case "2.1.0" => Utils.classForName(s"$INTERNAL_PACKAGE.Spark210Internals")
          case "2.1.1" => Utils.classForName(s"$INTERNAL_PACKAGE.Spark211Internals")
          case "2.3.2" => Utils.classForName(s"$INTERNAL_PACKAGE.Spark232Internals")
          case v => throw new SparkException(s"Unsupported Spark version $v")
        }
        internalImpl = implClass.newInstance().asInstanceOf[SparkInternals]
        internalImpl
      }
    }
  }

  private[sql] def clear(context: SparkContext): Unit = {
    if (context ne null) internals(context).clearSQLListener()
    internalImpl = null
  }
}
