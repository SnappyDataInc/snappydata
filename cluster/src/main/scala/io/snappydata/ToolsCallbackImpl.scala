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

import io.snappydata.impl.LeadImpl

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{OrderlessHashPartitioning, Partitioning}

object ToolsCallbackImpl extends ToolsCallback {

  override def invokeLeadStartAddonService(sc: SparkContext): Unit = {
    LeadImpl.invokeLeadStartAddonService(sc)
  }

  def getOrderlessHashPartitioning(partitionColumns: Seq[Expression],
      numPartitions: Int, numBuckets: Int): Partitioning = {
    OrderlessHashPartitioning(partitionColumns, numPartitions, numBuckets)
  }
}
