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
package org.apache.spark.sql

import org.apache.spark.sql.aqp.DefaultPlanner
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.InMemoryColumnarTableScan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.streaming._

/**
 * This trait is an extension to SparkPlanner and introduces number of enhancements specific to Snappy.
 */
private[sql] trait SnappyStrategies {

  self: DefaultPlanner =>

  object SnappyStrategies extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      val x: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
        case PhysicalOperation(projectList, filters,
        mem: org.apache.spark.sql.execution.columnar.InMemoryAppendableRelation) =>
          pruneFilterProject(
            projectList,
            filters,
            identity[Seq[Expression]], // All filters still need to be evaluated
            InMemoryColumnarTableScan(_, filters, mem)) :: Nil
      }

      x.orElse(sampleSnappyCase)(plan)

    }
  }

  /** Stream related strategies to map stream specific logical plan to physical plan */
  object StreamQueryStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case LogicalDStreamPlan(output, rowStream) =>
        PhysicalDStreamPlan(output, rowStream) :: Nil
      case WindowLogicalPlan(d, s, l@LogicalRelation(t: StreamPlan, _)) =>
        val child = PhysicalDStreamPlan(l.output, t.rowStream)
        WindowPhysicalPlan(d, s, child) :: Nil
      case WindowLogicalPlan(d, s, child) =>
        WindowPhysicalPlan(d, s, planLater(child)) :: Nil
      /* case l@LogicalRelation(t: StreamPlan, _) =>
        PhysicalDStreamPlan(l.output, t.rowStream) :: Nil */
      case _ => Nil
    }
  }

  /** Stream related strategies DDL stratgies */
  case class StreamDDLStrategy(sampleStreamCase: PartialFunction[LogicalPlan,
          Seq[SparkPlan]]) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {

      val x: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
        case StreamOperationsLogicalPlan(action, batchInterval) =>
          ExecutedCommand(
            SnappyStreamingActionsCommand(action, batchInterval)) :: Nil

      }
      x.orElse(sampleStreamCase)(plan)
    }
  }

    object LocalJoinStrategies extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, CanLocalJoin(right)) =>
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildRight)
        case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, CanLocalJoin(left), right) =>
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildLeft)
        case _ => Nil
      }
    }

    object CanLocalJoin {
      def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
        case PhysicalOperation(projects, filters,
        l@LogicalRelation(t: PartitionedDataSourceScan, _)) =>
          if (t.numPartitions == 1) Some(plan) else None
        case _ => None
      }
    }

  private[this] def makeLocalHashJoin(
                                       leftKeys: Seq[Expression],
                                       rightKeys: Seq[Expression],
                                       left: LogicalPlan,
                                       right: LogicalPlan,
                                       condition: Option[Expression],
                                       side: joins.BuildSide): Seq[SparkPlan] = {

    val localHashJoin = execution.joins.LocalJoin(
      leftKeys, rightKeys, side, planLater(left), planLater(right))
    condition.map(Filter(_, localHashJoin)).getOrElse(localHashJoin) :: Nil
  }

}
