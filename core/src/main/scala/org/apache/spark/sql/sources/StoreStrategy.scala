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
package org.apache.spark.sql.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTableUsingAsSelect, LogicalRelation}
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}
import org.apache.spark.sql.types.DataType

/**
 * Support for DML and other operations on external tables.
 *
 * @author rishim
 */
object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
    false, opts, allowExisting, _) =>
      ExecutedCommand(CreateMetastoreTableUsing(tableIdent, None,
        userSpecifiedSchema, None, SnappyContext.getProvider(provider,
          onlyBuiltIn = false), allowExisting, opts, isBuiltIn = false)) :: Nil

    case CreateTableUsingAsSelect(tableIdent, provider, false,
    partitionCols, mode, opts, query) =>
      // CreateTableUsingSelect is only invoked by DataFrameWriter etc
      // so that should support both builtin and external tables
      ExecutedCommand(CreateMetastoreTableUsingSelect(tableIdent, None,
        SnappyContext.getProvider(provider, onlyBuiltIn = false),
        partitionCols, mode, opts, query, isBuiltIn = false)) :: Nil

    case create: CreateMetastoreTableUsing =>
      ExecutedCommand(create) :: Nil
    case createSelect: CreateMetastoreTableUsingSelect =>
      ExecutedCommand(createSelect) :: Nil
    case drop: DropTable =>
      ExecutedCommand(drop) :: Nil

    case DMLExternalTable(name, storeRelation: LogicalRelation, insertCommand) =>
      ExecutedCommand(ExternalTableDMLCmd(storeRelation, insertCommand)) :: Nil

    case PutIntoTable(l@LogicalRelation(t: RowPutRelation, _), query) =>
      ExecutedCommand(PutIntoDataSource(l, t, query)) :: Nil

    case _ => Nil
  }
}

private[sql] case class ExternalTableDMLCmd(
    storeRelation: LogicalRelation,
    command: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    storeRelation.relation match {
      case relation: UpdatableRelation => relation.executeUpdate(command)
      case relation: RowPutRelation => relation.executeUpdate(command)
      case relation: SingleRowInsertableRelation =>
        relation.executeUpdate(command)
      case other => throw new AnalysisException("DML support requires " +
          "UpdatableRelation/SingleRowInsertableRelation/RowPutRelation" +
          " but found " + other)
    }
    Seq.empty
  }
}

private[sql] case class PutIntoTable(
    table: LogicalPlan,
    child: LogicalPlan)
    extends LogicalPlan {

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override def output: Seq[Attribute] = Seq.empty

  override lazy val resolved: Boolean = childrenResolved &&
      child.output.zip(table.output).forall {
        case (childAttr, tableAttr) =>
          DataType.equalsIgnoreCompatibleNullability(childAttr.dataType,
            tableAttr.dataType)
      }
}

/**
 * Puts the results of `query` in to a relation that extends [[RowPutRelation]].
 */
private[sql] case class PutIntoDataSource(
    logicalRelation: LogicalRelation,
    relation: RowPutRelation,
    query: LogicalPlan)
    extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.internalCreateDataFrame(data.queryExecution.toRdd,
      logicalRelation.schema)
    relation.put(df)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}
