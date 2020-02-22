/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.internal

import io.snappydata.Property

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SnappySession, SparkSession, SparkSupport}

/**
 * Helper object for PutInto operations for column tables.
 * This class takes the logical plans from SnappyParser
 * and converts it into another plan.
 */
object ColumnTableBulkOps extends SparkSupport {

  def transformPutPlan(session: SnappySession, originalPlan: PutIntoTable): LogicalPlan = {
    validateOp(originalPlan)
    val table = originalPlan.table
    val subQuery = originalPlan.child
    var transFormedPlan: LogicalPlan = originalPlan

    table.collectFirst {
      case lr: LogicalRelation if lr.relation.isInstanceOf[BulkPutRelation] =>
        val mutable = lr.relation.asInstanceOf[BulkPutRelation]
        val putKeys = mutable.getPutKeys(session) match {
          case None => throw new AnalysisException(
            s"PutInto in a column table requires key column(s) but got empty string")
          case Some(k) => k
        }
        val condition = prepareCondition(session, table, subQuery, putKeys)

        val analyzer = session.sessionState.analyzer
        val resolver = analyzer.resolver
        val keyColumns = getKeyColumns(table)
        var updateSubQuery: LogicalPlan = Join(table, subQuery, Inner, condition)
        val updateColumns = table.output.filterNot(a => keyColumns.exists(resolver(_, a.name)))
        val updateExpressions = subQuery.output.filterNot(
          a => keyColumns.exists(resolver(_, a.name)))
        if (updateExpressions.isEmpty) {
          throw new AnalysisException(
            s"PutInto is attempted without any column which can be updated." +
                s" Provide some columns apart from key column(s)")
        }

        val cacheSize = ExternalStoreUtils.sizeAsBytes(
          Property.PutIntoInnerJoinCacheSize.get(session.sqlContext.conf),
          Property.PutIntoInnerJoinCacheSize.name, -1, Long.MaxValue)

        val updatePlan = Update(table, updateSubQuery, Nil,
          updateColumns, updateExpressions)
        // val updateDS = new Dataset(session, updatePlan, RowEncoder(updatePlan.schema))
        var analyzedUpdate = analyzer.execute(updatePlan).asInstanceOf[Update]
        // updateDS.queryExecution.analyzed.asInstanceOf[Update]
        updateSubQuery = analyzedUpdate.child

        // explicitly project out only the updated expression references and key columns
        // from the sub-query to minimize cache (if it is selected to be done)
        val updateReferences = AttributeSet(updateExpressions.flatMap(_.references))
        updateSubQuery = Project(updateSubQuery.output.filter(a =>
          updateReferences.contains(a) || keyColumns.exists(resolver(_, a.name)) ||
              putKeys.exists(resolver(_, a.name))), updateSubQuery)

        val insertChild = session.cachePutInto(internals.getStatistics(subQuery)
            .sizeInBytes <= cacheSize, updateSubQuery, mutable.table) match {
          case None => subQuery
          case Some(newUpdateSubQuery) =>
            if (updateSubQuery ne newUpdateSubQuery) {
              updateSubQuery = newUpdateSubQuery
              analyzedUpdate = analyzedUpdate.copy(child = updateSubQuery)
            }
            // project out the columns already present in subQuery
            val subQueryOutput = subQuery.output
            if (subQueryOutput.intersect(updateSubQuery.output).nonEmpty) {
              updateSubQuery = Project(updateSubQuery.output.filterNot(
                subQueryOutput.contains), updateSubQuery)
            }
            Join(subQuery, updateSubQuery, LeftAnti, condition)
        }
        val insertPlan = internals.newInsertPlanWithCountOutput(table, Map.empty[String,
            Option[String]], Project(subQuery.output, insertChild),
          overwrite = false, ifNotExists = false)
        transFormedPlan = PutIntoColumnTable(table, analyzer.execute(insertPlan), analyzedUpdate)
      case _ => // Do nothing, original putInto plan is enough
    }
    transFormedPlan
  }

  def validateOp(originalPlan: PutIntoTable) {
    originalPlan match {
      case PutIntoTable(lr: LogicalRelation, query) if lr.relation.isInstanceOf[BulkPutRelation] =>
        val srcRelations = query.collect {
          case r: LogicalRelation => r.relation
        }
        if (srcRelations.contains(lr.relation)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }
      case _ => // OK
    }
  }

  private def prepareCondition(sparkSession: SparkSession,
      table: LogicalPlan,
      child: LogicalPlan,
      columnNames: Seq[String]): Option[Expression] = {
    val analyzer = sparkSession.sessionState.analyzer
    val leftKeys = columnNames.map { keyName =>
      table.output.find(attr => analyzer.resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"key column `$keyName` cannot be resolved on the left " +
            s"side of the operation. The left-side columns: [${
              table.
                  output.map(_.name).mkString(", ")
            }]")
      }
    }
    val rightKeys = columnNames.map { keyName =>
      child.output.find(attr => analyzer.resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"USING column `$keyName` cannot be resolved on the right " +
            s"side of the operation. The right-side columns: [${
              child.
                  output.map(_.name).mkString(", ")
            }]")
      }
    }
    val joinPairs = leftKeys.zip(rightKeys)
    val newCondition = joinPairs.map(EqualTo.tupled).reduceOption(And)
    newCondition
  }

  def getKeyColumns(table: LogicalPlan): Set[String] = {
    table.collectFirst {
      case lr: LogicalRelation if lr.relation.isInstanceOf[MutableRelation] =>
        lr.relation.asInstanceOf[MutableRelation].getKeyColumns.toSet
    } match {
      case None => throw new AnalysisException(
        s"Update/Delete requires a MutableRelation but got $table")
      case Some(k) => k
    }
  }

  def transformDeletePlan(session: SnappySession,
      originalPlan: DeleteFromTable): LogicalPlan = {
    val table = originalPlan.table
    val subQuery = originalPlan.child
    var transFormedPlan: LogicalPlan = originalPlan

    table.collectFirst {
      case lr: LogicalRelation if lr.relation.isInstanceOf[MutableRelation] =>
        val ks = lr.relation.asInstanceOf[MutableRelation].getPrimaryKeyColumns(session)
        if (ks.isEmpty) {
          throw new AnalysisException(
            s"DeleteFrom operation requires key columns(s) or primary key defined on table.")
        }
        val condition = prepareCondition(session, table, subQuery, ks)
        val exists = Join(subQuery, table, Inner, condition)
        val deletePlan = Delete(table, exists, Nil)
        val deleteDs = new Dataset(session, deletePlan, RowEncoder(deletePlan.schema))
        transFormedPlan = deleteDs.queryExecution.analyzed.asInstanceOf[Delete]
    }
    transFormedPlan
  }

  def bulkInsertOrPut(rows: Seq[Row], sparkSession: SparkSession,
      schema: StructType, resolvedName: String, putInto: Boolean): Int = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val tableIdent = session.tableIdentifier(resolvedName)
    val encoder = RowEncoder(schema)
    val ds = internals.internalCreateDataFrame(session, session.sparkContext.parallelize(
      rows.map(encoder.toRow)), schema)
    val plan = if (putInto) {
      PutIntoTable(
        table = UnresolvedRelation(tableIdent),
        child = ds.logicalPlan)
    } else {
      internals.newInsertPlanWithCountOutput(
        table = UnresolvedRelation(tableIdent),
        partition = Map.empty[String, Option[String]],
        child = ds.logicalPlan,
        overwrite = false,
        ifNotExists = false)
    }
    session.sessionState.executePlan(plan).executedPlan.executeCollect()
        // always expect to create a TableInsertExec
        .foldLeft(0)(_ + _.getInt(0))
  }
}

case class PutIntoColumnTable(table: LogicalPlan,
    insert: LogicalPlan, update: LogicalPlan) extends BinaryNode {

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override lazy val resolved: Boolean = childrenResolved &&
      update.output.zip(insert.output).forall {
        case (updateAttr, insertAttr) =>
          DataType.equalsIgnoreCompatibleNullability(updateAttr.dataType,
            insertAttr.dataType)
      }

  override def left: LogicalPlan = update

  override def right: LogicalPlan = insert
}
