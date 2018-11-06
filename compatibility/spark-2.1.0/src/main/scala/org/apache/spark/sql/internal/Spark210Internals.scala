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
package org.apache.spark.sql.internal

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import io.snappydata.{HintName, QueryHint}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, FunctionResource}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CurrentRow, ExprId, Expression, ExpressionInfo, FrameBoundary, FrameType, Generator, Literal, NamedExpression, NullOrdering, PredicateSubquery, SortDirection, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, ValueFollowing, ValuePreceding}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLBuilder, TableIdentifier}
import org.apache.spark.sql.execution.command.{ClearCacheCommand, CreateFunctionCommand, DescribeTableCommand}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation, PreWriteCheck}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.ui.{SQLTab, SnappySQLListener}
import org.apache.spark.sql.execution.{SparkOptimizer, SparkPlan, WholeStageCodegenExec, aggregate}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, Metadata}

/**
 * Implementation of [[SparkInternals]] for Spark 2.1.0.
 */
class Spark210Internals extends SparkInternals {

  override def version: String = "2.1.0"

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(plan.schema)
    spark.sharedState.cacheManager.uncacheQuery(Dataset(spark, plan), blocking)
  }

  /**
   * Apply a map function to each expression present in this query operator, and return a new
   * query operator based on the mapped expressions.
   *
   * Taken from the mapExpressions in Spark 2.1.1 and beyond.
   */
  override def mapExpressions(plan: LogicalPlan, f: Expression => Expression): LogicalPlan = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = f(e)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(e: Expression) => Some(transformExpression(e))
      case Some(seq: Traversable[_]) => Some(seq.map(recursiveTransform))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    /**
     * Efficient alternative to `productIterator.map(f).toArray`.
     */
    def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
      val arr = Array.ofDim[B](plan.productArity)
      var i = 0
      while (i < arr.length) {
        arr(i) = f(plan.productElement(i))
        i += 1
      }
      arr
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) plan.makeCopy(newArgs).asInstanceOf[plan.type] else plan
  }

  override def registerFunction(session: SparkSession, name: FunctionIdentifier,
      info: ExpressionInfo, function: Seq[Expression] => Expression): Unit = {
    session.sessionState.functionRegistry.registerFunction(name.unquotedString, info, function)
  }

  override def addClassField(ctx: CodegenContext, javaType: String,
      varName: String, initFunc: String => String,
      forceInline: Boolean, useFreshName: Boolean): String = {
    val variableName = if (useFreshName) ctx.freshName(varName) else varName
    ctx.addMutableState(javaType, varName, initFunc(variableName))
    variableName
  }

  override def addFunction(ctx: CodegenContext, funcName: String, funcCode: String,
      inlineToOuterClass: Boolean = false): String = {
    ctx.addNewFunction(funcName, funcCode)
    funcName
  }

  override def isFunctionAddedToOuterClass(ctx: CodegenContext, funcName: String): Boolean = {
    ctx.addedFunctions.contains(funcName)
  }

  override def splitExpressions(ctx: CodegenContext, expressions: Seq[String]): String = {
    ctx.splitExpressions(ctx.INPUT_ROW, expressions)
  }

  override def resetCopyResult(ctx: CodegenContext): Unit = ctx.copyResult = false

  override def isPredicateSubquery(expr: Expression): Boolean = expr match {
    case _: PredicateSubquery => true
    case _ => false
  }

  override def copyPredicateSubquery(expr: Expression, newPlan: LogicalPlan,
      newExprId: ExprId): Expression = {
    expr.asInstanceOf[PredicateSubquery].copy(plan = newPlan, exprId = newExprId)
  }

  override def newWholeStagePlan(plan: SparkPlan): WholeStageCodegenExec = {
    WholeStageCodegenExec(plan)
  }

  override def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String] = {
    new CaseInsensitiveMap(map)
  }

  def createAndAttachSQLListener(sparkContext: SparkContext): Unit = {
    // if the call is done the second time, then attach in embedded mode
    // too since this is coming from ToolsCallbackImpl
    val (forceAttachUI, listener) = SparkSession.sqlListener.get() match {
      case l: SnappySQLListener => true -> l // already set
      case _ =>
        val listener = new SnappySQLListener(sparkContext.conf)
        if (SparkSession.sqlListener.compareAndSet(null, listener)) {
          sparkContext.addSparkListener(listener)
        }
        false -> listener
    }
    // embedded mode attaches SQLTab later via ToolsCallbackImpl that also
    // takes care of injecting any authentication module if configured
    sparkContext.ui match {
      case Some(ui) if forceAttachUI || !SnappyContext.getClusterMode(sparkContext)
          .isInstanceOf[SnappyEmbeddedMode] => new SQLTab(listener, ui)
      case _ =>
    }
  }

  def createAndAttachSQLListener(state: SharedState): Unit = {
    // check that SparkSession.sqlListener should be set correctly
    SparkSession.sqlListener.get() match {
      case _: SnappySQLListener =>
      case l =>
        throw new IllegalStateException(s"expected SnappySQLListener to be set but was $l")
    }
  }

  def clearSQLListener(): Unit = {
    SparkSession.sqlListener.set(null)
  }

  override def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String = {
    val viewSQL = new SQLBuilder(plan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }
    viewSQL
  }

  override def createView(desc: CatalogTable, output: Seq[Attribute],
      child: LogicalPlan): LogicalPlan = child

  override def newCreateFunctionCommand(schemaName: Option[String], functionName: String,
      className: String, resources: Seq[FunctionResource], isTemp: Boolean,
      ignoreIfExists: Boolean, replace: Boolean): LogicalPlan = {
    if (ignoreIfExists) {
      throw new ParseException(s"CREATE FUNCTION does not support IF NOT EXISTS in Spark $version")
    }
    if (replace) {
      throw new ParseException(s"CREATE FUNCTION does not support REPLACE in Spark $version")
    }
    CreateFunctionCommand(schemaName, functionName, className, resources, isTemp)
  }

  override def newDescribeTableCommand(table: TableIdentifier,
      partitionSpec: Map[String, String], isExtended: Boolean): LogicalPlan = {
    DescribeTableCommand(table, partitionSpec, isExtended, isFormatted = false)
  }

  override def newClearCacheCommand(): LogicalPlan = ClearCacheCommand

  override def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
      ivyPath: Option[String], exclusions: Seq[String]): String = {
    SparkSubmitUtils.resolveMavenCoordinates(coordinates, remoteRepos, ivyPath, exclusions)
  }

  override def copyAttribute(attr: AttributeReference)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata): AttributeReference = {
    attr.copy(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId = attr.exprId, qualifier = attr.qualifier, isGenerated = attr.isGenerated)
  }

  override def newInsertPlanWithCountOutput(table: LogicalPlan,
      partition: Map[String, Option[String]], child: LogicalPlan,
      overwrite: Boolean, ifNotExists: Boolean): LogicalPlan = {
    new Insert(table, partition, child, OverwriteOptions(enabled = overwrite), ifNotExists)
  }

  override def newGroupingSet(groupingSets: Seq[Seq[Expression]],
      groupByExprs: Seq[Expression], child: LogicalPlan,
      aggregations: Seq[NamedExpression]): LogicalPlan = {
    val keyMap = groupByExprs.zipWithIndex.toMap
    val numExpressions = keyMap.size
    val mask = (1 << numExpressions) - 1
    val bitmasks: Seq[Int] = groupingSets.map(set => set.foldLeft(mask)((bitmap, col) => {
      if (!keyMap.contains(col)) {
        throw new ParseException(s"GROUPING SETS column '$col' does not appear in GROUP BY list")
      }
      bitmap & ~(1 << (numExpressions - 1 - keyMap(col)))
    }))
    GroupingSets(bitmasks, groupByExprs, child, aggregations)
  }

  override def newUnresolvedRelation(tableIdentifier: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    UnresolvedRelation(tableIdentifier, alias)
  }

  override def newSubqueryAlias(alias: String, child: LogicalPlan): SubqueryAlias = {
    SubqueryAlias(alias, child, view = None)
  }

  override def newUnresolvedColumnAliases(outputColumnNames: Seq[String],
      child: LogicalPlan): LogicalPlan = {
    if (outputColumnNames.isEmpty) child
    else {
      throw new ParseException(s"Aliases ($outputColumnNames) for column names " +
          s"of a sub-plan not supported in Spark $version")
    }
  }

  override def newSortOrder(child: Expression, direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = {
    SortOrder(child, direction, nullOrdering)
  }

  override def newRepartitionByExpression(partitionExpressions: Seq[Expression],
      numPartitions: Int, child: LogicalPlan): RepartitionByExpression = {
    RepartitionByExpression(partitionExpressions, child, Some(numPartitions))
  }

  override def newUnresolvedTableValuedFunction(functionName: String,
      functionArgs: Seq[Expression], outputNames: Seq[String]): UnresolvedTableValuedFunction = {
    if (outputNames.nonEmpty) {
      throw new ParseException(s"Aliases ($outputNames) for table value function " +
          s"'$functionName' not supported in Spark $version")
    }
    UnresolvedTableValuedFunction(functionName, functionArgs)
  }

  private def boundaryInt(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): Int = num match {
    case Some(l: Literal) => l.value.toString.toInt
    case _ => throw new ParseException(
      s"Expression ($num) in frame boundary ($boundaryType) not supported in Spark $version")
  }

  override def newFrameBoundary(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): FrameBoundary = {
    boundaryType match {
      case FrameBoundaryType.UnboundedPreceding => UnboundedPreceding
      case FrameBoundaryType.ValuePreceding => ValuePreceding(boundaryInt(boundaryType, num))
      case FrameBoundaryType.CurrentRow => CurrentRow
      case FrameBoundaryType.UnboundedFollowing => UnboundedFollowing
      case FrameBoundaryType.ValueFollowing => ValueFollowing(boundaryInt(boundaryType, num))
    }
  }

  override def newSpecifiedWindowFrame(frameType: FrameType, frameStart: Any,
      frameEnd: Any): SpecifiedWindowFrame = {
    SpecifiedWindowFrame(frameType, frameStart.asInstanceOf[FrameBoundary],
      frameEnd.asInstanceOf[FrameBoundary])
  }

  override def newLogicalPlanWithHints(child: LogicalPlan,
      hints: Map[QueryHint.Type, HintName.Type]): LogicalPlanWithHints = {
    new PlanWithHints(child, hints)
  }

  override def isHintPlan(plan: LogicalPlan): Boolean = plan.isInstanceOf[BroadcastHint]

  override def getHints(plan: LogicalPlan): Map[QueryHint.Type, HintName.Type] = plan match {
    case p: PlanWithHints => p.allHints
    case _: BroadcastHint => Map(QueryHint.JoinType -> HintName.JoinType_Broadcast)
    case _ => Map.empty
  }

  override def isBroadcastable(plan: LogicalPlan): Boolean = plan.statistics.isBroadcastable

  override def newOneRowRelation(): LogicalPlan = OneRowRelation

  override def newGeneratePlan(generator: Generator, outer: Boolean, qualifier: Option[String],
      generatorOutput: Seq[Attribute], child: LogicalPlan): LogicalPlan = {
    Generate(generator, join = true, outer, qualifier, generatorOutput, child)
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.write(mode, data)
    ds.copy(userSpecifiedSchema = Some(data.schema.asNullable)).resolveRelation()
  }

  override def newLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Option[Seq[AttributeReference]],
      catalogTable: Option[CatalogTable], isStreaming: Boolean): LogicalRelation = {
    if (isStreaming) {
      throw new ParseException(s"Streaming relations not supported in Spark $version")
    }
    LogicalRelation(relation, expectedOutputAttributes, catalogTable)
  }

  override def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange = {
    ShuffleExchange(newPartitioning, child)
  }

  override def getStatistics(plan: LogicalPlan): Statistics = plan.statistics

  override def supportsPartial(aggregate: AggregateFunction): Boolean = aggregate.supportsPartial

  override def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression], resultExpressions: Seq[NamedExpression],
      planChild: () => SparkPlan): Seq[SparkPlan] = {
    aggregate.AggUtils.planAggregateWithoutPartial(
      groupingExpressions,
      aggregateExpressions,
      resultExpressions,
      planChild())
  }

  override def compile(code: CodeAndComment): GeneratedClass = CodeGenerator.compile(code)

  override def newJSONOptions(parameters: Map[String, String],
      session: Option[SparkSession]): JSONOptions = new JSONOptions(parameters)

  override def newSparkOptimizer(sessionState: SnappySessionState): SparkOptimizer = {
    new SparkOptimizer(sessionState.catalog, sessionState.conf, sessionState.experimentalMethods)
        with DefaultOptimizer {
      override def state: SnappySessionState = sessionState
    }
  }

  override def newPreWriteCheck(sessionState: SnappySessionState): LogicalPlan => Unit = {
    PreWriteCheck(sessionState.conf, sessionState.catalog)
  }
}
