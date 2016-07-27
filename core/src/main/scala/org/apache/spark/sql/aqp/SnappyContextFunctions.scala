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
package org.apache.spark.sql.aqp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.{InternalRow, ParserDialect}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.DDLParser
import org.apache.spark.sql.hive.{QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

trait SnappyContextFunctions {

  def clear(): Unit

  def registerAQPErrorFunctions(context: SnappyContext)

  def postRelationCreation(relation: BaseRelation, snc: SnappyContext): Unit

  protected[sql] def executePlan(context: SnappyContext,
      plan: LogicalPlan): QueryExecution

  def getAQPRuleExecutor(context: SQLContext): RuleExecutor[SparkPlan]

  def createTopK(context: SnappyContext, topKName: String,
      keyColumnName: String, inputDataSchema: StructType,
      topkOptions: Map[String, String], ifExists: Boolean): Unit

  def dropTopK(context: SnappyContext, topKName: String): Unit

  def insertIntoTopK(context: SnappyContext, rows: RDD[Row],
      topKName: QualifiedTableName, time: Long): Unit

  def queryTopK(context: SnappyContext, topKName: String,
      startTime: String = null, endTime: String = null,
      k: Int = -1): DataFrame

  def queryTopK(context: SnappyContext, topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame

  def queryTopKRDD(context: SnappyContext, topK: String,
      startTime: String, endTime: String, schema: StructType): RDD[InternalRow]

  protected[sql] def collectSamples(context: SnappyContext, rows: RDD[Row],
      aqpTables: Seq[String], time: Long): Unit

  def withErrorDataFrame(df: DataFrame, error: Double,
      confidence: Double, behavior: String): DataFrame

  def createSampleDataFrameContract(context: SnappyContext,
      df: DataFrame, logicalPlan: LogicalPlan): SampleDataFrameContract

  def convertToStratifiedSample(options: Map[String, Any],
      snc: SnappyContext, logicalPlan: LogicalPlan): LogicalPlan

  def isStratifiedSample(logicalPlan: LogicalPlan): Boolean

  def getPlanner(context: SnappyContext): SparkPlanner

  def getSQLDialect(context: SnappyContext): ParserDialect

  def aqpTablePopulator(context: SnappyContext): Unit

  def getSnappyCatalog(context: SnappyContext): SnappyStoreHiveCatalog

  def getSnappyDDLParser(context: SnappyContext,
      planGenerator: String => LogicalPlan): DDLParser

  def createAnalyzer(context: SnappyContext): Analyzer

  def handleErrorLimitExceeded[T](fn: => (RDD[InternalRow], DataFrame) => T,
      rowRDD: RDD[InternalRow], df: DataFrame, lp: LogicalPlan): T

  def sql[T](fn: => T): T
}
