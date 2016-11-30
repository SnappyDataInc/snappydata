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

package org.apache.spark.sql.internal

import scala.collection.concurrent.TrieMap

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, ColocationHelper, PartitionedRegion}

import org.apache.spark.Partition
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.aqp.SnappyContextFunctions
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, ReorderJoin}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FindDataSourceTable, HadoopFsRelation, LogicalRelation, ResolveDataSource, StoreDataSourceStrategy}
import org.apache.spark.sql.execution.{QueryExecution, SparkOptimizer, SparkPlan, SparkPlanner, datasources}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, WindowLogicalPlan}
import org.apache.spark.sql.{AnalysisException, DMLExternalTable, SnappyAggregation, SnappySession, SnappySqlParser, SnappyStrategies, Strategy, _}
import org.apache.spark.streaming.Duration


class SnappySessionState(snappySession: SnappySession)
    extends SessionState(snappySession) {

  self =>

  @transient
  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  protected lazy val sharedState: SnappySharedState =
    snappySession.sharedState.asInstanceOf[SnappySharedState]

  lazy val metadataHive = sharedState.metadataHive.newSession()

  override lazy val sqlParser: SnappySqlParser =
    contextFunctions.newSQLParser(this.snappySession)

  override lazy val analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules =
      new PreprocessTableInsertOrPut(conf) ::
          new FindDataSourceTable(snappySession) ::
          DataSourceAnalysis(conf) ::
          ResolveRelationsExtended ::
          AnalyzeDMLExternalTables(snappySession) ::
          ResolveQueryHints(snappySession) ::
          (if (conf.runSQLonFile) new ResolveDataSource(snappySession) :: Nil else Nil)

    override val extendedCheckRules = Seq(
      datasources.PreWriteCheck(conf, catalog), PrePutCheck)
  }

  override lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods) {
    override def batches: Seq[Batch] = {
      implicit val ss = snappySession
      var insertedSnappyOpts = 0
      val modified = super.batches.map {
        case batch if batch.name.equalsIgnoreCase("Operator Optimizations") =>
          insertedSnappyOpts += 1
          val (left, right) = batch.rules.splitAt(batch.rules.indexOf(ReorderJoin))
          Batch(batch.name, batch.strategy, left ++ Some(ResolveIndex()) ++ right
              : _*)
        case b => b
      }

      if (insertedSnappyOpts != 1) {
        throw new AnalysisException("Snappy Optimizations not applied")
      }

      modified :+
          Batch("Streaming SQL Optimizers", Once, PushDownWindowLogicalPlan)
    }
  }

  object PushDownWindowLogicalPlan extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      var duration: Duration = null
      var slide: Option[Duration] = None
      var transformed: Boolean = false
      plan transformDown {
        case win@WindowLogicalPlan(d, s, child, false) =>
          child match {
            case LogicalRelation(_, _, _) |
                 LogicalDStreamPlan(_, _) => win
            case _ => duration = d
              slide = s
              transformed = true
              win.child
          }
        case c@(LogicalRelation(_, _, _) |
                LogicalDStreamPlan(_, _)) =>
          transformed match {
            case true => transformed = false
              WindowLogicalPlan(duration, slide, c, transformed = true)
            case _ => c
          }
      }
    }
  }


  override lazy val conf: SnappyConf = new SnappyConf(snappySession)

  /**
   * The partition mapping selected for the lead partitioned region in
   * a collocated chain for current execution
   */
  private[this] val leaderPartitions = new TrieMap[PartitionedRegion,
      Array[Partition]]()

  /**
    * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
    */
  object ResolveRelationsExtended extends Rule[LogicalPlan] with PredicateHelper {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i@PutIntoTable(u: UnresolvedRelation, _) =>
        i.copy(table = EliminateSubqueryAliases(getTable(u)))
      case d@DMLExternalTable(_, u: UnresolvedRelation, _) =>
        d.copy(query = EliminateSubqueryAliases(getTable(u)))
    }
  }

  case class AnalyzeDMLExternalTables(sparkSession: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case c: DMLExternalTable if !c.query.resolved =>
        c.copy(query = analyzeQuery(c.query))
    }

    private def analyzeQuery(query: LogicalPlan): LogicalPlan = {
      val qe = sparkSession.sessionState.executePlan(query)
      qe.assertAnalyzed()
      qe.analyzed
    }
  }

  /**
    * Internal catalog for managing table and database states.
    */
  override lazy val catalog = new SnappyStoreHiveCatalog(
      sharedState.externalCatalog,
      snappySession,
      metadataHive,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())

  override def planner: SparkPlanner = new DefaultPlanner(snappySession, conf,
    experimentalMethods.extraStrategies)


  override def executePlan(plan: LogicalPlan): QueryExecution = {
    conf.refreshNumShufflePartitions()
    leaderPartitions.clear()
    contextFunctions.executePlan(snappySession, plan)
  }

  def getTablePartitions(region: PartitionedRegion): Array[Partition] = {
    val leaderRegion = ColocationHelper.getLeaderRegion(region)
    leaderPartitions.getOrElseUpdate(leaderRegion,
      StoreUtils.getPartitionsPartitionedTable(snappySession, leaderRegion))
  }

  def getTablePartitions(region: CacheDistributionAdvisee): Array[Partition] =
    StoreUtils.getPartitionsReplicatedTable(snappySession, region)
}

private[sql] class SnappyConf(@transient val session: SnappySession)
    extends SQLConf with Serializable with CatalystConf {

  /**
   * Records the number of shuffle partitions to be used determined on runtime
   * from available cores on the system. A value <= 0 indicates that it was set
   * explicitly by user and should not use a dynamic value.
   */
  @volatile private[this] var dynamicShufflePartitions: Int = SQLConf
      .SHUFFLE_PARTITIONS.defaultValue match {
    case _ if session == null => -1
    case Some(d) => if (super.numShufflePartitions == d) {
      session.sparkContext.schedulerBackend.defaultParallelism()
    } else -1
    case None => session.sparkContext.schedulerBackend.defaultParallelism()
  }

  private[this] def checkShufflePartitionsKey(key: String): Unit = {
    if (key == SQLConf.SHUFFLE_PARTITIONS.key) dynamicShufflePartitions = -1
  }

  private[sql] def refreshNumShufflePartitions(): Unit = synchronized {
    if (dynamicShufflePartitions != -1 && session != null) {
      dynamicShufflePartitions = session.sparkContext.schedulerBackend
          .defaultParallelism()
    }
  }

  override def numShufflePartitions: Int = {
    val partitions = this.dynamicShufflePartitions
    if (partitions > 0) partitions else super.numShufflePartitions
  }

  override def setConfString(key: String, value: String): Unit = {
    checkShufflePartitionsKey(key)
    super.setConfString(key, value)
  }

  override def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    checkShufflePartitionsKey(entry.key)
    super.setConf[T](entry, value)
  }
}

class DefaultPlanner(snappySession: SnappySession, conf: SQLConf,
    extraStrategies: Seq[Strategy])
    extends SparkPlanner(snappySession.sparkContext, conf, extraStrategies)
        with SnappyStrategies {

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }

  private val storeOptimizedRules: Seq[Strategy] =
    Seq(StoreDataSourceStrategy, SnappyAggregation, LocalJoinStrategies)

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies,
      StoreStrategy, StreamQueryStrategy) ++
        storeOptimizedRules ++
        super.strategies
}

private[sql] final class PreprocessTableInsertOrPut(conf: SQLConf)
    extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r: SchemaInsertableRelation,
    _, _), _, child, _, _) if l.resolved && child.resolved =>
      r.insertableRelation(child.output) match {
        case Some(ir) =>
          val relation = LogicalRelation(ir.asInstanceOf[BaseRelation],
            l.expectedOutputAttributes, l.metastoreTableIdentifier)
          castAndRenameChildOutput(i.copy(table = relation),
            relation.output, null, child)
        case None =>
          throw new AnalysisException(s"$l requires that the query in the " +
              "SELECT clause of the INSERT INTO/OVERWRITE statement " +
              "generates the same number of columns as its schema.")
      }

    // Check for PUT
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case p@PutIntoTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l@LogicalRelation(_: RowInsertableRelation, _, _) =>
          // First, make sure the data to be inserted have the same number of
          // fields with the schema of the relation.
          val expectedOutput = l.output
          if (expectedOutput.size != child.output.size) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "SELECT clause of the PUT INTO statement " +
                "generates the same number of columns as its schema.")
          }
          castAndRenameChildOutput(p, expectedOutput, l, child)

        case _ => p
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, partition, child, _, _)
      if table.resolved && child.resolved => table match {
      case relation: CatalogRelation =>
        val metadata = relation.catalogTable
        preprocess(i, metadata.identifier.quotedString,
          metadata.partitionColumnNames)
      case LogicalRelation(h: HadoopFsRelation, _, identifier) =>
        val tblName = identifier.map(_.quotedString).getOrElse("unknown")
        preprocess(i, tblName, h.partitionSchema.map(_.name))
      case LogicalRelation(_: InsertableRelation, _, identifier) =>
        val tblName = identifier.map(_.quotedString).getOrElse("unknown")
        preprocess(i, tblName, Nil)
      case other => i
    }

  }

  private def preprocess(
      insert: InsertIntoTable,
      tblName: String,
      partColNames: Seq[String]): InsertIntoTable = {

    val expectedColumns = insert.expectedColumns
    val child = insert.child
    if (expectedColumns.isDefined && expectedColumns.get.length != child.schema.length) {
      throw new AnalysisException(
        s"Cannot insert into table $tblName because the number of columns are different: " +
            s"need ${expectedColumns.get.length} columns, " +
            s"but query has ${child.schema.length} columns.")
    }

    if (insert.partition.nonEmpty) {
      // the query's partitioning must match the table's partitioning
      // this is set for queries like: insert into ... partition (one = "a", two = <expr>)
      val samePartitionColumns =
      if (conf.caseSensitiveAnalysis) {
        insert.partition.keySet == partColNames.toSet
      } else {
        insert.partition.keySet.map(_.toLowerCase) == partColNames.map(_.toLowerCase).toSet
      }
      if (!samePartitionColumns) {
        throw new AnalysisException(
          s"""
             |Requested partitioning does not match the table $tblName:
             |Requested partitions: ${insert.partition.keys.mkString(",")}
             |Table partitions: ${partColNames.mkString(",")}
           """.stripMargin)
      }
      expectedColumns.map(castAndRenameChildOutput(insert, _, null, child))
          .getOrElse(insert)
    } else {
      // All partition columns are dynamic because because the InsertIntoTable
      // command does not explicitly specify partitioning columns.
      expectedColumns.map(castAndRenameChildOutput(insert, _, null, child))
          .getOrElse(insert).copy(partition = partColNames.map(_ -> None).toMap)
    }
  }

  /**
    * If necessary, cast data types and rename fields to the expected
    * types and names.
    */
  // TODO: do we really need to rename?
  def castAndRenameChildOutput[T <: LogicalPlan](
      plan: T,
      expectedOutput: Seq[Attribute],
      newRelation: LogicalRelation,
      child: LogicalPlan): T = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
            expected.name == actual.name) {
          actual
        } else {
          Alias(Cast(actual, expected.dataType), expected.name)()
        }
    }

    if (newChildOutput == child.output) {
      plan match {
        case p: PutIntoTable => p.copy(table = newRelation).asInstanceOf[T]
        case i: InsertIntoTable => plan
      }
    } else plan match {
      case p: PutIntoTable => p.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case i: InsertIntoTable => i.copy(child = Project(newChildOutput,
        child)).asInstanceOf[T]
    }
  }
}

private[sql] case object PrePutCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case PutIntoTable(LogicalRelation(t: RowPutRelation, _, _), query) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _) => src
        }
        if (srcRelations.contains(t)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }

      case PutIntoTable(table, query) =>
        throw Utils.analysisException(s"$table does not allow puts.")

      case _ => // OK
    }
  }
}
