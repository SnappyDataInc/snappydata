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

package org.apache.spark.sql.execution

import java.io.File
import java.lang
import java.nio.file.{Files, Paths}
import java.sql.SQLException
import java.util.Map.Entry
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.SystemFailure
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.internal.iapi.reference.{Property => GemXDProperty}
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.util.ServiceUtils
import io.snappydata.{Property, SnappyTableStatsProviderService}

import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortDirection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DescribeTableCommand, DropTableCommand, RunnableCommand, ShowTablesCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.BypassRowLevelSecurity
import org.apache.spark.sql.sources.DestroyRelation
import org.apache.spark.sql.types.{BooleanType, LongType, NullType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}
import org.apache.spark.{SparkContext, SparkEnv}

case class CreateTableUsingCommand(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    mode: SaveMode,
    options: Map[String, String],
    query: Option[LogicalPlan],
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val allOptions = session.addBaseTableOption(baseTable, options)
    session.createTableInternal(tableIdent, provider, userSpecifiedSchema,
      schemaDDL, mode, allOptions, isBuiltIn, query)
    Nil
  }
}

/**
 * Like Spark's DropTableCommand but checks for non-existent table case upfront to avoid
 * unnecessary warning logs from Spark's DropTableCommand.
 */
case class DropTableOrViewCommand(
    tableIdent: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean,
    purge: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.asInstanceOf[SnappySession].sessionCatalog

    if (!catalog.isTemporaryTable(tableIdent) && !catalog.tableExists(tableIdent)) {
      val resolved = catalog.resolveTableIdentifier(tableIdent)
      if (ifExists) return Nil
      else throw new TableNotFoundException(resolved.database.get, resolved.table)
    }

    DropTableCommand(tableIdent, ifExists, isView, purge).run(sparkSession)
  }
}

case class CreateSchemaCommand(ifNotExists: Boolean, schemaName: String,
    authId: Option[(String, Boolean)]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionCatalog
    val schema = catalog.formatDatabaseName(schemaName)

    // create schema in catalog first
    catalog.createSchema(schema, ifNotExists)

    // next in store if catalog was successful
    val authClause = authId match {
      case None => ""
      case Some((id, false)) => s""" AUTHORIZATION "$id""""
      case Some((id, true)) => s""" AUTHORIZATION ldapGroup: "$id""""
    }
    val conn = session.defaultPooledConnection(schema)
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(s"""CREATE SCHEMA "$schema"$authClause""")
      stmt.close()
    } catch {
      case se: SQLException if ifNotExists && se.getSQLState == "X0Y68" => // ignore
      case err: Error if SystemFailure.isJVMFailureError(err) =>
        SystemFailure.initiateFailure(err)
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err
      case t: Throwable =>
        // drop from catalog
        catalog.dropDatabase(schema, ignoreIfNotExists = true, cascade = false)
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure()
        throw t
    } finally {
      conn.close()
    }
    Nil
  }
}

case class DropSchemaCommand(schemaName: String, ifExists: Boolean, cascade: Boolean)
    extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionCatalog
    val schema = catalog.formatDatabaseName(schemaName)
    // drop from catalog first to cascade drop all objects if required
    catalog.dropDatabase(schema, ifExists, cascade)
    // drop the schema from store (no cascade required since catalog drop will take care)
    val checkIfExists = if (ifExists) " IF EXISTS" else ""
    val conn = session.defaultPooledConnection(schema)
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(s"""DROP SCHEMA$checkIfExists "$schema" RESTRICT""")
      stmt.close()
    } finally {
      conn.close()
    }
    Nil
  }
}

case class DropPolicyCommand(ifExists: Boolean,
    policyIdentifer: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    snc.dropPolicy(policyIdentifer, ifExists)
    Nil
  }
}

case class TruncateManagedTableCommand(ifExists: Boolean,
    table: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val catalog = session.asInstanceOf[SnappySession].sessionCatalog
    // skip if "ifExists" is true and table does not exist
    if (!(ifExists && !catalog.tableExists(table))) {
      catalog.resolveRelation(table) match {
        case lr: LogicalRelation if lr.relation.isInstanceOf[DestroyRelation] =>
          lr.relation.asInstanceOf[DestroyRelation].truncate()
        case plan => throw new AnalysisException(
          s"Table '$table' must be a DestroyRelation for truncate. Found plan: $plan")
      }
      session.sharedState.cacheManager.uncacheQuery(session.table(table))
    }
    Nil
  }
}

case class AlterTableAddColumnCommand(tableIdent: TableIdentifier,
    addColumn: StructField) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    snc.alterTable(tableIdent, isAddColumn = true, addColumn)
    Nil
  }
}

case class AlterTableToggleRowLevelSecurityCommand(tableIdent: TableIdentifier,
    enableRls: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    snc.alterTableToggleRLS(tableIdent, enableRls)
    Nil
  }
}

case class AlterTableDropColumnCommand(
    tableIdent: TableIdentifier, column: String) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    // drop column doesn't need anything apart from name so fill dummy values
    snc.alterTable(tableIdent, isAddColumn = false, StructField(column, NullType))
    Nil
  }
}

case class CreateIndexCommand(indexName: TableIdentifier,
    baseTable: TableIdentifier,
    indexColumns: Map[String, Option[SortDirection]],
    options: Map[String, String]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    snc.createIndex(indexName, baseTable, indexColumns, options)
    Nil
  }
}

case class CreatePolicyCommand(policyIdent: TableIdentifier,
    tableIdent: TableIdentifier,
    policyFor: String, applyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
    currentUser: String, filterStr: String,
    filter: BypassRowLevelSecurity) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    if (!Misc.isSecurityEnabled && !GemFireStore.ALLOW_RLS_WITHOUT_SECURITY) {
      throw Util.generateCsSQLException(SQLState.SECURITY_EXCEPTION_ENCOUNTERED,
        null, new IllegalStateException("CREATE POLICY failed: Security (" +
            com.pivotal.gemfirexd.Attribute.AUTH_PROVIDER + ") not enabled in the system"))
    }
    if (!Misc.getMemStoreBooting.isRLSEnabled) {
      throw Util.generateCsSQLException(SQLState.SECURITY_EXCEPTION_ENCOUNTERED,
        null, new IllegalStateException("CREATE POLICY failed: Row level security (" +
            GemXDProperty.SNAPPY_ENABLE_RLS + ") not enabled in the system"))
    }
    val snc = session.asInstanceOf[SnappySession]
    SparkSession.setActiveSession(snc)
    snc.createPolicy(policyIdent, tableIdent, policyFor, applyTo, expandedPolicyApplyTo,
      currentUser, filterStr, filter)
    Nil
  }
}

case class DropIndexCommand(ifExists: Boolean,
    indexName: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    snc.dropIndex(indexName, ifExists)
    Nil
  }
}

case class SetSchemaCommand(schemaName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.asInstanceOf[SnappySession].setCurrentSchema(schemaName)
    Nil
  }
}

case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Option[Duration]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {

    def creatingFunc(): SnappyStreamingContext = {
      // batchInterval will always be defined when action == 0
      new SnappyStreamingContext(session.sparkContext, batchInterval.get)
    }

    action match {
      case 0 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(_) => // TODO .We should create a named Streaming
          // Context and check if the configurations match
          case None => SnappyStreamingContext.getActiveOrCreate(creatingFunc)
        }
      case 1 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => x.start()
          case None => throw Utils.analysisException(
            "Streaming Context has not been initialized")
        }
      case 2 =>
        val ssc = SnappyStreamingContext.getActive
        ssc match {
          case Some(strCtx) => strCtx.stop(stopSparkContext = false,
            stopGracefully = true)
          case None => // throw Utils.analysisException(
          // "There is no running Streaming Context to be stopped")
        }
    }
    Nil
  }
}

/**
 * Alternative to Spark's CacheTableCommand that shows the plan being cached
 * in the GUI rather than count() plan for InMemoryRelation.
 */
case class SnappyCacheTableCommand(tableIdent: TableIdentifier, queryString: String,
    plan: Option[LogicalPlan], isLazy: Boolean) extends RunnableCommand {

  require(plan.isEmpty || tableIdent.database.isEmpty,
    "Schema name is not allowed in CACHE TABLE AS SELECT")

  override def output: Seq[Attribute] = AttributeReference(
    "batchCount", LongType)() :: Nil

  override protected def innerChildren: Seq[QueryPlan[_]] = plan match {
    case None => Nil
    case Some(p) => p :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val df = plan match {
      case None => session.table(tableIdent)
      case Some(lp) =>
        val df = Dataset.ofRows(session, lp)
        df.createTempView(tableIdent.quotedString)
        df
    }

    val isOffHeap = ServiceUtils.isOffHeapStorageAvailable(session)

    if (isLazy) {
      if (isOffHeap) df.persist(StorageLevel.OFF_HEAP) else df.persist()
      Nil
    } else {
      val queryShortString = CachedDataFrame.queryStringShortForm(queryString)
      val localProperties = session.sparkContext.getLocalProperties
      val previousJobDescription = localProperties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, queryShortString)
      try {
        session.sessionState.enableExecutionCache = true
        // Get the actual QueryExecution used by InMemoryRelation so that
        // "withNewExecutionId" runs on the same and shows proper metrics in GUI.
        val cachedExecution = try {
          if (isOffHeap) df.persist(StorageLevel.OFF_HEAP) else df.persist()
          session.sessionState.getExecution(df.logicalPlan)
        } finally {
          session.sessionState.enableExecutionCache = false
          session.sessionState.clearExecutionCache()
        }
        val memoryPlan = df.queryExecution.executedPlan.collectFirst {
          case plan: InMemoryTableScanExec => plan.relation
        }.get
        val planInfo = PartitionedPhysicalScan.getSparkPlanInfo(cachedExecution.executedPlan)
        Row(CachedDataFrame.withCallback(session, df = null, cachedExecution, "cache")(_ =>
          CachedDataFrame.withNewExecutionId(session, queryShortString, queryString,
            cachedExecution.toString(), planInfo)({
            val start = System.nanoTime()
            // Dummy op to materialize the cache. This does the minimal job of count on
            // the actual cached data (RDD[CachedBatch]) to force materialization of cache
            // while avoiding creation of any new SparkPlan.
            memoryPlan.cachedColumnBuffers.count()
            (Unit, System.nanoTime() - start)
          }))._2) :: Nil
      } finally {
        if (previousJobDescription ne null) {
          localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, previousJobDescription)
        } else {
          localProperties.remove(SparkContext.SPARK_JOB_DESCRIPTION)
        }
      }
    }
  }
}

/**
 * Changes the name of "database" column to "schemaName" over Spark's ShowTablesCommand.
 * Also when hive compatibility is turned on, then this does not include the schema name
 * or "isTemporary" to return hive compatible result.
 */
class ShowSnappyTablesCommand(session: SnappySession, schemaOpt: Option[String],
    tablePattern: Option[String]) extends ShowTablesCommand(schemaOpt, tablePattern) {

  private val hiveCompatible = Property.HiveCompatible.get(session.sessionState.conf)

  override val output: Seq[Attribute] = {
    if (hiveCompatible) AttributeReference("name", StringType, nullable = false)() :: Nil
    else {
      AttributeReference("schemaName", StringType, nullable = false)() ::
          AttributeReference("tableName", StringType, nullable = false)() ::
          AttributeReference("isTemporary", BooleanType, nullable = false)() :: Nil
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!hiveCompatible) return super.run(sparkSession)

    val catalog = sparkSession.sessionState.catalog
    val schemaName = schemaOpt match {
      case None => catalog.getCurrentDatabase
      case Some(s) => s
    }
    val tables = tableIdentifierPattern match {
      case None => catalog.listTables(schemaName)
      case Some(p) => catalog.listTables(schemaName, p)
    }
    tables.map(tableIdent => Row(tableIdent.table))
  }
}

case class ShowViewsCommand(session: SnappySession, schemaOpt: Option[String],
    viewPattern: Option[String]) extends RunnableCommand {

  private val hiveCompatible = Property.HiveCompatible.get(session.sessionState.conf)

  // The result of SHOW VIEWS has four columns: schemaName, tableName, isTemporary and isGlobal.
  override val output: Seq[Attribute] = {
    if (hiveCompatible) AttributeReference("viewName", StringType, nullable = false)() :: Nil
    else {
      AttributeReference("schemaName", StringType, nullable = false)() ::
          AttributeReference("viewName", StringType, nullable = false)() ::
          AttributeReference("isTemporary", BooleanType, nullable = false)() ::
          AttributeReference("isGlobal", BooleanType, nullable = false)() :: Nil
    }
  }

  private def getViewType(table: TableIdentifier,
      session: SnappySession): Option[(Boolean, Boolean)] = {
    val catalog = session.sessionCatalog
    if (catalog.isTemporaryTable(table)) Some(true -> !catalog.isLocalTemporaryView(table))
    else if (catalog.getTableMetadata(table).tableType != CatalogTableType.VIEW) None
    else Some(false -> false)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionCatalog
    val schemaName = schemaOpt match {
      case None => catalog.getCurrentDatabase
      case Some(s) => s
    }
    val tables = viewPattern match {
      case None => catalog.listTables(schemaName)
      case Some(p) => catalog.listTables(schemaName, p)
    }
    tables.map(tableIdent => tableIdent -> getViewType(tableIdent, session)).collect {
      case (viewIdent, Some((isTemp, isGlobalTemp))) =>
        if (hiveCompatible) Row(viewIdent.table)
        else Row(viewIdent.database.getOrElse(""), viewIdent.table, isTemp, isGlobalTemp)
    }
  }
}

/**
 * This extends Spark's describe to add support for CHAR and VARCHAR types.
 */
class DescribeSnappyTableCommand(table: TableIdentifier,
    partitionSpec: TablePartitionSpec, isExtended: Boolean, isFormatted: Boolean)
    extends DescribeTableCommand(table, partitionSpec, isExtended, isFormatted) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.asInstanceOf[SnappySession].sessionCatalog
    catalog.synchronized {
      // set the flag to return CharType/VarcharType if present
      catalog.convertCharTypesInMetadata = true
      try {
        super.run(sparkSession)
      } finally {
        catalog.convertCharTypesInMetadata = false
      }
    }
  }
}

case class DeployCommand(
    coordinates: String,
    alias: String,
    repos: Option[String],
    jarCache: Option[String],
    restart: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    try {
      val jarsstr = SparkSubmitUtils.resolveMavenCoordinates(coordinates, repos, jarCache)
      if (jarsstr.nonEmpty) {
        val jars = jarsstr.split(",")
        val sc = sparkSession.sparkContext
        val uris = jars.map(j => sc.env.rpcEnv.fileServer.addFile(new File(j)))
        SnappySession.addJarURIs(uris)
        RefreshMetadata.executeOnAll(sc, RefreshMetadata.ADD_URIS_TO_CLASSLOADER, uris)
        val deployCmd = s"$coordinates|${repos.getOrElse("")}|${jarCache.getOrElse("")}"
        ToolsCallbackInit.toolsCallback.addURIs(alias, jars, deployCmd)
      }
      Nil
    } catch {
      case ex: Throwable =>
        ex match {
          case err: Error =>
            if (SystemFailure.isJVMFailureError(err)) {
              SystemFailure.initiateFailure(err)
              // If this ever returns, rethrow the error. We're poisoned
              // now, so don't let this thread continue.
              throw err
            }
          case _ =>
        }
        Misc.checkIfCacheClosing(ex)
        if (restart) {
          logWarning(s"Following mvn coordinate" +
              s" could not be resolved during restart: $coordinates", ex)
          if (lang.Boolean.parseBoolean(System.getProperty("FAIL_ON_JAR_UNAVAILABILITY", "true"))) {
            throw ex
          }
          Nil
        } else {
          throw ex
        }
    }
  }
}

case class DeployJarCommand(
    alias: String,
    paths: String,
    restart: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (paths.nonEmpty) {
      val jars = paths.split(",")
      val (availableUris, unavailableUris) = jars.partition(f => Files.isReadable(Paths.get(f)))
      if (unavailableUris.nonEmpty) {
        logWarning(s"Following jars are unavailable" +
            s" for deployment during restart: ${unavailableUris.deep.mkString(",")}")
        if (restart && lang.Boolean.parseBoolean(
          System.getProperty("FAIL_ON_JAR_UNAVAILABILITY", "true"))) {
          throw new IllegalStateException(
            s"Could not find deployed jars: ${unavailableUris.mkString(",")}")
        }
        if (!restart) {
          throw new IllegalArgumentException(s"jars not readable: ${unavailableUris.mkString(",")}")
        }
      }
      val sc = sparkSession.sparkContext
      val uris = availableUris.map(j => sc.env.rpcEnv.fileServer.addFile(new File(j)))
      SnappySession.addJarURIs(uris)
      RefreshMetadata.executeOnAll(sc, RefreshMetadata.ADD_URIS_TO_CLASSLOADER, uris)
      ToolsCallbackInit.toolsCallback.addURIs(alias, jars, paths, isPackage = false)
    }
    Nil
  }
}

case class ListPackageJarsCommand(isJar: Boolean) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("alias", StringType, nullable = false)() ::
        AttributeReference("coordinate", StringType, nullable = false)() ::
        AttributeReference("isPackage", BooleanType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val commands = ToolsCallbackInit.toolsCallback.getGlobalCmndsSet
    val rows = new ArrayBuffer[Row]
    commands.forEach(new Consumer[Entry[String, String]] {
      override def accept(t: Entry[String, String]): Unit = {
        val alias = t.getKey
        val value = t.getValue
        val indexOf = value.indexOf('|')
        if (indexOf > 0) {
          // It is a package
          val pkg = value.substring(0, indexOf)
          rows += Row(alias, pkg, true)
        }
        else {
          // It is a jar
          val jars = value.split(',')
          val jarfiles = jars.map(f => {
            val lastIndexOf = f.lastIndexOf('/')
            val length = f.length
            if (lastIndexOf > 0) f.substring(lastIndexOf + 1, length)
            else {
              f
            }
          })
          rows += Row(alias, jarfiles.mkString(","), false)
        }
      }
    })
    rows
  }
}

case class UnDeployCommand(alias: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    ToolsCallbackInit.toolsCallback.removePackage(alias)
    Nil
  }
}
