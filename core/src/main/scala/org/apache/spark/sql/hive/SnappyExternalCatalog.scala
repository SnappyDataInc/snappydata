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

package org.apache.spark.sql.hive

import java.util

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.thrift.TException

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.client.HiveClient

private[spark] class SnappyExternalCatalog(var client: HiveClient, hadoopConf: Configuration)
    extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    classOf[HiveException].getCanonicalName,
    classOf[TException].getCanonicalName)


  /**
   * Whether this is an exception thrown by the hive client that should be wrapped.
   *
   * Due to classloader isolation issues, pattern matching won't work here so we need
   * to compare the canonical names of the exceptions, which we assume to be stable.
   */
  private def isClientException(e: Throwable): Boolean = {
    var temp: Class[_] = e.getClass
    var found = false
    while (temp != null && !found) {
      found = clientExceptions.contains(temp.getCanonicalName)
      temp = temp.getSuperclass
    }
    found
  }

  private def isDisconnectException(t: Throwable): Boolean = {
    if (t != null) {
      val tClass = t.getClass.getName
      tClass.contains("DisconnectedException") ||
          tClass.contains("DisconnectException") ||
          (tClass.contains("MetaException") && t.getMessage.contains("retries")) ||
          isDisconnectException(t.getCause)
    } else {
      false
    }
  }

  def withHiveExceptionHandling[T](function: => T): T = {
    try {
      function
    } catch {
      case he: HiveException if isDisconnectException(he) =>
        // stale JDBC connection
        Hive.closeCurrent()
        client = client.newSession()
        function
    }
  }

  /**
   * Run some code involving `client` in a [[synchronized]] block and wrap certain
   * exceptions thrown in the process in [[AnalysisException]].
   */
  private def withClient[T](body: => T): T = synchronized {
    try {
      body
    } catch {
      case NonFatal(e) if isClientException(e) =>
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    }
  }

  private def requireDbMatches(db: String, table: CatalogTable): Unit = {
    if (!table.identifier.database.contains(db)) {
      throw new AnalysisException(
        s"Provided database '$db' does not match the one specified in the " +
            s"table definition (${table.identifier.database.getOrElse("n/a")})")
    }
  }

  private def requireTableExists(db: String, table: String): Unit = {
    withClient {
      getTable(db, table)
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withClient {
    withHiveExceptionHandling(client.createDatabase(dbDefinition, ignoreIfExists))
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withClient {
    withHiveExceptionHandling(client.dropDatabase(db, ignoreIfNotExists, cascade))
  }

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: As of now, this only supports altering database properties!
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = withClient {
    val existingDb = getDatabase(dbDefinition.name)
    if (existingDb.properties == dbDefinition.properties) {
      logWarning(s"Request to alter database ${dbDefinition.name} is a no-op because " +
          s"the provided database properties are the same as the old ones. Hive does not " +
          s"currently support altering other database fields.")
    }
    withHiveExceptionHandling(client.alterDatabase(dbDefinition))
  }

  override def getDatabase(db: String): CatalogDatabase = withClient {
    withHiveExceptionHandling(client.getDatabase(db))
  }

  override def databaseExists(db: String): Boolean = withClient {
    withHiveExceptionHandling(client.getDatabaseOption(db).isDefined)
  }

  override def listDatabases(): Seq[String] = withClient {
    withHiveExceptionHandling(client.listDatabases("*"))
  }

  override def listDatabases(pattern: String): Seq[String] = withClient {
    withHiveExceptionHandling(client.listDatabases(pattern))
  }

  override def setCurrentDatabase(db: String): Unit = withClient {
    withHiveExceptionHandling(client.setCurrentDatabase(db))
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(
      db: String,
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = withClient {
    requireDbExists(db)
    requireDbMatches(db, tableDefinition)

    if (
    // If this is an external data source table...
      tableDefinition.properties.contains("spark.sql.sources.provider") &&
          tableDefinition.tableType == CatalogTableType.EXTERNAL &&
          // ... that is not persisted as Hive compatible format (external tables in Hive compatible
          // format always set `locationUri` to the actual data location and should NOT be hacked as
          // following.)
          tableDefinition.storage.locationUri.isEmpty
    ) {
      // !! HACK ALERT !!
      //
      // Due to a restriction of Hive metastore, here we have to set `locationUri` to a temporary
      // directory that doesn't exist yet but can definitely be successfully created, and then
      // delete it right after creating the external data source table. This location will be
      // persisted to Hive metastore as standard Hive table location URI, but Spark SQL doesn't
      // really use it. Also, since we only do this workaround for external tables, deleting the
      // directory after the fact doesn't do any harm.
      //
      // Please refer to https://issues.apache.org/jira/browse/SPARK-15269 for more details.
      val tempPath = {
        val dbLocation = getDatabase(tableDefinition.database).locationUri
        new Path(dbLocation, tableDefinition.identifier.table + "-__PLACEHOLDER__")
      }

      try {
        withHiveExceptionHandling(client.createTable(
          tableDefinition.withNewStorage(locationUri = Some(tempPath.toString)),
          ignoreIfExists))
      } finally {
        FileSystem.get(tempPath.toUri, hadoopConf).delete(tempPath, true)
      }
    } else {
      withHiveExceptionHandling(client.createTable(tableDefinition, ignoreIfExists))
    }
    SnappySession.clearAllCache()
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean): Unit = withClient {
    requireDbExists(db)
    withHiveExceptionHandling(client.dropTable(db, table, ignoreIfNotExists))
    SnappySession.clearAllCache()
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = withClient {
    val newTable = client.getTable(db, oldName)
        .copy(identifier = TableIdentifier(newName, Some(db)))
    withHiveExceptionHandling(client.alterTable(oldName, newTable))
    SnappySession.clearAllCache()
  }

  /**
   * Alter a table whose name that matches the one specified in `tableDefinition`,
   * assuming the table exists.
   *
   * Note: As of now, this only supports altering table properties, serde properties,
   * and num buckets!
   */
  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = withClient {
    requireDbMatches(db, tableDefinition)
    requireTableExists(db, tableDefinition.identifier.table)
    withHiveExceptionHandling(client.alterTable(tableDefinition))
    SnappySession.clearAllCache()
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    withHiveExceptionHandling(client.getTable(db, table))
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = withClient {
    withHiveExceptionHandling(client.getTableOption(db, table))
  }

  override def tableExists(db: String, table: String): Boolean = withClient {
    withHiveExceptionHandling(client.getTableOption(db, table).isDefined)
  }

  override def listTables(db: String): Seq[String] = withClient {
    requireDbExists(db)
    withHiveExceptionHandling(client.listTables(db))
  }

  override def listTables(db: String, pattern: String): Seq[String] = withClient {
    requireDbExists(db)
    withHiveExceptionHandling(client.listTables(db, pattern))
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      holdDDLTime: Boolean): Unit = withClient {
    requireTableExists(db, table)
    withHiveExceptionHandling(client.loadTable(
      loadPath,
      s"$db.$table",
      isOverwrite,
      holdDDLTime))
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = withClient {
    requireTableExists(db, table)

    val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
    getTable(db, table).partitionColumnNames.foreach { colName =>
      orderedPartitionSpec.put(colName, partition(colName))
    }

    withHiveExceptionHandling(client.loadPartition(
      loadPath,
      s"$db.$table",
      orderedPartitionSpec,
      isOverwrite,
      holdDDLTime,
      inheritTableSpecs,
      isSkewedStoreAsSubdir))
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withClient {
    requireTableExists(db, table)
    withHiveExceptionHandling(client.createPartitions(db, table, parts, ignoreIfExists))
    SnappySession.clearAllCache()
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit = withClient {
    requireTableExists(db, table)
    withHiveExceptionHandling(client.dropPartitions(db, table, parts, ignoreIfNotExists))
    SnappySession.clearAllCache()
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withClient {
    withHiveExceptionHandling(client.renamePartitions(db, table, specs, newSpecs))
    SnappySession.clearAllCache()
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    withHiveExceptionHandling(client.alterPartitions(db, table, newParts))
    SnappySession.clearAllCache()
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = withClient {
    withHiveExceptionHandling(client.getPartition(db, table, spec))
  }

  /**
   * Returns the partition names from hive metastore for a given table in a database.
   */
  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = withClient {
    withHiveExceptionHandling(client.getPartitions(db, table, partialSpec))
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(
      db: String,
      funcDefinition: CatalogFunction): Unit = withClient {
    // Hive's metastore is case insensitive. However, Hive's createFunction does
    // not normalize the function name (unlike the getFunction part). So,
    // we are normalizing the function name.
    val functionName = funcDefinition.identifier.funcName.toLowerCase
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    withHiveExceptionHandling(client.createFunction(db,
      funcDefinition.copy(identifier = functionIdentifier)))
    SnappySession.clearAllCache()
  }

  override def dropFunction(db: String, name: String): Unit = withClient {
    withHiveExceptionHandling(client.dropFunction(db, name))
    SnappySession.clearAllCache()
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withClient {
    withHiveExceptionHandling(client.renameFunction(db, oldName, newName))
    SnappySession.clearAllCache()
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = withClient {
    withHiveExceptionHandling(client.getFunction(db, funcName))
  }

  override def functionExists(db: String, funcName: String): Boolean = withClient {
    withHiveExceptionHandling(client.functionExists(db, funcName))
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withClient {
    withHiveExceptionHandling(client.listFunctions(db, pattern))
  }

  def closeCurrent(): Unit = {
    Hive.closeCurrent()
  }
}
