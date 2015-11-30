package org.apache.spark.sql.columnar

import java.nio.ByteBuffer
import java.sql.Connection
import java.util.Properties
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.collection.{Utils, UUIDRegionKey}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartitioningInfo, JDBCRelation, JdbcUtils}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDBaseDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{ExternalStore, JDBCSourceAsStore}
import org.apache.spark.sql.types.StructType

/**
 * A LogicalPlan implementation for an external column table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */

class JDBCAppendableRelation(
    val url: String,
    val table: String,
    val provider: String,
    val mode: SaveMode,
    userSchema: StructType,
    parts: Array[Partition],
    _poolProps: Map[String, String],
    val connProperties: Properties,
    val hikariCP: Boolean,
    val origOptions: Map[String, String],
    val externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext)(
    private var uuidList: ArrayBuffer[RDD[UUIDRegionKey]] =
    new ArrayBuffer[RDD[UUIDRegionKey]]())
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with DestroyRelation
    with Logging
    with Serializable {

  self =>

  private final val columnPrefix = "Col_"
  final val dialect = JdbcDialects.get(url)

  createTable(mode)
  private val bufferLock = new ReentrantReadWriteLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  private[sql] def readLock[A](f: => A): A = {
    val lock = bufferLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private[sql] def writeLock[A](f: => A): A = {
    val lock = bufferLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  override def schema: StructType = userSchema

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {

    val requestedColumns = if (requiredColumns.isEmpty) {
      val narrowField =
        schema.fields.minBy { a =>
          ColumnType(a.dataType).defaultSize
        }

      Array(narrowField.name)
    } else {
      requiredColumns
    }

    val cachedColumnBuffers: RDD[CachedBatch] = readLock {
      externalStore.getCachedBatchRDD(table,
        requestedColumns.map(column => columnPrefix + column), uuidList,
        sqlContext.sparkContext)
    }

    val outputTypes = requestedColumns.map { a => schema(a) }
    //val converter = outputTypes.map(CatalystTypeConverters.createToScalaConverter)
    val converter = CatalystTypeConverters.createToScalaConverter(StructType(outputTypes))
    cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.
      // If none are requested, use the narrowest (the field with
      // minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = requestedColumns.map { a =>
        schema.getFieldIndex(a).get -> schema(a).dataType
      }.unzip
      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)
      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]): Iterator[Row] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.zipWithIndex.map {
            case (schemaIndex, bufferIndex) =>
              ColumnAccessor(schema.fields(schemaIndex).dataType,
                ByteBuffer.wrap(cachedBatch.buffers(bufferIndex)))
          }
          // Extract rows via column accessors
          new Iterator[InternalRow] {
            private[this] val rowLen = nextRow.numFields

            override def next(): InternalRow = {
              var i = 0
              while (i < rowLen) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              if (requiredColumns.isEmpty) InternalRow.empty else nextRow
            }

            override def hasNext: Boolean = columnAccessors(0).hasNext
          }
        }
        rows.map(converter(_).asInstanceOf[Row])
      }
      cachedBatchesToRows(cachedBatchIterator)
    }
  }

  override def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    assert(df.schema.equals(schema))

    // We need to truncate the table
    if (overwrite)
      sqlContext.asInstanceOf[SnappyContext].truncateExternalTable(table)

    val useCompression = sqlContext.conf.useCompression
    val columnBatchSize = sqlContext.conf.columnBatchSize

    val output = df.logicalPlan.output
    val cached = df.mapPartitions { rowIterator =>
      def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
          batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
        val uuid = externalStore.storeCachedBatch(batch, table)
        accumulated += uuid
      }

      def columnBuilders = output.map { attribute =>
        val columnType = ColumnType(attribute.dataType)
        val initialBufferSize = columnType.defaultSize * columnBatchSize
        ColumnBuilder(attribute.dataType, initialBufferSize,
          attribute.name, useCompression)
      }.toArray

      val holder = new CachedBatchHolder(columnBuilders, 0, columnBatchSize, schema,
        new ArrayBuffer[UUIDRegionKey](1), uuidBatchAggregate)

      val batches = holder.asInstanceOf[CachedBatchHolder[ArrayBuffer[Serializable]]]
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowIterator.map(converter(_).asInstanceOf[InternalRow])
          .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator
    }
    // trigger an Action to materialize 'cached' batch
    cached.count()
    appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
  }

  def appendUUIDBatch(batch: RDD[UUIDRegionKey]) = writeLock {
    uuidList += batch
  }

  def truncate() = writeLock {
    val dialect = JdbcDialects.get(externalStore.url)
    externalStore.tryExecute(table, {
      case conn =>
        JdbcExtendedUtils.truncateTable(conn, table, dialect)
    })
    uuidList.clear()
  }

  def createTable(mode: SaveMode): Unit = {
    var conn: Connection = null
    val dialect = JdbcDialects.get(url)
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      val tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)
      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
    }
    createExternalTableForCachedBatches(table, externalStore)
  }

  private def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForCachedBatches: expected non-empty table name")

    val (primarykey, partitionStrategy) = dialect match {
      // The driver if not a loner should be an accesor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId)", d.getPartitionByClause("bucketId"))
      case _ => ("primary key (uuid)", "") // TODO. How to get primary key contraint from each DB
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, bucketId integer, stats blob, " +
        userSchema.fields.map(structField => columnPrefix + structField.name + " blob")
            .mkString(" ", ",", " ") +
        s", $primarykey) $partitionStrategy",
      tableName, dropIfExists = false) // for test make it false
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean) = {

    externalStore.tryExecute(tableName, {
      case conn =>
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
            ifExists = true)
        }
        val tableExists = JdbcExtendedUtils.tableExists(table, conn,
          dialect, sqlContext)
        if (!tableExists) {
          JdbcExtendedUtils.executeUpdate(tableStr, conn)
          dialect match {
            case d: JdbcExtendedDialect => d.initializeTable(tableName, conn)
            case _ => // do nothing

          }
        }
    })
  }

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  override def destroy(ifExists: Boolean): Unit = {
    // clean up the connection pool on executors first
    Utils.mapExecutors(sqlContext,
      JDBCAppendableRelation.removePool(table)).count()
    // then on the driver
    JDBCAppendableRelation.removePool(table)
    // drop the external table using a non-pool connection
    val conn = JdbcUtils.createConnection(url, connProperties)
    try {
      JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
    } finally {
      conn.close()
    }
  }
}

object JDBCAppendableRelation {
  def apply(url: String,
      table: String,
      provider: String,
      mode: SaveMode,
      schema: StructType,
      parts: Array[Partition],
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean,
      options: Map[String, String],
      sqlContext: SQLContext): JDBCAppendableRelation =
    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, parts,
      poolProps, connProps, hikariCP, options, null, sqlContext)()

  private def removePool(table: String): () => Iterator[Unit] = () => {
    ConnectionPool.removePoolReference(table)
    Iterator.empty
  }
}

final class DefaultSource extends ColumnarRelationProvider

class ColumnarRelationProvider
    extends SchemaRelationProvider
    with CreatableRelationProvider {

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options
    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext
    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    val partitionInfo = if (partitionColumn.isEmpty) {
      null
    } else {
      if (lowerBound.isEmpty || upperBound.isEmpty || numPartitions.isEmpty) {
        throw new IllegalArgumentException("JDBCAppendableRelation: " +
            "incomplete partitioning specified")
      }
      JDBCPartitioningInfo(
        partitionColumn.get,
        lowerBound.get.toLong,
        upperBound.get.toLong,
        numPartitions.get.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)

    val externalStore = getExternalSource(sc, url, driver, poolProps,
      connProps, hikariCP)

    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, parts,
      poolProps, connProps, hikariCP, options, externalStore, sqlContext)()
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists

    val rel = getRelation(sqlContext, options)
    rel.createRelation(sqlContext, mode, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseRelation = {
    val rel = getRelation(sqlContext, options)
    val relation = rel.createRelation(sqlContext, mode, options, data.schema)
    relation.insert(data, mode == SaveMode.Overwrite)
    relation
  }

  def getRelation(sqlContext: SQLContext,
      options: Map[String, String]): ColumnarRelationProvider = {

    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(sqlContext.sparkContext))
    val clazz = JdbcDialects.get(url) match {
      case d: GemFireXDBaseDialect => ResolvedDataSource.
          lookupDataSource("org.apache.spark.sql.columntable.DefaultSource")
      case _ => classOf[columnar.DefaultSource]
    }
    clazz.newInstance().asInstanceOf[ColumnarRelationProvider]
  }

  def getExternalSource(sc: SparkContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean): ExternalStore = {
    new JDBCSourceAsStore(url, driver, poolProps, connProps, hikariCP)
  }
}
