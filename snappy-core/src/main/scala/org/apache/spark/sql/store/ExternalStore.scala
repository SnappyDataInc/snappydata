package org.apache.spark.sql.store

import java.sql.Connection
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.CachedBatch

/**
 * Created by neeraj on 16/7/15.
 */
trait ExternalStore extends Serializable {


  def storeCachedBatch(batch: CachedBatch, tableName: String, maxPartitions: Int = -1): UUIDRegionKey

  def storeCachedBatch(batch: CachedBatch, batchID: UUID, bucketId : Int, tableName: String): UUIDRegionKey

  def getCachedBatchIterator(tableName: String,
      requiredColumns: Array[String],
      itr: Iterator[UUIDRegionKey],
      getAll: Boolean = false): Iterator[CachedBatch]

  def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch]

  def getConnection(id: String): java.sql.Connection

  def url: String

  def driver: String

  def poolProps: Map[String, String]

  def connProps: java.util.Properties

  def tryExecute[T: ClassTag](tableName: String,
      f: PartialFunction[(Connection), T],
      closeOnSuccess: Boolean = true): T = {
    val conn = getConnection(tableName)
    var isClosed = false
    try {
      f(conn)
    } catch {
      case t: Throwable =>
        conn.close()
        isClosed = true
        throw t
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }
}
