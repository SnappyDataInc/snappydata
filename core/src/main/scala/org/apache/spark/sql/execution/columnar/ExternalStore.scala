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
package org.apache.spark.sql.execution.columnar

import java.sql.Connection
import java.util.UUID

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.ConnectionProperties

trait ExternalStore extends Serializable {

  final val columnPrefix = "Col_"

  def storeCachedBatch(tableName: String, batch: CachedBatch,
      partitionId: Int = -1, batchId: Option[UUID] = None): Unit

  def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      session: SparkSession): RDD[CachedBatch]

  def getConnectedExternalStore(tableName: String, onExecutor: Boolean): ConnectedExternalStore

  def getConnection(id: String, onExecutor: Boolean): java.sql.Connection

  def connProperties: ConnectionProperties

  def tryExecute[T: ClassTag](tableName: String,
      f: Connection => T,
      closeOnSuccess: Boolean = true, onExecutor: Boolean = false)
    (implicit c: Option[Connection] = None): T = {
    var isClosed = false
    val conn = c.getOrElse(getConnection(tableName, onExecutor))
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

} // ExternalStore

trait ConnectedExternalStore extends ExternalStore {

  private[this] var dependentAction: Option[Connection => Unit] = None

  protected[this] val connectedInstance: Connection

  def conn: Connection = {
    assert(!connectedInstance.isClosed)
    connectedInstance
  }

  def commitAndClose(isSuccess: Boolean): Unit = {
    if (!connectedInstance.isClosed && isSuccess) {
      connectedInstance.commit()
    }
    connectedInstance.close()
  }

/*
  override def getConnection(id: String,
    onExecutor: Boolean): java.sql.Connection = connectedInstance
*/

  override def tryExecute[T: ClassTag](tableName: String,
    f: Connection => T,
    closeOnSuccess: Boolean = true, onExecutor: Boolean = false)
    (implicit c: Option[Connection]): T = {
    assert(!connectedInstance.isClosed)
    val ret = super.tryExecute(tableName, f,
      closeOnSuccess = false /* responsibility of the user to close later */,
      onExecutor)(
      implicitly, Some(connectedInstance))

    if (dependentAction.isDefined) {
      assert(!connectedInstance.isClosed)
      dependentAction.get(connectedInstance)
    }

    ret
  }

  def withDependentAction(f: Connection => Unit): ConnectedExternalStore = {
    dependentAction = Some(f)
    this
  }

}
