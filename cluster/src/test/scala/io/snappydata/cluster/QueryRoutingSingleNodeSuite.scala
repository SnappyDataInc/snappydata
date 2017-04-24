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
package io.snappydata.cluster

import java.sql.{DriverManager, ResultSet}

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.SnappySession

class QueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

  val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = ""
  val tableName = "order_line_col"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
  }

  override def afterAll(): Unit = {
    setDMLMaxChunkSize(default_chunk_size)
    super.afterAll()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def insertRows(numRows: Int): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($i, '1')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      println(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def query(): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(
        s"select ol_w_id  from $tableName ")
      var index = 0
      while (rs.next()) {
        rs.getInt(1)
        index += 1
      }
      println("Number of rows read " + index)
      rs.close()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  test("test serialization with lesser dml chunk size") {

    snc.sql("create table order_line_col (ol_w_id  integer,ol_d_id STRING) using column " +
        "options( partition_by 'ol_w_id, ol_d_id', buckets '5')")


    serverHostPort = TestUtil.startNetServer()
    println("network server started")
    insertRows(1000)


    (1 to 5).foreach(d => query())
  }

  def insertRows(tableName: String, numRows: Int, serverHostPort: String): Unit = {

    val conn: java.sql.Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt: java.sql.Statement = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($i, $i, '$i')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      // scalastyle:off println
      println(s"committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt.close()
      conn.close()
    }
  }

  private def verifyResults(qry: String, rs: ResultSet, results: Array[Int],
      cacheMapSize: Int): Unit = {
    val cacheMap = SnappySession.getPlanCache.asMap()
    assert( cacheMap.size() == cacheMapSize)

    var index = 0
    while (rs.next()) {
      val i = rs.getInt(1)
      val j = rs.getInt(2)
      val s = rs.getString(3)
      // scalastyle:off println
      println(s"$qry row($index) $i $j $s ")
      // scalastyle:on println
      index += 1

      assert(results.contains(i))
    }

    // scalastyle:off println
    println(s"$qry Number of rows read " + index)
    // scalastyle:on println
    assert(index == results.length)
    rs.close()
  }

  def query2(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn: java.sql.Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt: java.sql.Statement = conn.createStatement()
    try {
      val qry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int_id < 500 " +
          s" and ol_1_int2_id in (" +
          s"select ol_2_int_id " +
          s" from $tableName2 " +
          s" where ol_2_int_id in (100, 200, 300) " +
          s") " +
          s" limit 20" +
          s""
      verifyResults("qry-1", stmt.executeQuery(qry), Array(100, 200, 300), 1)

      val qry2 = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int_id < 900 " +
          s" and ol_1_int2_id in (" +
          s"select ol_2_int_id " +
          s" from $tableName2 " +
          s" where ol_2_int_id in (600, 700, 800) " +
          s") " +
          s" limit 20" +
          s""
      verifyResults("qry-2", stmt.executeQuery(qry2), Array(600, 700, 800), 1)
    } finally {
      stmt.close()
      conn.close()
    }
  }

  ignore("Tokenization test with IN SubQuery") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    try {
      val tableName1 = "order_line_1_col"
      val tableName2 = "order_line_2_col"
      snc.sql(s"create table $tableName1 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
          "options( partition_by 'ol_1_int_id, ol_1_int2_id', buckets '2')")

      snc.sql(s"create table $tableName2 (ol_2_int_id  integer," +
          s" ol_2_int2_id  integer, ol_2_str_id STRING) using column " +
          "options( partition_by 'ol_2_int_id, ol_2_int2_id', buckets '2')")


      val serverHostPort = TestUtil.startNetServer()
      // println("network server started")
      insertRows(tableName1, 1000, serverHostPort)
      insertRows(tableName2, 1000, serverHostPort)
      query2(tableName1, tableName2, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
    }
  }
}
