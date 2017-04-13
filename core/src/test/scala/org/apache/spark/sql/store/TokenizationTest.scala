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
package org.apache.spark.sql.store

import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import io.snappydata.core.Data
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.SnappySession.CachedKey
import org.apache.spark.sql._

/**
  * Tests for column tables in GFXD.
  */
class TokenizationTest
    extends SnappyFunSuite
        with Logging
        with BeforeAndAfter
        with BeforeAndAfterAll {

  val table  = "my_table"
  val table2 = "my_table2"
  val all_typetable = "my_table3"

  override def beforeAll(): Unit = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.sql.codegen.comments", "true")
    System.setProperty("spark.testing", "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.sql.codegen.comments")
    System.clearProperty("spark.testing")
    super.afterAll()
  }

  after {
    SnappyTableStatsProviderService.suspendCacheInvalidation = false
    SnappySession.clearAllCache()
    snc.dropTable(s"$table", ifExists = true)
    snc.dropTable(s"$table2", ifExists = true)
    snc.dropTable(s"$all_typetable", ifExists = true)
    snc.dropTable(s"$colTableName", ifExists = true)
  }

  test("like queries") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    val numRows = 100
    createSimpleTableAndPoupulateData(numRows, s"$table", true)

    try {
      val q = s"select * from $table where a like '10%'"
      var result = snc.sql(q).collect()

      val q2 = s"select * from $table where a like '20%'"
      var result2 = snc.sql(q2).collect()
      assert(!(result.sameElements(result2)) && result.length > 0)
    }
    SnappyTableStatsProviderService.suspendCacheInvalidation = false
  }

  test("same session from different thread") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    val numRows = 2
    createSimpleTableAndPoupulateData(numRows, s"$table", true)

    try {
      val q = (0 until numRows) map { x =>
        s"select * from $table where a = $x"
      }
      var result = snc.sql(q(0)).collect()
      assert(result.length === 1)
      result.foreach( r => {
        assert(r.get(0) == r.get(1) && r.get(0) == 0)
      })

      val runnable = new Runnable {
        override def run() = {
          var result = snc.sql(q(1)).collect()
          assert(result.length === 1)
          result.foreach( r => {
            assert(r.get(0) == r.get(1) && r.get(0) == 1)
          })
        }
      }
      val newthread = new Thread(runnable)
      newthread.start()
      newthread.join()

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert( cacheMap.size() == 1)
    }
    SnappyTableStatsProviderService.suspendCacheInvalidation = false
  }

  def getAllValidKeys(): Int = {
    val cacheMap = SnappySession.getPlanCache.asMap()
    cacheMap.keySet().toArray().filter(_.asInstanceOf[CachedKey].valid).length
  }

  test("Test tokenize and queryHints and noTokenize if limit or projection") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table", true)

    try {
      val q = (0 until numRows) map { x =>
        s"select * from $table where a = $x"
      }
      val start = System.currentTimeMillis()
      q.zipWithIndex.foreach  { case (x, i) =>
        var result = snc.sql(x).collect()
        assert(result.length === 1)
        result.foreach( r => {
          println(s"${r.get(0)}, ${r.get(1)}, ${r.get(2)}, ${i}")
          assert(r.get(0) == r.get(1) && r.get(2) == i)
        })
      }
      val end = System.currentTimeMillis()

      // snc.sql(s"select * from $table where a = 1200").collect()
      println("Time taken = " + (end - start))

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert( cacheMap.size() == 1)
      val x = cacheMap.keySet().toArray()(0).asInstanceOf[CachedKey].sqlText
      // We expect the first query in the head which is

      // assert(x === q.head, s"x = ${x} and q.head = ${q.head}")

      // Now test query hints -- arbitrary hint given
      val hintQuery = s"select * from $table /*+ XXXX( ) */ where a = 0"
      snc.sql(hintQuery).collect()
      assert( cacheMap.size() == 2)

      // test limit
      var query = s"select * from $table where a = 0 limit 1"
      var res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 3)

      query = s"select * from $table where a = 0 limit 10"
      var res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 4)

      // test constants in projection
      query = s"select a, 'x' from $table where a = 0"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 5)

      query = s"select a, 'y' from $table where a = 0"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 6)

      // check in based queries
      query = s"select * from $table where a in (0, 1)"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 7)

      assert( getAllValidKeys() == 7)
      // new plan should not be generated so size should be same
      query = s"select * from $table where a in (5, 7)"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 7)
      assert(!(res1.sameElements(res2)))

      // let us clear the plan cache
      snc.clear()
      assert( cacheMap.size() == 0)

      createSimpleTableAndPoupulateData(numRows, s"$table2")
      // creating table should not put anything in cache
      assert( cacheMap.size() == 0)
      // fire a join query
      query = s"select * from $table t1, $table2 t2 where t1.a = 0"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 1)

      query = s"select * from $table t1, $table2 t2 where t1.a = 5"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 1)
      assert(!(res1.sameElements(res2)))

      query = s"select * from $table t1, $table2 t2 where t2.a = 5"
      snc.sql(query).collect()
      assert( cacheMap.size() == 2)

      query = s"select * from $table t1, $table2 t2 where t1.a = t2.a"
      snc.sql(query).collect()
      assert( cacheMap.size() == 3)

      query = s"select * from $table t1, $table2 t2 where t1.a = t2.b"
      snc.sql(query).collect()
      assert( cacheMap.size() == 4)

      // let us clear the plan cache
      snc.clear()
      assert( cacheMap.size() == 0)

      // let us test for having
      query = s"select t1.b, SUM(t1.a) from $table t1 group by t1.b having SUM(t1.a) > 0"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 1)

      query = s"select t1.b, SUM(t1.a) from $table t1 group by t1.b having SUM(t1.a) > 5"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 1)
      assert(!res1.sameElements(res2))

      snc.sql(s"drop table $table")
      snc.sql(s"drop table $table2")

    } finally {
      snc.sql("set spark.sql.caseSensitive = false")
      snc.sql("set schema = APP")
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
    }
    logInfo("Successful")
  }

  ignore("Test tokenize for all data types") {
    val numRows = 10
    createAllTypeTableAndPoupulateData(numRows, s"$all_typetable")

    try {
      SnappyTableStatsProviderService.suspendCacheInvalidation = true
      val q = (0 until numRows).zipWithIndex.map { case (_, i) =>
        s"select * from $all_typetable where s = 'abc$i'"
      }
      val start = System.currentTimeMillis()
      q.zipWithIndex.foreach  { case (x, i) =>
        var result = snc.sql(x).collect()
        assert(result.length === 1)
        result.foreach( r => {
          assert(r.get(0) == i && r.get(4) == s"abc$i")
        })
      }
      val end = System.currentTimeMillis()

      // snc.sql(s"select * from $table where a = 1200").collect()
      println("Time taken = " + (end - start))

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert( cacheMap.size() == 1)
      val x = cacheMap.keySet().toArray()(0).asInstanceOf[CachedKey].sqlText
      assert(x.equals(q(0)))
      snc.sql(s"drop table $all_typetable")
    } finally {
      snc.sql("set spark.sql.caseSensitive = false")
      snc.sql("set schema = APP")
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
    }

    logInfo("Successful")
  }

  test("Test tokenize for sub-queries") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table", true)
    createSimpleTableAndPoupulateData(numRows, s"$table2")
    var query = s"select * from $table t1, $table2 t2 where t1.a in " +
      s"( select a from $table2 where b = 5 )"
    snc.sql(query).collect()

    val cacheMap = SnappySession.getPlanCache.asMap()

    assert( cacheMap.size() == 1)

    query = s"select * from $table t1, $table2 t2 where t1.a in " +
      s"( select a from $table2 where b = 100 )"
    snc.sql(query).collect()
    assert( cacheMap.size() == 1)
    logInfo("Successful")
    SnappyTableStatsProviderService.suspendCacheInvalidation = false
  }

  test("Test tokenize for joins and sub-queries") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table", true)
    createSimpleTableAndPoupulateData(numRows, s"$table2")
    var query = s"select * from $table t1, $table2 t2 where t1.a = t2.a and t1.b = 5 limit 2"
    //snc.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val result1 = snc.sql(query).collect()
    result1.foreach( r => {
      println(r.get(0) + ", " + r.get(1) + r.get(2) + ", " + r.get(3) + r.get(4) + ", " + r.get(5))
    })
    val cacheMap = SnappySession.getPlanCache.asMap()

    assert( cacheMap.size() == 1)

    query = s"select * from $table t1, $table2 t2 where t1.a = t2.a and t1.b = 7 limit 2"
    val result2 = snc.sql(query).collect()
    result2.foreach( r => {
      println(r.get(0) + ", " + r.get(1) + r.get(2) + ", " + r.get(3) + r.get(4) + ", " + r.get(5))
    })
    assert( cacheMap.size() == 1)
    SnappyTableStatsProviderService.suspendCacheInvalidation = false
    assert(!result1.sameElements(result2))
    assert(result1.length > 0)
    assert(result2.length > 0)
    logInfo("Successful")
  }

  test("Test tokenize for nulls") {
    logInfo("Successful")
  }

  test("Test tokenize for cast queries") {
    logInfo("Successful")
  }

  private def createSimpleTableAndPoupulateData(numRows: Int, name: String, dosleep: Boolean = false) = {
    val data = ((0 to numRows), (0 to numRows), (0 to numRows)).zipped.toArray
    val rdd = sc.parallelize(data, data.length)
      .map(s => Data(s._1, s._2, s._3))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql(s"Drop Table if exists $name")
    snc.sql(s"Create Table $name (a INT, b INT, c INT) " +
      "using column options()")
    dataDF.write.insertInto(s"$name")
    // This sleep was necessary as it has some dependency on the region size
    // collector thread frequency. Can't remember right now.
    if (dosleep) Thread.sleep(5000)
  }

  private def createAllTypeTableAndPoupulateData(numRows: Int,
      name: String,
      dosleep: Boolean = false) = {
    val ints = (0 to numRows).zipWithIndex.map {case (_, i) =>
      i
    }
    val longs = (0 to numRows).zipWithIndex.map    { case (_, i) => 1L*1000*i }
    val floats = (0 to numRows).zipWithIndex.map   { case (_, i) => 0.1f*i    }
    val decimals = (0 to numRows).zipWithIndex.map { case (_, i) => 0.1d*i    }
    val strs = (0 to numRows).zipWithIndex.map     { case (_, i) => s"abc$i"  }
    val dates = (0 to numRows).zipWithIndex.map    { case (_, i) => 1         }
    val tstmps = (0 to numRows).zipWithIndex.map   { case (_, i) => s"abc$i"  }

    val x =((((((ints, longs).zipped.toArray, floats).zipped.toArray,
      decimals).zipped.toArray, strs).zipped.toArray, dates).zipped.toArray, tstmps).zipped.toArray
    val data = x map { case ((((((i, l), f), d), s), dt), ts) =>
      (i, l, f, d, s, dt, ts)
    }

    val rdd = sc.parallelize(data, data.length)
      .map(s => (s._1, s._2, s._3, s._4, s._5, s._6, s._7))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql(s"Drop Table if exists $name")
    snc.sql(s"Create Table $name (a INT, b Long, c Float, d Double, s String, dt Int, ts Long) " +
      "using column options()")

    dataDF.write.insertInto(name)
    // This sleep was necessary as it has some dependency on the region size
    // collector thread frequency. Can't remember right now.
    if (dosleep) Thread.sleep(5000)
  }

  val colTableName = "airlineColTable"

  test("Test broadcast hash joins and scalar sub-queries") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    try {
      val ddlStr = "(YearI INT," + // NOT NULL
          "MonthI INT," + // NOT NULL
          "DayOfMonth INT," + // NOT NULL
          "DayOfWeek INT," + // NOT NULL
          "DepTime INT," +
          "CRSDepTime INT," +
          "ArrTime INT," +
          "CRSArrTime INT," +
          "UniqueCarrier VARCHAR(20)," + // NOT NULL
          "FlightNum INT," +
          "TailNum VARCHAR(20)," +
          "ActualElapsedTime INT," +
          "CRSElapsedTime INT," +
          "AirTime INT," +
          "ArrDelay INT," +
          "DepDelay INT," +
          "Origin VARCHAR(20)," +
          "Dest VARCHAR(20)," +
          "Distance INT," +
          "TaxiIn INT," +
          "TaxiOut INT," +
          "Cancelled INT," +
          "CancellationCode VARCHAR(20)," +
          "Diverted INT," +
          "CarrierDelay INT," +
          "WeatherDelay INT," +
          "NASDelay INT," +
          "SecurityDelay INT," +
          "LateAircraftDelay INT," +
          "ArrDelaySlot INT)"

      val hfile: String = getClass.getResource("/2015.parquet").getPath
      val snContext = snc
      snContext.sql("set spark.sql.shuffle.partitions=6")

      val airlineDF = snContext.read.load(hfile)
      val airlineparquetTable = "airlineparquetTable"
      airlineDF.registerTempTable(airlineparquetTable)

      // val colTableName = "airlineColTable"

      snc.sql(s"CREATE TABLE $colTableName $ddlStr" +
          "USING column options()")

      airlineDF.write.insertInto(colTableName)

      val rs0 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
          s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > 20 or taxiout > 20) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > 100) group by dest order " +
          s" by avgTaxiTime desc")

      val rows0 = rs0.collect()

      val rs1 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
          s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > 20 or taxiout > 20) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > 100) group by dest order " +
          s" by avgTaxiTime desc")

      val rows1 = rs1.collect()
      assert(rows0.sameElements(rows1))
      // rows1.foreach(println)

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert(cacheMap.size() == 1)

      val rs11 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
          s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > 10 or taxiout > 10) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > 10000) group by dest order " +
          s" by avgTaxiTime desc")

      val rows11 = rs11.collect()
      assert(!rows11.sameElements(rows1))
      assert(cacheMap.size() == 1)
    }
    finally {
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
    }
  }

  test("Test broadcast hash joins and scalar sub-queries - 2") {
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    val th = 10L * 1024 * 1024 * 1024
    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=$th")
    val ddlStr = "(YearI INT," + // NOT NULL
        "MonthI INT," + // NOT NULL
        "DayOfMonth INT," + // NOT NULL
        "DayOfWeek INT," + // NOT NULL
        "DepTime INT," +
        "CRSDepTime INT," +
        "ArrTime INT," +
        "CRSArrTime INT," +
        "UniqueCarrier VARCHAR(20)," + // NOT NULL
        "FlightNum INT," +
        "TailNum VARCHAR(20)," +
        "ActualElapsedTime INT," +
        "CRSElapsedTime INT," +
        "AirTime INT," +
        "ArrDelay INT," +
        "DepDelay INT," +
        "Origin VARCHAR(20)," +
        "Dest VARCHAR(20)," +
        "Distance INT," +
        "TaxiIn INT," +
        "TaxiOut INT," +
        "Cancelled INT," +
        "CancellationCode VARCHAR(20)," +
        "Diverted INT," +
        "CarrierDelay INT," +
        "WeatherDelay INT," +
        "NASDelay INT," +
        "SecurityDelay INT," +
        "LateAircraftDelay INT," +
        "ArrDelaySlot INT)"

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val snContext = snc
    snContext.sql("set spark.sql.shuffle.partitions=6")

    val airlineDF = snContext.read.load(hfile)
    val airlineparquetTable = "airlineparquetTable"
    airlineDF.registerTempTable(airlineparquetTable)

    snc.sql(s"CREATE TABLE $colTableName $ddlStr" +
        "USING column options()")

    airlineDF.write.insertInto(colTableName)

//    snc.sql(s"select Y.distance, Y.dest, X.distance, X.dest from (select distance, dest, count(*) " +
//        s" from $colTableName where taxiin > 20 or taxiout > 20" +
//        s" group by dest, distance) X " +
//        s" right outer join " +
//        s"(select distance, dest, count(*) " +
//        s" from $colTableName where taxiin > 10 or taxiout > 10" +
//        s" group by dest, distance) Y" +
//        s" on X.dest = Y.dest and X.distance = Y.distance").collect().foreach(println)

    var df = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
        s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
        s" where (taxiin > 10 or taxiout > 10) and dest in  (select dest from $colTableName " +
        s" where distance = 100 group by dest having count ( * ) > 100) group by dest order " +
        s" by avgTaxiTime desc")
    // df.explain(true)
    val res1 = df.collect()

    df = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
        s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
        s" where (taxiin > 20 or taxiout > 20) and dest in  (select dest from $colTableName " +
        s" where distance = 658 group by dest having count ( * ) > 100) group by dest order " +
        s" by avgTaxiTime desc")
    val res2 = df.collect()

    assert(!res1.sameElements(res2))
    assert( SnappySession.getPlanCache.asMap().size() == 1)

    SnappyTableStatsProviderService.suspendCacheInvalidation = false
  }
}
