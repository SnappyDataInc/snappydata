/*
 * Adapted from https://github.com/apache/spark/pull/13899 having the below
 * license.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.benchmark

import io.snappydata.SnappyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.util.Benchmark


class ColumnCacheBenchmark extends SnappyFunSuite {

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf = {
    val conf = new SparkConf()
        .setIfMissing("spark.master", "local[1]")
        .setAppName("microbenchmark")
    conf.set("spark.sql.shuffle.partitions", "1")
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  private val sparkSession = new SparkSession(sc)
  private val snappySession = snc.snappySession

  ignore("cache with randomized keys - end-to-end") {
    benchmarkRandomizedKeys(size = 20000000, readPathOnly = false)
  }

  test("cache with randomized keys - read path only") {
    benchmarkRandomizedKeys(size = 20000000, readPathOnly = true)
  }

  /**
   * Call collect on a [[DataFrame]] after deleting all existing temporary files.
   * This also checks whether the collected result matches the expected answer.
   */
  private def collect(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer, checkToRDD = false) match {
      case Some(errMessage) => throw new RuntimeException(errMessage)
      case None => // all good
    }
  }

  def addCaseWithCleanup(
      benchmark: Benchmark,
      name: String,
      numIters: Int = 0,
      prepare: () => Unit,
      cleanup: () => Unit,
      testCleanup: () => Unit)(f: Int => Unit): Unit = {
    val timedF = (timer: Benchmark.Timer) => {
      timer.startTiming()
      f(timer.iteration)
      timer.stopTiming()
      testCleanup()
    }
    benchmark.benchmarks += Benchmark.Case(name, timedF, numIters, prepare, cleanup)
  }

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  /**
   * Benchmark caching randomized keys created from a range.
   *
   * NOTE: When running this benchmark, you will get a lot of WARN logs complaining that the
   * shuffle files do not exist. This is intentional; we delete the shuffle files manually
   * after every call to `collect` to avoid the next run to reuse shuffle files written by
   * the previous run.
   */
  private def benchmarkRandomizedKeys(size: Int, readPathOnly: Boolean): Unit = {
    val numIters = 10
    val benchmark = new Benchmark("Cache random keys", size)
    sparkSession.sql(s"set ${SQLConf.COLUMN_BATCH_SIZE.key} = 10000")
    snappySession.sql(s"set ${SQLConf.COLUMN_BATCH_SIZE.key} = 10000")
    sparkSession.sql("drop table if exists test")
    var testDF = sparkSession.range(size)
        .selectExpr("id", "floor(rand() * 10000) as k")
    val testDF2 = snappySession.range(size)
        .selectExpr("id", "floor(rand() * 10000) as k")
    // var testDF = snappySession.range(size)
    //    .selectExpr("id", "concat('val', cast((id % 100) as string)) as k")
    testDF.createOrReplaceTempView("test")
    val query = "select avg(k), avg(id) from test"
    val expectedAnswer = sparkSession.sql(query).collect().toSeq
    val expectedAnswer2 = testDF2.selectExpr("avg(k)", "avg(id)").collect().toSeq

    /**
     * Add a benchmark case, optionally specifying whether to cache the dataset.
     */
    def addBenchmark(name: String, cache: Boolean, params: Map[String, String] = Map(),
        snappy: Boolean = false, nullable: Boolean = true): Unit = {
      val defaults = params.keys.flatMap { k => sparkSession.conf.getOption(k).map((k, _)) }
      val defaults2 = params.keys.flatMap { k => snappySession.conf.getOption(k).map((k, _)) }
      def prepare(): Unit = {
        params.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        params.foreach { case (k, v) => snappySession.conf.set(k, v) }
        if (!nullable) {
          testDF = ColumnCacheBenchmark.applySchema(testDF,
            StructType(testDF.schema.fields.map(_.copy(nullable = false))))
          testDF.createOrReplaceTempView("test")
        }
        doGC()
        if (cache) {
          testDF.createOrReplaceTempView("test")
          sparkSession.catalog.cacheTable("test")
        } else if (snappy) {
          val nullableStr = if (nullable) "" else " not null"
          snappySession.sql("drop table if exists test")
          snappySession.sql(s"create table test (id bigint $nullableStr, " +
              s"k bigint $nullableStr) using column")
          if (readPathOnly) {
            testDF2.write.insertInto("test")
          }
        }
        if (readPathOnly) {
          if (snappy) {
            collect(snappySession.sql(query), expectedAnswer2)
          } else {
            collect(sparkSession.sql(query), expectedAnswer)
          }
          testCleanup()
        }
        doGC()
      }
      def cleanup(): Unit = {
        defaults.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        defaults2.foreach { case (k, v) => snappySession.conf.set(k, v) }
        sparkSession.catalog.clearCache()
        snappySession.sql("drop table if exists test")
        doGC()
      }
      def testCleanup(): Unit = {
      }
      addCaseWithCleanup(benchmark, name, numIters, prepare, cleanup, testCleanup) { _ =>
        if (readPathOnly) {
          if (snappy) {
            collect(snappySession.sql(query), expectedAnswer2)
          } else {
            collect(sparkSession.sql(query), expectedAnswer)
          }
        } else {
          // also benchmark the time it takes to build the column buffers
          if (snappy) {
            snappySession.sql("truncate table test")
            testDF2.write.insertInto("test")
            val ds = snappySession.sql(query)
            collect(ds, expectedAnswer2)
            collect(ds, expectedAnswer2)
          } else {
            val ds = sparkSession.sql(query)
            collect(ds, expectedAnswer)
            collect(ds, expectedAnswer)
          }
        }
      }
    }

    // All of these are codegen = T hashmap = T
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.VECTORIZED_AGG_MAP_MAX_COLUMNS.key, "1024")
    snappySession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    // Benchmark cases:
    //   (1) Caching without compression
    //   (2) Caching with column batches without compression
    // addBenchmark("cache = F", cache = false)
    addBenchmark("cache = T compress = F nullable = F", cache = true, Map(
      SQLConf.COMPRESS_CACHED.key -> "false"
    ), nullable = false)

    addBenchmark("cache = F snappyCompress = F nullable = F", cache = false, Map(
      SQLConf.COMPRESS_CACHED.key -> "false"
    ), snappy = true, nullable = false)

    benchmark.run()
  }
}

object ColumnCacheBenchmark {

  def applySchema(df: DataFrame, newSchema: StructType): DataFrame = {
    df.sqlContext.internalCreateDataFrame(df.queryExecution.toRdd, newSchema)
  }
}
