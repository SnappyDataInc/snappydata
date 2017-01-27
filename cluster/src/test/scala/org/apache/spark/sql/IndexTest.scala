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
package org.apache.spark.sql

import java.util.TimeZone

import com.pivotal.gemfirexd.internal.engine.db.FabricDatabase
import io.snappydata.benchmark.snappy.{SnappyAdapter, SnappyTPCH, TPCH, TPCH_Snappy}
import io.snappydata.{PlanTest, SnappyFunSuite}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.util.Benchmark

class IndexTest extends SnappyFunSuite with PlanTest with BeforeAndAfterEach {
  var existingSkipSPSCompile = false

  override def beforeAll(): Unit = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.testing", "true")
    existingSkipSPSCompile = FabricDatabase.SKIP_SPS_PRECOMPILE
    FabricDatabase.SKIP_SPS_PRECOMPILE = true
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.testing")
    FabricDatabase.SKIP_SPS_PRECOMPILE = existingSkipSPSCompile
    super.afterAll()
  }
/*

  test("dd") {
    // scalastyle:off println
    val toks = Seq("[dd]", "[dd1]", "date '[DATE]'", "date '[DATE]' + interval '1' year",
      "[Quantity]", "[dd2]")

    val args = Seq("y", "1-1-1999", "1", "zz")

    val newArgs = toks.zipWithIndex.sliding(2).flatMap(_.toList match {
      case (l, i) :: (r, _) :: Nil
        if l.indexOf("date '[DATE]'") >= 0 && r.indexOf("date '[DATE]' ") >= 0 =>
        Seq(args(i), args(i))
      case (_, i) :: _ if i < args.length =>
        Seq(args(i))
      case x =>
        Seq.empty
    }).toList

    def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
      val maxLeftSize = left.map(_.length).max
      val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))(" ")
      val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))(" ")
      leftPadded.zip(rightPadded).map {
        case (l, r) => l + (" " * ((maxLeftSize - l.length) + 3)) + r
      }
    }

    if(toks.length != newArgs.length) {
      println(sideBySide(toks, newArgs).mkString("\n"))
    }
    println(newArgs)
    // scalastyle:on println
  }
*/

  test("tpch queries") {
    // scalastyle:off println
    val qryProvider = new TPCH with SnappyAdapter

    val queries = Array("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19",
      "q20", "q21", "q22")

    TPCHUtils.createAndLoadTables(snc, true)

    val existing = snc.getConf(io.snappydata.Property.EnableExperimentalFeatures.name)
    snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name, "true")

    for ((q, i) <- queries.zipWithIndex)
    {
      val qNum = i + 1
      val (expectedAnswer, _) = qryProvider.execute(qNum, str => {
        snc.sql(str)
      })
      val (newAnswer, df) = TPCH_Snappy.queryExecution(q, snc, false, false)
      val isSorted = df.logicalPlan.collect { case s: Sort => s }.nonEmpty
      QueryTest.sameRows(expectedAnswer, newAnswer, isSorted).map { results =>
        s"""
           |Results do not match for query: $qNum
           |Timezone: ${TimeZone.getDefault}
           |Timezone Env: ${sys.env.getOrElse("TZ", "")}
           |
           |${df.queryExecution}
           |== Results ==
           |$results
       """.stripMargin
      }
      println(s"Done $qNum")
    }
    snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name, existing)

  }

  ignore("Benchmark tpch") {

    val queries = Array("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19",
      "q20", "q21", "q22")
/*
    val queries = Array("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19",
      "q20", "q21", "q22")
*/

    TPCHUtils.createAndLoadTables(snc, true)

    snc.sql(s"""CREATE INDEX idx_orders_cust ON orders(o_custkey)
             options (COLOCATE_WITH 'customer')
          """)

    snc.sql(s"""CREATE INDEX idx_lineitem_part ON lineitem(l_partkey)
             options (COLOCATE_WITH 'part')
          """)

    val tables = Seq("nation", "region", "supplier", "customer", "orders", "lineitem", "part",
      "partsupp")

    val tableSizes = tables.map { tableName =>
      (tableName, snc.table(tableName).count())
    }.toMap

    tableSizes.foreach(println)
    queries.foreach(q => benchmark(q, tableSizes))

    snc.sql(s"DROP INDEX idx_orders_cust")
    snc.sql(s"DROP INDEX idx_lineitem_part")
  }

  private def benchmark(qNum: String, tableSizes: Map[String, Long]) = {

    val qryProvider = new TPCH with SnappyAdapter
    val query = qNum.substring(1).toInt
    def executor(str: String) = snc.sql(str)

    val size = qryProvider.estimateSizes(query, tableSizes, executor)
    println(s"$qNum size $size")
    val b = new Benchmark(s"JoinOrder optimization", size, minNumIters = 10)

    def case1(): Unit = snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
      "false")

    def case2(): Unit = snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
      "true")

    def case3(): Unit = {
      snc.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
        "true")
    }

    def evalSnappyMods(genPlan: Boolean) = TPCH_Snappy.queryExecution(qNum, snc, useIndex = false,
      genPlan = genPlan)._1.foreach(_ => ())

    def evalBaseTPCH = qryProvider.execute(query, executor)


    b.addCase(s"$qNum baseTPCH index = F", prepare = case1)(i => evalBaseTPCH)
//    b.addCase(s"$qNum baseTPCH joinOrder = T", prepare = case2)(i => evalBaseTPCH)
//    b.addCase(s"$qNum snappyMods joinOrder = F", prepare = case1)(i => evalSnappyMods(false))
//    b.addCase(s"$qNum snappyMods joinOrder = T", prepare = case2)(i => evalSnappyMods(false))
    b.addCase(s"$qNum baseTPCH index = T", prepare = case3)(i =>
      evalBaseTPCH)
    b.run()

  }

  test("northwind queries") {
    println("")
    //    val sctx = sc(c => c.set("spark.sql.inMemoryColumnarStorage.batchSize", "40000"))
    //    val snc = getOrCreate(sctx)
    //    NorthWindDUnitTest.createAndLoadColumnTables(snc)
    //    val s = "select distinct shipcountry from orders"
    //    snc.sql(s).show()
    //    NWQueries.assertJoin(snc, NWQueries.Q42, "Q42", 22, 1, classOf[LocalJoin])
    /*
        Thread.sleep(1000 * 60 * 60)
        NWQueries.assertJoin(snc, NWQueries.Q42, "Q42", 22, 1, classOf[LocalJoin])
    */
  }

}
