/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.northwind

import java.io.{File, PrintWriter}


import io.snappydata.hydra.SnappyTestUtils

import org.apache.spark.sql._

object NWTestUtil {

  def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers(snc).write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table)
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table)
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table)
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table)
    NWQueries.suppliers(snc).write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table)
    NWQueries.territories(snc).write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table)
    NWQueries.employee_territories(snc).write.insertInto("employee_territories")
  }

  /*
  Method for validating number of rows and fullresultset with default data for northwind schema
  size data
  */
  def validateQueries(snc: SnappyContext, tableType: String, pw: PrintWriter, sqlContext:
  SQLContext): String = {
    var failedQueries = ""
    if (SnappyTestUtils.validateFullResultSet) {
      NWTestUtil.createAndLoadSparkTables(sqlContext)
    }
    for (q <- NWQueries.queries) {
      var queryExecuted = true;
      var hasValidationFailed = false;
      q._1 match {
        case "Q1" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q1, 8,
          "Q1", pw, sqlContext)
        case "Q2" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q2, 91,
          "Q2", pw, sqlContext)
        case "Q3" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q3, 830,
          "Q3", pw, sqlContext)
        case "Q4" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q4, 9,
          "Q4", pw, sqlContext)
        case "Q5" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q5, 9,
          "Q5", pw, sqlContext)
        case "Q6" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q6, 9,
          "Q6", pw, sqlContext)
        case "Q7" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q7, 9,
          "Q7", pw, sqlContext)
        case "Q8" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q8, 6,
          "Q8", pw, sqlContext)
        case "Q9" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q9, 3,
          "Q9", pw, sqlContext)
        case "Q10" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q10, 2,
          "Q10", pw, sqlContext)
        case "Q11" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q11, 4,
          "Q11", pw, sqlContext)
        case "Q12" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q12, 2,
          "Q12", pw, sqlContext)
        case "Q13" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q13, 2,
          "Q13", pw, sqlContext)
        case "Q14" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q14, 69,
          "Q14", pw, sqlContext)
        case "Q15" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q15, 5,
          "Q15", pw, sqlContext)
        case "Q16" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q16, 8,
          "Q16", pw, sqlContext)
        case "Q17" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q17, 3,
          "Q17", pw, sqlContext)
        case "Q18" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q18, 9,
          "Q18", pw, sqlContext)
        case "Q20" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q20, 1,
          "Q20", pw, sqlContext)
        case "Q21" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q21, 1,
          "Q21", pw, sqlContext)
        case "Q22" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q22, 1,
          "Q22", pw, sqlContext)
        case "Q23" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q23, 1,
          "Q23", pw, sqlContext)
        case "Q24" => hasValidationFailed = SnappyTestUtils.assertQuery(snc, NWQueries.Q24, 4,
          "Q24", pw, sqlContext)
        case "Q25" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q25, 1,
          "Q25", pw, sqlContext)
        case "Q26" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q26, 86,
          "Q26", pw, sqlContext)
        case "Q27" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q27, 9,
          "Q27", pw, sqlContext)
        case "Q28" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q28, 12,
          "Q28", pw, sqlContext)
        case "Q29" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q29, 8,
          "Q29", pw, sqlContext)
        case "Q30" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q30, 8,
          "Q30", pw, sqlContext)
        case "Q31" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q31, 830,
          "Q31", pw, sqlContext)
        case "Q32" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q32, 8,
          "Q32", pw, sqlContext)
        case "Q33" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q33, 37,
          "Q33", pw, sqlContext)
        case "Q34" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q34, 5,
          "Q34", pw, sqlContext)
        case "Q35" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q35, 3,
          "Q35", pw, sqlContext)
        case "Q36" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q36, 290,
          "Q36", pw, sqlContext)
        case "Q37" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q37, 77,
          "Q37", pw, sqlContext)
        case "Q38" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q38, 2155,
          "Q38", pw, sqlContext)
        case "Q39" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q39, 9,
          "Q39", pw, sqlContext)
        case "Q40" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q40, 830,
          "Q40", pw, sqlContext)
        case "Q41" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q41, 2155,
          "Q41", pw, sqlContext)
        case "Q42" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q42, 22,
          "Q42", pw, sqlContext)
        case "Q43" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q43, 830,
          "Q43", pw, sqlContext)
        // LeftSemiJoinHash
        case "Q44" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q44, 830,
          "Q44", pw, sqlContext)
        case "Q45" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q45,
          1788650, "Q45", pw, sqlContext)
        case "Q46" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q46,
          1788650, "Q46", pw, sqlContext)
        case "Q47" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q47,
          1788650, "Q47", pw, sqlContext)
        case "Q48" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q48,
          1788650, "Q48", pw, sqlContext)
        case "Q49" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q49,
          1788650, "Q49", pw, sqlContext)
        case "Q50" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q50, 2155,
          "Q50", pw, sqlContext)
        case "Q51" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q51, 2155,
          "Q51", pw, sqlContext)
        case "Q52" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q52, 2155,
          "Q52", pw, sqlContext)
        case "Q53" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q53, 2155,
          "Q53", pw, sqlContext)
        case "Q54" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q54, 2155,
          "Q54", pw, sqlContext)
        case "Q55" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q55, 21,
          "Q55", pw, sqlContext)
        case "Q56" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q56, 8,
          "Q56", pw, sqlContext)
        case "Q57" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q57, 120,
          "Q57", pw, sqlContext)
        case "Q58" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q58, 1,
          "Q58", pw, sqlContext)
        case "Q59" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q59, 1,
          "Q59", pw, sqlContext)
        case "Q60" => hasValidationFailed = SnappyTestUtils.assertJoin(snc, NWQueries.Q60, 947,
          "Q60", pw, sqlContext)
        // scalastyle:off println
        case _ =>
          pw.println(s"Query ${q._1} has not been executed.")
          queryExecuted = false
      }
      if (queryExecuted) {
        pw.println(s"Execution completed for query ${q._1}")
      }
      if (hasValidationFailed) {
        failedQueries = SnappyTestUtils.addToFailedQueryList(failedQueries, q._1)
      }
    }
    return failedQueries;
  }

  def validateSelectiveQueriesFullResultSet(snc: SnappyContext, tableType: String, pw:
  PrintWriter, sqlContext: SQLContext): Unit = {
    SnappyTestUtils.validateFullResultSet = true
    for (q <- NWQueries.queries) {
      q._1 match {
        // case "Q1" => SnappyTestUtils.assertQuery(snc, NWQueries.Q1, "Q1",
        //  pw, sqlContext)
        // case "Q2" => SnappyTestUtils.assertQuery(snc, NWQueries.Q2, "Q2",
        //  pw, sqlContext)
        // case "Q3" => SnappyTestUtils.assertQuery(snc, NWQueries.Q3, "Q3",
        //  pw, sqlContext)
        // case "Q4" => SnappyTestUtils.assertQuery(snc, NWQueries.Q4, "Q4",
        //  pw, sqlContext)
        // case "Q5" => SnappyTestUtils.assertQuery(snc, NWQueries.Q5, "Q5",
        //  pw, sqlContext)
        case "Q6" => SnappyTestUtils.assertQuery(snc, NWQueries.Q6, "Q6",
           pw, sqlContext)
        case "Q7" => SnappyTestUtils.assertQuery(snc, NWQueries.Q7, "Q7",
           pw, sqlContext)
        // case "Q8" => SnappyTestUtils.assertQuery(snc, NWQueries.Q8, "Q8",
        //  pw, sqlContext)
        case "Q9" => SnappyTestUtils.assertQuery(snc, NWQueries.Q9, "Q9",
           pw, sqlContext)
        // case "Q10" => SnappyTestUtils.assertQuery(snc, NWQueries.Q10, "Q10",
        //  pw, sqlContext)
        case "Q11" => SnappyTestUtils.assertQuery(snc, NWQueries.Q11, "Q11",
           pw, sqlContext)
        case "Q12" => SnappyTestUtils.assertQuery(snc, NWQueries.Q12, "Q12",
           pw, sqlContext)
        case "Q13" => SnappyTestUtils.assertQuery(snc, NWQueries.Q13, "Q13",
           pw, sqlContext)
        case "Q14" => SnappyTestUtils.assertQuery(snc, NWQueries.Q14, "Q14",
           pw, sqlContext)
        case "Q15" => SnappyTestUtils.assertQuery(snc, NWQueries.Q15, "Q15",
           pw, sqlContext)
        case "Q16" => SnappyTestUtils.assertQuery(snc, NWQueries.Q16, "Q16",
           pw, sqlContext)
        case "Q17" => SnappyTestUtils.assertQuery(snc, NWQueries.Q17, "Q17",
           pw, sqlContext)
        case "Q18" => SnappyTestUtils.assertQuery(snc, NWQueries.Q18, "Q18",
           pw, sqlContext)
        case "Q19" => SnappyTestUtils.assertQuery(snc, NWQueries.Q19, "Q19",
           pw, sqlContext)
        case "Q20" => SnappyTestUtils.assertQuery(snc, NWQueries.Q20, "Q20",
           pw, sqlContext)
        case "Q21" => SnappyTestUtils.assertQuery(snc, NWQueries.Q21, "Q21",
           pw, sqlContext)
        case "Q22" => SnappyTestUtils.assertQuery(snc, NWQueries.Q22, "Q22",
           pw, sqlContext)
        // case "Q23" => SnappyTestUtils.assertQuery(snc, NWQueries.Q23, "Q23",
        //  pw, sqlContext)
        case "Q24" => SnappyTestUtils.assertQuery(snc, NWQueries.Q24, "Q24",
           pw, sqlContext)
        case "Q25" => SnappyTestUtils.assertJoin(snc, NWQueries.Q25, "Q25",
           pw, sqlContext)
        case "Q26" => SnappyTestUtils.assertJoin(snc, NWQueries.Q26, "Q26",
           pw, sqlContext)
        case "Q27" => SnappyTestUtils.assertJoin(snc, NWQueries.Q27, "Q27",
           pw, sqlContext)
        case "Q28" => SnappyTestUtils.assertJoin(snc, NWQueries.Q28, "Q28",
           pw, sqlContext)
        // case "Q29" => SnappyTestUtils.assertJoin(snc, NWQueries.Q29, "Q29",
        //  pw, sqlContext)
        case "Q30" => SnappyTestUtils.assertJoin(snc, NWQueries.Q30, "Q30",
           pw, sqlContext)
        case "Q31" => SnappyTestUtils.assertJoin(snc, NWQueries.Q31, "Q31",
           pw, sqlContext)
        case "Q32" => SnappyTestUtils.assertJoin(snc, NWQueries.Q32, "Q32",
           pw, sqlContext)
        case "Q33" => SnappyTestUtils.assertJoin(snc, NWQueries.Q33, "Q33",
           pw, sqlContext)
        case "Q34" => SnappyTestUtils.assertJoin(snc, NWQueries.Q34, "Q34",
           pw, sqlContext)
/*        case "Q35" => SnappyTestUtils.assertJoin(snc, NWQueries.Q35, "Q35",
           pw, sqlContext) */
        case "Q36" => SnappyTestUtils.assertJoin(snc, NWQueries.Q36, "Q36",
           pw, sqlContext)
        case "Q37" => SnappyTestUtils.assertJoin(snc, NWQueries.Q37, "Q37",
           pw, sqlContext)
        case "Q38" => SnappyTestUtils.assertJoin(snc, NWQueries.Q38, "Q38",
           pw, sqlContext)
        case "Q39" => SnappyTestUtils.assertJoin(snc, NWQueries.Q39, "Q39",
           pw, sqlContext)
        case "Q40" => SnappyTestUtils.assertJoin(snc, NWQueries.Q40, "Q40",
           pw, sqlContext)
        case "Q41" => SnappyTestUtils.assertJoin(snc, NWQueries.Q41, "Q41",
           pw, sqlContext)
        case "Q42" => SnappyTestUtils.assertJoin(snc, NWQueries.Q42, "Q42",
           pw, sqlContext)
        case "Q43" => SnappyTestUtils.assertJoin(snc, NWQueries.Q43, "Q43",
           pw, sqlContext)
        /* case "Q44" => SnappyTestUtils.assertJoin(snc, NWQueries.Q44, "Q44",
            pw, sqlContext) */ // LeftSemiJoinHash
        /* case "Q45" => SnappyTestUtils.assertJoin(snc, NWQueries.Q45, "Q45",
           pw, sqlContext) */
        /* case "Q46" => SnappyTestUtils.assertJoin(snc, NWQueries.Q46, "Q46",
         pw, sqlContext)
        case "Q47" => SnappyTestUtils.assertJoin(snc, NWQueries.Q47, "Q47",
          pw, sqlContext)
        case "Q48" => SnappyTestUtils.assertJoin(snc, NWQueries.Q48, "Q48",
          pw, sqlContext)
        case "Q49" => SnappyTestUtils.assertJoin(snc, NWQueries.Q49, "Q49",
          pw, sqlContext)
        case "Q50" => SnappyTestUtils.assertJoin(snc, NWQueries.Q50, "Q50",
          pw, sqlContext)
        case "Q51" => SnappyTestUtils.assertJoin(snc, NWQueries.Q51, "Q51",
          pw, sqlContext)
        case "Q52" => SnappyTestUtils.assertJoin(snc, NWQueries.Q52, "Q52",
          pw, sqlContext)
        case "Q53" => SnappyTestUtils.assertJoin(snc, NWQueries.Q53, "Q53",
          pw, sqlContext) */
        /* case "Q54" => SnappyTestUtils.assertJoin(snc, NWQueries.Q54, "Q54",
           pw, sqlContext) */
        case "Q55" => SnappyTestUtils.assertJoin(snc, NWQueries.Q55, "Q55",
           pw, sqlContext)
        case "Q56" => SnappyTestUtils.assertJoin(snc, NWQueries.Q56, "Q56",
           pw, sqlContext)
/*        case "Q57" => SnappyTestUtils.assertQuery(snc, NWQueries.Q57, "Q57",
           pw, sqlContext) */
        case "Q58" => SnappyTestUtils.assertQuery(snc, NWQueries.Q58, "Q58",
           pw, sqlContext)
        case "Q59" => SnappyTestUtils.assertQuery(snc, NWQueries.Q59, "Q59",
           pw, sqlContext)
        // case "Q60" => SnappyTestUtils.assertQuery(snc, NWQueries.Q60,"Q60",
        //  pw, sqlContext)
        // scalastyle:off println
        case _ => println(s"Did not execute query ${q._1}")
        // scalastyle:on println
      }
    }
  }

  def createAndLoadPartitionedTables(snc: SnappyContext,
                                     createLargeOrdertable: Boolean = false): Unit = {

    if (createLargeOrdertable) {
      snc.sql(NWQueries.large_orders_table +
          " using row options (partition_by 'OrderId', buckets '13', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.large_order_details_table +
          " using row options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.products_table +
          " using row options ( partition_by 'ProductID,SupplierID', buckets '17', redundancy '1'" +
          " , PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.categories_table)
    } else {
      snc.sql(NWQueries.regions_table)
      snc.sql(NWQueries.categories_table)
      snc.sql(NWQueries.shippers_table)
      snc.sql(NWQueries.employees_table + " using row options(partition_by 'PostalCode,Region'," +
          "  buckets '19', redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.customers_table +
          " using row options( partition_by 'PostalCode,Region', buckets '19', colocate_with " +
          "'employees', redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.orders_table +
          " using row options (partition_by 'OrderId', buckets '13', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.order_details_table +
          " using row options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")

      snc.sql(NWQueries.products_table +
          " using row options ( partition_by 'ProductID,SupplierID', buckets '17', redundancy " +
          "'1', " +
          " PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")

      snc.sql(NWQueries.suppliers_table +
          " USING row options (PARTITION_BY 'SupplierID', buckets '123',redundancy '1', " +
          "PERSISTENT " +
          "'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.territories_table +
          " using row options (partition_by 'TerritoryID', buckets '3', redundancy '1', " +
          "PERSISTENT " +
          "'sync', EVICTION_BY 'LRUHEAPPERCENT')")

      snc.sql(NWQueries.employee_territories_table +
          " using row options(partition_by 'EmployeeID', buckets '1', redundancy '1', PERSISTENT " +
          "'sync', EVICTION_BY 'LRUHEAPPERCENT')")
    }
    if (createLargeOrdertable) {
      NWQueries.orders(snc).selectExpr("*", s" $bigcomment as bigComment").
          write.insertInto("orders")
      NWQueries.order_details(snc).selectExpr("*", s" $bigcomment as bigComment").
          write.insertInto("order_details")
      NWQueries.categories(snc).write.insertInto("categories")
      NWQueries.products(snc).write.insertInto("products")


    } else {
      NWQueries.regions(snc).write.insertInto("regions")

      NWQueries.categories(snc).write.insertInto("categories")

      NWQueries.shippers(snc).write.insertInto("shippers")

      NWQueries.employees(snc).write.insertInto("employees")

      NWQueries.customers(snc).write.insertInto("customers")
      NWQueries.orders(snc).write.insertInto("orders")
      NWQueries.order_details(snc).write.insertInto("order_details")
      NWQueries.products(snc).write.insertInto("products")

      NWQueries.suppliers(snc).write.insertInto("suppliers")

      NWQueries.territories(snc).write.insertInto("territories")

      NWQueries.employee_territories(snc).write.insertInto("employee_territories")
    }
  }

  def ingestMoreData(snc: SnappyContext, numTimes: Int): Unit = {
    for (i <- 1 to numTimes) {
      NWQueries.orders(snc).selectExpr("*", s" $bigcomment as bigComment").
          write.insertInto("orders")
      NWQueries.order_details(snc).selectExpr("*", s" $bigcomment as bigComment").
          write.insertInto("order_details")
    }
  }

  val bigcomment: String = "'bigcommentstart" + ("a" * 500) + "bigcommentend'"

  def createAndLoadColumnTables(snc: SnappyContext,
                                createLargeOrdertable: Boolean = false): Unit = {

    if (createLargeOrdertable) {
      snc.sql(NWQueries.large_orders_table +
          " using column options (partition_by 'OrderId', buckets " +
          "'13', redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.large_order_details_table +
          " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.products_table +
          " USING column options (partition_by 'ProductID,SupplierID', buckets '17', redundancy " +
          "'1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.categories_table)
    } else {
      snc.sql(NWQueries.regions_table)
      snc.sql(NWQueries.categories_table)
      snc.sql(NWQueries.shippers_table)
      snc.sql(NWQueries.employees_table + " using column options(partition_by 'City,Country', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.customers_table + " using column options(partition_by 'City,Country', " +
          "COLOCATE_WITH 'employees', redundancy '1', PERSISTENT 'sync', EVICTION_BY " +
          "'LRUHEAPPERCENT')")
      snc.sql(NWQueries.orders_table + " using column options (partition_by 'OrderId', buckets " +
          "'13', redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.order_details_table +
          " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', " +
          "redundancy '1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.products_table +
          " USING column options (partition_by 'ProductID,SupplierID', buckets '17', redundancy " +
          "'1', PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.suppliers_table +
          " USING column options (PARTITION_BY 'SupplierID', buckets '123', redundancy '1',  " +
          "PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.territories_table +
          " using column options (partition_by 'TerritoryID', buckets '3', redundancy '1', " +
          "PERSISTENT 'sync', EVICTION_BY 'LRUHEAPPERCENT')")
      snc.sql(NWQueries.employee_territories_table +
          " using row options(partition_by 'EmployeeID', buckets '1', redundancy '1', PERSISTENT " +
          "'sync', EVICTION_BY 'LRUHEAPPERCENT')")
    }
    if (createLargeOrdertable) {
      NWQueries.orders(snc).selectExpr("*", s" $bigcomment as bigComment").
          write.insertInto("orders")
      NWQueries.order_details(snc).selectExpr("*", s" $bigcomment as bigComment").
          write.insertInto("order_details")
      NWQueries.categories(snc).write.insertInto("categories")
      NWQueries.products(snc).write.insertInto("products")
    } else {
      NWQueries.orders(snc).write.insertInto("orders")
      NWQueries.order_details(snc).write.insertInto("order_details")
      NWQueries.regions(snc).write.insertInto("regions")

      NWQueries.categories(snc).write.insertInto("categories")

      NWQueries.shippers(snc).write.insertInto("shippers")

      NWQueries.employees(snc).write.insertInto("employees")

      NWQueries.customers(snc).write.insertInto("customers")
      NWQueries.products(snc).write.insertInto("products")

      NWQueries.suppliers(snc).write.insertInto("suppliers")

      NWQueries.territories(snc).write.insertInto("territories")

      NWQueries.employee_territories(snc).write.insertInto("employee_territories")
    }
  }

  def createAndLoadColocatedTables(snc: SnappyContext): Unit = {
    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers(snc).write.insertInto("shippers")

    snc.sql(NWQueries.employees_table +
        " using row options( partition_by 'PostalCode,Region', buckets '19', redundancy '1')")
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table +
        " using column options( partition_by 'PostalCode,Region', buckets '19', colocate_with " +
        "'employees', redundancy '1')")
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table +
        " using row options (partition_by 'CustomerID, OrderID', buckets '19', redundancy '1')")
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
        " using row options ( partition_by 'ProductID', buckets '329', redundancy '1')")
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " USING column options ( partition_by 'ProductID', buckets '329'," +
        " colocate_with 'order_details', redundancy '1')")
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '123', redundancy '1')")
    NWQueries.suppliers(snc).write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using column options (partition_by 'TerritoryID', buckets '3', redundancy '1')")
    NWQueries.territories(snc).write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
        " using row options(partition_by 'TerritoryID', buckets '3', colocate_with 'territories'," +
        " redundancy '1') ")
    NWQueries.employee_territories(snc).write.insertInto("employee_territories")
  }

  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    // scalastyle:off println
    NWQueries.regions(sqlContext).registerTempTable("regions")
    println(s"regions Table created successfully in spark")
    NWQueries.categories(sqlContext).registerTempTable("categories")
    println(s"categories Table created successfully in spark")
    NWQueries.shippers(sqlContext).registerTempTable("shippers")
    println(s"shippers Table created successfully in spark")
    NWQueries.employees(sqlContext).registerTempTable("employees")
    println(s"employees Table created successfully in spark")
    NWQueries.customers(sqlContext).registerTempTable("customers")
    println(s"customers Table created successfully in spark")
    NWQueries.orders(sqlContext).registerTempTable("orders")
    println(s"orders Table created successfully in spark")
    NWQueries.order_details(sqlContext).registerTempTable("order_details")
    println(s"order_details Table created successfully in spark")
    NWQueries.products(sqlContext).registerTempTable("products")
    println(s"products Table created successfully in spark")
    NWQueries.suppliers(sqlContext).registerTempTable("suppliers")
    println(s"suppliers Table created successfully in spark")
    NWQueries.territories(sqlContext).registerTempTable("territories")
    println(s"territories Table created successfully in spark")
    NWQueries.employee_territories(sqlContext).registerTempTable("employee_territories")
    println(s"employee_territories Table created successfully in spark")
    // scalastyle:on println
  }

  def dropTables(snc: SnappyContext): Unit = {
    // scalastyle:off println
    snc.sql("drop table if exists regions")
    println("regions table dropped successfully.");
    snc.sql("drop table if exists categories")
    println("categories table dropped successfully.");
    snc.sql("drop table if exists products")
    println("products table dropped successfully.");
    snc.sql("drop table if exists order_details")
    println("order_details table dropped successfully.");
    snc.sql("drop table if exists orders")
    println("orders table dropped successfully.");
    snc.sql("drop table if exists customers")
    println("customers table dropped successfully.");
    snc.sql("drop table if exists employees")
    println("employees table dropped successfully.");
    snc.sql("drop table if exists employee_territories")
    println("employee_territories table dropped successfully.");
    snc.sql("drop table if exists shippers")
    println("shippers table dropped successfully.");
    snc.sql("drop table if exists suppliers")
    println("suppliers table dropped successfully.");
    snc.sql("drop table if exists territories")
    println("territories table dropped successfully.");
    // scalastyle:on println
  }

}
