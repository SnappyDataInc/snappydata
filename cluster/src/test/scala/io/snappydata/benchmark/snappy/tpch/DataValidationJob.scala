/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package io.snappydata.benchmark.snappy.tpch

import java.io.{File, FileOutputStream, PrintStream}

import com.typesafe.config.Config

import org.apache.spark.sql.TPCHUtils.{getClass, logWarning}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappyContext, SnappyJobInvalid, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

/**
  * Created by kishor on 13/4/17.
  */
object DataValidationJob extends SnappySQLJob {

  var queries: Array[String] = _
  var isDynamic: Boolean = _
  var isResultCollection: Boolean = _
  var isSnappy: Boolean = true
  var warmUp: Integer = _
  var runsForAverage: Integer = _
  var expectedResultsAvailableAt: String = _
  var actualResultsAvailableAt: String = _


  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val sc = snSession.sparkContext

    val fineName = if (!isDynamic) {
      if (isSnappy) "ResultValidattion_Snappy.out" else "ResultValidattion__Spark.out"
    } else {
      "ResultValidattion_Snappy_Tokenization.out"
    }

    val resultFileStream: FileOutputStream = new FileOutputStream(new File(fineName))
    val resultOutputStream: PrintStream = new PrintStream(resultFileStream)

    // scalastyle:off
    for (query <- queries) {
      println(s"For Query $query")

      val expectedFile = sc.textFile(s"file://$expectedResultsAvailableAt/1_Spark_${query}_Results.out")

      val actualFile = sc.textFile(s"file://$actualResultsAvailableAt/1_Snappy_${query}_Results.out")

      val expectedLineSet = expectedFile.collect().toList.sorted
      val actualLineSet = actualFile.collect().toList.sorted

      if (!actualLineSet.equals(expectedLineSet)) {
        if (!(expectedLineSet.size == actualLineSet.size)) {
          resultOutputStream.println(s"For $query " +
              s"result count mismatched observed with " +
              s"expected ${expectedLineSet.size} and actual ${actualLineSet.size}")
        } else {
          for ((expectedLine, actualLine) <- expectedLineSet zip actualLineSet) {
            if (!expectedLine.equals(actualLine)) {
              resultOutputStream.println(s"For $query result mismatched observed")
              resultOutputStream.println(s"Expected  : $expectedLine")
              resultOutputStream.println(s"Found     : $actualLine")
              resultOutputStream.println(s"-------------------------------------")
            }
          }
        }
      }

    }
    // scalastyle:on
    resultOutputStream.close()
    resultFileStream.close()

    val resultOutputFile = sc.textFile(fineName)

    if(!isDynamic) {
      assert(resultOutputFile.count() == 0,
        s"Query result mismatch Observed. Look at Result_Snappy.out for detailed failure")
      /* if (resultOutputFile.count() != 0) {
        logWarning(
          s"QUERY RESULT MISMATCH OBSERVED. Look at Result_Snappy.out for detailed failure")
      } */
    } else {
      assert(resultOutputFile.count() == 0,
        s"Query result match Observed. Look at Result_Snappy_Tokenization.out for detailed failure")
      /* if (resultOutputFile.count() != 0) {
        logWarning(
          s"QUERY RESYLT MATCH OBSERVED. Look at Result_Snappy_Tokenization.out for detailed" +
              s" failure")
      } */
    }
  }


  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    val tempqueries = if (config.hasPath("queries")) {
      config.getString("queries")
    } else {
      return SnappyJobInvalid("Specify Query number to be executed")
    }

    // scalastyle:off println
    println(s"tempqueries : $tempqueries")
    queries = tempqueries.split("-")

    isDynamic = if (config.hasPath("isDynamic")) {
      config.getBoolean("isDynamic")
    } else {
      return SnappyJobInvalid("Specify whether to use dynamic paramters")
    }

    expectedResultsAvailableAt = if (config.hasPath("ExpectedResultsAvailableAt")) {
      config.getString("ExpectedResultsAvailableAt")
    } else {
      return SnappyJobInvalid("Specify ExpectedResultsAvailableAt")
    }

    actualResultsAvailableAt = if (config.hasPath("ActualResultsAvailableAt")) {
      config.getString("ActualResultsAvailableAt")
    } else {
      return SnappyJobInvalid("Specify ActualResultsAvailableAt")
    }

    SnappyJobValid()
  }
}

