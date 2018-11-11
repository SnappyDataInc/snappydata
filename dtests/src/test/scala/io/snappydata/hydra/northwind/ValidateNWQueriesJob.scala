/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.io.{File, FileOutputStream, PrintWriter}

import util.TestException
import com.typesafe.config.Config
import io.snappydata.hydra.{SnappyTestUtils, northwind}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.util.{Failure, Success, Try}

class ValidateNWQueriesJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tableType = jobConfig.getString("tableType")
    val outputFile = "ValidateNWQueries_" + tableType + "_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val isSmokeRun: Boolean = jobConfig.getString("isSmokeRun").toBoolean
    val fullResultSetValidation: Boolean = jobConfig.getString("fullResultSetValidation").toBoolean
    val numRowsValidation: Boolean = jobConfig.getString("numRowsValidation").toBoolean
    SnappyTestUtils.validateFullResultSet = fullResultSetValidation
    SnappyTestUtils.numRowsValidation = numRowsValidation
    SnappyTestUtils.tableType = tableType
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      var failedQueries: String = "";
      snc.sql("set spark.sql.shuffle.partitions=23")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      northwind.NWQueries.snc = snc
      NWQueries.dataFilesLocation = dataFilesLocation
      // scalastyle:off println
      val startTime = System.currentTimeMillis()
      pw.println(s"ValidateQueries for ${tableType} tables started ..")
      if (isSmokeRun) {
        failedQueries = NWTestUtil.validateSelectiveQueriesFullResultSet(snc, tableType, pw,
          sqlContext)
      }
      else {
        failedQueries = NWTestUtil.validateQueries(snc, tableType, pw, sqlContext)
      }
      val finishTime = System.currentTimeMillis()
      val totalTime = (finishTime -startTime)/1000
      if (!failedQueries.isEmpty) {
        println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. " +
            s"See ${getCurrentDirectory}/${outputFile}")
        pw.println(s"Total execution took ${totalTime} seconds.")
        pw.println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. ")
        pw.close()
        throw new TestException(s"Validation task failed for ${tableType}. " +
            s"See ${getCurrentDirectory}/${outputFile}")
      }
      pw.println(s"ValidateQueries for ${tableType} tables completed successfully in " +
          totalTime + " seconds ")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
