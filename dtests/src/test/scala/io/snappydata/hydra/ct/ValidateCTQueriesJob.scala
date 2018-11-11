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

package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import util.TestException

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class ValidateCTQueriesJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val threadID = Thread.currentThread().getId
    val outputFile =
      "ValidateCTQueriesJob_thread_" + threadID + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val tableType = jobConfig.getString("tableType")

    Try {
      val snc = snSession.sqlContext
      snc.sql("set spark.sql.shuffle.partitions=23")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      CTQueries.snc = snc
      // scalastyle:off println
      pw.println(s"Validation for $tableType tables started in snappy Job")
      val numRowsValidation: Boolean = jobConfig.getBoolean("numRowsValidation")
      val fullResultSetValidation: Boolean = jobConfig.getBoolean("fullResultSetValidation")
      SnappyTestUtils.validateFullResultSet = fullResultSetValidation
      SnappyTestUtils.numRowsValidation = numRowsValidation
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      val startTime = System.currentTimeMillis
      val failedQueries = CTTestUtil.executeQueries(snc, tableType, pw, sqlContext)
      val endTime = System.currentTimeMillis
      val totalTime = (endTime - startTime) / 1000
      if(!failedQueries.isEmpty) {
        println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. " +
            s"See ${getCurrentDirectory}/${outputFile}")
        pw.println(s"Total execution took ${totalTime} seconds.")
        pw.println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. ")
        pw.close()
        throw new TestException(s"Validation task failed for ${tableType}. " +
            s"See ${getCurrentDirectory}/${outputFile}")
      }
      println(s"Validation for $tableType tables completed sucessfully. " +
          s"See ${getCurrentDirectory}/${outputFile}")
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
