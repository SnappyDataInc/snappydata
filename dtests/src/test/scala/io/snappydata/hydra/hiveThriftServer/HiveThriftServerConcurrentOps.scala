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
package io.snappydata.hydra.hiveThriftServer

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException}
import java.util

import com.typesafe.config.Config
import org.apache.spark.sql._
import scala.util.Random

class HiveThriftServerConcurrentOps extends SnappySQLJob {

  override def isValidJob(snappySession: SnappySession, config: Config):
  SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
//    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
//    val threadID = jobConfig.getInt("tid")
    val outputFile = "ValidateHiveThriftServerConcurrency" + "_" + System.currentTimeMillis() + jobConfig.getString("logFileName")
//   val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sqlContext : SQLContext = spark.sqlContext

    val queryFile: String = jobConfig.getString("queryFile");
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")

    for (j <- 0 to queryArray.length - 1) {
      val index = new Random().nextInt(queryArray.length )
      println("Executing Query : " + queryArray(index))
      snc.sql(queryArray(index))
    }
    }
 }

