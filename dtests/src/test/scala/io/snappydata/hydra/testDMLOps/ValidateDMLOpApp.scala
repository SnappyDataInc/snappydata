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

package io.snappydata.hydra.testDMLOps

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object ValidateDMLOpApp {

  def main(args: Array[String]) {
    val threadID = args(1)
    val conf = new SparkConf().
        setAppName("DMLOp_App_thr_" + threadID + "_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "DMLOpsApp_thr_" + threadID + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    val stmt = args(0)
    // scalastyle:off println
    pw.println(s"stmt : ${stmt}")
    pw.println(s"Executing ${stmt}")
    pw.flush()
    val startTime = System.currentTimeMillis
    val df = snc.sql(stmt)
    val endTime = System.currentTimeMillis
    pw.println(df.show);
    val totalTime = (endTime - startTime) / 1000
    pw.println(s"Total time for execution is :: ${totalTime} seconds.")
    println(s"Operation completed successfully. See ${getCurrentDirectory}/${outputFile}")
    pw.println(s"Operation completed successfully.")
    pw.close()
  }
}
