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
package io.snappydata.hydra.streaming_sink

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class CreateTableInSnappyJob extends SnappySQLJob{
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val isRowTable: Boolean = jobConfig.getBoolean("isRowTable")
    val withKeyColumn: Boolean = jobConfig.getBoolean("withKeyColumn")
    val outputFile = "CreateTablesJob_output.txt"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    // scalastyle:off println
    pw.println("dropping tables...")
    snc.sql("drop table if exists persoon")
    pw.println("dropped tables. now creating table in snappy...")
    pw.flush()
    def provider = if (isRowTable) "row" else "column"
    def options = if (!isRowTable && withKeyColumn) "options(key_columns 'id')" else ""
    def primaryKey = if (isRowTable && withKeyColumn) "primary key" else ""
    val s = s"create table persoon (id long $primaryKey, name varchar(40), age int) using " +
        s" $provider $options"
    pw.println(s"Creating table $s")
    snc.sql(s)
    pw.println("created table.")
    pw.flush()
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}
