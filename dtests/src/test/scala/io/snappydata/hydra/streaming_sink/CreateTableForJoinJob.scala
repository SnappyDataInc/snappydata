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
package io.snappydata.hydra.streaming_sink

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class CreateTableForJoinJob extends SnappySQLJob{

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val tableName = jobConfig.getString("tableName")
    val isRowTable: Boolean = jobConfig.getBoolean("isRowTable")
    val withKeyColumn: Boolean = jobConfig.getBoolean("withKeyColumn")
    val outputFile = "CreateTablesJob_output.txt"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    var isPartitioned: Boolean = false
    if (jobConfig.getString("isPartitioned") != null) {
      isPartitioned = jobConfig.getBoolean("isPartitioned")
    }
    // scalastyle:off println
    pw.println("dropping tables...")
    snc.sql("drop table if exists persoon")
    pw.println("dropped tables. now creating table in snappy...")
    pw.flush()
    def provider = if (isRowTable) "row" else "column"
    var options: String = "options( PERSISTENT 'sync'"
    if (!isRowTable && withKeyColumn) {
      options = options + ",redundancy '1',key_columns 'id'"
    }
    if (isPartitioned) {
      options = options + ",partition_by 'id', redundancy '1'"
    }
    def primaryKey = if (isRowTable && withKeyColumn) ", primary key (id)"
    else ""
    options = options + ")"
    val s = s"create table $tableName (" +
        s"id long, " +
        s"firstName varchar(30), " +
        s"middleName varchar(30), " +
        s"lastName varchar(30), " +
        s"title varchar(5), " +
        s"address varchar(40), " +
        s"country varchar(10), " +
        s"phone varchar(12), " +
        s"dateOfBirth date, " +
        s"birthTime timestamp, " +
        s"age int, " +
        s"status varchar(10), " +
        s"email varchar(30), " +
        s"education varchar(20), " +
        s"gender varchar(12)," +
        s"weight double," +
        s"height double," +
        s"bloodGrp varchar(3)," +
        s"occupation varchar(15), " +
        s"hasChildren boolean," +
        s"numChild int," +
        s"hasSiblings boolean, " +
        s"languages varchar(10)" +
        s" $primaryKey" +
        s") using $provider $options"
    pw.println(s"Creating table $s")
    pw.flush()
    snc.sql(s)
    pw.println("Created table.")
    pw.flush()
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}
