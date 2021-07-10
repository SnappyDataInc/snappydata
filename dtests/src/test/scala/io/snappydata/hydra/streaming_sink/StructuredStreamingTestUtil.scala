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

import java.io.{File, PrintWriter}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SnappySession}

object StructuredStreamingTestUtil {

  def structFields_persoon_genCol(withEventTypeColumn: Boolean = true): List[StructField] = {
    StructField("id", LongType, nullable = false) ::
        StructField("col2", StringType, nullable = true) ::
        StructField("col3", StringType, nullable = true) ::
        StructField("col4", StringType, nullable = true) ::
        StructField("col5", StringType, nullable = true) ::
        StructField("col6", StringType, nullable = true) ::
        StructField("col7", StringType, nullable = true) ::
        StructField("col8", StringType, nullable = true) ::
        StructField("col9", StringType, nullable = true) ::
        StructField("col10", StringType, nullable = true) ::
        StructField("col11", IntegerType, nullable = true) ::
        StructField("col12", StringType, nullable = true) ::
        StructField("col13", StringType, nullable = true) ::
        StructField("col14", StringType, nullable = true) ::
        StructField("col15", StringType, nullable = true)::
        StructField("col16", DoubleType, nullable = true)::
        StructField("col17", DoubleType, nullable = true)::
        StructField("col18", StringType, nullable = true)::
        StructField("col19", StringType, nullable = true)::
        StructField("col20", BooleanType, nullable = true)::
        StructField("col21", IntegerType, nullable = true)::
        StructField("col22", BooleanType, nullable = true)::
    (if (withEventTypeColumn) {
          StructField("_eventType", IntegerType, nullable = false) :: Nil
        } else {
          Nil
        })
  }

  def structFields_persoon(withEventTypeColumn: Boolean = true): List[StructField] = {
    StructField("id", LongType, nullable = false) ::
        StructField("firstName", StringType, nullable = true) ::
        StructField("middleName", StringType, nullable = true) ::
        StructField("lastName", StringType, nullable = true) ::
        StructField("title", StringType, nullable = true) ::
        StructField("address", StringType, nullable = true) ::
        StructField("country", StringType, nullable = true) ::
        StructField("phone", StringType, nullable = true) ::
        StructField("dateOfBirth", StringType, nullable = true) ::
        StructField("birthTime", StringType, nullable = true) ::
        StructField("age", IntegerType, nullable = true) ::
        StructField("status", StringType, nullable = true) ::
        StructField("email", StringType, nullable = true) ::
        StructField("education", StringType, nullable = true) ::
        StructField("gender", StringType, nullable = true)::
        StructField("weight", DoubleType, nullable = true)::
        StructField("height", DoubleType, nullable = true)::
        StructField("bloodGrp", StringType, nullable = true)::
        StructField("occupation", StringType, nullable = true)::
        StructField("hasChildren", BooleanType, nullable = true)::
        StructField("numChild", IntegerType, nullable = true)::
        StructField("hasSiblings", BooleanType, nullable = true)::
        (if (withEventTypeColumn) {
          StructField("_eventType", IntegerType, nullable = false) :: Nil
        } else {
          Nil
        })
  }

  def getStreamingDF(snc: SnappySession, broker: String, topic: String) : DataFrame = {
    return snc
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
  }

  def getSchema(tableName: String) : StructType = {
    tableName match {
      case "persoon" => return StructType(structFields_persoon())
      case "persoon_genCol" => return StructType(structFields_persoon_genCol())
    }
  }

  def createAndStartStreamingQueryGenCol(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File(".")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName + "_genCol")
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 23) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean,
              r(22).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean)
          }
        }).writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

  def createAndStartStreamingQuery(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File(".")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid
    pw.println("checkpoint dir is : " + checkpointDirectory)

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 23) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean,
              r(22).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean)
          }
        }).writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

  def createAndStartAggStreamingQuerySum(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean, outputMode: String): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File(".")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 23) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean,
              r(22).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean)
          }
        }).groupBy("id").sum("age", "numChild")
        .writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)

    streamWriter.outputMode(outputMode) // update or complete
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

  def createAndStartAggStreamingQueryAvg(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean, outputMode: String): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File(".")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 23) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean,
              r(22).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean)
          }
        }).groupBy("id").avg("age", "numChild")
        .writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)

    streamWriter.outputMode(outputMode) // update or complete
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

  def createAndStartAggStreamingQueryCount(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean, outputMode: String): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File(".")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 23) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean,
              r(22).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean)
          }
        }).groupBy("age").count()
        .writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)

    streamWriter.outputMode(outputMode) // update or complete
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

  def createAndStartAggStreamingQueryJoin(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean, outputMode: String): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File(".")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 23) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean,
              r(22).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8),
              r(9), r(10).toInt, r(11), r(12), r(13), r(14), r(15).toDouble,
              r(16).toDouble, r(17), r(18), r(19).toBoolean, r(20).toInt, r(21).toBoolean)
          }
        }).join(snc.table("persoon_details"), "id")
        .writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)

    streamWriter.outputMode(outputMode) // update or complete
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }
}
