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

package io.snappydata.hydra.complexdatatypes

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class ArraysOfStringInMapAsValue extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("ArraysofStringInMapAsValue Type Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory : String = new File(".").getCanonicalPath()
    val outputFile = "ValidateArraysOfStringInMaptype" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val printContent : Boolean = false

    val Array_Map_Q1 = "SELECT * FROM FamousPeopleView"
    val Array_Map_Q2 = "SELECT country, value[0],value[1],value[2],value[3],value[4] FROM " +
      "FamousPeopleView WHERE key = 'Prime Ministers'"
    val Array_Map_Q3 = "SELECT country, value FROM FamousPeopleView WHERE key = 'Authors'"

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA FP")
    snc.sql("CREATE TABLE IF NOT EXISTS FP.FamousPeople(country String, " +
                     "celebrities MAP<String,Array<String>>) USING column")

    snc.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'United States', " +
      "MAP('Presidents',ARRAY('George Washington','Abraham Lincoln','Thomas Jefferson'," +
      "'John F. Kennedy','Franklin D. Roosevelt'))")
    snc.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'India', " +
      "MAP('Prime Ministers',ARRAY('Jawaharlal Nehru','Indira Gandhi'," +
      "'Lal Bahadur Shastri','Narendra Modi','PV Narsimha Rao'))")
    snc.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'India', " +
      "MAP('Actors',ARRAY('Amithab Bachhan','Sanjeev Kumar','Dev Anand'," +
      "'Akshay Kumar','Shahrukh Khan','Salman Khan'))")
    snc.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'United States', " +
      "MAP('Actors',ARRAY('Brad Pitt','Jim Carry','Bruce Willis'," +
      "'Tom Cruise','Michael Douglas','Dwayne Johnson'))")
    snc.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'India', " +
      "MAP('Authors',ARRAY('Chetan Bhagat','Jay Vasavada','Amish Tripathi'," +
      "'Khushwant Singh','Premchand','Kalidas'))")
    snc.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'United States', " +
      "MAP('Authors',ARRAY('Mark Twain','Walt Whitman','J.D. Salinger'," +
      "'Emily Dickinson','Willa Cather','William Faulkner'))")

    snc.sql("CREATE TEMPORARY VIEW FamousPeopleView AS " +
      "SELECT country, explode(celebrities) FROM FP.FamousPeople")

    snc.sql(Array_Map_Q1)
    snc.sql(Array_Map_Q2)
    snc.sql(Array_Map_Q3)

    if(printContent) {
      println("snc : Array_Map_Q1 " + snc.sql(Array_Map_Q1).show)
      println("snc : Array_Map_Q2 " + snc.sql(Array_Map_Q2).show)
      println("snc : Array_Map_Q3 " + snc.sql(Array_Map_Q3).show)
    }

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA FP")
    spark.sql("CREATE TABLE IF NOT EXISTS FP.FamousPeople(country String, " +
      "celebrities MAP<String,Array<String>>) USING PARQUET")

    spark.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'United States', " +
      "MAP('Presidents',ARRAY('George Washington','Abraham Lincoln','Thomas Jefferson'," +
      "'John F. Kennedy','Franklin D. Roosevelt'))")
    spark.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'India', " +
      "MAP('Prime Ministers',ARRAY('Jawaharlal Nehru','Indira Gandhi'," +
      "'Lal Bahadur Shastri','Narendra Modi','PV Narsimha Rao'))")
    spark.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'India', " +
      "MAP('Actors',ARRAY('Amithab Bachhan','Sanjeev Kumar','Dev Anand'," +
      "'Akshay Kumar','Shahrukh Khan','Salman Khan'))")
    spark.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'United States', " +
      "MAP('Actors',ARRAY('Brad Pitt','Jim Carry','Bruce Willis'," +
      "'Tom Cruise','Michael Douglas','Dwayne Johnson'))")
    spark.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'India', " +
      "MAP('Authors',ARRAY('Chetan Bhagat','Jay Vasavada','Amish Tripathi'," +
      "'Khushwant Singh','Premchand','Kalidas'))")
    spark.sql("INSERT INTO FP.FamousPeople " +
      "SELECT 'United States', " +
      "MAP('Authors',ARRAY('Mark Twain','Walt Whitman','J.D. Salinger'," +
      "'Emily Dickinson','Willa Cather','William Faulkner'))")

    spark.sql("CREATE TEMPORARY VIEW FamousPeopleView AS " +
      "SELECT country, explode(celebrities) FROM FP.FamousPeople")

    spark.sql(Array_Map_Q1)
    spark.sql(Array_Map_Q2)
    spark.sql(Array_Map_Q3)

    if(printContent) {
      println("spark : Array_Map_Q1 " + spark.sql(Array_Map_Q1).show)
      println("spark : Array_Map_Q2 " + spark.sql(Array_Map_Q2).show)
      println("spark : Array_Map_Q3 " + spark.sql(Array_Map_Q2).show)
    }

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Map_Q1, "Array_Map_Q1",
      "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Map_Q2, "Array_Map_Q2",
      "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Map_Q3, "Array_Map_Q3",
      "column", pw, sqlContext)

    /* --- Clean up --- */

    snc.sql("DROP TABLE IF EXISTS FP.FamousPeople")
    snc.sql("DROP VIEW IF EXISTS FamousPeopleView")
    spark.sql("DROP TABLE IF EXISTS FP.FamousPeople")
    spark.sql("DROP VIEW IF EXISTS FamousPeopleView")
    snc.sql("DROP SCHEMA IF EXISTS FP")
    spark.sql("DROP SCHEMA IF EXISTS FP")
  }
}
