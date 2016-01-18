package io.snappydata.examples

import java.io.{PrintWriter}
import com.typesafe.config.Config
import org.apache.spark.sql.SnappyAQP._
import org.apache.spark.sql.{DataFrame, SnappySQLJob}
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Fetches already created tables. Airline table is already persisted in
 * Snappy store. Cache the airline table in Spark cache as well for
 * comparison. Sample airline table and persist it in Snappy store.
 * Run a aggregate query on all the three tables and return the results in
 * a Map.This Map will be sent over REST.
 */
object AirlineDataJob extends SnappySQLJob {

  override def runJob(snc: C, jobConfig: Config): Any = {
    val colTable = "AIRLINE"
    val parquetTable = "STAGING_AIRLINE"
    val rowTable = "AIRLINEREF"
    val sampleTable = "AIRLINE_SAMPLE"

    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val pw = new PrintWriter("AirlineDataJob.out")

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTable)
    val airlineCodeDF: DataFrame = snc.table(rowTable)
    val airlineParquetDF: DataFrame = snc.table(parquetTable)
    val sampleDF: DataFrame = snc.table(sampleTable)

    // Cache the airline data in a Spark table as well
    airlineParquetDF.cache()
    airlineParquetDF.count()

    // Data Frame query on Airline table :Which Airlines Arrive On Schedule? JOIN with reference table
    val actualResult = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineCodeDF("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    val start = System.currentTimeMillis
    val result = actualResult.collect()
    val totalTime = (System.currentTimeMillis - start)
    pw.println(s"****** Query Execution on Airline Snappy table took ${totalTime}ms ******")
    result.foreach(rs => {
      pw.println(rs.toString)
    })

    // Data Frame query on Spark table :Which Airlines Arrive On Schedule? JOIN with reference table
    val parquetResult = airlineParquetDF.join(airlineCodeDF, airlineParquetDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineParquetDF("UniqueCarrier"),
      airlineCodeDF("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    val startP = System.currentTimeMillis
    val resultP = parquetResult.collect()
    val totalTimeP = (System.currentTimeMillis - startP)
    pw.println(s"\n****** Query Execution on Airline Spark table took ${totalTimeP}ms******")
    resultP.foreach(rs => {
      pw.println(rs.toString)
    })

    // Data Frame query on Sample table :Which Airlines Arrive On Schedule? JOIN with reference table
    val sampleResult = sampleDF.join(airlineCodeDF, sampleDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(sampleDF("UniqueCarrier"),
      airlineCodeDF("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    val startS = System.currentTimeMillis
    val resultS = sampleResult.collect()
    val totalTimeS = (System.currentTimeMillis - startS)
    pw.println(s"\n****** Query Execution on Airline Sample table took ${totalTimeS}ms******")
    resultS.foreach(rs => {
      pw.println(rs.toString)
    })

    //Sampling on Base table(i.e Airline table) with error and confidence clause
    val sampleBaseTableResult = airlineDF.groupBy(airlineDF("Year_"))
        .agg("ArrDelay" -> "avg")
        .orderBy("Year_").withError(0.20,0.80)
    val startSB = System.currentTimeMillis
    val resultSB = sampleBaseTableResult.collect()
    val totalTimeSB = (System.currentTimeMillis - startSB)
    pw.println(s"\n****** Query Execution with sampling on Airline table took ${totalTimeSB}ms******")
    resultSB.foreach(rs => {
      pw.println(rs.toString)
    })

    pw.close()
    Map("The output of the queries is in the following file: " -> s"${getCurrentDirectory}/AirlineDataJob.out")
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
