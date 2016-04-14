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
package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Subquery}
import org.apache.spark.sql.sources.PutIntoTable

/**
 * Implicit conversions used by Snappy.
 */
// scalastyle:off
object snappy extends Serializable {
// scalastyle:on

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyDataFrameOperations = {
    df.sqlContext match {
      case sc: SnappyContext => SnappyDataFrameOperations(sc, df)
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def samplingOperationsOnDataFrame(df: DataFrame): SampleDataFrame = {
    df.sqlContext match {
      case sc: SnappyContext =>
        val plan = snappy.unwrapSubquery(df.logicalPlan)
        if (sc.snappyContextFunctions.isStratifiedSample(plan)) {
          new SampleDataFrame(sc, plan)
        } else {
          throw new AnalysisException("Stratified sampling " +
              "operations require stratifiedSample plan and not " +
              s"${plan.getClass.getSimpleName}")
        }
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def convertToAQPFrame(df: DataFrame): AQPDataFrame = {
    AQPDataFrame(df.sqlContext.asInstanceOf[SnappyContext], df.queryExecution)
  }

  def unwrapSubquery(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Subquery(_, child) => unwrapSubquery(child)
      case _ => plan
    }
  }

  implicit class RDDExtensions[T: ClassTag](rdd: RDD[T]) extends Serializable {

    /**
     * Return a new RDD by applying a function to all elements of this RDD.
     *
     * This variant also preserves the preferred locations of parent RDD.
     */
    def mapPreserve[U: ClassTag](f: T => U): RDD[U] = rdd.withScope {
      val cleanF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD[U, T](rdd,
        (context, pid, iter) => iter.map(cleanF))
    }

    /**
     * Return a new RDD by applying a function to each partition of given RDD.
     *
     * This variant also preserves the preferred locations of parent RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves
     * the partitioner, which should be `false` unless this is a pair RDD and
     * the input function doesn't modify the keys.
     */
    def mapPartitionsPreserve[U: ClassTag](
        f: Iterator[T] => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd,
        (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
        preservesPartitioning)
    }

    /**
     * Return a new RDD by applying a function to each partition of given RDD,
     * while tracking the index of the original partition.
     *
     * This variant also preserves the preferred locations of parent RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves
     * the partitioner, which should be `false` unless this is a pair RDD and
     * the input function doesn't modify the keys.
     */
    def mapPartitionsPreserveWithIndex[U: ClassTag](
        f: (Int, Iterator[T]) => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd,
        (context: TaskContext, index: Int, iter: Iterator[T]) =>
          cleanedF(index, iter),
        preservesPartitioning)
    }
  }

  implicit class DataFrameWriterExtensions(writer: DataFrameWriter)
      extends Serializable {

    /**
     * "Puts" the content of the [[DataFrame]] to the specified table. It
     * requires that the schema of the [[DataFrame]] is the same as the schema
     * of the table. If some rows are already present then they are updated.
     *
     * This ignores all SaveMode.
     */
    def putInto(tableName: String): Unit = {
      // unfortunately everything including DataFrame is private in
      // DataFrameWriter so have to use reflection
      val dfField = writer.getClass.getDeclaredFields.find { f =>
        f.getName == "df" || f.getName.endsWith("$df")
      }.getOrElse(sys.error("Failed to obtain DataFrame from DataFrameWriter"))
      dfField.setAccessible(true)
      val df: DataFrame = dfField.get(writer).asInstanceOf[DataFrame]
      val context = df.sqlContext match {
        case sc: SnappyContext => sc
        case _ => sys.error("Expected a SnappyContext for putInto operation")
      }
      val parColMethod = writer.getClass.getDeclaredMethod("normalizedParCols")
      parColMethod.setAccessible(true)
      val normalizedParCols = parColMethod.invoke(writer)
          .asInstanceOf[Option[Seq[String]]]
      // A partitioned relation's schema can be different from the input
      // logicalPlan, since partition columns are all moved after data columns.
      // We Project to adjust the ordering.
      // TODO: this belongs to the analyzer.
      val input = normalizedParCols.map { parCols =>
        val (inputPartCols, inputDataCols) = df.logicalPlan.output.partition {
          attr => parCols.contains(attr.name)
        }
        Project(inputDataCols ++ inputPartCols, df.logicalPlan)
      }.getOrElse(df.logicalPlan)

      df.sqlContext.executePlan(
        PutIntoTable(
          UnresolvedRelation(context.catalog.newQualifiedTableName(tableName)),
          input)).toRdd
    }
  }
}

private[sql] case class SnappyDataFrameOperations(context: SnappyContext,
    df: DataFrame) {


  /**
   * Creates stratified sampled data from given DataFrame
   * {{{
   *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
   * }}}
   */
  def stratifiedSample(options: Map[String, Any]): SampleDataFrame =
    new SampleDataFrame(context, context.snappyContextFunctions.convertToStratifiedSample(
      options, context, df.logicalPlan))


  /**
   * Creates a DataFrame for given time instant that will be used when
   * inserting into top-K structures.
   *
   * @param time the time instant of the DataFrame as millis since epoch
   * @return
   */
  def withTime(time: Long): DataFrameWithTime =
    new DataFrameWithTime(context, df.logicalPlan, time)


  /**
   * Append to an existing cache table.
   * Automatically uses #cacheQuery if not done already.
   */
  def appendToTempTableCache(tableName: String): Unit =
    context.appendToTempTableCache(df, tableName)
}

