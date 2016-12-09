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
package io.snappydata.cluster

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.store.CreateIndexTest
import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 * Tests various distributed index related tests.
 */
class DistributedIndexDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  val tablesToDrop = new ListBuffer[String]
  val indexesToDrop = new ListBuffer[String]
  override def tearDown2(): Unit = {
    try {
      val snContext = SnappyContext(sc)
      if (snContext != null) {
        snContext.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
          io.snappydata.Property.EnableExperimentalFeatures.configEntry.defaultValueString)
        snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get.toString)
        indexesToDrop.reverse.foreach(i => snContext.sql(s"DROP INDEX $i "))
        tablesToDrop.reverse.foreach(t => snContext.sql(s"DROP TABLE $t "))
        indexesToDrop.clear()
        tablesToDrop.clear()
      }
    } finally {
      super.tearDown2()
    }
  }

  def createBaseTable(snContext: SnappyContext, tableName: String): Unit = {
    val props = Map(
      "PARTITION_BY" -> "col1")
    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111, "aaa", "hello"),
      Seq(222, "bbb", "halo"),
      Seq(333, "aaa", "hello"),
      Seq(444, "bbb", "halo"),
      Seq(555, "ccc", "halo"),
      Seq(666, "ccc", "halo")
    )

    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    tablesToDrop += tableName
  }

  def testPartitionedSingleColumnTable(): Unit = {
    val tableName = "tabOne"

    val snContext = SnappyContext(sc)
    snContext.setConf(io.snappydata.Property.EnableExperimentalFeatures.configEntry.key, "true")
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    createBaseTable(snContext, tableName)
    ClusterManagerTestBase.logger.info("Creating indexes")
    val indexOne = s"${tableName}_IdxOne"
    val indexTwo = s"${tableName}_IdxTwo"
    val indexThree = s"${tableName}_IdxThree"
//    snContext.sql(s"create index $indexOne on $tableName (COL1)")
//    indexesToDrop += indexOne
    snContext.sql(s"create index $indexTwo on $tableName (COL2, COL3)")
    indexesToDrop += indexTwo
    snContext.sql(s"create index $indexThree on $tableName (COL1, COL3)")
    indexesToDrop += indexThree

    val executeQ = CreateIndexTest.QueryExecutor(snContext)
//    executeQ(s"select * from $tableName where col1 = 111") {
//      CreateIndexTest.validateIndex(Seq(indexOne))(_)
//    }

//    executeQ(s"select * from $tableName where col2 = 'aaa' ") {
//      CreateIndexTest.validateIndex(Seq.empty, tableName)(_)
//    }

    executeQ(s"select * from $tableName where col2 = 'bbb' and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexTwo))(_)
    }

    executeQ(s"select * from $tableName where col1 = 111 and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexThree))(_)
    }
  }

}
