package io.snappydata.dunit.externalstore

import scala.util.Random

import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.SerializableCallable

import org.apache.spark.sql.columntable.ColumnFormatRelation
import org.apache.spark.sql.{Row, SaveMode, SnappyContext}

/**
 * Some basic column table tests.
 */
class ColumnTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {


  def testTableCreation(): Unit = {
    startSparkJob()
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    startSparkJob2()
  }

  def testCreateInsertAndDropOfTableProjectionQuery(): Unit = {
    startSparkJob3()
  }

  def testCreateInsertAndDropOfTableWithPartition(): Unit = {
    startSparkJob4()
  }

  def testCreateInsertAPI(): Unit = {
    startSparkJob5()
  }

  def testCreateAndSingleInsertAPI(): Unit = {
    startSparkJob6()
  }

  def testCreateAndInsertCLOB(): Unit = {
    startSparkJob7()
  }

  // changing the test to such that batches are created
  // and looking for column table stats
  def testSNAP205_InsertLocalBuckets(): Unit = {
    val snc = SnappyContext(sc)

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, data.length).map(
      s => new Data(s(0), s(1), s(2)))

    val dataDF = snc.createDataFrame(rdd)

    // Now column table with partition only can expect
    //local insertion. After Suranjan's change we can expect
    //cached batches to inserted locally if no partitioning is given.
    //TDOD : Merge and validate test after SNAP-105
    val p = Map[String,String]("PARTITION_BY"-> "col1")
    snc.createTable(tableName, "column", dataDF.schema, p)

    // we don't expect any increase in put distribution stats
    val columnTableRegionName = ColumnFormatRelation.
        cachedBatchTableName(tableName).toUpperCase
    val getPRMessageCount = new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        Int.box(Misc.getRegionForTable(columnTableRegionName, true).
            asInstanceOf[PartitionedRegion].getPrStats.getPartitionMessagesSent)
      }
    }
    val counts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    dataDF.write.mode(SaveMode.Append).saveAsTable(tableName)
    val newCounts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    newCounts.zip(counts).foreach { case (c1, c2) =>
      assert(c1 == c2, s"newCount=$c1 count=$c2")
    }

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 1007)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }

  // changing the test to such that batches are created
  // and looking for column table stats
  def testSNAP205_InsertLocalBucketsNonPartitioning(): Unit =   {
    val snc = SnappyContext(sc)

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 3).map(
      s => new Data(s(0), s(1), s(2)))

    val dataDF = snc.createDataFrame(rdd)

    // Now column table with partition only can expect
    // local insertion. After Suranjan's change we can expect
    // cached batches to inserted locally if no partitioning is given.

    // For COLUMNTABLE, there will be distribution for the messages beyond
    // cached batches.

    //TDOD : Merge and validate test after SNAP-105
    val p = Map.empty[String, String]
    snc.createTable(tableName, "column", dataDF.schema, p)
    val columnTableRegionName = ColumnFormatRelation.
        cachedBatchTableName(tableName).toUpperCase
    // we don't expect any increase in put distribution stats
    val getPRMessageCount = new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        Int.box(Misc.getRegionForTable(columnTableRegionName, true).
            asInstanceOf[PartitionedRegion].getPrStats.getPartitionMessagesSent)
      }
    }
    val counts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    dataDF.write.mode(SaveMode.Append).saveAsTable(tableName)
    val newCounts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    newCounts.zip(counts).foreach { case (c1, c2) =>
      assert(c1 == c2, s"newCount=$c1 count=$c2")
    }

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 1008)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }


  def testPreservePartition(): Unit =   {
    val snc = SnappyContext(sc)

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 5).map(
      s => new Data(s(0), s(1), s(2)))

    val dataDF = snc.createDataFrame(rdd)

    val p = Map("PARTITION_BY"->"col1,col3","PRESERVE_PARTITION"->"true","BUCKETS"->"5")
    snc.createTable(tableName, "column", dataDF.schema, p)
    val columnTableRegionName = ColumnFormatRelation.
        cachedBatchTableName(tableName).toUpperCase
    // we don't expect any increase in put distribution stats
    val getPRMessageCount = new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        Int.box(Misc.getRegionForTable(columnTableRegionName, true).
            asInstanceOf[PartitionedRegion].getPrStats.getPartitionMessagesSent)
      }
    }
    val counts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    dataDF.write.mode(SaveMode.Append).saveAsTable(tableName)
    val newCounts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    newCounts.zip(counts).foreach { case (c1, c2) =>
      assert(c1 == c2, s"newCount=$c1 count=$c2")
    }

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 1008)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }

  private val tableName: String = "ColumnTable"
  private val tableNameWithPartition: String = "ColumnTablePartition"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob2(): Unit = {
    val snc = SnappyContext(sc)

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()

    println("ABCD the size is " + r.length)
    assert(r.length == 1005)

    val region = Misc.getRegionForTable(s"APP.${tableName.toUpperCase()}",
      true).asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(ColumnFormatRelation.
        cachedBatchTableName(tableName).toUpperCase(),
      true).asInstanceOf[PartitionedRegion]

    //1005/region.getTotalNumberOfBuckets
    //region.getTotalNumberOfBuckets
    //GemFireCacheImpl.getColumnBatchSize

    println("startSparkJob2 " + region.size())

    println("startSparkJob2 " + shadowRegion.size())

    assert(shadowRegion.size() > 0)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob3(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    snc.sql(s"CREATE TABLE $tableNameWithPartition(Col1 INT ,Col2 INT, Col3 INT)" +
        "USING column " +
        "options " +
        "(" +
        "BUCKETS '1'," +
        "REDUNDANCY '0')")

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableNameWithPartition)

    val result = snc.sql("SELECT Col2 FROM " + tableNameWithPartition)


    val r = result.collect()

    println("ABCDDDD " + r.length)

    assert(r.length == 1005)
    val region = Misc.getRegionForTable(s"APP.${tableNameWithPartition.toUpperCase()}",
      true).asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(
      ColumnFormatRelation.cachedBatchTableName(tableNameWithPartition).toUpperCase(),
      true).asInstanceOf[PartitionedRegion]

    println("startSparkJob3 " + region.size())
    println("startSparkJob3 " + shadowRegion.size())

    assert(shadowRegion.size() > 0)

    snc.dropTable(tableNameWithPartition, ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob4(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    snc.sql(s"CREATE TABLE $tableNameWithPartition" +
        s"(Key1 INT ,Value STRING, other1 STRING, other2 STRING )" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "REDUNDANCY '2')")

    var data = Seq(Seq(1, 2, 3, 4), Seq(7, 8, 9, 4), Seq(9, 2, 3, 4),
      Seq(4, 2, 3, 4), Seq(5, 6, 7, 4))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(4)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => new PartitionData(s(0),
      s(1).toString, s(2).toString, s(3).toString))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableNameWithPartition)

    var result = snc.sql("SELECT Value FROM " + tableNameWithPartition)
    var r = result.collect()
    assert(r.length == 1005)

    result = snc.sql("SELECT other1 FROM " + tableNameWithPartition)
    r = result.collect()
    val colValues = Seq(3 ,9, 3, 3, 7)
    val resultValues = r map{ row =>
      row.getString(0).toInt
    }
    assert(resultValues.length == 1005)
    colValues.foreach(v => assert(resultValues.contains(v)))

    val region = Misc.getRegionForTable(s"APP.${tableNameWithPartition.toUpperCase()}",
      true).asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(
      ColumnFormatRelation.cachedBatchTableName(tableNameWithPartition).toUpperCase(),
      true).asInstanceOf[PartitionedRegion]

    println("startSparkJob4 " + region.size())
    println("startSparkJob4 " + shadowRegion.size())

    assert(shadowRegion.size() > 0)

    snc.dropTable(tableNameWithPartition, ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob5(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

//    snc.sql(s"CREATE TABLE $tableNameWithPartition(Key1 INT ,Value STRING, other1 STRING, other2 STRING )" +
//        "USING column " +
//        "options " +
//        "(" +
//        "PARTITION_BY 'Key1'," +
//        "REDUNDANCY '2')")

    var data = Seq(Seq(1, 2, 3, 4), Seq(7, 8, 9, 4), Seq(9, 2, 3, 4),
      Seq(4, 2, 3, 4), Seq(5, 6, 7, 4))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(4)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 10).map(s => new PartitionDataInt(s(0),
      s(1), s(2), s(3)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableNameWithPartition, "column", dataDF.schema, props)

    data.map { r =>
      snc.insert(tableNameWithPartition, Row.fromSeq(r))
    }

    var result = snc.sql("SELECT Value FROM " + tableNameWithPartition)
    var r = result.collect()

    println("HELLO " + r.length)
    assert(r.length == 1005)

    result = snc.sql("SELECT other1 FROM " + tableNameWithPartition)
    r = result.collect()
    println("HELLO other1 " + r.length)
    val colValues = Seq(3 ,9, 3, 3, 7)
    val resultValues = r map{ row =>
      row.getInt(0)
    }
    assert(resultValues.length == 1005)
    colValues.foreach(v => assert(resultValues.contains(v)))

    val region = Misc.getRegionForTable(s"APP.${tableNameWithPartition.toUpperCase()}",
      true).asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(
      ColumnFormatRelation.cachedBatchTableName(tableNameWithPartition).toUpperCase(),
      true).asInstanceOf[PartitionedRegion]

    println("startSparkJob5 " + region.size())
    println("startSparkJob5 " + shadowRegion.size())

    assert(1005 == (region.size() +
        GemFireCacheImpl.getColumnBatchSize * shadowRegion.size()))
    assert(shadowRegion.size() > 0)

    snc.dropTable(tableNameWithPartition, ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob6(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    snc.sql(s"CREATE TABLE COLUMNTABLE4(Key1 INT ,Value INT)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1'," +
        "REDUNDANCY '2')")

    snc.sql("insert into COLUMNTABLE4 VALUES(1,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(2,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(3,11)")

    snc.sql("insert into COLUMNTABLE4 VALUES(4,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(5,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(6,11)")

    snc.sql("insert into COLUMNTABLE4 VALUES(7,11)")

    var data = Seq(Seq(1, 2), Seq(7, 8), Seq(9, 2), Seq(4, 2), Seq(5, 6))
    1 to 10000 foreach { _ =>
      data = data :+ Seq.fill(2)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 50).map(s => new TData(s(0), s(1)))

    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable("COLUMNTABLE4")

    val result = snc.sql("SELECT Value FROM COLUMNTABLE4")
    val r = result.collect()
    println("total region.size() " + r.length)


    val region = Misc.getRegionForTable("APP.COLUMNTABLE4", true).
        asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(
      ColumnFormatRelation.cachedBatchTableName("COLUMNTABLE4").toUpperCase(),
      true).asInstanceOf[PartitionedRegion]

    println("region.size() " + region.size())
    println("shadowRegion.size()" + shadowRegion.size())

    assert(r.length == 10012)

    println("startSparkJob6 " + region.size())
    println("startSparkJob6 " + shadowRegion.size())

    //assert(0 == region.size())
    assert(shadowRegion.size() > 0)

    snc.dropTable("COLUMNTABLE4", ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob7(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    snc.sql(s"CREATE TABLE COLUMNTABLE4(Key1 INT ,Value INT, other1 VARCHAR(20), other2 STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1, Value '," +
        "BUCKETS '1'," +
        "REDUNDANCY '2')")

    snc.sql("insert into COLUMNTABLE4 VALUES(1,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(2,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(3,11)")

    snc.sql("insert into COLUMNTABLE4 VALUES(4,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(5,11)")
    snc.sql("insert into COLUMNTABLE4 VALUES(6,11)")

    snc.sql("insert into COLUMNTABLE4 VALUES(7,11)")

    var data = Seq(Seq(1, 2,3,4), Seq(7, 8,9,10), Seq(9, 2,3,4), Seq(4, 2,5,7), Seq(5, 6,2,3))
    1 to 10000 foreach { _ =>
      data = data :+ Seq.fill(4)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 50).map(s => new PartitionData(s(0),
      s(1).toString, s(2).toString, s(3).toString))

    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable("COLUMNTABLE4")

    val result = snc.sql("SELECT Value,other1 FROM COLUMNTABLE4")
    val r = result.collect()
    println("total region.size() " + r.length)


    val region = Misc.getRegionForTable("APP.COLUMNTABLE4", true).
        asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable(ColumnFormatRelation.
        cachedBatchTableName("COLUMNTABLE4").toUpperCase(),
        true).asInstanceOf[PartitionedRegion]

    println("region.size() " + region.size())
    println("shadowRegion.size()" + shadowRegion.size())

    assert(r.length == 10012)

    println("startSparkJob6 " + region.size())
    println("startSparkJob6 " + shadowRegion.size())

    //assert(0 == region.size())
    assert(shadowRegion.size() > 0)

    snc.dropTable("COLUMNTABLE4", ifExists = true)
    getLogWriter.info("Successful")
  }
}

case class TData(Key1: Int, Value: Int)

case class Data(col1: Int, col2: Int, col3: Int)

case class PartitionData(col1: Int, Value: String, other1: String, other2: String)

case class PartitionDataInt(col1: Int, Value: Int, other1: Int, other2: Int)
