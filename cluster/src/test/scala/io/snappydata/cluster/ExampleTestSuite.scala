package io.snappydata.cluster

import io.snappydata.SnappyTestRunner

/**
 * Extending SnappyTestRunner. This class tests the old quickstart as well as the examples enumerated in
 * Snappy examples folder
 */
class ExampleTestSuite extends SnappyTestRunner {

  def quickStartJar = s"$snappyHome/examples/jars/quickstart.jar"

  val localLead = "localhost:8090"
  val snappyExamples = "org.apache.spark.examples.snappydata"

  test("old quickstart") {

    SnappyShell("quickStartScripts", Seq("connect client 'localhost:1527';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_column_table.sql';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_row_table.sql';",
      s"run '$snappyHome/quickstart/scripts/create_and_load_sample_table.sql';",
      s"run '$snappyHome/quickstart/scripts/status_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/oltp_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_queries.sql';",
      s"run '$snappyHome/quickstart/scripts/olap_approx_queries.sql';",
      "exit;"))


    Job("io.snappydata.examples.AirlineDataJob", localLead, quickStartJar)

    Job("io.snappydata.examples.CreateAndLoadAirlineDataJob", localLead, quickStartJar)

    SparkSubmit("AirlineDataApp", appClass = "io.snappydata.examples.AirlineDataSparkApp",
      confs = Seq("snappydata.store.locators=localhost:10334", "spark.ui.port=4041"),
      appJar = quickStartJar)

    SparkSubmit("PythonAirlineDataApp", appClass = "",
      confs = Seq("snappydata.store.locators=localhost:10334", "spark.ui.port=4041"),
      appJar = s"$snappyHome/quickstart/python/AirlineDataPythonApp.py")

  }

  test("JDBCExample") {
    RunExample("JDBCExample", "snappydata.JDBCExample")
  }

  test("JDBCWithComplexTypes") {
    RunExample("JDBCWithComplexTypes", "snappydata.JDBCWithComplexTypes")
  }

  test("CollocatedJoinExample") {
    Job(s"$snappyExamples.CollocatedJoinExample",
      localLead, quickStartJar)
  }

  test("CreateColumnTable") {
    Job(s"$snappyExamples.CreateColumnTable",
      localLead, quickStartJar, Seq(s"data_resource_folder=$snappyHome/quickstart/src/main/resources"))
  }

  test("CreatePartitionedRowTable") {
    Job(s"$snappyExamples.CreatePartitionedRowTable",
      localLead, quickStartJar)
  }

  test("CreateReplicatedRowTable") {
    Job(s"$snappyExamples.CreateReplicatedRowTable",
      localLead, quickStartJar)
  }

  test("WorkingWithObjects") {
    Job(s"$snappyExamples.WorkingWithObjects",
      localLead, quickStartJar)
  }

  test("WorkingWithJson") {
    Job(s"$snappyExamples.WorkingWithJson",
      localLead, quickStartJar,
      Seq(s"json_resource_folder=$snappyHome/quickstart/src/main/resources"))
  }

  test("SmartConnectorExample") {
    SnappyShell("smartConnectorSetup", Seq("connect client 'localhost:1527';",
      s"CREATE TABLE SNAPPY_COL_TABLE(r1 Integer, r2 Integer) USING COLUMN;",
      s"insert into SNAPPY_COL_TABLE VALUES(1,1);",
      s"insert into SNAPPY_COL_TABLE VALUES(2,2);",
      "exit;"))

    RunExample("SmartConnectorExample", "snappydata.SmartConnectorExample")
  }

  test("StreamingExample") {
    RunExample("StreamingExample", "snappydata.StreamingExample")

  }

  test("SynopsisDataExample") {
    Job(s"$snappyExamples.SynopsisDataExample",
      localLead, quickStartJar,
      Seq(s"data_resource_folder=$snappyHome/quickstart/data"))
  }

}
