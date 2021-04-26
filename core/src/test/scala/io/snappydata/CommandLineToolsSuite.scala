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

package io.snappydata

import java.io._
import java.sql.{Connection, DriverManager}

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._

import org.apache.spark.sql.collection.Utils

class CommandLineToolsSuite extends SnappyTestRunner {

  override def servers: String = s"$localHostName\n"

  override def clusterSuccessString: String = "Distributed system now has 3 members"

  private val snappyProductDir = System.getenv("SNAPPY_HOME")
  private val snappyNativeTestDir = s"$snappyProductDir/../../../store/native/tests"

  test("exec scala") {
    val conn = getJdbcConnection(1527)
    val stmnt = conn.createStatement()
    val cmdOutput = "exec-scala-output.txt"
    try {
      SnappyShell("quickStartScripts", Seq("connect client 'localhost:1527';",
        s"create table test_app (col1 int not null, col2 int not null) using column options();;",
        s"insert into test_app values (1, 1), (2, 2);",
        s"insert into test_app values (5, 3), (6, 4);",
        "exit;"), cmdOutput)
      val conn = getJdbcConnection(1527)
      val stmnt = conn.createStatement()
      assert(stmnt.execute("select * from test_app"))
      val rs1 = stmnt.getResultSet
      var cnt = 0
      while (rs1.next()) {
        cnt = cnt + 1
        val v1 = rs1.getInt(1)
        rs1.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6)
      }
      assert(cnt === 4)
      // Now execute some exec scala and verify from connection itself
      assert(stmnt.execute("exec scala snappysession.sql(\"insert into test_app values(10, 10), (20, 20)\")"))
      var rs = stmnt.getResultSet
      assert(rs.next())
      assert(stmnt.execute("exec scala snappysession.sql(\"create table test_exec(col1 int not null, col2 int not null)\")"))
      rs = stmnt.getResultSet
      // insert few rows from jdbc connection itself
      stmnt.execute("insert into test_exec values(1000, 1000), (2000, 2000)")
      assert(stmnt.execute("exec scala val df1 = snappysession.table(\"test_app\")\n" +
                                            "// Let's union two dfs and save the union as another table\n" +
                                            "val df2 = snappysession.table(\"test_exec\")\n" +
                                            "val df3 = df1.union(df2)\n" +
                                            "df3.show"))
      stmnt.execute("exec scala options(returnDF 'df33') val df33 = snappysession.table(\"test_exec\")")
      rs = stmnt.getResultSet
      assert(rs.next())
      assert(rs.getMetaData.getColumnCount == 2)
      assert(stmnt.execute("exec scala options(returnDF 'df4') val df4 = snappysession.sql(\"select * from test_exec\")"))
      rs = stmnt.getResultSet
      assert(rs.next())
      assert(rs.getMetaData.getColumnCount == 2)
      assert(stmnt.execute("exec scala snappysession.sql(\"delete from test_exec\")"))
      assert(stmnt.execute("select * from test_exec"))
      rs = stmnt.getResultSet
      assert(!rs.next())
      assert(stmnt.execute("exec scala options(returnDF 'ds2') case class ClassData(a: String, b: Int)\n" +
                           "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n" +
                           "import sqlContext.implicits._\n" +
                           "val ds1 = Seq((\"a\", 1), (\"b\", 2), (\"c\", 3)).toDF(\"a\", \"b\").as[ClassData]\n" +
                           "var rdd = sc.parallelize(Seq((\"a\", 1), (\"b\", 2), (\"c\", 3)), 1)\n" +
                           "val ds2 = rdd.toDF(\"a\", \"b\").as[ClassData]"))
      rs = stmnt.getResultSet
      assert(rs.getMetaData.getColumnCount == 2)
      assert(rs.next())
      var col2val = rs.getInt(2)
      assert((col2val == 1 || col2val == 2 || col2val == 3))
      assert(rs.next())
      col2val = rs.getInt(2)
      assert(col2val == 1 || col2val == 2 || col2val == 3)
      assert(rs.next())
      val col1val = rs.getString(1)
      assert(col1val.equals("a") || col1val.equals("b") || col1val.equals("c"))

      // test temp table
      assert(stmnt.execute("exec scala val x = 5\n" +
                           "println(5)\n" +
                           "println(x)\n" +
                           "snappysession.sql(\"select 1\").show\n" +
                           "val df = snappysession.sql(\"select 10\")\n" +
                           "df.createOrReplaceTempView(\"TEMPTABLE\")"))
      assert(stmnt.execute("select * from TEMPTABLE"))
      rs = stmnt.getResultSet
      assert(rs.getMetaData.getColumnCount == 1)
      assert(rs.next())
      assert(rs.getInt(1) == 10)

      stmnt.execute("exec scala :help")
      rs = stmnt.getResultSet
      assert(rs.getMetaData.getColumnCount == 1)
      assert(rs.next())
      assert(rs.getString(1).contains("Most of the commands can be abbreviated"))
      var cmdCnt = 0
      while(rs.next()) {
        if (rs.getString(1).startsWith(":")) {
          cmdCnt += 1
        }
      }
      assert(cmdCnt >= 15)
      assert(cmdCnt < 20)
    } finally {
      // do cleanup
      stmnt.execute("drop table if exists test_app")
      stmnt.execute("drop table if exists test_exec")
    }
  }

  test("snappy scala") {
    val scala_code1 = Seq(
      "case class TestData(c1: Int, c2: String)",
      "val x = TestData(1, \"1\")",
      "snappysession.sql(\"create table tmptable(c1 int not null, c2 string)\")",
      "snappysession.sql(\"insert into tmptable values(1, \'1\')\")"
    )
    val conn = getJdbcConnection(1527)
    var stmnt = conn.createStatement()
    var cmdOutput = "snappyscala-output.txt"
    try {
      SnappyScalaShell("scala_code", scala_code1, cmdOutput)
      stmnt.execute("create table testtable as select * from tmptable")
      assert(stmnt.execute("select count(*) from testtable"))
      var rs = stmnt.getResultSet
      assert(rs.next())
      assert(rs.getInt(1) == 1)

      val scala_code2 = Seq(
        "class NewClass(i: Int, s: String) extends Serializable {",
        "}",
        "var rdd = sc.parallelize((1 to 10).map(i => new NewClass(i, i.toString)))",
        "rdd.count",
        "println(\"Just before calling rdd.collect\")",
        "rdd.collect.foreach(println(_))"
      )
      stmnt = conn.createStatement()
      cmdOutput = "snappyscala-output1.txt"
      // Failing in suite. Need to check.
      // SnappyScalaShell("scala_code2", scala_code2, cmdOutput)
    } finally {
      stmnt.execute("drop table if exists testtable")
      stmnt.execute("drop table if exists tmptable")
    }
  }

  test("snappy scala run") {
    val scala_code1 = Seq(
      "snappysession.sql(\"create table tmptable(c1 int not null, c2 string)\")",
      "snappysession.sql(\"insert into tmptable values(1, \'1\')\")"
    )
    val scala_code2 = Seq(
      "snappysession.sql(\"create table tmptable2(c1 int not null, c2 string)\")",
      "snappysession.sql(\"insert into tmptable2 values(1, \'1\')\")"
    )
    val scala_code3 = Seq(
      "snappysession.sql(\"create table tmptable3(c1 int not null, c2 string)\")",
      "snappysession.sql(\"insert into tmptable3 values(1, \'1\')\")"
    )
    val conn = getJdbcConnection(1527)
    val stmnt = conn.createStatement()
    val cmdOutput = "snappyscala-output.txt"
    val scalaFileToRun1 = "snappyrun1.scala"
    val scalaFileToRun2 = "snappyrun2.scala"
    val scalaFileToRun3 = "snappyrun3.scala"
    try {
      Seq(scalaFileToRun1, scalaFileToRun2, scalaFileToRun3).zip(
        Seq(scala_code1, scala_code2, scala_code3)).foreach(x => {
        val file = new File(x._1)
        val printWriter = new PrintWriter(file)
        x._2.foreach(s => {
          printWriter.write(s)
          printWriter.write("\n")
        })
        printWriter.close()
      })

      SnappyScalaShell("scala_code", Seq.empty, cmdOutput,
        s"$snappyscalaShell -r $scalaFileToRun1")
      stmnt.execute("create table testtable as select * from tmptable")
      assert(stmnt.execute("select count(*) from testtable"))
      var rs = stmnt.getResultSet
      assert(rs.next())
      assert(rs.getInt(1) == 1)

      // Two files
      SnappyScalaShell("scala_code", Seq.empty, cmdOutput,
        s"$snappyscalaShell -r $scalaFileToRun2,$scalaFileToRun3")
      assert(stmnt.execute("select count(*) from tmptable2"))
      rs = stmnt.getResultSet
      assert(rs.next())
      assert(rs.getInt(1) == 1)
      assert(stmnt.execute("select count(*) from tmptable3"))
      rs = stmnt.getResultSet
      assert(rs.next())
      assert(rs.getInt(1) == 1)

    } finally {
      stmnt.execute("drop table if exists testtable")
      stmnt.execute("drop table if exists tmptable")
    }
  }
      
  // scalastyle:off println
  test("backup restore") {
    val debugWriter = new PrintWriter(s"$snappyHome/CommandLineToolsSuite.debug")
    val backupDir = new File(s"/tmp/backup_dir.${System.currentTimeMillis()}")
    try {
      SnappyShell("quickStartScripts", Seq("connect client 'localhost:1527';",
        s"create table test_app (col1 int not null, col2 int not null) using column options();;",
        s"insert into test_app values (1, 1), (2, 2);",
        s"insert into test_app values (5, 3), (6, 4);",
        s"create table testDD (col1 int not null, col2 int not null)" +
            s" using row options(DISKSTORE 'GFXD-DD-DISKSTORE');",
        s"insert into testDD values (1, 1), (2, 2);",
        s"insert into testDD values (5, 3), (6, 4);",
        "exit;"), commandOutput)

      if (backupDir.exists) {
        assert(backupDir.delete(), s"could not delete $backupDir")
      }
      assert(backupDir.mkdir(), s"could not create backup dir in $snappyHome")

      // online backup command
      val backupcommand = s"$snappyHome/bin/snappy backup $backupDir -locators=localhost:10334"
      val (out, _) = executeCommand(backupcommand)

      if (!out.contains("successful")) {
        throw new Exception(s"Could not take successful backup")
      }
      stopCluster()

      val (_, err1) = executeCommand(s"rm -rf $snappyHome/work")

      if (err1 != null && err1.length > 0) {
        throw new Exception(s"Failed to remove work dir")
      }
      // Find all the restore scripts
      val (out3, _) = executeCommand(s"find $backupDir -name restore.sh")

      val restoreCmnds = out3.split("\n")
      assert(restoreCmnds.length == 2, "expected 2 restore commands")
      assert(restoreCmnds(0).contains("restore.sh") && restoreCmnds(1).contains("restore.sh"))

      executeCommand(restoreCmnds(0))
      executeCommand(restoreCmnds(1))

      startupCluster()

      val conn = getJdbcConnection(1527)
      val stmnt = conn.createStatement()
      assert(stmnt.execute("select * from test_app"))
      val rs1 = stmnt.getResultSet
      var cnt = 0
      while (rs1.next()) {
        cnt = cnt + 1
        val v1 = rs1.getInt(1)
        rs1.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6)
      }
      assert(cnt === 4)

      assert(stmnt.execute("select * from testDD"))
      val rs2 = stmnt.getResultSet
      cnt = 0
      while (rs2.next()) {
        cnt = cnt + 1
        val v1 = rs2.getInt(1)
        rs2.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6)
      }
      assert(cnt === 4)

      // add more data in both the table
      stmnt.execute("insert into test_app values (100, 100), (200, 200)")
      stmnt.execute("insert into testDD values (100, 100), (200, 200)")

      // online incremental backup command
      val incre_backupcommand = s"$snappyHome/bin/snappy" +
          s" backup -baseline=$backupDir $backupDir -locators=localhost:10334"
      val (out4, _) = executeCommand(incre_backupcommand)

      if (!out4.contains("successful")) {
        throw new Exception(s"Could not take successful backup")
      }
      stopCluster()
      val (_, err5) = executeCommand(s"rm -rf $snappyHome/work")
      if (err5 != null && err5.length > 0) {
        throw new Exception(s"Failed to remove work dir")
      }

      debugWriter.println(s"backup dir  = $backupDir")
      // Find the latest two restore scripts
      // val backupDirFile = new File(backupDir)
      val backupDirs = backupDir.listFiles()
      assert(backupDirs.length == 2)

      val dir1 = new File(backupDir.getAbsolutePath, backupDirs(0).getName)
      val dir2 = new File(backupDir.getAbsolutePath, backupDirs(1).getName)

      var lastbackDir: File = null
      if (dir2.lastModified() > dir1.lastModified()) {
        lastbackDir = dir2
      }
      else {
        lastbackDir = dir1
      }

      debugWriter.println(s"lastBackDir abs path = ${lastbackDir.getAbsolutePath}")

      val (out6, _) = executeCommand(s"find ${lastbackDir.getAbsolutePath} -iname restore.sh")

      val restoreCmnds2 = out6.split("\n")
      assert(restoreCmnds2.length == 2, "expected 2 restore commands")

      debugWriter.println(s"after incre restore1  = ${restoreCmnds2(0)}")
      debugWriter.println(s"after incre restore2  = ${restoreCmnds2(1)}")

      assert(restoreCmnds2(0).contains("restore.sh") && restoreCmnds(1).contains("restore.sh"))

      executeCommand(restoreCmnds2(0))
      executeCommand(restoreCmnds2(1))

      startupCluster()

      val conn2 = getJdbcConnection(1527)
      val stmnt2 = conn2.createStatement()
      assert(stmnt2.execute("select * from test_app"))
      val rs11 = stmnt2.getResultSet
      cnt = 0
      while (rs11.next()) {
        cnt = cnt + 1
        val v1 = rs11.getInt(1)
        rs11.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6 || v1 === 100 || v1 === 200)
      }
      assert(cnt === 6)

      assert(stmnt2.execute("select * from testDD"))
      val rs22 = stmnt2.getResultSet
      cnt = 0
      while (rs22.next()) {
        cnt = cnt + 1
        val v1 = rs22.getInt(1)
        rs22.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6 || v1 === 100 || v1 === 200)
      }
      assert(cnt === 6)
    } finally {
      debugWriter.close()
      executeCommand(s"rm -rf $snappyHome/backup*")
    }
  }

  test("-dir option with old locator launch script") {
    try {
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir not specified: $consoleOutput")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443 -client-port=2000 -dir=").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified with a value")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443 -client-port=2000 -dir= " +
          "/does/not/exist").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir does not exist $consoleOutput")

      "mkdir ./SNAP-2631-work-locator".!!
      consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443 -client-port=2000 " +
          "-dir=./SNAP-2631-work-locator").!!
      assert(consoleOutput.contains("running"), s"Locator launch failed: $consoleOutput")

    } finally {
      (snappyProductDir +
          "/sbin/snappy-locator.sh stop -dir=./SNAP-2631-work-locator").!!
      "rm -r ./SNAP-2631-work-locator".!!
    }
  }

  test("-dir option with old server launch script") {
    try {
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir not specified: $consoleOutput")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001 -dir=").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified with a value")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001 -dir= " +
          "/does/not/exist").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir does not exist $consoleOutput")

      "mkdir ./SNAP-2631-work-server".!!
      consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001  " +
          "-dir=./SNAP-2631-work-server").!!
      assert(consoleOutput.contains("running"), s"Server launch failed: $consoleOutput")

    } finally {
      (snappyProductDir +
          "/sbin/snappy-server.sh stop -dir=./SNAP-2631-work-server").!!
      "rm -r ./SNAP-2631-work-server".!!
    }
  }

  test("-dir option with old lead launch script") {
    try {
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002" +
          " -dir=").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified with a value")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002 -dir= " +
          "/does/not/exist").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir does not exist $consoleOutput")

      "mkdir ./SNAP-2631-work-lead".!!

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002 " +
          "-dir=./SNAP-2631-work-lead").!!

      assert(consoleOutput.contains("standby"),
        s"lead launch failed: $consoleOutput")

    } finally {
      (snappyProductDir +
          "/sbin/snappy-lead.sh stop -dir=./SNAP-2631-work-lead").!!
      "rm -r ./SNAP-2631-work-lead".!!
    }
  }

  test("test_run_command") {
    val out = new StringBuilder
    val code = (snappyProductDir +
        "/bin/snappy run file='somefile.sql'").!(ProcessLogger(s => out.append(s)))
    assert(code != 0)
    assert(!out.toString().contains("[-locators=<addresses>]"),
      s"-locators option still displayed in run command's usage text!")
  }

  test("ODBC_FailOverTest_AddServer_BeforeConn") {
    val flag = System.getenv("SNAPPY_NATIVE_ENABLED")
    if (flag != null && flag.equalsIgnoreCase("true")) {
      var scriptPath = s"$snappyNativeTestDir/failoverTest_AddServer_BeforeConn.sh"
      var consoleOutput = s"$scriptPath $snappyProductDir $snappyNativeTestDir".!!
      assert(consoleOutput.contains("Test executed successfully connected to"),
        s"FailOver failed $consoleOutput")
      System.out.println(consoleOutput)
    }
  }

  test("ODBC_FailOverTest_AddServer_AfterConn") {
    val flag = System.getenv("SNAPPY_NATIVE_ENABLED")
    if (flag != null && flag.equalsIgnoreCase("true")) {
      var scriptPath = s"$snappyNativeTestDir/failoverTest_AddServer_AfterConn.sh"
      var consoleOutput = s"$scriptPath $snappyProductDir $snappyNativeTestDir".!!
      assert(consoleOutput.contains("Test executed successfully connected to"),
        s"Failed to create new connection $consoleOutput")
      System.out.println(consoleOutput)
    }
  }

  test("SNAP-3223 Executing query using driver class com.snappydata.jdbc.ClientDriver"){
    // Adding test case here to reuse the infrastructure of already running cluster
    // created by this suite.
    def getJdbcConnectionWithComSnappyData(netPort: Int): Connection = {
      val driver = "com.snappydata.jdbc.ClientDriver"
      Utils.classForName(driver).newInstance
      var url: String = "jdbc:snappydata://localhost:" + netPort + "/"
      DriverManager.getConnection(url)
    }

    val jdbcConn = getJdbcConnectionWithComSnappyData(1527)
    val stmt = jdbcConn.createStatement()
    assert(stmt.execute("show schemas"))
    val result = stmt.getResultSet
    var count = 0
    while(result.next()){
      println(result.getString(1))
      count = count + 1
    }
  }
}
