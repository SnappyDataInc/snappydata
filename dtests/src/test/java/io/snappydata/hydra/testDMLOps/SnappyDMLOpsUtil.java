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


package io.snappydata.hydra.testDMLOps;


import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import hydra.Log;
import hydra.Prms;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.consistency.SnappyConsistencyTest;
import org.apache.commons.lang.ArrayUtils;
import sql.SQLHelper;
import sql.sqlutil.GFXDStructImpl;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class SnappyDMLOpsUtil extends SnappyTest {

  public static boolean hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
  public static boolean testUniqueKeys = TestConfig.tab().booleanAt(SnappySchemaPrms.testUniqueKeys, true);
  public static boolean isHATest = TestConfig.tab().booleanAt(SnappySchemaPrms.isHATest, false);
  public static boolean largeDataSet = TestConfig.tab().booleanAt(SnappySchemaPrms
      .largeDataSet, false);

  protected static hydra.blackboard.SharedLock bbLock;

  protected static SnappyDMLOpsUtil testInstance;
  public static DerbyTestUtils derbyTestUtils = null;

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnappyDMLOpsUtil();
    int dmlTableLength = SnappySchemaPrms.getDMLTables().length;
    ArrayList<Integer> insertCounters = new ArrayList<>();
    ArrayList<Integer> deleteCounters = new ArrayList<>();
    for (int i = 0; i < dmlTableLength; i++) {
      insertCounters.add(1);
      deleteCounters.add(0);
    }
    if (!SnappyDMLOpsBB.getBB().getSharedMap().containsKey("insertCounters"))
      SnappyDMLOpsBB.getBB().getSharedMap().put("insertCounters", insertCounters);
    if (!SnappyDMLOpsBB.getBB().getSharedMap().containsKey("deleteCounters"))
      SnappyDMLOpsBB.getBB().getSharedMap().put("deleteCounters", insertCounters);
    if(derbyTestUtils == null)
      derbyTestUtils = new DerbyTestUtils();
  }

  //ENUM for DML Ops
  public enum DMLOp {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete"),
    PUTINTO("putinto");

    String opType;

    DMLOp(String opType) {
      this.opType = opType;
    }

    public String getOpType() {
      return opType;
    }

    public static DMLOp getOperation(String dmlOp) {
      if (dmlOp.equals(INSERT.getOpType())) {
        return INSERT;
      } else if (dmlOp.equals(UPDATE.getOpType())) {
        return UPDATE;
      } else if (dmlOp.equals(DELETE.getOpType())) {
        return DELETE;
      } else if(dmlOp.equals(PUTINTO.getOpType())) {
        return PUTINTO;
      }else return null;
    }
  }

  //ENUM for Connection Type
  public enum ConnType {
    JDBC("jdbc"),
    SNAPPY("snappy"),
    SMARTCONNECTOR("smartconnector");

    String connType;

    ConnType(String connType) {
      this.connType = connType;
    }

    public String getConnType() {
      return connType;
    }

    public static SnappyDMLOpsUtil.ConnType getOperation(String conn) {
      if (conn.equals(JDBC.getConnType())) {
        return JDBC;
      } else if (conn.equals(SNAPPY.getConnType())) {
        return SNAPPY;
      } else if (conn.equals(SMARTCONNECTOR.getConnType())) {
        return SMARTCONNECTOR;
      } else return null;
    }
  }

  public static void HydraTask_changeMetaDataDirForSpark() {
    String sparkDir = hd.getGemFireHome() + ".." + sep + ".." + sep + ".." + sep + "spark" + sep;
    String filePath = sparkDir + "launcher" + sep + "build-artifacts" + sep + "scala-2.11" +
        sep + "resources" + sep + "test" + sep + "spark-defaults.conf";
    try {
      File file = new File(filePath);
      FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write("derby.system.home=test_db");
      bw.newLine();
      bw.close();
    } catch (IOException ie) {
      throw new TestException("Error while writing to spark file.");
    }
  }

  public static void HydraTask_registerDMLThreads() {
    testInstance.getBBLock();
    ArrayList<Integer> dmlthreads;
    if (SnappyDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlthreads = (ArrayList<Integer>) SnappyDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
    else
      dmlthreads = new ArrayList<>();
    if (!dmlthreads.contains(testInstance.getMyTid())) {
      dmlthreads.add(testInstance.getMyTid());
      SnappyDMLOpsBB.getBB().getSharedMap().put("dmlThreads", dmlthreads);
    }
    testInstance.releaseBBLock();
  }

  public static void HydraTask_registerSelectThreads() {
    testInstance.getBBLock();
    ArrayList<Integer> selectThreads;
    if (SnappyDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads"))
      selectThreads = (ArrayList<Integer>) SnappyDMLOpsBB.getBB().getSharedMap()
          .get("selectThreads");
    else
      selectThreads = new ArrayList<>();
    if (!selectThreads.contains(testInstance.getMyTid())) {
      selectThreads.add(testInstance.getMyTid());
      SnappyDMLOpsBB.getBB().getSharedMap().put("selectThreads", selectThreads);
    }
    testInstance.releaseBBLock();
  }

  public static void HydraTask_saveTableMetaDataToBB() {
    testInstance.saveTableMetaDataToBB();
  }

  public void saveTableMetaDataToBB() {
    try {
      Connection conn = getLocatorConnection();
      String[] tableNames = SnappySchemaPrms.getTableNames();
      for (String table : tableNames) {
        ResultSet rs = conn.createStatement().executeQuery("select * from " + table);
        ResultSetMetaData rsmd = rs.getMetaData();
        int numOfColumns = rsmd.getColumnCount();

        ObjectType[] oTypes = new ObjectType[numOfColumns];
        String[] fieldNames = new String[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
          Class<?> clazz = null;
          clazz = Class.forName(rsmd.getColumnClassName(i + 1));
          oTypes[i] = new ObjectTypeImpl(clazz);
          fieldNames[i] = rsmd.getColumnName(i + 1);
        }
        StructTypeImpl sType = new StructTypeImpl(fieldNames, oTypes);
        SnappyDMLOpsBB.getBB().getSharedMap().put("tableMetaData_" + table.toUpperCase(), sType);
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while saving table metadata to BB. Exception is : ",
          se);
    } catch (ClassNotFoundException ce) {
      throw new TestException("Got exception while saving table metadata to BB.. Exception is : ", ce);
    }
  }

  public static void HydraTask_createSnappySchemas() {
    testInstance.createSnappySchemas();
  }

  protected void createSnappySchemas() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("creating schemas in snappy.");
      createSchemas(conn, false);
      Log.getLogWriter().info("done creating schemas in snappy.");
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while creating schemas.", se);
    }
  }

  protected void createSchemas(Connection conn, boolean isDerby) {
    String[] schemas = SnappySchemaPrms.getCreateSchemas();
    StringBuffer aStr = new StringBuffer("Created schemas \n");
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < schemas.length; i++) {
        s.execute(schemas[i]);
        aStr.append(schemas[i] + "\n");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Y68") || se.getSQLState().equals("42000")) {
        Log.getLogWriter().info("got schema existing exception if multiple threads" +
            " try to create schema, continuing tests");
      } else
        SQLHelper.handleSQLException(se);
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public static void HydraTask_dropSnappySchemas() {
    testInstance.dropSnappySchemas();
  }

  protected void dropSnappySchemas() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("dropping schemas in snappy.");
      dropSchemas(conn, false);
      Log.getLogWriter().info("done dropping schemas in snappy.");
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while dropping schemas.", se);
    }
  }
  protected void dropSchemas(Connection conn, boolean isDerby) {
    String[] schemas = SnappySchemaPrms.getDropSchemas();
    StringBuffer aStr = new StringBuffer("Dropped schemas \n");
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < schemas.length; i++) {
        s.execute(schemas[i]);
        // sleepForMs(5);
        // s.cancel();
        aStr.append(schemas[i] + "\n");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
        throw new TestException("Got exception while dropping schemas in snappy...", se);
    }
    Log.getLogWriter().info(aStr.toString());
  }


  public static synchronized void HydraTask_createSnappyTables() {
    testInstance.createSnappyTables();
  }

  protected void createSnappyTables() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("dropping tables in snappy.");
      dropTables(conn); //drop table before creating it
      Log.getLogWriter().info("done dropping tables in snappy");
      Log.getLogWriter().info("creating tables in snappy.");
      createTables(conn, false);
      Log.getLogWriter().info("done creating tables in snappy.");
      //loadTables(conn);
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while creating tables in snappy.", se);
    }
  }

  protected void createTables(Connection conn, boolean isDerby) {
    //to get create table statements from config file
    String[] createTablesDDL = SnappySchemaPrms.getCreateTablesStatements();
    String[] ddlExtn = SnappySchemaPrms.getSnappyDDLExtn();
    StringBuffer aStr = new StringBuffer("Created tables \n");
    try {
      Statement s = conn.createStatement();
      String createDDL;
      for (int i = 0; i < createTablesDDL.length; i++) {
        if (isDerby)
          createDDL = createTablesDDL[i];
        else
          createDDL = createTablesDDL[i] + ddlExtn[i];
        Log.getLogWriter().info("About to create table : " + createDDL);
        s.execute(createDDL);
        Log.getLogWriter().info("Table created.");
        aStr.append(createDDL + "\n");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create tables\n"
          + TestHelper.getStackTrace(se));
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public String addTIDtoCsv() {
    String[] tableNames = SnappySchemaPrms.getTableNames();
    String[] csvFileNames = SnappySchemaPrms.getCSVFileNames();
    String dataLocation = SnappySchemaPrms.getDataLocations();
    int tid = getMyTid();
    ArrayList<Integer> dmlthreads = null;
    if (SnappyDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlthreads = (ArrayList<Integer>) SnappyDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
    StringBuilder sb = new StringBuilder();

    String destFile = "", currDir, destLoc;
    File destLocDir;
    PrintWriter pw = null;
    try {
      currDir = new java.io.File(".").getCanonicalPath();
      destLoc = currDir + File.separator + ".." + File.separator + "csvFiles";
      destLocDir = new File(destLoc);
      if (!destLocDir.exists()) {
        destLocDir.mkdirs();
      }
    } catch (IOException ie) {
      throw new TestException("Got exception while creating new csv file.", ie);
    }

    for (int i = 0; i < tableNames.length; i++) {
      String tableName = tableNames[i].toUpperCase();
      String csvFilePath = dataLocation + File.separator + csvFileNames[i];
      Log.getLogWriter().info("Adding tid data into " + tableName + " using CSV : " +
          csvFilePath);
      try {
        destFile = destLoc + File.separator + csvFileNames[i];
        File file = new File(destFile);
        if (!file.exists()) {
          pw = new PrintWriter(new FileOutputStream(destFile));
          FileInputStream fs = new FileInputStream(csvFilePath);
          BufferedReader br = new BufferedReader(new InputStreamReader(fs));
          String row = "";
          try {
            boolean headerRow = true;
            while ((row = br.readLine()) != null) {
              if (headerRow) {
                row = br.readLine();
                headerRow = false;
              }

              if (dmlthreads != null)
                tid = dmlthreads.get(new Random().nextInt(dmlthreads.size()));
              row = row + "," + tid;
              sb.append(row);
              int len = sb.length();
              //if (len > 0) sb.setLength(len - 1);
              sb.append('\n');
              if (sb.length() >= 1048576) {
                pw.append(sb);
                pw.flush();
                sb = new StringBuilder();
              }
            }
            if (sb.length() > 0) {
              pw.append(sb);
              pw.flush();
              sb = new StringBuilder();
            }
          } catch (IOException ie) {
            throw new TestException("Got exception while creating new csv file.", ie);
          }
        }
      } catch (FileNotFoundException fe) {
        throw new TestException("Got exception while creating new csv file.", fe);
      }
    }
    return destLoc;
  }

  public static void HydraTask_populateTablesUsingSysProc() {
    testInstance.populateTablesUsingSysProc();
  }

  protected void populateTablesUsingSysProc() {
    String dataLocation = SnappySchemaPrms.getDataLocations();
    if (testUniqueKeys) {
      dataLocation = addTIDtoCsv();
    }
    Log.getLogWriter().info("Loading data in snappy...");
    loadTablesInSnappy(dataLocation);
    Log.getLogWriter().info("Loaded data in snappy.");
    if(hasDerbyServer) {
      Log.getLogWriter().info("Loading data in derby...");
      loadTablesInDerby(dataLocation);
      Log.getLogWriter().info("Loaded data in derby.");
    }
  }

  public void loadTablesInSnappy(String dataLocation) {
    int tid = getMyTid();
    dynamicAppProps.put(tid, "dataFilesLocation=" + dataLocation);
    String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
    executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
        jarPath, SnappyPrms.getUserAppName());
  }

  public void loadTablesInDerby(String dataLocation) {
    String[] tableNames = SnappySchemaPrms.getTableNames();
    String[] csvFileNames = SnappySchemaPrms.getCSVFileNames();
    Connection dConn = derbyTestUtils.getDerbyConnection();
    for (int i = 0; i < tableNames.length; i++) {
      String tableName = tableNames[i].toUpperCase();
      String csvFilePath = dataLocation + File.separator + csvFileNames[i];
      Log.getLogWriter().info("Loading data into " + tableName + " using csv " + csvFilePath);

      String[] table = tableName.split("\\.");
      try {
        PreparedStatement ps = dConn.prepareStatement(
            "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(?,?,?,null,null,null,0)");
        ps.setString(1, table[0]);
        ps.setString(2, table[1]);
        ps.setString(3, csvFilePath);
        //ps.setString(4, ",");
        ps.execute();
        Log.getLogWriter().info("Loaded data into " + tableName);
      } catch (SQLException se) {
        throw new TestException("Exception while loading data to derby table. Exception is " + se
            .getSQLState() + " : " + se.getMessage());
      }
    }
    derbyTestUtils.closeDiscConnection(dConn, true);
  }

  public static void HydraTask_populateTables() {
    String[] tableNames = SnappySchemaPrms.getTableNames();
    if (!SnappySchemaPrms.hasCsvData()) {
      int numInserts = 1000000;
      for (int i = 0; i < tableNames.length; i++) {
        Log.getLogWriter().info("Loading data for " + tableNames[i]);
        testInstance.performInsertUsingBatch(tableNames[i], i, numInserts, true);
      }
    } else testInstance.populateTables();
  }

  protected void populateTables() {
    String[] tableNames = SnappySchemaPrms.getTableNames();
    String[] csvFileNames = SnappySchemaPrms.getCSVFileNames();
    String dataLocation = SnappySchemaPrms.getDataLocations();
    List<String> insertList = Arrays.asList(SnappySchemaPrms.getInsertStmtsForNonDMLTables());
    insertList.addAll(Arrays.asList(SnappySchemaPrms.getInsertStmts()));
    int numDivs = 1;
    boolean loadDataInParts = SnappySchemaPrms.getLoadDataInParts();
    if (loadDataInParts)
      numDivs = SnappySchemaPrms.getNumPartsForDataFiles();
    ExecutorService pool = Executors.newFixedThreadPool((tableNames.length) * numDivs);
    int tid = getMyTid();
    for (int i = 0; i < tableNames.length; i++) {
      String csvFilePath = null;
      String tableName = tableNames[i].toUpperCase();
      String insertStmt = insertList.get(i);
      if (loadDataInParts) {
        for (int j = 1; j <= numDivs; j++) {
          csvFilePath = dataLocation + File.separator + csvFileNames[i] + "_" + j + ".csv";
          pool.execute(new PopulateTableThread(tableName, csvFilePath, insertStmt, tid));
        }
      } else {
        csvFilePath = dataLocation + File.separator + csvFileNames[i];
        pool.execute(new PopulateTableThread(tableName, csvFilePath, insertStmt, tid));
      }
    }
    pool.shutdown();
    try {
      pool.awaitTermination(2400, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Log.getLogWriter().info("Got Exception while waiting for all threads to complete populate" +
          " tasks");
    }
  }

  class PopulateTableThread implements Runnable {
    String tableName = "";
    String csvFilePath = "";
    String insertStmt = "";
    int tid;

    public PopulateTableThread(String tableName, String csvFilePath, String insertStmt, int tid) {
      this.tableName = tableName;
      this.csvFilePath = csvFilePath;
      this.insertStmt = insertStmt;
      this.tid = tid;
    }

    // Keep each entry alive for atleast 5 mins.
    public void run() {
      try {
        Log.getLogWriter().info("Loading data into " + tableName + " using CSV : " +
            csvFilePath);
        Connection conn = getLocatorConnection(), dConn = null;
        if (hasDerbyServer)
          dConn = derbyTestUtils.getDerbyConnection();
        PreparedStatement snappyPS = null, derbyPS = null;
        int batchSize = 65000;
        FileInputStream fs = new FileInputStream(csvFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String row = "";
        int rowCnt = 0;
        ArrayList<Integer> dmlthreads = null;
        boolean headerRow = true;
        if (SnappyDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
          dmlthreads = (ArrayList<Integer>) SnappyDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
        while ((row = br.readLine()) != null) {
          if (headerRow) {
            row = br.readLine();
            headerRow = false;
          }
          if (dmlthreads != null)
            tid = dmlthreads.get(new Random().nextInt(dmlthreads.size()));
          row = row + "," + tid;
          //Log.getLogWriter().info("Row is : " +  row);
          snappyPS = getPreparedStatement(conn, snappyPS, tableName, insertStmt, row);
          if (hasDerbyServer)
            derbyPS = getPreparedStatement(dConn, derbyPS, tableName, insertStmt, row);
          if (rowCnt < batchSize) {
            snappyPS.addBatch();
            if (hasDerbyServer)
              derbyPS.addBatch();
            rowCnt++;
          } else { //reached the batch size,so execute the batch
            snappyPS.addBatch();
            snappyPS.executeBatch();
            snappyPS = null;
            rowCnt = 0;
            if (hasDerbyServer) {
              derbyPS.addBatch();
              derbyPS.executeBatch();
              derbyPS = null;
            }

          }
        }
        if (snappyPS != null)
          snappyPS.executeBatch();
        snappyPS.close();
        conn.close();
        if (hasDerbyServer) {
          if (derbyPS != null)
            derbyPS.executeBatch();
          derbyPS.close();
          derbyTestUtils.closeDiscConnection(dConn, true);
        }
        Log.getLogWriter().info("Done loading data into table " + tableName + " from " + csvFilePath);
      } catch (IOException ie) {
        Log.getLogWriter().info(TestHelper.getStackTrace(ie));
        throw new TestException("Got exception while populating table.", ie);
      } catch (SQLException se) {
        Log.getLogWriter().info(TestHelper.getStackTrace(se));
        throw new TestException("Got exception while populating table.", se);
      } catch (Exception e) {
        Log.getLogWriter().info(TestHelper.getStackTrace(e));
        throw new TestException("Got exception", e);
      }
    }
  }

  protected void dropTables(Connection conn) {
    String sql = null;
    String[] tables = SnappySchemaPrms.getTableNames();
    sql = "drop table if exists ";
    try {
      for (String table : tables) {
        Statement s = conn.createStatement();
        s.execute(sql + table);
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while dropping table.", se);
    }
  }

  /*
   Hydra task to drop table and recreate again with different schema
   */
  public static void HydraTask_changeTableSchema() {
    testInstance.changeTableSchema();
    SnappyBB.getBB().getSharedMap().put("schemaChanged", true);
    HydraTask_saveTableMetaDataToBB();
    HydraTask_populateTables();
  }

  public void changeTableSchema() {
    Connection dConn = derbyTestUtils.getDerbyConnection();
    Connection conn = null;
    try {
      conn = getLocatorConnection();
    } catch (SQLException se) {
      throw new TestException("Got Exception while obtaining connection.");
    }
    dropTables(conn);
    String logFile = "changeTableSchema";
    if(hasDerbyServer) {
      dropTables(dConn);
      recreateTables(dConn, true);
    }
    int tid = getMyTid();
    String[] createTablesDDL = SnappySchemaPrms.getRecreateTablesStatements();
    String[] ddlExtn = SnappySchemaPrms.getSnappyDDLExtn();
    for(int i = 0; i< createTablesDDL.length; i++) {
      String stmt = createTablesDDL[i] + ddlExtn[i];
      dynamicAppProps.put(tid, "stmt=\\\"" + stmt + "\\\",tid=" + tid);
      executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
          jarPath, SnappyPrms.getUserAppName());
    }
  }

  protected void recreateTables(Connection conn, boolean isDerby) {
    //to get create table statements from config file
    String[] createTablesDDL = SnappySchemaPrms.getRecreateTablesStatements();
    String[] ddlExtn = SnappySchemaPrms.getSnappyDDLExtn();
    StringBuffer aStr = new StringBuffer("Created tables \n");
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < createTablesDDL.length; i++) {
        String createDDL = createTablesDDL[i];
        if (!isDerby)
          createDDL += ddlExtn[i];
        Log.getLogWriter().info("About to create table : " + createDDL);
        s.execute(createDDL);
        Log.getLogWriter().info("Table created.");
        aStr.append(createDDL + "\n");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create tables\n"
          + TestHelper.getStackTrace(se));
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public static void HydraTask_performDMLOpsInAppAfterSchemaChange() {
    for (int i = 0; i < 10; i++)
      testInstance.performDMLOp(ConnType.SMARTCONNECTOR);
  }

  public static void HydraTask_restartSnappyCluster() {
    HydraTask_stopSnappyCluster();
    HydraTask_startSnappyCluster();
  }

  /*
   Hydra task to perform DMLOps which can be insert, update, delete
   */
  public static void HydraTask_performDMLOp() {
    testInstance.performDMLOp(ConnType.JDBC);
  }

  public void performDMLOp(ConnType connType) {
    //perform DML operation which can be insert, update, delete.
    String operation = SnappySchemaPrms.getDMLOperations();
    switch (SnappyDMLOpsUtil.DMLOp.getOperation(operation)) {
      case INSERT:
        Log.getLogWriter().info("Performing insert operation.");
        if(!SnappySchemaPrms.hasCsvData()) {
          String[] dmlTable = SnappySchemaPrms.getDMLTables();
          int rand = new Random().nextInt(dmlTable.length);
          String tableName = dmlTable[rand].toUpperCase();
          int numInserts = 100;
          performInsertUsingBatch(tableName, rand, numInserts, true);
        }
        else
          performInsert();
        break;
      case UPDATE:
        Log.getLogWriter().info("Performing update operation.");
        if (connType.equals(ConnType.JDBC))
          performUpdate();
        else
          performUpdateInSnappy(connType);
        break;
      case DELETE:
        Log.getLogWriter().info("Performing delete operation.");
        if (connType.equals(ConnType.JDBC))
          performDelete();
        else
          performDeleteInSnappy(connType);
        break;
      /*case PUTINTO:
        Log.getLogWriter().info("Performing putinto operation...");
        if (connType.equals(ConnType.JDBC))
          performPutInto();
        else
          performPutIntoInSnappy(connType);
          */
      default:
        Log.getLogWriter().info("Invalid operation. ");
        throw new TestException("Invalid operation type.");
    }
  }

  public String addTidToQuery(String sql, int tid){
    if (sql.toUpperCase().contains("WHERE"))
      sql = sql + " AND tid=" + tid;
    else sql = sql + " WHERE tid=" + tid;
    return sql;
  }

  public void performInsertUsingBatch(String tableName, int index, int batchSize,
      boolean isPopulate) {
    Connection conn;
    String stmt;
    if (SnappyBB.getBB().getSharedMap().containsKey("schemaChanged") && ((boolean) SnappyBB.getBB().getSharedMap().get("schemaChanged")))
      stmt = SnappySchemaPrms.getInsertStmtAfterReCreateTable().get(index);
    else {
      if(isPopulate) {
        String[] insertStmts = SnappySchemaPrms.getInsertStmtsForNonDMLTables();
        if (insertStmts != null) stmt = insertStmts[index];
        else stmt = SnappySchemaPrms.getInsertStmts()[index];
      }
      else stmt = SnappySchemaPrms.getInsertStmts()[index];
    }
    try {
      conn = getLocatorConnection();
      performInsertUsingBatch(conn, tableName, stmt, index, batchSize, getMyTid(), isPopulate);
      conn.close();
    } catch (SQLException se) {
      throw new TestException("Got exception while getting connection", se);
    }
  }

  public int getInitialCounter(int index, int batchSize){
    getBBLock();
    List<Integer> counters = (List<Integer>)SnappyDMLOpsBB.getBB().getSharedMap().get("insertCounters");
    int initCounter = counters.get(index);
    counters.set(index, initCounter + batchSize);
    SnappyDMLOpsBB.getBB().getSharedMap().put("insertCounters", counters);
    releaseBBLock();
    return initCounter;
  }

  public int getDeleteCounter(int index, int batchSize){
    getBBLock();
    List<Integer> counters = (List<Integer>)SnappyDMLOpsBB.getBB().getSharedMap().get("deleteCounters");
    int delCounter = counters.get(index) + batchSize;
    counters.set(index, delCounter);
    SnappyDMLOpsBB.getBB().getSharedMap().put("deleteCounters", counters);
    releaseBBLock();
    return delCounter;
  }

  public void performInsertUsingBatch(Connection conn, String tableName,
      String stmt, int index, int batchSize, int tid, boolean isPopulate){
    int initCounter = getInitialCounter(index, batchSize);
    String uniqueKey = SnappySchemaPrms.getUniqueColumnName()[index];
    Connection dConn = null;
    Log.getLogWriter().info("Inserting using " + stmt + " with batchSize " + batchSize + " in " +
        "table " + tableName + " from " + initCounter);
    PreparedStatement snappyPS = null, derbyPS = null;
    try {
      snappyPS = conn.prepareStatement(stmt);
      if (hasDerbyServer) {
        String derbyStmt = stmt;
        dConn = derbyTestUtils.getDerbyConnection();
        if(derbyStmt.startsWith("put ")) derbyStmt = derbyStmt.replace("put ", "insert ");
        derbyPS = dConn.prepareStatement(derbyStmt);
      }
    } catch (SQLException se) {
      Log.getLogWriter().info("Got exception while getting derby connection or preparing " +
          "statement");
      throw new TestException("Got exception while getting connection", se);
    }

    ArrayList<Integer> dmlthreads = null;
    if (SnappyDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlthreads = (ArrayList<Integer>) SnappyDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
    
    StructTypeImpl sType = (StructTypeImpl)SnappyDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName.toUpperCase());
    ObjectType[] oTypes = sType.getFieldTypes();
    String[] fieldNames = sType.getFieldNames();

    int batchCnt = 0;
    try {
      String value;
      for (int j = initCounter ; j < (batchSize + initCounter); j++) {
        int replaceQuestion = 1;
        for (int i = 0; i < oTypes.length; i++) {
          if (fieldNames[i].equalsIgnoreCase(uniqueKey)) value = j + "";
          else value = "1";
          String clazz = oTypes[i].getSimpleClassName();
          switch (clazz) {
            case "Date":
              Date dt = new Date(System.currentTimeMillis());
              snappyPS.setDate(replaceQuestion, dt);
              if (hasDerbyServer) derbyPS.setDate(replaceQuestion, dt);
              break;
            case "String":
              snappyPS.setString(replaceQuestion, fieldNames[i] + j);
              if (hasDerbyServer) derbyPS.setString(replaceQuestion, fieldNames[i] + j);
              break;
            case "Timestamp":
              Timestamp ts = new Timestamp(System.currentTimeMillis());
              snappyPS.setTimestamp(replaceQuestion, ts);
              if (hasDerbyServer) derbyPS.setTimestamp(replaceQuestion, ts);
              break;
            case "Integer":
              if (fieldNames[i].equalsIgnoreCase("tid")){
                if(isPopulate) value = dmlthreads.get(random.nextInt(dmlthreads.size())) + "";
                    else value = tid + "";
              }
              snappyPS.setInt(replaceQuestion, Integer.parseInt(value));
              if (hasDerbyServer) derbyPS.setInt(replaceQuestion, Integer.parseInt(value));
              break;
            case "Double":
              snappyPS.setDouble(replaceQuestion, Double.parseDouble(value));
              if (hasDerbyServer) derbyPS.setDouble(replaceQuestion, Double.parseDouble(value));
              break;
            case "Long":
              snappyPS.setLong(replaceQuestion, Long.parseLong(value));
              if (hasDerbyServer) derbyPS.setLong(replaceQuestion, Long.parseLong(value));
              break;
            case "BigDecimal":
              snappyPS.setBigDecimal(replaceQuestion, BigDecimal.valueOf(Double.parseDouble(value)));
              if (hasDerbyServer) derbyPS.setBigDecimal(replaceQuestion,
                  BigDecimal.valueOf(Double.parseDouble(value)));
              break;
            default:
              Log.getLogWriter().info("Object class type not found.");
              throw new TestException("Object class type not found.");
          }
          replaceQuestion += 1;
        }
        snappyPS.addBatch();
        if (hasDerbyServer) derbyPS.addBatch();
        batchCnt++;
        if (batchCnt == 65000) {
          Log.getLogWriter().info("Executing batch statement for insert.");
          snappyPS.executeBatch();
          if (hasDerbyServer) derbyPS.executeBatch();
          Log.getLogWriter().info("Executed batch statement for insert.");
          batchCnt = 0;
        }
      }
      Log.getLogWriter().info("Executing the remaining batch...");
      if(!isPopulate)  SnappyConsistencyTest.waitForBarrier(tid + "", 2);
      snappyPS.executeBatch();
      snappyPS.close();
      Log.getLogWriter().info("Executed the batch.");
      if(!isPopulate)  SnappyConsistencyTest.waitForBarrier(tid + "", 2);
      if (hasDerbyServer) {
        derbyPS.executeBatch();
        derbyPS.close();
        derbyTestUtils.closeDiscConnection(dConn, true);
      }
    } catch (Exception se) {
      Log.getLogWriter().info("Got exception while creating prepared statement" + se.getMessage());
      throw new TestException("Exception while creating PreparedStatement.", se);
    }
  }

  public void performInsert() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      String[] dmlTable = SnappySchemaPrms.getDMLTables();
      int rand = new Random().nextInt(dmlTable.length);
      String tableName = dmlTable[rand].toUpperCase();
      String row = getRowFromCSV(tableName, rand);
      if (row == null)
        return;
      if (testUniqueKeys)
        row = row + "," + getMyTid();

      //Log.getLogWriter().info("Selected row is : " + row);
      PreparedStatement snappyPS, derbyPS = null;
      String insertStmt = (SnappySchemaPrms.getInsertStmts())[rand];
      snappyPS = getPreparedStatement(conn, null, tableName, insertStmt, row);
      Log.getLogWriter().info("Inserting in snappy : " + insertStmt + " with " +
          "values(" + row + ")");
      int rowCount = snappyPS.executeUpdate();
      Log.getLogWriter().info("Inserted " + rowCount + " row in snappy.");
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        derbyPS = getPreparedStatement(dConn, null, tableName, insertStmt, row);
        Log.getLogWriter().info("Inserting in derby : " + insertStmt + " with " +
            "values(" + row + ")");
        int derbyRowCount = derbyPS.executeUpdate();
        Log.getLogWriter().info("Inserted " + derbyRowCount + " row in derby.");
        if (rowCount != derbyRowCount)
          Log.getLogWriter().info("Insert statement failed to insert same rows in derby and " +
              "snappy. Derby inserted " + derbyRowCount + " and snappy inserted " + rowCount + ".");
        derbyTestUtils.closeDiscConnection(dConn, true);
      }
      closeConnection(conn);

    } catch (SQLException se) {
      if (se.getMessage().contains("23505")) {
        Log.getLogWriter().info("Got expected Exception, continuing test: " + se.getMessage());
        return;
      } else throw new TestException("Got exception while performing insert operation.", se);
    }
  }

  public void getAndExecuteSelect(Connection conn, String stmt, boolean isDerby) {
    String selectString = stmt.toUpperCase().substring(stmt.indexOf("(SELECT") + 1, stmt.indexOf
        (") AND"));
    try {
      Log.getLogWriter().info("Executing " + selectString + " on snappy.");
      ResultSet rs = conn.createStatement().executeQuery(selectString);
      StructTypeImpl rsSti = ResultSetHelper.getStructType(rs);
      List<Struct> rsList = ResultSetHelper.asList(rs, rsSti, isDerby);
      rs.close();
      Log.getLogWriter().info("Result from sub-select query is :" + listToString(rsList));
    } catch (SQLException se) {
      Log.getLogWriter().info("Statement execution failed" + se.getStackTrace().toString());
    }
  }

  public void performUpdate() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null; //get the derby connection here
      String updateStmt[] = SnappySchemaPrms.getUpdateStmts();
      int numRows = 0;
      int rand = new Random().nextInt(updateStmt.length);
      String stmt = updateStmt[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid", "" + tid);
      if (testUniqueKeys)
        stmt = addTidToQuery(stmt, tid);
      Log.getLogWriter().info("Executing " + stmt + " on snappy.");
      if (stmt.toUpperCase().contains("SELECT"))
        getAndExecuteSelect(conn, stmt, false);
      numRows = conn.createStatement().executeUpdate(stmt);
      Log.getLogWriter().info("Updated " + numRows + " rows in snappy.");
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        if (stmt.toUpperCase().contains("SELECT"))
          getAndExecuteSelect(dConn, stmt, true);
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Updated " + derbyRows + " rows in derby.");
        if (numRows != derbyRows) {
          String errMsg = "Update statement failed to update same rows in derby and " +
              "snappy. Derby updated " + derbyRows + " and snappy updated " + numRows + ".";
          Log.getLogWriter().info(errMsg);
          //throw new TestException(errMsg);
        }
        derbyTestUtils.closeDiscConnection(dConn, true);
        String tableName = SnappySchemaPrms.getUpdateTables()[rand];
        String selectQuery = SnappySchemaPrms.getAfterUpdateSelectStmts()[rand];
        String orderByClause = "";
        //String[] dmlTables = SnappySchemaPrms.getDMLTables();
        //orderByClause = (SnappySchemaPrms.getOrderByClause())[Arrays.asList(dmlTables)
        //    .indexOf(tableName)];
        String message = verifyResultsForTable(selectQuery, tableName, orderByClause, true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after update on table " + tableName + "." +
              message);
        }
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing update operation.", se);
    }
  }

  public void performDelete() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null; //get the derby connection here
      String deleteStmt[] = SnappySchemaPrms.getDeleteStmts();
      int numRows = 0;
      int rand = new Random().nextInt(deleteStmt.length);
      String stmt = deleteStmt[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid", "" + tid);
      if (testUniqueKeys) stmt = addTidToQuery(stmt, tid);
      Log.getLogWriter().info("Executing " + stmt + " on snappy.");
      if (stmt.toUpperCase().contains("SELECT"))
        getAndExecuteSelect(conn, stmt, false);
      numRows = conn.createStatement().executeUpdate(stmt);
      Log.getLogWriter().info("Deleted " + numRows + " rows in snappy.");
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        if (stmt.toUpperCase().contains("SELECT"))
          getAndExecuteSelect(dConn, stmt, true);
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Deleted " + derbyRows + " rows in derby.");
        if (numRows != derbyRows) {
          String errMsg = "Delete statement failed to delete same rows in derby and " +
              "snappy. Derby deleted " + derbyRows + " and snappy deleted " + numRows + ".";
          Log.getLogWriter().info(errMsg);
          //throw new TestException(errMsg);
        }
        derbyTestUtils.closeDiscConnection(dConn, true);
        String tableName = SnappySchemaPrms.getDeleteTables()[rand];
        String selectQuery = SnappySchemaPrms.getAfterDeleteSelectStmts()[rand];
        String orderByClause = "";
//        String[] dmlTables = SnappySchemaPrms.getDMLTables();
//        orderByClause = SnappySchemaPrms.getOrderByClause()[Arrays.asList(dmlTables)
//            .indexOf(tableName)];
        String message = verifyResultsForTable(selectQuery, tableName, orderByClause, true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after executing delete on table " + tableName + "." + message);
        }
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing delete operation.", se);
    }
  }

  /*
  Hydra task to execute select queries
  */
  public static void HydraTask_executeQueries() {
    testInstance.executeQuery();
  }

  public void executeQuery() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      String selectStmt[] = SnappySchemaPrms.getSelectStmts();
      ResultSet snappyRS, derbyRS = null;
      int rand = new Random().nextInt(selectStmt.length);
      String query = selectStmt[rand];
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      try {
        snappyRS = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
      } catch (SQLException se) {
        if (se.getSQLState().equals("21000") || se.getSQLState().equals("0A000")) {
          //retry select query with routing
          Log.getLogWriter().info("Got exception while executing select query, retrying with " +
              "executionEngine as spark.");
          String query1 = query + " --GEMFIREXD-PROPERTIES executionEngine=Spark";
          snappyRS = conn.createStatement().executeQuery(query1);
          Log.getLogWriter().info("Executed query on snappy.");
        } else throw new SQLException(se);
      }
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);

      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + query + " on derby.");
        derbyRS = dConn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
        StructTypeImpl derbySti = ResultSetHelper.getStructType(derbyRS);
        List<Struct> derbyList = ResultSetHelper.asList(derbyRS, derbySti, true);
        compareResultSets(derbyList, snappyList);
        derbyRS.close();
        derbyTestUtils.closeDiscConnection(dConn, true);
      } else {
        int numRows = 0;
        while (snappyRS.next()) numRows++;
        Log.getLogWriter().info("Snappy returned " + numRows + " rows.");
      }
      snappyRS.close();
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  public static void compareResultSets(List<Struct> derbyResultSet,
                                       List<Struct> snappyResultSet) {
    compareResultSets(derbyResultSet, snappyResultSet, "derby", "snappy");
  }

  public static void compareResultSets(List<Struct> firstResultSet,
                                       List<Struct> secondResultSet, String first, String second) {
    Log.getLogWriter().info("size of resultSet from " + first + " is " + firstResultSet.size());
    //Log.getLogWriter().info("Result from " + first + " is :" + listToString(firstResultSet));
    Log.getLogWriter().info("size of resultSet from " + second + " is " + secondResultSet.size());
    //Log.getLogWriter().info("Result from " + second + " is :" + listToString(secondResultSet));

    List<Struct> secondResultSetCopy = new ArrayList<Struct>(secondResultSet);

    StringBuffer aStr = new StringBuffer();
    for (int i = 0; i < firstResultSet.size(); i++) {
      secondResultSetCopy.remove(firstResultSet.get(i));
    }
    List<Struct> unexpected = secondResultSetCopy;
    List<Struct> missing = null;

    if (firstResultSet.size() != secondResultSet.size() || unexpected.size() > 0) {
      List<Struct> firstResultSetCopy = new ArrayList<Struct>(firstResultSet);
      for (int i = 0; i < secondResultSet.size(); i++) {
        firstResultSetCopy.remove(secondResultSet.get(i));
      }
      missing = firstResultSetCopy;

      if (missing.size() > 0) {
        aStr.append("the following " + missing.size() + " elements were missing from "
            + second + " resultSet: " + listToString(missing));
      }
    }
    if (unexpected.size() > 0) {
      aStr.append("the following " + unexpected.size() + " unexpected elements in " +
          second + " resultSet: " + listToString(unexpected));
    }

    if (aStr.length() != 0) {
      Log.getLogWriter().info("ResultSet from " +
          first + " is " + listToString(firstResultSet));
      Log.getLogWriter().info("ResultSet from " +
          second + " is " + listToString(secondResultSet));
      Log.getLogWriter().info("ResultSet difference is " + aStr.toString());
      throw new TestException(aStr.toString());
    }
    if (firstResultSet.size() == secondResultSet.size()) {
      Log.getLogWriter().info("verified that results are correct");
    } else if (firstResultSet.size() < secondResultSet.size()) {
      throw new TestException("There are more data in " + second + " ResultSet");
    } else {
      throw new TestException("There are fewer data in " + second + " ResultSet");
    }
  }

  public static String listToString(List<Struct> aList) {
    if (aList == null) {
      throw new TestException("test issue, need to check in the test and not pass in null list here");
    }
    StringBuffer aStr = new StringBuffer();
    aStr.append("The size of list is " + (aList == null ? "0" : aList.size()) + "\n");

    for (int i = 0; i < aList.size(); i++) {
      Object aStruct = aList.get(i);
      if (aStruct instanceof com.gemstone.gemfire.cache.query.Struct) {
        GFXDStructImpl si = (GFXDStructImpl) (aStruct);
        aStr.append(si.toString());
      }
      aStr.append("\n");
    }
    return aStr.toString();
  }

  public static void listToFile(List<Struct> aList, String fileName) {
    if (aList == null) {
      throw new TestException("test issue, need to check in the test and not pass in null list here");
    }
    File rsFile = new File(fileName);
    StringBuilder sb = new StringBuilder();
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new FileOutputStream(rsFile));
    } catch (IOException ie) {
      throw new TestException("Got exception while saving resultset to file", ie);
    }
    for (int i = 0; i < aList.size(); i++) {
      Object aStruct = aList.get(i);
      if (aStruct instanceof com.gemstone.gemfire.cache.query.Struct) {
        GFXDStructImpl si = (GFXDStructImpl) (aStruct);
        sb.append(si.toString());
      }
      sb.append("\n");
      if (sb.length() >= 1048576) {
        pw.append(sb);
        pw.flush();
        sb = new StringBuilder();
      }
    }
    if (sb.length() > 0) {
      pw.append(sb);
      pw.flush();
      sb = new StringBuilder();
    }
  }

  /*
 *   Verify results in the begining of the test
 */
  public static void HydraTask_verifyResultsInitTask() {
    String selectQuery = "select CAST(count(*) as integer) as numRows from  ";
    testInstance.verifyResults(selectQuery);
  }

  /*
   *   Verify results at the end of the test
   */
  public static void HydraTask_verifyResults() {
    String selectQuery = "select * from ";
    testInstance.verifyResults(selectQuery);
  }

  public void verifyResults(String query) {
    String[] tables = SnappySchemaPrms.getTableNames();
    StringBuffer mismatchString = new StringBuffer();
    for (String table : tables) {
      mismatchString.append(verifyResultsForTable(query + table, table, "", false,true));
    }
    if (mismatchString.length() > 0) {
      Log.getLogWriter().info(mismatchString.toString());
      if(!query.contains("select * ")) {
        mismatchString.setLength(0);
        query = "select * from ";
        for (String table : tables) {
          mismatchString.append(verifyResultsForTable(query + table, table, "", false, true));
        }
      }
      throw new TestException(mismatchString.toString());
    }
  }

  public String verifyResultsForTable(String selectStmt, String table, String orderByClause,
      boolean useTid) {
    return verifyResultsForTable(selectStmt, table, "", useTid,false);
  }

  public String verifyResultsForTable(String selectStmt, String table, String orderByClause,
      boolean useTid,boolean isCloseTask) {
    StringBuffer mismatchString = new StringBuffer();
    Connection conn, dConn;
    try {
      conn = getLocatorConnection();
      dConn = derbyTestUtils.getDerbyConnection();
      if (useTid) selectStmt = addTidToQuery(selectStmt, getMyTid());
      if (orderByClause.length() > 0)
        selectStmt = selectStmt + " " + orderByClause;
      Log.getLogWriter().info("Verifying results for " + table + " using " + selectStmt);
      ResultSet snappyRS = conn.createStatement().executeQuery(selectStmt);
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);
      snappyRS.close();

      ResultSet derbyRS = dConn.createStatement().executeQuery(selectStmt);
      StructTypeImpl derbySti = ResultSetHelper.getStructType(derbyRS);
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, derbySti, true);
      derbyRS.close();

      closeConnection(conn);
      derbyTestUtils.closeDiscConnection(dConn, true);

      if (!largeDataSet) {
        try {
          compareResultSets(derbyList, snappyList);
        } catch (TestException te) {
          mismatchString = mismatchString.append("Result mismatch in " + table + " :\n");
          mismatchString = mismatchString.append(te.getMessage()).append("\n");
        }
      } else {
        Log.getLogWriter().info("size of resultSet from snappy is " + snappyList.size());
        Log.getLogWriter().info("size of resultSet from derby is " + derbyList.size());

        if (snappyList.size() != derbyList.size())
          mismatchString.append(" Result mismatch observed for table " + table + ". Number of " +
              "rows in snappy is " + snappyList.size() + " and derby is " + derbyList.size() + ".");

        String queryResultDirPath = "";

        try {
          queryResultDirPath = new File(".").getCanonicalPath() + File.separator + "queryResults";
        } catch (IOException ie) {
          throw new TestException("Got Exception while creating directory for queryResults", ie);
        }
        String snappyRSFileName = queryResultDirPath + File.separator + "snappyRS_" +
            ((isCloseTask)? table +"_":"") + getMyTid() + ".out";
        String derbyRSFileName = queryResultDirPath + File.separator + "derbyRS_" +
            ((isCloseTask)? table +"_":"") + getMyTid() + ".out";
        File queryResultDir = new File(queryResultDirPath);
        if (!queryResultDir.exists())
          queryResultDir.mkdirs();
        listToFile(snappyList, snappyRSFileName);
        listToFile(derbyList, derbyRSFileName);
        snappyList.clear();
        derbyList.clear();
        mismatchString.append(compareFiles(queryResultDirPath, snappyRSFileName, derbyRSFileName,
         isCloseTask,table));
        if (mismatchString.length() > 0)
          Log.getLogWriter().info("Got resultset mismtach for " + table  + ".For query results " +
              "please check : \n " + snappyRSFileName + " and \n " + derbyRSFileName);
        Log.getLogWriter().info(mismatchString.toString());
      }
    } catch (SQLException se) {
      throw new TestException("Got SQLException while verifying the table data.", se);
    }
    return mismatchString.toString();
  }

  public String compareFiles(String dir, String snappyFileName, String derbyFileName, boolean
      isCloseTask,String table) {
    StringBuilder aStr = new StringBuilder();
    ProcessBuilder pb = null;
    int tid = getMyTid();
    String command;
    String missingFileName = dir + File.separator + "missing_" +
    ((isCloseTask)? table +"_":"") + tid + ".txt";
    String upexpectedFileName = dir + File.separator + "unexpected_" +
    ((isCloseTask)? table +"_":"") + tid + ".txt";
    try {
      PrintWriter writer = new PrintWriter(missingFileName);
      writer.print("");
      writer.close();
      writer = new PrintWriter(upexpectedFileName);
      writer.print("");
      writer.close();
    } catch (FileNotFoundException fe) {
      throw new TestException("Log exception while overwirting the result mismatch files", fe);
    }
    File unexpectedResultsFile = new File(upexpectedFileName);
    File missingResultsFile = new File(missingFileName);

    command = "grep -v -F -x -f " + derbyFileName + " " + snappyFileName;
    pb = new ProcessBuilder("/bin/bash", "-c", command);
    Log.getLogWriter().info("Executing command : " + command);
    //get the unexpected rows in snappy
    executeProcess(pb, unexpectedResultsFile);

    command = "grep -v -F -x -f " + snappyFileName + " " + derbyFileName;
    pb = new ProcessBuilder("/bin/bash", "-c", command);
    Log.getLogWriter().info("Executing command : " + command);
    //get the missing rows in snappy
    executeProcess(pb, missingResultsFile);

    BufferedReader unexpectedRsReader, missingRsReader;
    try {
      unexpectedRsReader = new BufferedReader(new FileReader(unexpectedResultsFile));
      missingRsReader = new BufferedReader(new FileReader(missingResultsFile));
    } catch (FileNotFoundException fe) {
      throw new TestException("Could not find file to compare results.", fe);
    }
    String line;
    List<String> unexpected = new ArrayList<>(), missing = new ArrayList<>();
    try {
      while ((line = unexpectedRsReader.readLine()) != null)
        unexpected.add("\n  " + line);
      while ((line = missingRsReader.readLine()) != null)
        missing.add("\n  " + line);
      unexpectedRsReader.close();
      missingRsReader.close();
    } catch (IOException ie) {
      throw new TestException("Got exception while reading resultset files", ie);
    }
    if (missing.size() > 0) {
      if(missing.size() <20) {
        aStr.append("\nThe following " + missing.size() + " rows are missing from snappy resultset:");
        aStr.append(missing.toString());
      } else
        aStr.append("There are " + missing.size() + " rows missing in snappy for " + table + ". " +
          "Please check " + missingFileName);
      aStr.append("\n");
    }
    if (unexpected.size() > 0) {
      if(unexpected.size() <20) {
        aStr.append("\nThe following " + unexpected.size() + " rows from snappy resultset are unexpected: ");
        aStr.append(unexpected.toString());
      } else
        aStr.append("There are " + unexpected.size() + " rows unexpected in snappy for " +
          table + ". Please check " + upexpectedFileName);
      aStr.append("\n");
    }
   /* if(missing.size() == 0 && unexpected.size() == 0)
      Log.getLogWriter().info("Verified that the resulst are correct.");
*/    return aStr.toString();
  }

  public PreparedStatement getPreparedStatement(Connection conn, PreparedStatement ps, String
      tableName, String stmt, String row) {
    String columnString = stmt.substring(stmt.indexOf("(") + 1, stmt.indexOf(")"));
    ArrayList<String> columnList = new ArrayList<String>
        (Arrays.asList(columnString.split(",")));
    //Log.getLogWriter().info("columnList from insert is" + columnList.toString());
    ArrayList<String> columnValues = new ArrayList<>();
    if (row.contains("\"")) {
      String str3 = row;
      while (str3.contains("\"")) {
        int beginIndex = row.indexOf("\"") + 1;
        int endIndex = row.indexOf("\"", beginIndex);
        String str1 = row.substring(0, beginIndex - 2);
        String str2 = row.substring(beginIndex, endIndex);
        if (endIndex < str3.length())
          str3 = row.substring(endIndex + 2, str3.length());
        else str3 = "";
        columnValues.addAll(new ArrayList<>(Arrays.asList(str1.split(","))));
        columnValues.add(str2);
      }
      if (str3.length() > 0) {
        columnValues.addAll(new ArrayList<>(Arrays.asList(str3.split(","))));
      }
    } else {
      columnValues = new ArrayList<>(Arrays.asList(row.split(",")));
    }
    try {
      if (ps == null)
        ps = conn.prepareStatement(stmt);
      StructTypeImpl sType = (StructTypeImpl) SnappyDMLOpsBB.getBB().getSharedMap().get
          ("tableMetaData_" + tableName);
      ObjectType[] oTypes = sType.getFieldTypes();
      String[] fieldNames = sType.getFieldNames();
      int replaceQuestion = 1;
      for (int i = 0; i < oTypes.length; i++) {
        String clazz = oTypes[i].getSimpleClassName();
        String columnValue = columnValues.get(i);
//      Log.getLogWriter().info
//          ("Column : " + fieldNames[i] + " with value : " + columnValue + " and " +
//              "clazz :" + clazz + ";column from insert stmt is : " + columnList.get(i));
        if (!columnList.get(i).equalsIgnoreCase(fieldNames[i])) {
          Log.getLogWriter().info("Inside if column name mismatch.");
          columnList.add(i, fieldNames[i]);
        } else {
          switch (clazz) {
            case "Date":
            case "String":
              if (columnValue.equalsIgnoreCase("NULL"))
                ps.setNull(replaceQuestion, Types.VARCHAR);
              else
                ps.setString(replaceQuestion, columnValue);
              break;
            case "Timestamp":
              if (columnValue.equalsIgnoreCase("NULL"))
                ps.setNull(replaceQuestion, Types.TIMESTAMP);
              else {
                Timestamp ts = Timestamp.valueOf(columnValue);
                ps.setTimestamp(replaceQuestion, ts);
              }
              break;
            case "Integer":
              if (columnValue.equalsIgnoreCase("NULL"))
                ps.setNull(replaceQuestion, Types.INTEGER);
              else
                ps.setInt(replaceQuestion, Integer.parseInt(columnValue));
              break;
            case "Double":
              if (columnValue.equalsIgnoreCase("NULL"))
                ps.setNull(replaceQuestion, Types.DOUBLE);
              else
                ps.setDouble(replaceQuestion, Double.parseDouble(columnValue));
              break;
            case "Long":
              if (columnValue.equalsIgnoreCase("NULL"))
                ps.setNull(replaceQuestion, Types.BIGINT);
              else
                ps.setLong(replaceQuestion, Long.parseLong(columnValue));
              break;
            case "BigDecimal":
              if (columnValue.equalsIgnoreCase("NULL"))
                ps.setNull(replaceQuestion, Types.NUMERIC);
              else
                ps.setBigDecimal(replaceQuestion, BigDecimal.valueOf(Double.parseDouble(columnValue)));
              break;
            default:
              Log.getLogWriter().info("Object class type not found.");
              throw new TestException("Object class type not found.");
          }
          replaceQuestion += 1;
        }
      }
    } catch (SQLException se) {
      throw new TestException("Exception while creating PreparedStatement.", se);
    }
    return ps;
  }

  public String getRowFromCSV(String tableName, int randTable) {
    String row = null;
    int insertCounter;
    String csvFilePath = SnappySchemaPrms.getCsvLocationforLargeData();
    String csvFileName = SnappySchemaPrms.getInsertCsvFileNames()[randTable];
    getBBLock();
    List<Integer> counters = (List<Integer>) SnappyDMLOpsBB.getBB().getSharedMap().get
        ("insertCounters");
    insertCounter = counters.get(randTable);
    counters.set(randTable, insertCounter + 1);
    SnappyDMLOpsBB.getBB().getSharedMap().put("insertCounters", counters);
    releaseBBLock();
    //Log.getLogWriter().info("insert Counter is :" + insertCounter + " for csv " + csvFilePath +
    //    File.separator + csvFileName);
    try (Stream<String> lines = Files.lines(Paths.get(csvFilePath + File.separator + csvFileName))) {
      row = lines.skip(insertCounter).findFirst().get();
    } catch (NoSuchElementException nse) {
      if (SnappyPrms.insertDuplicateData()) {
        getBBLock();
        counters.set(randTable, 1);
        SnappyDMLOpsBB.getBB().getSharedMap().put("insertCounters", counters);
        releaseBBLock();
      } else throw new TestException("Reached the end of csv file: " + csvFilePath + File
          .separator + csvFileName + ", no new record to insert.");
    } catch (IOException io) {
      throw new TestException("File not found at specified location " +
          (csvFilePath + File.separator + csvFileName));
    }
    return row;
  }

  protected void getBBLock() {
    if (bbLock == null)
      bbLock = SnappyDMLOpsBB.getBB().getSharedLock();
    bbLock.lock();
  }

  protected void releaseBBLock() {
    bbLock.unlock();
  }


  public String getStmt(String stmt, String row, String tableName) {
    String[] columnValues = row.split(",");
    String replaceString = stmt;
    StructTypeImpl sType = (StructTypeImpl) SnappyDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName);
    ObjectType[] oTypes = sType.getFieldTypes();
    for (int i = 0; i < oTypes.length; i++) {
      String clazz = oTypes[i].getSimpleClassName();
      String columnValue = columnValues[i];
      switch (clazz) {
        case "String":
          replaceString = setString(replaceString, columnValue);
          break;
        case "Timestamp":
          Timestamp ts = Timestamp.valueOf(columnValue);
          replaceString = setString(replaceString, ts.toString());
          break;
        case "Integer":
        case "Double":
          replaceString = setInt(replaceString, columnValue);
          break;

      }
    }
    Log.getLogWriter().info("Insert statement is :" + replaceString);
    return replaceString;
  }

  public String setString(String stmt, String value) {
    stmt = stmt.replaceFirst("\\?", "'" + value + "'");
    return stmt;
  }

  public String setInt(String stmt, String value) {
    stmt = stmt.replaceFirst("\\?", value);
    return stmt;
  }

  public static void HydraTask_performDMLOpsInJob() {
    testInstance.performDMLOp(ConnType.SNAPPY);
  }

  public static void HydraTask_performDMLOpsInApp() {
    testInstance.performDMLOp(ConnType.SMARTCONNECTOR);
  }

  public void performUpdateInSnappy(ConnType connType) {
    try {
      Connection dConn = null; //get the derby connection here
      String updateStmt[] = SnappySchemaPrms.getUpdateStmts();
      int rand = new Random().nextInt(updateStmt.length);
      String stmt = updateStmt[rand];
      String tableName = SnappySchemaPrms.getUpdateTables()[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid", "" + tid);
      if (testUniqueKeys) stmt = addTidToQuery(stmt, tid);
      if (connType.equals(ConnType.SNAPPY)) {
        dynamicAppProps.put(tid, "stmt=\\\"" + stmt + "\\\",tableName=" + tableName + ",tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        dynamicAppProps.put(tid, "\"" + stmt + "\"" + " " + tid);
        String logFile = "sparkAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Updated " + derbyRows + " rows in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String selectQuery = SnappySchemaPrms.getAfterUpdateSelectStmts()[rand];
        String orderByClause = "";
//        String[] dmlTables = SnappySchemaPrms.getDMLTables();
//        orderByClause = SnappySchemaPrms.getOrderByClause()[Arrays.asList(dmlTables)
//            .indexOf(tableName)];
        String message = verifyResultsForTable(selectQuery, tableName, orderByClause, true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after update table " + tableName + "." + message);
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while performing update operation.", se);
    }
  }

  public void performDeleteInSnappy(ConnType connType) {
    try {
      Connection dConn = null; //get the derby connection here
      String deleteStmt[] = SnappySchemaPrms.getDeleteStmts();
      int numRows = 0;
      int rand = new Random().nextInt(deleteStmt.length);
      String stmt = deleteStmt[rand];
      String tableName = SnappySchemaPrms.getDeleteTables()[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid", "" + tid);
      if (testUniqueKeys) stmt = addTidToQuery(stmt, tid);
      if (connType.equals(ConnType.SNAPPY)) {
        dynamicAppProps.put(tid, "stmt=\\\"" + stmt + "\\\",tableName=" + tableName + ",tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        dynamicAppProps.put(tid, "\"" + stmt + "\"" + " " + tid);
        String logFile = "sparkAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Deleted " + derbyRows + " rows in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String selectQuery = SnappySchemaPrms.getAfterDeleteSelectStmts()[rand];
        String orderByClause = "";
//        String[] dmlTables = SnappySchemaPrms.getDMLTables();
//        orderByClause = SnappySchemaPrms.getOrderByClause()[Arrays.asList(dmlTables)
//            .indexOf(tableName)];
        String message = verifyResultsForTable(selectQuery, tableName, orderByClause, true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after executing delete on table " + tableName + "." + message);
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while performing delete operation.", se);
    }
  }

  public void performInsertInSnappy(ConnType connType) {
    try {
      Connection dConn = null;
      String[] dmlTable = SnappySchemaPrms.getDMLTables();
      int rand = new Random().nextInt(dmlTable.length);
      String tableName = dmlTable[rand];
      String row = getRowFromCSV(tableName, rand);
      if (testUniqueKeys)
        row = row + "," + getMyTid();
      String stmt = (SnappySchemaPrms.getInsertStmts())[rand];
      String insertStmt = getStmt(stmt, row, tableName);
      int tid = getMyTid();

      if (connType.equals(ConnType.SNAPPY)) {
        dynamicAppProps.put(tid, "stmt=\\\"" + insertStmt + "\\\",tableName=" + tableName + "," +
            "tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        dynamicAppProps.put(tid, "\"" + insertStmt + "\"" + " " + tid);
        String logFile = "sparkAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Inserting in derby : " + insertStmt);
        int derbyRowCount = dConn.createStatement().executeUpdate(insertStmt);
        Log.getLogWriter().info("Inserted " + derbyRowCount + " row in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String message = verifyResultsForTable("select * from ", tableName, "", true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after insert in table " + tableName + "." + message);
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while performing insert operation.", se);
    }
  }

  public String buildUpdateStmt(String tableName) {
    String updateStmt = "update $tableName set $updateList where $whereClause";
    //String[] tables = SnappySchemaPrms.getDMLTables();
    //String tableName = tables[new Random().nextInt(tables.length)];
    updateStmt = updateStmt.replace("$tableName", tableName);
    String whereClause = "";
    int tid = getMyTid();
    StructTypeImpl sType = (StructTypeImpl) SnappyDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName);
    String[] columnNames = sType.getFieldNames();
    ObjectType[] oTypes = sType.getFieldTypes();
    String updateList = "";
    int numColumnsToUpdate = new Random().nextInt(2);
    for (int i = 0; i < numColumnsToUpdate; i++) {
      if (updateList.length() != 0)
        updateList.concat(" , ");
      int randomInt = new Random().nextInt(columnNames.length);
      String updateColumn = columnNames[randomInt];
      ArrayUtils.remove(columnNames, randomInt);
      updateColumn.concat("=");

      updateList.concat("");
    }
    updateStmt = updateStmt.replace("$updateList", updateList);
    whereClause = "";
    if (whereClause.length() != 0)
      whereClause.concat(" AND ");
    whereClause.concat(" tid = " + tid);
    updateStmt = updateStmt.replace("$whereClause", whereClause);
    return updateStmt;
  }

  public String buildDeleteStmt(String tableName) {
    String deleteStmt = "delete from $tableName where $whereClause";
    //String[] tables = SnappySchemaPrms.getDMLTables();
    //String tableName = tables[new Random().nextInt(tables.length)];
    deleteStmt = deleteStmt.replace("$tableName", tableName);
    String whereClause = "";
    int tid = getMyTid();
    StructTypeImpl sType = (StructTypeImpl) SnappyDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName);
    String[] columnNames = sType.getFieldNames();
    ObjectType[] oTypes = sType.getFieldTypes();

    whereClause = "";
    if (whereClause.length() != 0)
      whereClause.concat(" AND ");
    whereClause.concat(" tid = " + tid);
    deleteStmt = deleteStmt.replace("$whereClause", whereClause);
    return deleteStmt;
  }

  //ENUM for where clause operator
  public enum WhereClauseOperator {
    LESSTHAN("<"),
    GREATERTHAN(">"),
    EQUALTO("=");

    String opType;

    WhereClauseOperator(String opType) {
      this.opType = opType;
    }

    public String getOpType() {
      return opType;
    }

    public static SnappyDMLOpsUtil.WhereClauseOperator getOperation(String op) {
      if (op.equals(LESSTHAN.getOpType())) {
        return LESSTHAN;
      } else if (op.equals(GREATERTHAN.getOpType())) {
        return GREATERTHAN;
      } else if (op.equals(EQUALTO.getOpType())) {
        return EQUALTO;
      } else return null;
    }
  }

  //ENUM for Connection Type
  public enum ExprOperator {
    ADDITION("+"),
    SUBTRACTION("-"),
    MULTIPLICATION("*");

    String opType;

    ExprOperator(String opType) {
      this.opType = opType;
    }

    public String getOpType() {
      return opType;
    }

    public static SnappyDMLOpsUtil.ExprOperator getOperation(String conn) {
      if (conn.equals(ADDITION.getOpType())) {
        return ADDITION;
      } else if (conn.equals(SUBTRACTION.getOpType())) {
        return SUBTRACTION;
      } else if (conn.equals(MULTIPLICATION.getOpType())) {
        return MULTIPLICATION;
      } else return null;
    }
  }
}
