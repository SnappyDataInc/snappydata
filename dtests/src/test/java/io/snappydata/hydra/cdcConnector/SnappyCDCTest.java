package io.snappydata.hydra.cdcConnector;

import java.io.*;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.RemoteTestModule;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyStartUpTest;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.dataExtractorTool.SnappyDataExtractorToolTestPrms;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.test.util.TestException;
import org.apache.commons.io.FileUtils;
import sql.sqlutil.ResultSetHelper;

public class SnappyCDCTest extends SnappyTest {
  public static SnappyCDCTest snappyCDCTest;

  public SnappyCDCTest() {
  }

  public static void HydraTask_meanKillProcesses() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.meanKillProcesses();
  }

  public void meanKillProcesses() {
    Process pr = null;
    ProcessBuilder pb;
    File logFile, log = null, serverKillOutput;
    Set<String> pidList;
    Boolean isStopStart = SnappyCDCPrms.getIsStopStartCluster();
    String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
    String nodeType = SnappyCDCPrms.getNodeType();
    int numNodes = SnappyCDCPrms.getNumNodesToStop();
    String pidString;
    try {
      pidList = SnappyStartUpTest.getServerPidList();
      Log.getLogWriter().info("The pidList size is " + pidList.size());
      log = new File(".");
      String server = log.getCanonicalPath() + File.separator + "server.sh";
      logFile = new File(server);
      String serverKillLog = log.getCanonicalPath() + File.separator + "serverKill.log";
      serverKillOutput = new File(serverKillLog);
      FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      List asList = new ArrayList(pidList);
      Collections.shuffle(asList);
      for (int i = 0; i < numNodes; i++) {
        pidString = String.valueOf(asList.get(i));
        int pid = Integer.parseInt(pidString);
        Log.getLogWriter().info("Server Pid chosen for abrupt kill : " + pidString);
        String pidHost = snappyTest.getPidHost(Integer.toString(pid));
        if (pidHost.equalsIgnoreCase("localhost")) {
          bw.write("/bin/kill -KILL " + pid);
        } else {
          bw.write("ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " +
              pidHost + " /bin/kill -KILL " + pid);
        }
        bw.newLine();
        try {
          RemoteTestModule.Master.removePID(hd, pid);
        } catch (RemoteException e) {
          String s = "Failed to remove PID from nukerun script: " + pid;
          throw new HydraRuntimeException(s, e);
        }
      }

      bw.close();
      fw.close();
      logFile.setExecutable(true);
      pb = new ProcessBuilder(server);
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(serverKillOutput));

      //wait for 1 min before issuing mean kill
      Thread.sleep(60000);
      pr = pb.start();
      pr.waitFor();
    } catch (IOException e) {
      throw new util.TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new util.TestException(s, e);
    }
    Log.getLogWriter().info("The parameters for clusterRestart are snappyPath = " + snappyPath + "\nnodeType = " + nodeType + "\n isStopStart = " + isStopStart);
    clusterRestart(snappyPath, isStopStart, nodeType, false, "", false);
  }

  public static void HydraTask_performRebalance() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.performRebalance();
  }

  public void performRebalance() {
    Connection conn = null;
    try {
      conn = SnappyTest.getLocatorConnection();
      Long startTime = System.currentTimeMillis();
      conn.createStatement().execute("call sys.rebalance_all_buckets();");
      Long endTime = System.currentTimeMillis();
      Long totalTime = endTime - startTime;
      Log.getLogWriter().info("The rebalance procedure took  " + totalTime + " ms");
    } catch (SQLException ex) {
      throw new util.TestException("Caught exception in performRebalance() " + ex.getMessage());
    } finally {
      if(conn!=null)
        closeConnection(conn);
    }
  }

  public static void HydraTask_addNewNode() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.addNewNode();
  }

  public void addNewNode() {
    String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
    if(snappyPath.isEmpty()) snappyPath = productConfDirPath;
    Boolean isNewNodeFirst = SnappyCDCPrms.getIsNewNodeFirst();
    String nodeType = SnappyCDCPrms.getNodeType();
    String dirPath = SnappyCDCPrms.getDataLocation();
    Boolean isKeepOrgConf = SnappyCDCPrms.getIsKeepOrgConf();

    File orgName = new File(snappyPath + "/conf/" + nodeType);
    File bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");
    String nodeInfo;
    Vector hostList = SnappyCDCPrms.getNodeName();
    String nodeName = String.valueOf(hostList.get(0));

    if (nodeType.equalsIgnoreCase("servers"))
      nodeInfo = nodeName + " -dir=" + dirPath + File.separator + nodeName + " -heap-size=5g " +
          "-memory-size=5g ";
    else
      nodeInfo = nodeName + " -dir=" + dirPath + "/lead";

    Log.getLogWriter().info("The nodeInfo is  " + nodeInfo);
    String nodeConfig = nodeInfo + "\n";
    try {
      if (!isNewNodeFirst) {
        FileUtils.copyFile(orgName, bkName);
        FileWriter fw = new FileWriter(orgName, true);
        fw.write(nodeConfig);
        fw.close();
        //restart the cluster
        clusterRestart(snappyPath, false, nodeType, false, "", false);
      } else {
        // stopCluster(snappyPath,);
        FileUtils.copyFile(orgName, bkName);

        File tempConfFile = new File(snappyPath + "/conf/" + nodeType + "_temp");
        if (nodeType.equalsIgnoreCase("servers")) {
          FileWriter fw = new FileWriter(tempConfFile, true);
          fw.write(nodeConfig);
          if (fw != null)
            fw.close();
          FileInputStream fis = new FileInputStream(orgName);
          BufferedReader br = new BufferedReader(new InputStreamReader(fis));

          FileWriter fw1 = new FileWriter(tempConfFile, true);
          BufferedWriter bw = new BufferedWriter(fw1);
          String aLine = null;
          while ((aLine = br.readLine()) != null) {
            bw.write(aLine);
            bw.newLine();
          }
          br.close();
          bw.close();
          if (tempConfFile.renameTo(orgName)) {
            Log.getLogWriter().info("File renamed to " + orgName);
          } else {
            Log.getLogWriter().info("Error while renaming file");
          }

          clusterRestart(snappyPath, true, "", false, "", false);
        } else if (nodeType.equalsIgnoreCase("leads")) {
          clusterRestart(snappyPath, true, nodeType, false, "", false);
        }

        //delete the temp conf file created.
        if (tempConfFile.delete()) {
          Log.getLogWriter().info(tempConfFile.getName() + " is deleted!");
        } else {
          Log.getLogWriter().info("Delete the temp conf file operation failed.");
        }
      }
      if (isKeepOrgConf)
        FileUtils.copyFile(bkName, orgName);
    } catch (FileNotFoundException e) {
      Log.getLogWriter().info("Caught FileNotFoundException in addNewNode method " + e.getMessage());
    } catch (IOException e) {
      Log.getLogWriter().info("Caught IOException in addNewNode method " + e.getMessage());
    } catch (Exception e) {
      Log.getLogWriter().info("Caught Exception in addNewNode method" + e.getMessage());
    }
  }

  public void clusterConfModifyAndRestart(String snappyPath, String nodeType, String nodeConfig) {
    File orgName = new File(snappyPath + "/conf/" + nodeType);
    File bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");
    Set<String> pidList = SnappyStartUpTest.getServerPidList();
    List hostList = new ArrayList(pidList);
    if (orgName.renameTo(bkName)) {
      Log.getLogWriter().info("File renamed to " + bkName);
    } else {
      Log.getLogWriter().info("Error while renaming file");
    }
    try {
      File tempConfFile = new File(snappyPath + "/conf/" + nodeType);
      FileWriter fw = new FileWriter(tempConfFile, true);
      Log.getLogWriter().info("The hostLost size = " + hostList.size());
      for (int i = 0; i < hostList.size(); i++) {
        String pidString = String.valueOf(hostList.get(i));
        Log.getLogWriter().info("pidString : " + pidString);
        int pid = Integer.parseInt(pidString);
        String pidHost = snappyTest.getPidHost(Integer.toString(pid));
        String newConfig = pidHost + " " + nodeConfig + "/" + pidHost + "  -critical-heap-percentage=95 \n";
        fw.write(newConfig);
      }
      fw.close();
    } catch (IOException ex) {
      Log.getLogWriter().info("Caught exception in clusterConfModifyAndRestart " + ex.getMessage());
    }
  }

  public void clusterRestart(String snappyPath, Boolean isStopStart, String nodeType, Boolean isBackupRecovery, String nodeConfig, Boolean isModifyConf) {
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterRestart.log";
      File logFile = new File(dest);
      String dirPath = SnappyCDCPrms.getDataLocation();
      //Stop cluster
      if (isStopStart) {
        stopCluster(snappyPath, logFile);
      }
      if (nodeType.equalsIgnoreCase("leads")) {
        //start locator:
        ProcessBuilder pb = new ProcessBuilder(snappyPath + "/sbin/snappy-locators.sh", "start");
        snappyTest.executeProcess(pb, logFile);
        //start leader:
        ProcessBuilder pb1 = new ProcessBuilder(snappyPath + "/sbin/snappy-leads.sh", "start");
        snappyTest.executeProcess(pb1, logFile);
      }
      if (nodeType.equalsIgnoreCase("servers")) {
        //only start servers first:
        ProcessBuilder pb1 = new ProcessBuilder(snappyPath + "/sbin/snappy-servers.sh", "start");
        snappyTest.executeProcess(pb1, logFile);
      }
      if (isBackupRecovery) {
        removeDiskStoreFiles(dirPath);
      }
      if (isModifyConf) {
        clusterConfModifyAndRestart(snappyPath, nodeType, nodeConfig);
      }
      //Start the cluster after 1 min
      // Thread.sleep(60000);
      Log.getLogWriter().info("Invoking startCluster");
      startCluster(snappyPath, logFile, isModifyConf);
      snappyTest.recordSnappyProcessIDinNukeRun("ServerLauncher");
    } catch (GemFireConfigException ex) {
      Log.getLogWriter().info("Got the expected exception when starting a server ,without locators " + ex.getMessage());
    } catch (IOException ex) {
      Log.getLogWriter().info("Caught exception in cluster restart " + ex.getMessage());
    } catch (Exception ex) {
      Log.getLogWriter().info("Caught exception during cluster restart " + ex.getMessage());
    }
  }

  public static void HydraTask_storeDataCount() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.storeDataCount();
  }

  public void storeDataCount() {
    int tableCnt = 0;
    Boolean isBeforeRestart = SnappyCDCPrms.getIsBeforeRestart();
    String fileName = SnappyCDCPrms.getDataLocation();
    String schema = SnappyDataExtractorToolTestPrms.getSchemaName();
    try {
      Connection con = SnappyTest.getLocatorConnection();
      String tableCntQry = "SELECT COUNT(*) FROM SYS.SYSTABLES WHERE TABLESCHEMANAME='" +schema+ "' AND TABLENAME NOT LIKE 'SNAPPYSYS_INTERNA%'";
      ResultSet rs = con.createStatement().executeQuery(tableCntQry);
      while (rs.next())
        tableCnt = rs.getInt(1);
      rs.close();
      String[] tableArr = new String[tableCnt];
      Map<String, Long> tableCntMap = null;
      if (SnappyBB.getBB().getSharedMap().containsKey("tableCntMap")){
        tableCntMap = (Map<String,Long>) SnappyBB.getBB().getSharedMap().get("tableCntMap");
      }
      else
        tableCntMap = new HashMap<>();
      int cnt = 0;
      String tableQry = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLESCHEMANAME='" +schema+ "' AND TABLENAME NOT LIKE 'SNAPPYSYS_INTERNA%'";
      ResultSet rs1 = con.createStatement().executeQuery(tableQry);
      while (rs1.next()) {
        String tableName = rs1.getString("TABLENAME");
        tableArr[cnt] = tableName;
        cnt++;
      }
      rs1.close();
      for (int i = 0; i < tableArr.length; i++) {
        long count = 0;
        String tableName = tableArr[i];
        String cntQry = "SELECT COUNT(*) FROM " + tableName;
        ResultSet rs3 = con.createStatement().executeQuery(cntQry);
        while (rs3.next()) {
          count = rs3.getLong(1);
          Log.getLogWriter().info("The tableName = " + tableName + " count = " + count);
        }
        if(!tableCntMap.containsKey(tableName)) //To avoid overwriting the existing key(table)
        {
          Log.getLogWriter().info("Inserting in the table as " + tableName + " not in the map" );
          tableCntMap.put(tableName, count);
        }
          rs3.close();
      }
      SnappyBB.getBB().getSharedMap().put("tableCntMap", tableCntMap);
   //  if(!isBeforeRestart)
        getResultSet(con, isBeforeRestart, fileName);
      con.close();
    } catch (SQLException ex) {
      throw new io.snappydata.test.util.TestException("Caught exception in storeDataCount() " + ex.getMessage() + " SQL State is " + ex.getSQLState());
    }
  }

  public static void HydraTask_validateDataCount() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.validateDataCount();
  }

  public void validateDataCount() {
    Boolean isBeforeRestart = SnappyCDCPrms.getIsBeforeRestart();
    String fileName = SnappyCDCPrms.getDataLocation();
    Map<String, Long> tableCntMap = (Map<String, Long>) SnappyBB.getBB().getSharedMap().get("tableCntMap");
    Log.getLogWriter().info("tableCntMap size = " + tableCntMap.size() );
    try {
      Connection con = SnappyTest.getLocatorConnection();
      for (Map.Entry<String, Long> val : tableCntMap.entrySet()) {
        long snappyCnt = 0;
        String tableName = val.getKey();
        long BBCnt = val.getValue();
        String cntQry = "SELECT COUNT(*) FROM " + tableName;
        Log.getLogWriter().info("The query to be executed is " + cntQry);
        ResultSet rs3 = con.createStatement().executeQuery(cntQry);
        while (rs3.next())
          snappyCnt = rs3.getLong(1);
        rs3.close();
        if (snappyCnt == BBCnt)
          Log.getLogWriter().info("SUCCESS : The cnt for table " + tableName + " = " + snappyCnt + " is EQUAL to the BB count = " + BBCnt);
        else
          Log.getLogWriter().info("FAILURE :The cnt fot table " + tableName + " = " + snappyCnt + " is NOT EQUAL to the BB count = " + BBCnt);
      }
      getResultSet(con, isBeforeRestart, fileName);
    } catch (SQLException ex) {
      throw new io.snappydata.test.util.TestException("ValidateDataCount got SQLException " + ex.getMessage());
    } catch (Exception ex) {
      throw new io.snappydata.test.util.TestException("ValidateDataCount got exception " + ex.getMessage());
    }
  }

  public void getResultSet(Connection conn, Boolean isBeforeRestart, String fileName) {
    SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
    Boolean isFirstCluster = SnappyCDCPrms.getIsFirstClusterForCPDE();
    Boolean isCPDE = SnappyCDCPrms.getIsCPDE();
    String logFile = getCurrentDirPath()+ File.separator + "queryResultFiles";
    File queryResultDir = new File(logFile);
    if (!queryResultDir.exists()) queryResultDir.mkdir();
    String outputFile;
    try {
      ArrayList<String> queryList = getQueryList(fileName);
      for (int i = 0; i < queryList.size(); i++) {
        if (isBeforeRestart) {
          if(isFirstCluster)
            outputFile = logFile + File.separator + "beforeRestartResultSet_query_firstCluster_" + i + ".out";
           else
            outputFile = logFile + File.separator + "beforeRestartResultSet_query_" + i + ".out";
        } else
          outputFile = logFile + File.separator + "afterRestartResultSet_query_" +System.currentTimeMillis() + "_" + i + ".out";
        String qStr = queryList.get(i);
        Log.getLogWriter().info("The query for validation is " + qStr);
        ResultSet snappyRS = conn.createStatement().executeQuery(qStr);
        StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
        List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);
        snappyRS.close();
        testInstance.listToFile(snappyList, outputFile);
        if (!isBeforeRestart) {
          String beforeRestartFileName;
          if(isCPDE && (i < 11)) {
            beforeRestartFileName = logFile + File.separator + "beforeRestartResultSet_query_firstCluster_" + i + ".out";
            Log.getLogWriter().info("SP: Inside isCPDE and i = " + i + " filename = " + beforeRestartFileName);
          }
          else {
            beforeRestartFileName = logFile + File.separator + "beforeRestartResultSet_query_" + i + ".out";
            Log.getLogWriter().info("SP: Inside NotisBeforeRestart and i = " + i + " filename = " + beforeRestartFileName);
          }
          String mismatchStr = testInstance.compareFiles(logFile, outputFile, beforeRestartFileName, true, "query_" + i);
          if(mismatchStr.length() > 0){
            throw new TestException("Observed data mismatch in query  " + qStr);
          }
        }
      }
    } catch (SQLException ex) {
      throw new io.snappydata.test.util.TestException("Caught sqlException in getResultSet method " + ex.getMessage());
    }
    catch (Exception ex) {
      throw new io.snappydata.test.util.TestException("Caught exception in getResultSet " + ex.getMessage());
    }
  }

  public ArrayList getQueryList(String fileName) {
    ArrayList queryList = new ArrayList<String>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] splitData = line.split(";");
        for (int i = 0; i < splitData.length; i++) {
          if (!(splitData[i] == null) || !(splitData[i].length() == 0)) {
            String qry = splitData[i];
            System.out.println("The query is " + qry);
            queryList.add(qry);
          }
        }
      }
      br.close();

    } catch (FileNotFoundException e) {
      System.out.println("Caught FileNotFoundException in getQuery() " + e.getMessage());
    } catch (IOException io) {
      System.out.println("Caught IOException in getQuery() " + io.getMessage());
    }
    return queryList;
  }

  public static void HydraTask_runConcurrencyJob() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.runConcurrencyTestJob();
  }

  public static void HydraTask_clusterRestart() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    Boolean isStopStart = SnappyCDCPrms.getIsStopStartCluster();
    String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
    String nodeType = SnappyCDCPrms.getNodeType();
    String nodeConfig = SnappyCDCPrms.getNodeInfoForHA();
    Boolean isModifyConf = SnappyCDCPrms.getIsModifyConf();
    Boolean isBackUpRecovery = SnappyCDCPrms.getIsBackUpRecovery();
    snappyCDCTest.clusterRestart(snappyPath, isStopStart, nodeType, isBackUpRecovery, nodeConfig, isModifyConf);
  }

  public static void HydraTask_stopCluster() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterStop.log";
      File logFile = new File(dest);
      snappyCDCTest.stopCluster(snappyPath, logFile);
    } catch (IOException ex) {
      Log.getLogWriter().info("HydraTask_stopCluster got exception " + ex.getMessage());
    }
  }

  public static void HydraTask_startCluster() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterStart.log";
      File logFile = new File(dest);
      snappyCDCTest.startCluster(snappyPath, logFile, false);
    } catch (IOException ex) {
      Log.getLogWriter().info("HydraTask_startCluster got exception " + ex.getMessage());
    }
  }


  public void runConcurrencyTestJob() {
    try {
      CDCPerfSparkJob cdcPerfSparkJob = new CDCPerfSparkJob();
      List<String> endpoints = validateLocatorEndpointData();
      int threadCnt = SnappyCDCPrms.getThreadCnt();
      String queryPath = SnappyCDCPrms.getDataLocation();
      Boolean isScanQuery = SnappyCDCPrms.getIsScanQuery();
      Boolean isBulkDelete = SnappyCDCPrms.getIsBulkDelete();
      Boolean isPointLookUp = SnappyCDCPrms.getIsPointLookUP();
      Boolean isMixedQuery = SnappyCDCPrms.getIsMixedQuery();
      String sqlServerInst = SnappyCDCPrms.getSqlServerInstance();
      String dataBaseName = SnappyCDCPrms.getDataBaseName();
      int intStartRange = SnappyCDCPrms.getInitStartRange();
      Log.getLogWriter().info("Inside runConcurrencyTestJob() parameters are  " + threadCnt + " " + queryPath + " "
          + endpoints.get(0) + " " + isScanQuery + " " + isBulkDelete + " " + isPointLookUp + " " + intStartRange + " "
          + sqlServerInst + " " + dataBaseName);
      cdcPerfSparkJob.runConcurrencyTestJob(threadCnt, queryPath, endpoints.get(0), isScanQuery, isBulkDelete,
          isPointLookUp, isMixedQuery, intStartRange, sqlServerInst);

    } catch (Exception ex) {
      Log.getLogWriter().info("Caught Exception" + ex.getMessage() + " in runConcurrencyTestJob() method");
    }
  }

  public static void HydraTask_runIngestionApp() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.runIngestionApp();
  }

  public void runIngestionApp() {
    try {
      Integer threadCnt = SnappyCDCPrms.getThreadCnt();
      Integer sRange = SnappyCDCPrms.getInitStartRange();
      Integer eRange = SnappyCDCPrms.getInitEndRange();
      String queryPath = SnappyCDCPrms.getDataLocation();
      String sqlServer = SnappyCDCPrms.getSqlServerInstance();
      List<String> endpoints = validateLocatorEndpointData();
      CDCIngestionApp.runIngestionApp(sRange, eRange, threadCnt, queryPath, sqlServer, endpoints.get(0));
    } catch (Exception ex) {
      Log.getLogWriter().info("Caught Exception" + ex.getMessage() + " in runIngestionApp() method");
    }
  }

  public static void HydraTask_closeStreamingJob() {
    String curlCmd = null;
    ProcessBuilder pb = null;
    String appName = SnappyCDCPrms.getAppName();
    int noOfApps = 5;
    Boolean isResubmitApp = SnappyCDCPrms.getIsResubmitApp();
    String logFileName = "sparkStreamingStopResult_" + System.currentTimeMillis() + ".log";
    File log = null;
    File logFile = null;
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    try {
      InetAddress myHost = InetAddress.getLocalHost();
      String hostName[] = myHost.toString().split("/");
      for (int i = 1; i <= noOfApps; i++) {
        curlCmd = "curl -d \"name=" + appName + i + "&terminate=true\" -X POST http://" + hostName[0] + ":8080/app/killByName/";
        Log.getLogWriter().info("The curlCmd  is " + curlCmd);
        pb = new ProcessBuilder("/bin/bash", "-c", curlCmd);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + logFileName + "i";
        logFile = new File(dest);
        snappyTest.executeProcess(pb, logFile);
      }
      if (isResubmitApp) {
        Log.getLogWriter().info("Resubmitting the app " + appName);
        HydraTask_executeSparkJob();
      }
    } catch (Exception ex) {
      Log.getLogWriter().info("Exception in HydraTask_closeStreamingJob() " + ex.getMessage());
    }
  }

  public String getNodeInfo(String hostName, String snappyPath, String nodeType) {
    String nodeInfo = "";
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "nodeNameForHA_" + nodeType + "_" + hostName + ".log";
      File logFile = new File(dest);
      Process pr = null;
      if (!logFile.exists()) {
        String cmd = " grep  ^" + hostName + " " + snappyPath + "/conf/" + nodeType + " | head -n1";
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
        pr = pb.start();
        pr.waitFor();
        FileInputStream fis = new FileInputStream(logFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String str;
        while ((str = br.readLine()) != null) {
          nodeInfo = str;
        }
      }
    } catch (IOException io) {
      Log.getLogWriter().info("The exception in getNodeInfo is " + io.getMessage());
    } catch (InterruptedException ex) {
      Log.getLogWriter().info("The exception in getNodeInfo is " + ex.getMessage());
    }
    return nodeInfo;
  }

  public static void HydraTask_performHA() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.performHA();
  }

  public void performHA() {
    String scriptName;
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      String nodeType = SnappyCDCPrms.getNodeType();
      Boolean isOnlyStop = SnappyCDCPrms.getIsOnlyStop();
      String nodeInfoforHA;

      if (nodeType.equals("allNodes")) {
        File log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "clusterRestart.log";
        Log.getLogWriter().info("The destination file is " + dest);
        File logFile = new File(dest);
        ProcessBuilder pbClustStop = new ProcessBuilder(snappyPath + "/sbin/snappy-stop-all.sh");
        Long startTime = System.currentTimeMillis();
        snappyTest.executeProcess(pbClustStop, logFile);
        Long totalTime = (System.currentTimeMillis() - startTime);
        Log.getLogWriter().info("The cluster took " + totalTime + " ms to shut down");

        //Restart the cluster after 10 mins
        Thread.sleep(600000);
        ProcessBuilder pbClustStart = new ProcessBuilder(snappyPath + "/sbin/snappy-start-all.sh");
        Long startTime1 = System.currentTimeMillis();
        snappyTest.executeProcess(pbClustStart, logFile);
        Long totalTime1 = (System.currentTimeMillis() - startTime1);
        Log.getLogWriter().info("The cluster took " + totalTime1 + " ms to shut down");
      } else {
        Random rnd = new Random();
        Vector hostList = SnappyCDCPrms.getNodeName();
        int num = rnd.nextInt(hostList.size());
        String hostName = String.valueOf(hostList.get(num));//String.valueOf(rnd.nextInt(hostList.size()));
        if (hostName.isEmpty()) {
          Log.getLogWriter().info("The hostname is empty ,hence trying to get another hostname");
          hostName = String.valueOf(hostList.get(1));
        }
        Log.getLogWriter().info("The hostname is  " + hostName);

        if (nodeType.equalsIgnoreCase("servers")) {
          scriptName = "/sbin/snappy-servers.sh";
          nodeInfoforHA = getNodeInfo(hostName, snappyPath, nodeType);
          Log.getLogWriter().info("The nodeInfoForHA is " + nodeInfoforHA);
        } else if (nodeType.equalsIgnoreCase("leads")) {
          nodeInfoforHA = getNodeInfo(hostName, snappyPath, nodeType);
          Log.getLogWriter().info("The nodeInfoForHA is " + nodeInfoforHA);
          scriptName = "/sbin/snappy-leads.sh";
        } else {
          scriptName = "/sbin/snappy-locators.sh";
          nodeInfoforHA = getNodeInfo(hostName, snappyPath, nodeType);
          Log.getLogWriter().info("The nodeInfoForHA is " + nodeInfoforHA);
        }

        File orgName = new File(snappyPath + "/conf/" + nodeType);
        File bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");

        //rename original conf file
        if (orgName.renameTo(bkName)) {
          Log.getLogWriter().info("File renamed to " + bkName);
        } else {
          Log.getLogWriter().info("Error");
        }

        //write a new file in conf
        try {
          File tempConfFile = new File(snappyPath + "/conf/" + nodeType);
          FileWriter fw = new FileWriter(tempConfFile);
          fw.write(nodeInfoforHA);
          fw.close();
          File log = new File(".");
          String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
          Log.getLogWriter().info("The destination file is " + dest);
          File logFile = new File(dest);

          if (isOnlyStop)
            stopIndividualNode(snappyPath, scriptName, logFile);
          else {
            Log.getLogWriter().info("The nodeType is " + nodeType + " script to stop is " + scriptName);
            stopIndividualNode(snappyPath, scriptName, logFile);

            Thread.sleep(30000); //sleep for 3 min before restarting the node.

            Log.getLogWriter().info("The nodeType is " + nodeType + " script to start is " + scriptName);
            startIndividualNode(snappyPath, scriptName, logFile);
            Thread.sleep(60000);

            //delete the temp conf file created.
            if (tempConfFile.delete()) {
              System.out.println(tempConfFile.getName() + " is deleted!");
            } else {
              System.out.println("Delete the temp conf file operation failed.");
            }
            //restore the back up to its originals.
            if (bkName.renameTo(orgName)) {
              Log.getLogWriter().info("File renamed to " + orgName);
            } else {
              Log.getLogWriter().info("Error in renaming the file");
            }
          }
        } catch (FileNotFoundException e) {
          Log.getLogWriter().info("Caught FileNotFoundException in performHA method " + e.getMessage());
        } catch (IOException e) {
          Log.getLogWriter().info("Caught IOException in performHA method " + e.getMessage());
        }
      }
    } catch (Exception e) {
      Log.getLogWriter().info("Caught Exception in performHA " + e.getMessage());
    }
  }

  public static void HydraTask_removeDiskStore() {
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.removeDiskStore();
  }

  public void removeDiskStore() {
    Vector hostList = SnappyCDCPrms.getNodeName();
    String nodeName = String.valueOf(hostList.get(0));
    Log.getLogWriter().info("the nodeName is " + nodeName);
    removeDiskStoreFiles(nodeName);
  }

  public void stopCluster(String snappyPath, File logFile) {
    ProcessBuilder pbClustStop = new ProcessBuilder(snappyPath + "/sbin/snappy-stop-all.sh");
    Long startTime = System.currentTimeMillis();
    snappyTest.executeProcess(pbClustStop, logFile);
    Long totalTime = (System.currentTimeMillis() - startTime);
    Log.getLogWriter().info("The cluster took " + totalTime + " ms to shut down");
    getClusterStatus(snappyPath, logFile);
  }

  public void startCluster(String snappyPath, File logFile, Boolean isModifyCluster) {
    ProcessBuilder pbClustStart = new ProcessBuilder(snappyPath + "/sbin/snappy-start-all.sh");
    Long startTime1 = System.currentTimeMillis();
    snappyTest.executeProcess(pbClustStart, logFile);
    Long totalTime1 = (System.currentTimeMillis() - startTime1);
    Log.getLogWriter().info("The cluster took " + totalTime1 + " ms to start up");
    getClusterStatus(snappyPath, logFile);
  }

  public void getClusterStatus(String snappyPath, File logFile) {
    Log.getLogWriter().info("Now getting cluster status");
    ProcessBuilder pbClustStats = new ProcessBuilder(snappyPath + "/sbin/snappy-status-all.sh");
    Long startTime1 = System.currentTimeMillis();
    snappyTest.executeProcess(pbClustStats, logFile);
    Long totalTime1 = (System.currentTimeMillis() - startTime1);
    Log.getLogWriter().info("The cluster took " + totalTime1 + " ms to report status");
  }

  public void stopIndividualNode(String snappyPath, String script, File logFile) {
    try {
      ProcessBuilder pbStop = new ProcessBuilder(snappyPath + script, "stop");
      snappyTest.executeProcess(pbStop, logFile);
      Thread.sleep(60000);
    } catch (InterruptedException ex) {
      Log.getLogWriter().info("Caught exception in stopIndividualNode " + ex.getMessage());
    }
  }

  public void startIndividualNode(String snappyPath, String script, File logFile) {
    ProcessBuilder pbStart = new ProcessBuilder(snappyPath + script, "start");
    snappyTest.executeProcess(pbStart, logFile);
  }

  public void removeDiskStoreFiles(String nodeName) {
    try {
      String dirName = "vm_2_snappyStore1_"+nodeName;
      String dirPath = getCurrentDirPath() + File.separator + dirName;
      Log.getLogWriter().info("The dir path is " + dirPath);
      File dir = new File(dirPath);

      String[] extensions = new String[]{"crf", "drf", "krf", "idxkrf", "if"};
      Log.getLogWriter().info("Getting files with specified extension " + dir.getCanonicalPath());
      List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, false);
      Log.getLogWriter().info("The files length is " + files.size());
      for (File file : files) {
        Log.getLogWriter().info("file: " + file.getCanonicalPath());
        String fileName = file.getName();
        if (file.delete())
          Log.getLogWriter().info("The diskstore file deleted is " + fileName);
        else
          Log.getLogWriter().info("The diskstore file " + fileName + " is not deleted");
      }
    } catch (IOException io) {
      Log.getLogWriter().info("Caught exception in getFileWithDiffExt() " + io.getMessage());
    }
  }
}
