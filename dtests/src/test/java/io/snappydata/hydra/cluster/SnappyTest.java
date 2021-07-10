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
package io.snappydata.hydra.cluster;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import hydra.*;
import io.snappydata.hydra.cdcConnector.SnappyCDCPrms;
import io.snappydata.hydra.connectionPool.HikariConnectionPool;
import io.snappydata.hydra.connectionPool.SnappyConnectionPoolPrms;
import io.snappydata.hydra.connectionPool.TomcatConnectionPool;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SnappyContext;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.dmlStatements.DMLStmtIF;
import sql.sqlutil.DMLStmtsFactory;
import util.*;

import static hydra.Prms.maxResultWaitSec;

public class SnappyTest implements Serializable {

  private static transient SnappyContext snc = SnappyContext.apply(SnappyContext
      .globalSparkContext());
  protected static SnappyTest snappyTest;
  protected static HostDescription hd = TestConfig.getInstance().getMasterDescription()
      .getVmDescription().getHostDescription();
  protected static char sep = hd.getFileSep();
  private static String gemfireHome = hd.getGemFireHome() + sep;
  protected static String productDir = gemfireHome + ".." + sep + "snappy" + sep;
  protected static String productConfDirPath = productDir + "conf" + sep;
  private static String productLibsDir = productDir + "lib" + sep;
  private static String productSbinDir = productDir + "sbin" + sep;
  private static String productBinDir = productDir + "bin" + sep;
  protected static String SnappyShellPath = productBinDir + "snappy-sql";
  private static String dtests = gemfireHome + ".." + sep + ".." + sep + ".." + sep + "dtests" + sep;
  private static String dtestsLibsDir = dtests + "build-artifacts" + sep + "scala-2.11" + sep + "libs" + sep;
  private static String dtestsResourceLocation = dtests + "src" + sep + "resources" + sep;
  private static String dtestsScriptLocation = dtestsResourceLocation + "scripts" + sep;
  private static String dtestsDataLocation = dtestsResourceLocation + "data" + sep;
  private static String quickstartScriptLocation = productDir + "quickstart" + sep + "scripts" + sep;
  private static String quickstartDataLocation = productDir + "quickstart" + sep + "data" + sep;
  private static String logFile = null;

  private static Set<Integer> pids = new LinkedHashSet<Integer>();
  private static Set<File> dirList = new LinkedHashSet<File>();
  public static String userAppJar = null;
  private static String simulateStreamScriptName = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptName, "simulateFileStream");
  private static String simulateStreamScriptDestinationFolder = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptDestinationFolder, dtests);
  public static boolean isLongRunningTest = TestConfig.tab().booleanAt(SnappyPrms.isLongRunningTest, false);  //default to false
  public static boolean isUserConfTest = TestConfig.tab().booleanAt(SnappyPrms.isUserConfTest, false);  //default to false
  public static boolean isCppTest = TestConfig.tab().booleanAt(SnappyPrms.isCppTest, false);  //default to false
  public static boolean useRowStore = TestConfig.tab().booleanAt(SnappyPrms.useRowStore, false);  //default to false
  public static boolean isRestarted = false;
  public static boolean useSmartConnectorMode = TestConfig.tab().booleanAt(SnappyPrms.useSmartConnectorMode, false);  //default to false
  /*public static boolean useThinClientSmartConnectorMode = TestConfig.TestConfig.tab().booleanAt(SnappyPrms.useThinClientSmartConnectorMode, false);*/  //default to false
  public static boolean isStopMode = TestConfig.tab().booleanAt(SnappyPrms.isStopMode, false);  //default to false
  public static boolean forceStart = TestConfig.tab().booleanAt(SnappyPrms.forceStart, false);  //default to false
  public static boolean forceCopy = TestConfig.tab().booleanAt(SnappyPrms.forceCopy, false);  //default to false
  private static String primaryLocator = null;
  public static String leadHost = null;
  public static Long waitTimeBeforeStreamingJobStatus = TestConfig.tab().longAt(SnappyPrms.streamingJobExecutionTimeInMillis, 6000);
  public static long maxResultWaitSecs = TestConfig.tab().longAt(maxResultWaitSec);
  private static Boolean logDirExists = false;
  private static Boolean doneCopying = false;
  protected static Boolean doneRandomizing = false;
  private static Boolean doneRestore = false;
  private static Boolean diskDirExists = false;
  private static Boolean runGemXDQuery = false;
  protected static int[] dmlTables = SQLPrms.getTables();
  public static final Random random = new Random(SQLPrms.getRandSeed());
  protected static DMLStmtsFactory dmlFactory = new DMLStmtsFactory();

  protected static boolean cycleVms = TestConfig.tab().booleanAt(SnappyPrms.cycleVms, false);
  public static final String LASTCYCLEDTIME = "lastCycledTime"; //used in SnappyBB
  public static final String LASTCYCLEDTIMEFORLEAD = "lastCycledTimeForLead"; //used in SnappyBB
  public static final String LASTCYCLEDTIMEFORLOCATOR = "lastCycledTimeForLocator"; //used in
  // SnappyBB
  public static long lastCycledTime = 0;
  public static long lastCycledTimeForLead = 0;
  public static int waitTimeBeforeNextCycleVM = TestConfig.tab().intAt(SnappyPrms.waitTimeBeforeNextCycleVM, 20); //secs
  public static final int THOUSAND = 1000;
  public static String cycleVMTarget = TestConfig.tab().stringAt(SnappyPrms.cycleVMTarget, "snappyStore");
  public static String cycleLeadVMTarget = TestConfig.tab().stringAt(SnappyPrms.cycleVMTarget, "lead");
  public static String cycleLocatorVMTarget = TestConfig.tab().stringAt(SnappyPrms.cycleVMTarget,
      "locator");
  //public static final String LEAD_PORT = "8090";
  public static final String MASTER_PORT = "7077";
  private static int jobSubmissionCount = 0;
  protected static String jarPath = gemfireHome + ".." + sep + ".." + sep + ".." + sep;

  private Connection connection = null;
  public static String connPool = TestConfig.tab().stringAt(SnappyConnectionPoolPrms.useConnPool, "");
  public static int connPoolType = SnappyConnectionPoolPrms.getConnPoolType(connPool);
  private static HydraThreadLocal localconnection = new HydraThreadLocal();

  public static int finalStart = SnappyCDCPrms.getInitEndRange() + 1;
  public static int finalEnd = SnappyCDCPrms.getInitEndRange() + 10;


  /**
   * (String) APP_PROPS to set dynamically
   */
  public static Map<Integer, String> dynamicAppProps = new ConcurrentHashMap<>();

  public enum SnappyNode {
    LOCATOR, SERVER, LEAD, WORKER
  }

  SnappyNode snappyNode;

  public SnappyTest() {
  }

  public SnappyTest(SnappyNode snappyNode) {
    this.snappyNode = snappyNode;
  }

  public static void HydraTask_stopSnappy() {
    SparkContext sc = SnappyContext.globalSparkContext();
    if (sc != null) sc.stop();
    Log.getLogWriter().info("SnappyContext stopped successfully");
  }

  public static synchronized void HydraTask_initializeSnappyTest() {
    if (snappyTest == null) {
      snappyTest = new SnappyTest();
      snappyTest.getClientHostDescription();
      int tid = RemoteTestModule.getCurrentThread().getThreadId();
      if (tid == 0) {
        snappyTest.generateConfig("locators");
        snappyTest.generateConfig("servers");
        snappyTest.generateConfig("leads");
        if (useSmartConnectorMode) {
          snappyTest.generateConfig("slaves");
          snappyTest.generateConfig("spark-env.sh");
        }
        if (isLongRunningTest) {
          snappyTest.generateConfig("locatorConnInfo");
          snappyTest.generateConfig("leadHost");
          snappyTest.generateConfig("leadPort");
          snappyTest.generateConfig("locatorList");
          snappyTest.generateConfig("primaryLocatorHost");
          snappyTest.generateConfig("primaryLocatorPort");
        }


      }
    }
  }


  public static void initSnappyArtifacts() {
    snappyTest = new SnappyTest();
    HostDescription hd = TestConfig.getInstance().getMasterDescription()
        .getVmDescription().getHostDescription();
    char sep = hd.getFileSep();
    String gemfireHome = hd.getGemFireHome() + sep;
    String productDir = gemfireHome + ".." + sep + "snappy" + sep;
    String productConfDirPath = productDir + "conf" + sep;
    String productLibsDir = productDir + "lib" + sep;
    String productSbinDir = productDir + "sbin" + sep;
    String productBinDir = productDir + "bin" + sep;
    String SnappyShellPath = productBinDir + "snappy-sql";
    String dtests = gemfireHome + ".." + sep + ".." + sep + ".." + sep + "dtests" + sep;
    String dtestsLibsDir = dtests + "build-artifacts" + sep + "scala-2.11" + sep + "libs" + sep;
    String dtestsResourceLocation = dtests + "src" + sep + "resources" + sep;
    String dtestsScriptLocation = dtestsResourceLocation + "scripts" + sep;
    String dtestsDataLocation = dtestsResourceLocation + "data" + sep;
    String quickstartScriptLocation = productDir + "quickstart" + sep + "scripts" + sep;
    String quickstartDataLocation = productDir + "quickstart" + sep + "data" + sep;
  }

  protected String getStoreTestsJar() {
    String storeTestsJar = hd.getTestDir() + hd.getFileSep() + ".." + hd.getFileSep() + ".." +
        hd.getFileSep() + ".." +
        hd.getFileSep() + "libs" + hd.getFileSep() + "snappydata-store-hydra-tests-" +
        ProductVersionHelper.getInfo().getProperty(ProductVersionHelper.SNAPPYRELEASEVERSION) +
        "-all.jar";
    return storeTestsJar;
  }

  protected String getSnappyTestsJar() {
    String snappyTestsJar = getUserAppJarLocation("snappydata-store-scala-tests*.jar",
        hd.getGemFireHome() + hd.getFileSep() + ".." + hd.getFileSep() + ".." +
            hd.getFileSep() + ".." + hd.getFileSep() + "dtests" + hd.getFileSep());
    return snappyTestsJar;
  }

  protected String getClusterTestsJar() {
    String clusterTestsJar = getUserAppJarLocation("snappydata-cluster*tests.jar", hd
        .getGemFireHome() + hd.getFileSep() + ".." + hd.getFileSep() + ".." +
        hd.getFileSep() + ".." + hd.getFileSep());
    return clusterTestsJar;
  }

  public void getClientHostDescription() {
    hd = TestConfig.getInstance()
        .getClientDescription(RemoteTestModule.getMyClientName())
        .getVmDescription().getHostDescription();
  }

  protected static String getUserAppJarLocation(final String jarName, String jarPath) {
    String userAppJarPath = null;
    if (new File(jarName).exists()) {
      return jarName;
    } else {
      File baseDir = new File(jarPath);
      try {
        IOFileFilter filter = new WildcardFileFilter(jarName);
        List<File> files = (List<File>) FileUtils.listFiles(baseDir, filter, TrueFileFilter.INSTANCE);
        Log.getLogWriter().info("Jar file found: " + Arrays.asList(files));
        for (File file1 : files) {
          if (!file1.getAbsolutePath().contains("/work/") || !file1.getAbsolutePath().contains("/scala-2.10/"))
            userAppJarPath = file1.getAbsolutePath();
        }
      } catch (Exception e) {
        Log.getLogWriter().info("Unable to find " + jarName + " jar at " + jarPath + " location.");
      }
      return userAppJarPath;
    }
  }

  public String getDataLocation(String paramName) {
    if (paramName.equals(" ")) return paramName;
    String scriptPath = null;
    if (new File(paramName).exists()) {
      return paramName;
    } else {
      scriptPath = quickstartDataLocation + paramName;
      if (new File(scriptPath).exists()) return scriptPath;
      else scriptPath = dtestsDataLocation + paramName;
      if (new File(scriptPath).exists()) return scriptPath;
      else {
        String s = "Data doesn't exists at any expected location.";
        throw new TestException(s);
      }
    }
  }

  public String getScriptLocation(String scriptName) {
    String scriptPath = null;
    if (new File(scriptName).exists()) return scriptName;
    scriptPath = productSbinDir + scriptName;
    if (!new File(scriptPath).exists()) {
      scriptPath = productBinDir + scriptName;
      if (new File(scriptPath).exists()) return scriptPath;
      else
        scriptPath = getUserAppJarLocation(scriptName, dtestsScriptLocation);
      if (new File(scriptPath).exists()) return scriptPath;
      else
        scriptPath = quickstartScriptLocation + scriptName;
      if (new File(scriptPath).exists()) return scriptPath;
      else {
        String s = "Unable to find the script at any expected location.";
        throw new TestException(s);
      }
    }
    return scriptPath;
  }

  /**
   * Generates the configuration data required to start the snappy locator.
   */
  public static synchronized void HydraTask_generateSnappyLocatorConfig() {
    SnappyTest locator = new SnappyTest(SnappyNode.LOCATOR);
    locator.generateNodeConfig("locatorLogDir", false);
  }

  /**
   * Generates the configuration data required to start the snappy Server.
   */
  public static synchronized void HydraTask_generateSnappyServerConfig() {
    SnappyTest server = new SnappyTest(SnappyNode.SERVER);
    server.generateNodeConfig("serverLogDir", false);
  }

  /**
   * Generates the configuration data required to start the snappy Server.
   */
  public static synchronized void HydraTask_generateSnappyLeadConfig() {
    SnappyTest lead = new SnappyTest(SnappyNode.LEAD);
    lead.generateNodeConfig("leadLogDir", false);
  }

  /**
   * Generates the configuration data required to start the snappy Server.
   */
  public static synchronized void HydraTask_generateSparkWorkerConfig() {
    SnappyTest worker = new SnappyTest(SnappyNode.WORKER);
    worker.generateNodeConfig("workerLogDir", false);
  }

  protected void generateNodeConfig(String logDir, boolean returnNodeLogDir) {
    if (logDirExists) return;
    String addr = HostHelper.getHostAddress();
    int port = PortHelper.getRandomPort();
    String endpoint = addr + ":" + port;
    String clientPort = " -client-port=";
    String locators = " -locators=";
    String locPortString = " -peer-discovery-port=";
    String locatorsList = null;
    String dirPath = snappyTest.getLogDir();
    String nodeLogDir = null;
    String hostnameForPrimaryLocator = null;
    switch (snappyNode) {
      case LOCATOR:
        int locPort;
        do locPort = PortHelper.getRandomPort();
        while (locPort < 0 || locPort > 65535);
        nodeLogDir = HostHelper.getLocalHost() + " -dir=" + dirPath + clientPort + port +
            locPortString + locPort + SnappyPrms.getTimeStatistics() + SnappyPrms.getLogLevel() +
            " -Dgemfire.DISALLOW_RESERVE_SPACE=true " +
            " " + SnappyPrms.getLocatorLauncherProps();
        SnappyBB.getBB().getSharedMap().put("locatorHost" + "_" + RemoteTestModule.getMyVmid(),
            HostHelper.getLocalHost());
        SnappyBB.getBB().getSharedMap().put("locatorPort" + "_" + RemoteTestModule.getMyVmid(),
            Integer.toString(port));
        SnappyBB.getBB().getSharedMap().put("locatorMcastPort" + "_" + RemoteTestModule.getMyVmid
            (), Integer.toString(locPort));
        SnappyBB.getBB().getSharedMap().put("locators" + "_" + RemoteTestModule.getMyVmid(),
            HostHelper.getLocalHost() + ":" + Integer.toString(locPort));
        SnappyBB.getBB().getSharedMap().put(Integer.toString(locPort), HostHelper.getLocalHost());
        Log.getLogWriter().info("Generated locator endpoint: " + endpoint);
        SnappyNetworkServerBB.getBB().getSharedMap().put("locator" + "_" + RemoteTestModule
            .getMyVmid(), endpoint);
        int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB
            .primaryLocatorStarted);
        if (num == 1) {
          primaryLocator = endpoint;
          hostnameForPrimaryLocator = HostHelper.getLocalHost();
          SnappyBB.getBB().getSharedMap().put("primaryLocatorHost", hostnameForPrimaryLocator);
          SnappyBB.getBB().getSharedMap().put("primaryLocatorPort", Integer.toString(port));
        }
        break;
      case SERVER:
        boolean excludeDirCreation = false;
        if (SnappyPrms.getServerLauncherProps().contains("-dir")) {
          excludeDirCreation = true;
        }
        locatorsList = getLocatorsList("locators");
        nodeLogDir = HostHelper.getLocalHost() + locators + locatorsList + " -dir=" +
            dirPath + clientPort + port + SnappyPrms.getServerMemory()
            + SnappyPrms.getConserveSockets() +
            " -J-Dgemfirexd.table-default-partitioned=" +
            SnappyPrms.getTableDefaultDataPolicy() + SnappyPrms.getTimeStatistics() +
            SnappyPrms.getLogLevel() + SnappyPrms.getPersistIndexes() +
            " -J-Dgemfire.CacheServerLauncher.SHUTDOWN_WAIT_TIME_MS=50000" +
            SnappyPrms.getFlightRecorderOptions(dirPath) +
            SnappyPrms.getGCOptions(dirPath) + " " +
            " -Dgemfire.DISALLOW_RESERVE_SPACE=true " +
            SnappyPrms.getServerLauncherProps() +
            " -classpath=" + getStoreTestsJar();
        Log.getLogWriter().info("Generated peer server endpoint: " + endpoint);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numServers);
        SnappyNetworkServerBB.getBB().getSharedMap().put("server" + "_" + RemoteTestModule
            .getMyVmid(), endpoint);
        break;
      case LEAD:
        locatorsList = getLocatorsList("locators");
        String leadHost;
        int leadPort = PortHelper.getRandomPort();
                /*do leadPort = PortHelper.getRandomPort();
                while (leadPort < 8091 || leadPort > 8099);*/
        nodeLogDir = HostHelper.getLocalHost() + locators + locatorsList +
            SnappyPrms.getExecutorCores() + SnappyPrms.getDriverMaxResultSize() +
            " -spark.local.dir=" + snappyTest.getTempDir("temp") +
            " -dir=" + dirPath + clientPort + port +
            " -jobserver.waitForInitialization=true " +
            SnappyPrms.getLeadMemory() + SnappyPrms.getSparkSqlBroadcastJoinThreshold()
            + " -spark.jobserver.port=" + leadPort + SnappyPrms.getSparkSchedulerMode()
            + /*" -spark.sql.inMemoryColumnarStorage.compressed="
                        + SnappyPrms.getCompressedInMemoryColumnarStorage() +*/
            SnappyPrms.getColumnBatchSize() + SnappyPrms.getConserveSockets() +
            " -table-default-partitioned=" + SnappyPrms.getTableDefaultDataPolicy() +
            SnappyPrms.getTimeStatistics() +
            SnappyPrms.getLogLevel() + SnappyPrms.getNumBootStrapTrials() +
            SnappyPrms.getClosedFormEstimates() + SnappyPrms.getZeppelinInterpreter() +
            " -classpath=" + getStoreTestsJar() +
            " -J-Dgemfire.CacheServerLauncher.SHUTDOWN_WAIT_TIME_MS=50000" +
            SnappyPrms.getFlightRecorderOptions(dirPath) +
            SnappyPrms.getGCOptions(dirPath) + " " +
            " -Dgemfire.DISALLOW_RESERVE_SPACE=true " +
            SnappyPrms.getLeaderLauncherProps() +
            " -spark.driver.extraClassPath=" + getStoreTestsJar() +
            " -spark.executor.extraClassPath=" + getStoreTestsJar();
        try {
          leadHost = HostHelper.getIPAddress().getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          String s = "Lead host not found...";
          throw new HydraRuntimeException(s, e);
        }
        SnappyBB.getBB().getSharedMap().put("leadHost_" + RemoteTestModule.getMyClientName()
            + "_" + RemoteTestModule.getMyVmid(), leadHost);
        SnappyBB.getBB().getSharedMap().put("leadPort_" + RemoteTestModule
            .getMyClientName() + "_" + RemoteTestModule.getMyVmid(), Integer.toString(leadPort));
        break;
      case WORKER:
        nodeLogDir = HostHelper.getLocalHost();
        String sparkLogDir = "SPARK_LOG_DIR=" + hd.getUserDir();
        String sparkWorkerDir = "SPARK_WORKER_DIR=" + hd.getUserDir();
        /*String sparkMasterHost = (String) SnappyBB.getBB().getSharedMap().get("sparkMasterHost");
        if (sparkMasterHost == null) {
          try {
            String masterHost = HostHelper.getIPAddress().getLocalHost().getHostName();
            sparkMasterHost = "SPARK_MASTER_HOST=" + masterHost;
            Log.getLogWriter().info("sparkMasterHost is : " + masterHost);
            SnappyBB.getBB().getSharedMap().put("masterHost", masterHost);
            SnappyBB.getBB().getSharedMap().put("sparkMasterHost", sparkMasterHost);
          } catch (UnknownHostException e) {
            String s = "spark master host not found...";
            throw new HydraRuntimeException(s, e);
          }
        }*/
        SnappyBB.getBB().getSharedMap().put("sparkLogDir" + "_" + snappyTest.getMyTid(), sparkLogDir);
        SnappyBB.getBB().getSharedMap().put("sparkWorkerDir" + "_" + snappyTest.getMyTid(), sparkWorkerDir);
        break;
    }
    SnappyBB.getBB().getSharedMap().put(logDir + "_" + RemoteTestModule.getMyVmid() + "_" +
        snappyTest.getMyTid(), nodeLogDir);
    SnappyBB.getBB().getSharedMap().put("logDir_" + RemoteTestModule.getMyClientName() + "_" +
        RemoteTestModule.getMyVmid(), dirPath);
    Log.getLogWriter().info("nodeLogDir is : " + nodeLogDir);
    if (returnNodeLogDir)
      SnappyBB.getBB().getSharedMap().put("newNodelogDir" + "_" + RemoteTestModule.getMyVmid() +
          "_" + snappyTest.getMyTid(), nodeLogDir);
    logDirExists = true;
  }

  protected static Set<String> getFileContents(String userKey, Set<String> fileContents) {
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith(userKey)) {
        String value = (String) SnappyBB.getBB().getSharedMap().get(key);
        fileContents.add(value);
      }
    }
    return fileContents;
  }

  protected String getTempDir(String dirName) {
    File log = new File(".");
    String dest = null;
    try {
      String dirString = log.getCanonicalPath();
      dest = log.getCanonicalPath() + File.separator + dirName;
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile " +
          "path  " + log + "\nError Message:" + e.getMessage());
    }
    File tempDir = new File(dest);
    if (!tempDir.exists()) tempDir.mkdir();
    return tempDir.getAbsolutePath();
  }

  protected static ArrayList<String> getWorkerFileContents(String userKey, ArrayList<String>
      fileContents) {
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith(userKey)) {
        String value = (String) SnappyBB.getBB().getSharedMap().get(key);
        fileContents.add(value);
      }
    }
    Log.getLogWriter().info("ArrayList contains : " + fileContents.toString());
    return fileContents;
  }

  protected static Set<File> getDirList(String userKey) {
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith(userKey)) {
        File value = (File) SnappyBB.getBB().getSharedMap().get(key);
        dirList.add(value);
      }
    }
    return dirList;
  }

  protected static String getLocatorsList(String userKey) {
    String locatorsList = null;
    if (isLongRunningTest) {
      locatorsList = getDataFromFile("locatorList");
      if (isUserConfTest) {
        locatorsList = SnappyPrms.getLocatorList();
        snappyTest.writeNodeConfigData("locatorList", locatorsList, false);
        writeLocatorConnectionInfo();
      }
      if (locatorsList == null) locatorsList = getLocatorList(userKey);
    } else locatorsList = getLocatorList(userKey);
    return locatorsList;
  }

  protected static String getLocatorList(String userKey) {
    String locatorsList = null;
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    Set<String> locatorHostPortList = new LinkedHashSet<>();
    for (String key : keys) {
      if (key.startsWith(userKey)) {
        String value = (String) SnappyBB.getBB().getSharedMap().get(key);
        locatorHostPortList.add(value);
      }
    }
    if (locatorHostPortList.size() == 0) {
      return "";
    }
    locatorsList = StringUtils.join(locatorHostPortList, ",");
    return locatorsList;
  }

  /**
   * Write the configuration data required to start the snappy locator/s in locators file under
   * conf directory at snappy build location.
   */
  public static void HydraTask_writeLocatorConfigData() {
    snappyTest.writeLocatorConfigData("locators", "locatorLogDir");
    if (isLongRunningTest) writeLocatorConnectionInfo();
  }

  /**
   * This method checks whether primary lead node is up and running in the cluster.
   *
   * @return Lead node status
   */
  public static boolean isPrimaryLeadUpAndRunning() {
    boolean isLeadNodeRunning = false;
    ResultSet rs;
    try {
      Connection conn = getLocatorConnection();
      rs = conn.createStatement().executeQuery("select STATUS, PID, HOST from sys.members where KIND='primary lead'");
      while (rs.next()) {
        Log.getLogWriter().info("Checking the Lead node status...");
        if (rs.getString("STATUS").equals("RUNNING")) {
          isLeadNodeRunning = true;
          SnappyBB.getBB().getSharedMap().put("PrimaryLeadPID", rs.getString("PID"));
          Log.getLogWriter().info("Primary Lead node is up and running...");
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got SQLException while running the query on sys.members: " + se.getMessage());
    }
    return isLeadNodeRunning;
  }

  /**
   * Write the configuration data required to start the snappy server/s in servers file under
   * conf directory at snappy build location.
   */
  public static void HydraTask_writeServerConfigData() {
    snappyTest.writeConfigData("servers", "serverLogDir");
  }

  /**
   * Write the configuration data required to start the snappy lead/s in leads file under conf
   * directory at snappy build location.
   */
  public static void HydraTask_writeLeadConfigData() {
    snappyTest.writeConfigData("leads", "leadLogDir");
    //if (isLongRunningTest) writeLeadHostInfo();
  }

  /**
   * Write the primary lead host,port info into the file(leadHost) under conf directory at
   * snappy build location. This is required for long running test scenarios where in  cluster
   * will be started  in first test and then rest all tests will use the same cluster
   */
  public static void HydraTask_writeLeadHostPortInfo() {
    writeLeadHostPortInfo();
  }

  /**
   * Write the spark master host info into the file(masterHost) under conf directory at
   * snappy build location. This is required for long running test scenarios where in cluster
   * will be started in first test and then rest all tests will use the same cluster
   */
  public static void HydraTask_writeMasterConnInfo() {
    writeSparkMasterConnInfo();
  }

  /**
   * Write the primary locator host,port info into the file under conf directory at
   * snappy build location. This is required for long running test scenarios where in cluster
   * will be started in first test and then rest all tests will use the same cluster
   */
  public static void HydraTask_writePrimaryLocatorHostPortInfo() {
    writePrimaryLocatorHostPortInfo();
  }

  /**
   * Write the locator list containing host:port information for all locators in the snappy
   * cluster into the file(locatorList) under conf directory at snappy build location.
   * This is required for long running test scenarios where in cluster
   * will be started in first test and then rest all tests will use the same cluster
   */
  public static void HydraTask_writeLocatorInfo() {
    writeLocatorInfo();
  }

  /**
   * Write the configuration data required to start the spark worker/s in slaves file and the
   * log directory locations in spark-env.sh file under conf directory at snappy build location.
   */
  public static void HydraTask_writeWorkerConfigData() {
    snappyTest.writeWorkerConfigData("slaves", "workerLogDir");
    snappyTest.writeConfigData("spark-env.sh", "sparkLogDir");
    snappyTest.writeConfigData("spark-env.sh", "sparkWorkerDir");
    //snappyTest.writeConfigData("spark-env.sh", "sparkMasterHost");
  }

  protected void writeConfigData(String fileName, String logDir) {
    String filePath = productConfDirPath + fileName;
    File file = new File(filePath);
    if (fileName.equalsIgnoreCase("spark-env.sh")) file.setExecutable(true);
    Set<String> fileContent = new LinkedHashSet<String>();
    fileContent = snappyTest.getFileContents(logDir, fileContent);
    if (fileContent.size() == 0) {
      String s = "No data found for writing to " + fileName + " file under conf directory";
      throw new TestException(s);
    }
    for (String s : fileContent) {
      snappyTest.writeToFile(s, file, true);
    }
  }

  protected void writeLocatorConfigData(String fileName, String logDir) {
    String filePath = productConfDirPath + fileName;
    File file = new File(filePath);
    String peerDiscoveryPort = null;
    Set<String> fileContent = new LinkedHashSet<String>();
    fileContent = snappyTest.getFileContents(logDir, fileContent);
    if (fileContent.size() == 0) {
      String s = "No data found for writing to " + fileName + " file under conf directory";
      throw new TestException(s);
    }
    String locatorsList = getLocatorsList("locators");
    for (String s : fileContent) {
      String[] splited = s.split(" ");
      for (String str : splited) {
        String peerDiscoveryPortString = "-peer-discovery-port=";
        if (str.contains(peerDiscoveryPortString)) {
          peerDiscoveryPort = str.substring(str.indexOf("=") + 1);
        }
      }
      String host = (String) SnappyBB.getBB().getSharedMap().get(peerDiscoveryPort);
      String replaceString = host + ":" + peerDiscoveryPort + ",";
      String newLocatorsList;
      if (locatorsList.contains(replaceString)) {
        newLocatorsList = locatorsList.replace(replaceString, "");
      } else {
        replaceString = host + ":" + peerDiscoveryPort;
        newLocatorsList = locatorsList.replace(replaceString, "");
      }
      String nodeLogDir = s.concat(" -locators=" + newLocatorsList);
      snappyTest.writeToFile(nodeLogDir, file, true);
    }
  }

  protected void writeNodeConfigData(String fileName, String nodeLogDir, boolean append) {
    String filePath = productConfDirPath + fileName;
    File file = new File(filePath);
    snappyTest.writeToFile(nodeLogDir, file, append);
  }

  protected void writeWorkerConfigData(String fileName, String logDir) {
    String filePath = productConfDirPath + fileName;
    File file = new File(filePath);
    ArrayList<String> fileContent = new ArrayList<>();
    fileContent = snappyTest.getWorkerFileContents(logDir, fileContent);
    if (fileContent.size() == 0) {
      String s = "No data found for writing to " + fileName + " file under conf directory";
      throw new TestException(s);
    }
    for (String s : fileContent) {
      snappyTest.writeToFile(s, file, true);
    }
  }

  /**
   * Returns all network locator endpoints from the {@link
   * SnappyNetworkServerBB} map, a possibly empty list.  This includes all
   * network servers that have ever started, regardless of their distributed
   * system or current active status.
   */
  public static List getNetworkLocatorEndpoints() {
    return getEndpoints("locator");
  }

  /**
   * Returns all network server endpoints from the {@link
   * SnappyNetworkServerBB} map, a possibly empty list.  This includes all
   * network servers that have ever started, regardless of their distributed
   * system or current active status.
   */
  public static List getNetworkServerEndpoints() {
    return getEndpoints("server");
  }

  protected HashMap getclientHostPort() {
    HashMap<String, Integer> hostPort = new HashMap<String, Integer>();
    String endpoint = null;
    List<String> endpoints = getNetworkLocatorEndpoints();
    if (endpoints.size() == 0) {
      if (isLongRunningTest) {
        endpoints = getLocatorEndpointFromFile();
      }
    }
    endpoint = endpoints.get(0);
    String host = endpoint.substring(0, endpoint.indexOf(":"));
    Log.getLogWriter().info("Client Host is:" + host);
    String port = endpoint.substring(endpoint.indexOf(":") + 1);
    int clientPort = Integer.parseInt(port);
    Log.getLogWriter().info("Client Port is :" + clientPort);
    hostPort.put(host, clientPort);
    return hostPort;
  }

  /**
   * Returns all endpoints of the given type.
   */
  private static synchronized List<String> getEndpoints(String type) {
    List<String> endpoints = new ArrayList();
    Set<String> keys = SnappyNetworkServerBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith(type.toString())) {
        String endpoint = (String) SnappyNetworkServerBB.getBB().getSharedMap().getMap().get(key);
        endpoints.add(endpoint);
      }
    }
    Log.getLogWriter().info("Returning endpoint list: " + endpoints);
    return endpoints;
  }


  /**
   * Returns PIDs for all the processes started in the test, e.g. locator, server, lead .
   */
  private static synchronized Set<String> getPidList() {
    Set<String> pidList = new HashSet<>();
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith("pid")) {
        String pid = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
        pidList.add(pid);
      }
    }
    Log.getLogWriter().info("Returning pid list: " + pidList);
    return pidList;
  }


  /**
   * Returns hostname of the process
   */
  protected static synchronized String getPidHost(String pid) {
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    String pidHost = null;
    for (String key : keys) {
      if (key.startsWith("host") && key.contains(pid)) {
        pidHost = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
      }
    }
    Log.getLogWriter().info("PID Host for : " + pid + " : " + pidHost);
    return pidHost;
  }


  /**
   * Returns primary lead port .
   */
  private static synchronized String getPrimaryLeadPort(String clientName) {
    List<String> portList = new ArrayList();
    String port = null;
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith("leadPort_") && key.contains(clientName)) {
        port = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
      }
    }
    Log.getLogWriter().info("Returning primary lead port: " + port);
    return port;
  }


  protected void initHydraThreadLocals() {
    this.connection = getConnection();
  }

  protected Connection getConnection() {
    Connection connection = (Connection) localconnection.get();
    return connection;
  }

  protected void setConnection(Connection connection) {
    localconnection.set(connection);
  }

  protected void updateHydraThreadLocals() {
    setConnection(this.connection);
  }

  public static void HydraTask_getClientConnection_Snappy() throws SQLException {
    SnappyTest st = new SnappyTest();
    st.connectThinClient();
    st.updateHydraThreadLocals();
  }

  private void connectThinClient() throws SQLException {
    connection = getLocatorConnection();
  }

  public static Connection getClientConnection() {
    SnappyTest st = new SnappyTest();
    st.initHydraThreadLocals();
    return st.getConnection();
  }

  public static void HydraTask_getClientConnection() throws SQLException {
    getLocatorConnection();
  }

  /**
   * Mandatory to use this method in case of HA test.
   * As per current implementation, for starting the locator snappy-locators.sh script is used,
   * which starts the locators based on the data in locators conf file.
   * In HA test, the framework deletes the old locators file and creates the new one with the
   * config data specific to locator which is getting recycled.
   * So, we need to backup the original locators conf file. This will be required at the end of the
   * test for stopping all locators which have been started in the test.
   **/
  public static synchronized void backUpLocatorConfigData() {
    snappyTest.copyConfigData("locators");
  }

  /**
   * Mandatory to use this method in case of HA test.
   * As per current implementation, for starting the server snappy-servers.sh script is used, which starts
   * the servers based on the data in servers conf file.
   * In HA test, the framework deletes the old servers file and creates the new one with the config data specific
   * to server which is getting recycled.
   * So, we need to backup the original servers conf file. This will be required at the end of the test for stopping all servers
   * which have been started in the test.
   **/
  public static synchronized void backUpServerConfigData() {
    snappyTest.copyConfigData("servers");
  }

  /**
   * Mandatory to use this method in case of HA test.
   * As per current implementation, for starting the locator snappy-locators.sh script is used,
   * which starts the locators based on the data in locators conf file.
   * In HA test, the framework deletes the old locators file and creates the new one with the
   * config data specific to locator which is getting recycled.
   * So, we need to restore the original locators conf file. This will be required at the end of
   * the test for stopping all locators which have been started in the test.
   **/
  public static synchronized void restoreLocatorConfigData() {
    if(snappyTest == null) snappyTest = new SnappyTest();
    snappyTest.restoreConfigData("locators");
  }

  /**
   * Mandatory to use this method in case of HA test.
   * As per current implementation, for starting the server snappy-servers.sh script is used, which starts
   * the servers based on the data in servers conf file.
   * In HA test, the framework deletes the old servers file and creates the new one with the config data specific
   * to server which is getting recycled.
   * So, we need to restore the original servers conf file. This will be required at the end of the test for stopping all servers
   * which have been started in the test.
   **/
  public static synchronized void restoreServerConfigData() {
    if(snappyTest == null) snappyTest = new SnappyTest();
    snappyTest.restoreConfigData("servers");
  }

  /**
   * Mandatory to use this method in case of HA test.
   * As per current implementation, for starting the lead members, snappy-leads.sh script is used, which starts
   * the lead members based on the data in leads conf file.
   * In HA test, the framework deletes the old leads file and creates the new one with the config data specific
   * to lead member which is getting recycled.
   * So, we need to restore the original leads conf file. This will be required at the end of the test for stopping all leads
   * which have been started in the test.
   **/
  public static synchronized void restoreLeadConfigData() {
    if(snappyTest == null) snappyTest = new SnappyTest();
    snappyTest.restoreConfigData("leads");
  }

  /**
   * Mandatory to use this method in case of HA test.
   * As per current implementation, for starting the lead members, snappy-leads.sh script is used, which starts
   * the lead members based on the data in leads conf file.
   * In HA test, the framework deletes the old leads file and creates the new one with the config data specific
   * to lead member which is getting recycled.
   * So, we need to backup the original leads conf file. This will be required at the end of the test for stopping all leads
   * which have been started in the test.
   **/
  public static synchronized void backUpLeadConfigData() {
    snappyTest.copyConfigData("leads");
  }

  protected void copyConfigData(String fileName) {
    if (doneCopying && !forceCopy) return;
    String filePath = productConfDirPath + fileName;
    File srcFile = new File(filePath);
    try {
      File destDir = new File(".");
      FileUtils.copyFileToDirectory(srcFile, destDir);
      Log.getLogWriter().info("Done copying " + fileName + " file from " + srcFile + " to " + destDir.getAbsolutePath());
    } catch (IOException e) {
      throw new TestException("Error occurred while copying data from file: " + srcFile + "\n " + e.getMessage());
    }
    doneCopying = true;
  }

  protected void restoreConfigData(String fileName) {
    if (doneRestore) return;
    String filePath = productConfDirPath + fileName;
    File srcDir = new File(".");
    File srcFile = null, destDir = null;
    try {
      String srcFilePath = srcDir.getCanonicalPath() + File.separator + fileName;
      srcFile = new File(srcFilePath);
      destDir = new File(filePath);
      if (destDir.exists()) destDir.delete();
      destDir = new File(productConfDirPath);
      FileUtils.copyFileToDirectory(srcFile, destDir);
      Log.getLogWriter().info("Done restoring " + fileName + " file from " + srcFile.getAbsolutePath() + " to " + destDir);
    } catch (IOException e) {
      throw new TestException("Error occurred while copying data from file: " + srcFile + "\n " + e.getMessage());
    }
    doneRestore = true;
  }

  protected void copyUserConfs(String fileName) {
    String filePath = productConfDirPath + fileName;
    String userConfLocation = SnappyPrms.getUserConfLocation();
    if (userConfLocation == null) {
      throw new TestException("User conf location is not provided for starting the cluster..");
    }
    File srcDir = new File(userConfLocation);
    File srcFile = null, destDir = null;
    try {
      String srcFilePath = srcDir.getCanonicalPath() + File.separator + fileName;
      srcFile = new File(srcFilePath);
      destDir = new File(filePath);
      if (destDir.exists()) destDir.delete();
      destDir = new File(productConfDirPath);
      FileUtils.copyFileToDirectory(srcFile, destDir);
      Log.getLogWriter().info("Done copying " + fileName + " file from " + srcFile.getAbsolutePath() + " to " + destDir);
    } catch (IOException e) {
      throw new TestException("Error occurred while copying conf from file: " + srcFile + "\n " + e.getMessage());
    }
  }

  /**
   * Mandatory to use this method in case of stating the cluster with user specified confs for SnappyData members.
   * These confs includes servers, leads and locators files for starting data store nodes, snappydata leader and
   * locator nodes in embedded mode and additionaly slaves conf in case of smart connector mode.
   **/
  public static synchronized void HydraTask_copyUserConfs() {
    snappyTest.copyUserConfs("servers");
    snappyTest.copyUserConfs("leads");
    snappyTest.copyUserConfs("locators");
    if (useSmartConnectorMode) {
      snappyTest.copyUserConfs("slaves");
    }
  }


  public static synchronized void HydraTask_copyDiskFiles() {
    if (diskDirExists) return;
    else {
      String dirName = snappyTest.generateLogDirName();
      File destDir = new File(dirName);
      String diskDirName = dirName.substring(0, dirName.lastIndexOf("_")) + "_disk";
      File dir = new File(diskDirName);
      for (File srcFile : dir.listFiles()) {
        try {
          if (srcFile.isDirectory()) {
            FileUtils.copyDirectoryToDirectory(srcFile, destDir);
            Log.getLogWriter().info("Done copying diskDirFile directory from: " + srcFile + " to " + destDir);
          } else {
            FileUtils.copyFileToDirectory(srcFile, destDir);
            Log.getLogWriter().info("Done copying diskDirFile from: " + srcFile + " to " + destDir);
          }
        } catch (IOException e) {
          throw new TestException("Error occurred while copying data from file: " + srcFile + "\n " + e.getMessage());
        }
      }
      diskDirExists = true;
    }
  }

  public static synchronized void HydraTask_copyDiskFiles_gemToSnappyCluster() {
    Set<File> myDirList = getDirList("dirName_");
    if (diskDirExists) return;
    else {
      String dirName = snappyTest.generateLogDirName();
      File destDir = new File(dirName);
      String[] splitedName = RemoteTestModule.getMyClientName().split("snappy");
      String newName = splitedName[1];
      File currentDir = new File(".");
      for (File srcFile1 : currentDir.listFiles()) {
        if (!doneCopying) {
          if (srcFile1.getAbsolutePath().contains(newName) && srcFile1.getAbsolutePath().contains("_disk")) {
            if (myDirList.contains(srcFile1)) {
              Log.getLogWriter().info("List contains entry for the file... " + myDirList.toString());
            } else {
              SnappyBB.getBB().getSharedMap().put("dirName_" + RemoteTestModule.getMyPid() + "_" + snappyTest.getMyTid(), srcFile1);
              File dir = new File(srcFile1.getAbsolutePath());
              Log.getLogWriter().info("Match found for file: " + srcFile1.getAbsolutePath());
              for (File srcFile : dir.listFiles()) {
                try {
                  if (srcFile.isDirectory()) {
                    FileUtils.copyDirectoryToDirectory(srcFile, destDir);
                    Log.getLogWriter().info("Done copying diskDirFile directory from ::" + srcFile + "to " + destDir);
                  } else {
                    FileUtils.copyFileToDirectory(srcFile, destDir);
                    Log.getLogWriter().info("Done copying diskDirFile from ::" + srcFile + "to " + destDir);
                  }
                  doneCopying = true;
                } catch (IOException e) {
                  throw new TestException("Error occurred while copying data from file: " + srcFile + "\n " + e.getMessage());
                }
              }
            }
          }
        }
      }
      diskDirExists = true;
    }
  }

  public static void HydraTask_doDMLOp() {
    snappyTest.doDMLOp();
  }

  protected void doDMLOp() {
    runGemXDQuery = true;
    try {
      Connection conn = getLocatorConnection();
      doDMLOp(conn);
      closeConnection(conn);
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
  }

  protected void doDMLOp(Connection conn) {
    //No derby connection required for snappyTest. So providign the bull connection to existing methods
    Connection dConn = null;
//        protected void doDMLOp(Connection dConn, Connection gConn) {
    //perform the opeartions
    //randomly select a table to perform dml
    //randomly select an operation to perform based on the dmlStmt (insert, update, delete, select)
    Log.getLogWriter().info("doDMLOp-performing dmlOp, myTid is " + getMyTid());
    int table = dmlTables[random.nextInt(dmlTables.length)]; //get random table to perform dml
    DMLStmtIF dmlStmt = dmlFactory.createDMLStmt(table); //dmlStmt of a table
    int numOfOp = random.nextInt(5) + 1;
    int size = 1;

    String operation = TestConfig.tab().stringAt(SQLPrms.dmlOperations);
    Log.getLogWriter().info("doDMLOp-operation=" + operation + "  numOfOp=" + numOfOp);
    if (operation.equals("insert")) {
      for (int i = 0; i < numOfOp; i++) {
        dmlStmt.insert(dConn, conn, size);
        commit(conn);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.insertCounter);
      }
    } else if (operation.equals("put")) {
      for (int i = 0; i < numOfOp; i++) {
        dmlStmt.put(dConn, conn, size);
        commit(conn);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.insertCounter);
      }
    } else if (operation.equals("update")) {
      for (int i = 0; i < numOfOp; i++) {
        dmlStmt.update(dConn, conn, size);
        commit(conn);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.updateCounter);
      }
    } else if (operation.equals("delete")) {
      dmlStmt.delete(dConn, conn);
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.deleteCounter);
    } else if (operation.equals("query")) {
      dmlStmt.query(dConn, conn);
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.queryCounter);

    } else {
      throw new TestException("Unknown entry operation: " + operation);
    }
    commit(conn);
  }

  /**
   * Writes the locator host:port information to the locatorConnInfo file under conf directory.
   */
  protected static void writeLocatorConnectionInfo() {
    List<String> endpoints = validateLocatorEndpointData();
    snappyTest.writeNodeConfigData("locatorConnInfo", endpoints.get(0), false);
  }

  /**
   * Writes the comma seperated host:port list of all locators started in a cluster to the
   * locatorList file under conf directory.
   */
  protected static void writeLocatorInfo() {
    String locatorList = getLocatorsList("locators");
    snappyTest.writeNodeConfigData("locatorList", locatorList, false);
  }

  /**
   * Writes the lead host information to the leadHost file under conf directory.
   */
  protected static void writeLeadHostPortInfo() {
    leadHost = snappyTest.getLeadHost();
    snappyTest.writeNodeConfigData("leadHost", leadHost, false);
    String leadPort = snappyTest.getLeadPort();
    snappyTest.writeNodeConfigData("leadPort", leadPort, false);
  }

  /**
   * Writes the master host information to the masterHost file under conf directory.
   */
  protected static void writeSparkMasterConnInfo() {
    String masterHost = getSparkMasterHost();
    Log.getLogWriter().info("masterHost is : " + masterHost);
    snappyTest.writeNodeConfigData("masterHost", masterHost, false);
  }

  protected static void writePrimaryLocatorHostPortInfo() {
    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    snappyTest.writeNodeConfigData("primaryLocatorHost", primaryLocatorHost, false);
    snappyTest.writeNodeConfigData("primaryLocatorPort", primaryLocatorPort, false);
  }

  protected static String getDataFromFile(String fileName) {
    File logFile = getLogFile(fileName);
    String data = null;
    try {
      BufferedReader br = readDataFromFile(logFile);
      String str = null;
      while ((str = br.readLine()) != null) {
        data = str;
      }
      br.close();
    } catch (IOException e) {
      String s = "Problem while reading the file : " + logFile.getAbsolutePath();
      throw new TestException(s, e);
    }
    return data;
  }

  protected static BufferedReader readDataFromFile(File filename) {
    BufferedReader br = null;
    try {
      FileInputStream fis = new FileInputStream(filename);
      br = new BufferedReader(new InputStreamReader(fis));
    } catch (FileNotFoundException e) {
      String s = "Unable to find file: " + filename.getAbsolutePath();
      throw new TestException(s);
    }
    return br;
  }

  protected static File getLogFile(String filename) {
    String dest = productConfDirPath + filename;
    File logFile = new File(dest);
    if (!logFile.exists()) try {
      logFile.createNewFile();
    } catch (IOException e) {
      String s = "Unable to create file: " + logFile.getAbsolutePath();
      throw new TestException(s);
    }
    return logFile;
  }

  protected static List<String> getLocatorEndpointFromFile() {
    List<String> endpoints = new ArrayList<String>();
    File logFile = getLogFile("locatorConnInfo");
    try {
      BufferedReader br = readDataFromFile(logFile);
      String str = null;
      while ((str = br.readLine()) != null) {
        endpoints.add(str);
      }
      br.close();
    } catch (IOException e) {
      String s = "Problem while reading the file : " + logFile.getAbsolutePath();
      throw new TestException(s, e);
    }
    return endpoints;
  }

  public static List<String> validateLocatorEndpointData() {
    List<String> endpoints = getNetworkLocatorEndpoints();
    if (endpoints.size() == 0) {
      if (isLongRunningTest) {
        endpoints = getLocatorEndpointFromFile();
        if (isUserConfTest) {
          endpoints = Arrays.asList(SnappyPrms.getLocatorList().split(","));
          snappyTest.writeNodeConfigData("locatorConnInfo", endpoints.get(0), true);
        }
      }
    }
    if (endpoints.size() == 0) {
      String s = "No network locator endpoints found";
      throw new TestException(s);
    }
    return endpoints;
  }

  protected static List<String> validateServerEndpointData() {
    List<String> endpoints = getNetworkServerEndpoints();
    /*if (endpoints.size() == 0) {
      if (isLongRunningTest) {
        endpoints = getServerConnection();
      }
    }*/
    if (endpoints.size() == 0) {
      String s = "No network server endpoints found";
      throw new TestException(s);
    }
    return endpoints;
  }

  public static synchronized void setConnPoolType() {
    if (!SnappyBB.getBB().getSharedMap().containsKey("connPoolType"))
      SnappyBB.getBB().getSharedMap().put("connPoolType", SnappyConnectionPoolPrms
          .getConnPoolType(connPool));
    connPoolType = (int) SnappyBB.getBB().getSharedMap().get("connPoolType");
  }

  /**
   * Gets Client connection.
   */
  public static Connection getLocatorConnection() throws SQLException {
    Connection conn = null;

    if (!SnappyBB.getBB().getSharedMap().containsKey("connPoolType"))
      setConnPoolType();
    else
      connPoolType = (int) SnappyBB.getBB().getSharedMap().get("connPoolType");

    if (connPoolType == 0) {
      conn = HikariConnectionPool.getConnection();
    } else if (connPoolType == 1) {
      conn = TomcatConnectionPool.getConnection();
    } else {
      List<String> endpoints = validateLocatorEndpointData();

      if (!runGemXDQuery) {
        String url = "jdbc:snappydata://" + endpoints.get(0);
        Log.getLogWriter().info("url is " + url);
        conn = getConnection(url, "io.snappydata.jdbc.ClientDriver");
      } else {
        String url = "jdbc:gemfirexd://" + endpoints.get(0);
        Log.getLogWriter().info("url is " + url);
        conn = getConnection(url, "io.snappydata.jdbc.ClientDriver");
      }
    }
    return conn;
  }

  public static Connection getLocatorConnectionUsingProps() throws
      SQLException {
    List<String> endpoints = validateLocatorEndpointData();
    Properties props = new Properties();
    Vector connPropsList = SnappyPrms.getConnPropsList();
    for (int i = 0; i < connPropsList.size(); i++) {
      String conProp = (String) connPropsList.elementAt(i);
      String conPropKey = conProp.substring(0, conProp.indexOf("="));
      String conPropValue = conProp.substring(conProp.indexOf("=") + 1);
      props.setProperty(conPropKey, conPropValue);
    }
    Connection conn = null;
    String url = "jdbc:snappydata://" + endpoints.get(0) + "/";
    conn = getConnection(url, "io.snappydata.jdbc.ClientDriver", props);
    return conn;
  }

  public static Connection getServerConnection() throws SQLException {
    List<String> endpoints = validateServerEndpointData();
    Connection conn = null;
    if (!runGemXDQuery) {
      String url = "jdbc:snappydata://" + endpoints.get(0);
      Log.getLogWriter().info("url is " + url);
      conn = getConnection(url, "io.snappydata.jdbc.ClientDriver");
    } else {
      String url = "jdbc:gemfirexd://" + endpoints.get(0);
      Log.getLogWriter().info("url is " + url);
      conn = getConnection(url, "io.snappydata.jdbc.ClientDriver");
    }
    return conn;
  }

  private static Connection getConnection(String protocol, String driver) throws SQLException {
    Log.getLogWriter().info("Creating connection using " + driver + " with " + protocol);
    loadDriver(driver);
    Connection conn = DriverManager.getConnection(protocol);
    return conn;
  }

  private static Connection getConnection(String protocol, String driver, Properties props)
      throws
      SQLException {
    Log.getLogWriter().info("Creating connection using " + driver + " with " + protocol +
        " and user specified properties list: = " + props.toString());
    loadDriver(driver);
    Connection conn = DriverManager.getConnection(protocol, props);
    return conn;
  }

  public static void closeConnection(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to release the connection " + TestHelper.getStackTrace(e));
    }
  }

  public void commit(Connection conn) {
    if (conn == null) return;
    try {
      Log.getLogWriter().info("committing the ops.. ");
      conn.commit();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }

  /**
   * The JDBC driver is loaded by loading its class.  If you are using JDBC 4.0
   * (Java SE 6) or newer, JDBC drivers may be automatically loaded, making
   * this code optional.
   * <p/>
   * In an embedded environment, any static Derby system properties
   * must be set before loading the driver to take effect.
   */
  public static void loadDriver(String driver) {
    try {
      Class.forName(driver).newInstance();
    } catch (ClassNotFoundException e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new TestException(s, e);
    } catch (InstantiationException e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new TestException(s, e);
    } catch (IllegalAccessException e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new TestException(s, e);
    }
  }

  public static void runQuery() throws SQLException {
    Connection conn = getLocatorConnection();
    // String query1 = "SELECT count(*) FROM airline";
    Vector<String> queryVect = SnappyPrms.getQueryList();
    for (String query : queryVect) {
      long startTime = System.currentTimeMillis();
      ResultSet rs = conn.createStatement().executeQuery(query);
      long endTime = System.currentTimeMillis();
      while (rs.next()) {
        // Log.getLogWriter().info("Qyery executed successfully and query result is ::" + rs.getLong(1));
      }
      Log.getLogWriter().info("Time to executed the query::  " + ((endTime - startTime) / 1000) + " s");
    }
    closeConnection(conn);
  }

  public static void HydraTask_writeCountQueryResultsToSnappyBB() {
    snappyTest.writeCountQueryResultsToBB();
  }

  public static void HydraTask_writeUpdatedCountQueryResultsToSnappyBB() {
    snappyTest.writeUpdatedCountQueryResultsToBB();
  }

  public static void HydraTask_verifyUpdateOpOnSnappyCluster() {
    snappyTest.updateQuery();
  }

  public static void HydraTask_verifyDeleteOpOnSnappyCluster() {
    snappyTest.deleteQuery();
  }

  protected void deleteQuery() {
    runGemXDQuery = true;
    try {
      Connection conn = getLocatorConnection();
      String query1 = "select count(*) from trade.txhistory";
      long rowCountBeforeDelete = runSelectQuery(conn, query1);
      String query2 = "delete from trade.txhistory where type = 'buy'";
      int rowCount = conn.createStatement().executeUpdate(query2);
      commit(conn);
      Log.getLogWriter().info("Deleted " + rowCount + " rows in trade.txhistory table in snappy.");
      String query3 = "select count(*) from trade.txhistory";
      String query4 = "select count(*) from trade.txhistory where type = 'buy'";
      long rowCountAfterDelete = 0, rowCountForquery4;
      rowCountAfterDelete = runSelectQuery(conn, query3);
      Log.getLogWriter().info("RowCountBeforeDelete: " + rowCountBeforeDelete);
      Log.getLogWriter().info("RowCountAfterDelete: " + rowCountAfterDelete);
      long expectedRowCountAfterDelete = rowCountBeforeDelete - rowCount;
      Log.getLogWriter().info("ExpectedRowCountAfterDelete: " + expectedRowCountAfterDelete);
      if (!(rowCountAfterDelete == expectedRowCountAfterDelete)) {
        String misMatch = "Test Validation failed due to mismatch in countQuery results for table trade.txhistory. countQueryResults after performing delete ops should be : " + expectedRowCountAfterDelete + ", but it is : " + rowCountAfterDelete;
        throw new TestException(misMatch);
      }
      rowCountForquery4 = runSelectQuery(conn, query4);
      Log.getLogWriter().info("Row count for query: select count(*) from trade.txhistory where type = 'buy' is: " + rowCountForquery4);
      if (!(rowCountForquery4 == 0)) {
        String misMatch = "Test Validation failed due to wrong row count value for table trade.txhistory. Expected row count value is : 0, but found : " + rowCountForquery4;
        throw new TestException(misMatch);
      }
      closeConnection(conn);
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
  }

  protected void updateQuery() {
    runGemXDQuery = true;
    try {
      Connection conn = getLocatorConnection();
      String query1 = "select count(*) from trade.customers";
      long rowCountBeforeUpdate = runSelectQuery(conn, query1);
      String query2 = "update trade.customers set addr = 'Pune'";
      Log.getLogWriter().info("update query is: " + query2);
      int rowCount = conn.createStatement().executeUpdate(query2);
      commit(conn);
      Log.getLogWriter().info("Updated " + rowCount + " rows in trade.customers table in snappy.");
      String query4 = "select count(*) from trade.customers";
      String query5 = "select count(*) from trade.customers where addr != 'Pune'";
      String query6 = "select count(*) from trade.customers where addr = 'Pune'";
      long rowCountAfterUpdate = 0, rowCountForquery5 = 0, rowCountForquery6 = 0;
      rowCountAfterUpdate = runSelectQuery(conn, query4);
      if (!(rowCountBeforeUpdate == rowCountAfterUpdate)) {
        String misMatch = "Test Validation failed due to mismatch in countQuery results for table trade.customers. countQueryResults after performing update ops should be : " + rowCountBeforeUpdate + " , but it is : " + rowCountAfterUpdate;
        throw new TestException(misMatch);
      }
      rowCountForquery6 = runSelectQuery(conn, query6);
      Log.getLogWriter().info("RowCountBeforeUpdate:" + rowCountBeforeUpdate);
      Log.getLogWriter().info("RowCountAfterUpdate:" + rowCountAfterUpdate);
      if (!(rowCountForquery6 == rowCount)) {
        String misMatch = "Test Validation failed due to mismatch in row count value for table trade.customers. Row count after performing update ops should be : " + rowCount + " , but it is : " + rowCountForquery6;
        throw new TestException(misMatch);
      }
      rowCountForquery5 = runSelectQuery(conn, query5);
      Log.getLogWriter().info("Row count for query: select count(*) from trade.customers where addr != 'Pune' is: " + rowCountForquery5);
      if (!(rowCountForquery5 == 0)) {
        String misMatch = "Test Validation failed due to wrong row count value for table trade.customers. Expected row count value is : 0, but found : " + rowCountForquery5;
        throw new TestException(misMatch);
      }
      closeConnection(conn);
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
  }

  protected static Long runSelectQuery(Connection conn, String query) {
    long rowCount = 0;
    try {
      ResultSet rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {
        rowCount = rs.getLong(1);
        Log.getLogWriter().info(query + " query executed successfully and query result is : " + rs.getLong(1));
      }
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return rowCount;
  }

  protected static void writeCountQueryResultsToBB() {
    runGemXDQuery = true;
    try {
      Connection conn = getLocatorConnection();
      String selectQuery = "select count(*) from ";
      ArrayList<String[]> tables = (ArrayList<String[]>) SQLBB.getBB().getSharedMap().get("tableNames");
      for (String[] table : tables) {
        String schemaTableName = table[0] + "." + table[1];
        String query = selectQuery + schemaTableName.toLowerCase();
        getCountQueryResult(conn, query, schemaTableName);
      }
      closeConnection(conn);
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
  }

  protected static void writeUpdatedCountQueryResultsToBB() {
    runGemXDQuery = true;
    try {
      Connection conn = getLocatorConnection();
      String selectQuery = "select count(*) from ";
      ArrayList<String[]> tables = (ArrayList<String[]>) SQLBB.getBB().getSharedMap().get("tableNames");
      for (String[] table : tables) {
        String schemaTableName = table[0] + "." + table[1];
        String query = selectQuery + schemaTableName.toLowerCase();
        getCountQueryResult(conn, query, schemaTableName + "AfterOps");
      }
      closeConnection(conn);
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
  }

  protected static void getCountQueryResult(Connection conn, String query, String tableName) {
    try {
      ResultSet rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {
        Log.getLogWriter().info("Query:: " + query + "\nResult in Snappy:: " + rs.getLong(1));
        SnappyBB.getBB().getSharedMap().put(tableName, rs.getLong(1));
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }

  public static void HydraTask_verifyCountQueryResults() {
    ArrayList<String[]> tables = (ArrayList<String[]>) SQLBB.getBB().getSharedMap().get("tableNames");
    for (String[] table1 : tables) {
      String schemaTableName = table1[0] + "." + table1[1];
      String tableName = schemaTableName;
      Long countQueryResultInSnappy = (Long) SnappyBB.getBB().getSharedMap().get(tableName);
      Log.getLogWriter().info("countQueryResult for table " + tableName + " in Snappy: " + countQueryResultInSnappy);
      Long countQueryResultInGemXD = (Long) SQLBB.getBB().getSharedMap().get(tableName);
      Log.getLogWriter().info("countQueryResult for table " + tableName + " in GemFireXD: " + countQueryResultInGemXD);
      if (!(countQueryResultInSnappy.equals(countQueryResultInGemXD))) {
        String misMatch = "Test Validation failed as countQuery result for table  " + tableName + " in GemFireXD: " + countQueryResultInGemXD + " did not match not match with countQuery result for table " + tableName + " in Snappy: " + countQueryResultInSnappy;
        throw new TestException(misMatch);
      }
    }
  }

  public static void HydraTask_verifyInsertOpOnSnappyCluster() {
    snappyTest.insertQuery();
  }

  public static String getCurrentTimeStamp() {
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Date now = new Date();
    String strDate = sdfDate.format(now);
    return strDate;
  }

  protected void insertQuery() {
    runGemXDQuery = true;
    try {
      Connection conn = getLocatorConnection();
      String query1 = "select count(*) from trade.txhistory";
      long rowCountBeforeInsert = runSelectQuery(conn, query1);
      for (int i = 0; i < 100; i++) {
        int cid = random.nextInt(20000);
        int tid = random.nextInt(40);
        int oid = random.nextInt(20000);
        int sid = random.nextInt(20000);
        int qty = random.nextInt(20000);
        int price = random.nextInt(Integer.MAX_VALUE) * 0;
        String ordertime = getCurrentTimeStamp();
        String type = "buy";
        String query2 = "insert into trade.txhistory (cid, oid, sid, qty, price, ordertime, type, tid )values (" + cid + ", " + oid + ", " + sid + ", " + qty + ", " + price + ", '" + ordertime + "', '" + type + "', " + tid + ")";
        int rowCount = conn.createStatement().executeUpdate(query2);
        commit(conn);
        Log.getLogWriter().info("Inserted " + rowCount + " rows into trade.txhistory table in snappy with values : " + "(" + cid + ", " + oid + ", " + sid + ", " + qty + ", " + price + ", '" + ordertime + "', '" + type + "', " + tid + ")");
      }
      String query3 = "select count(*) from trade.txhistory";
      long rowCountAfterInsert = 0;
      rowCountAfterInsert = runSelectQuery(conn, query3);
      Log.getLogWriter().info("RowCountBeforeInsert: " + rowCountBeforeInsert);
      Log.getLogWriter().info("RowCountAfterInsert: " + rowCountAfterInsert);
      long expectedRowCountAfterInsert = rowCountBeforeInsert + 100;
      Log.getLogWriter().info("ExpectedRowCountAfterInsert: " + expectedRowCountAfterInsert);
      if (!(rowCountAfterInsert == expectedRowCountAfterInsert)) {
        String misMatch = "Test Validation failed due to mismatch in countQuery results for table trade.txhistory. countQueryResults after performing insert ops should be : " + expectedRowCountAfterInsert + ", but it is : " + rowCountAfterInsert;
        throw new TestException(misMatch);
      }
      closeConnection(conn);
    } catch (SQLException e) {
      throw new TestException("Not able to get connection " + TestHelper.getStackTrace(e));
    }
  }

  protected void writeToFile(String logDir, File file, boolean append) {
    try {
      FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(logDir);
      bw.newLine();
      bw.close();
    } catch (IOException e) {
      throw new TestException("Error occurred while writing to a file: " + file +
          e.getMessage());
    }
  }

  /**
   * Executes user SQL scripts.
   */
  public static synchronized void HydraTask_executeSQLScripts() {
    Vector scriptNames, dataLocationList = null, persistenceModeList = null,
        colocateWithOptionList = null, partitionByOptionList = null, numPartitionsList =
        null, redundancyOptionList = null, recoverDelayOptionList = null,
        maxPartitionSizeList = null, evictionByOptionList = null;
    File log = null, logFile = null;
    scriptNames = SnappyPrms.getSQLScriptNames();
    if (scriptNames == null) {
      String s = "No Script names provided for executing in the Hydra TASK";
      throw new TestException(s);
    }
    try {
      dataLocationList = SnappyPrms.getDataLocationList();
      persistenceModeList = SnappyPrms.getPersistenceModeList();
      colocateWithOptionList = SnappyPrms.getColocateWithOptionList();
      partitionByOptionList = SnappyPrms.getPartitionByOptionList();
      numPartitionsList = SnappyPrms.getNumPartitionsList();
      redundancyOptionList = SnappyPrms.getRedundancyOptionList();
      recoverDelayOptionList = SnappyPrms.getRecoverDelayOptionList();
      maxPartitionSizeList = SnappyPrms.getMaxPartitionSizeList();
      evictionByOptionList = SnappyPrms.getEvictionByOptionList();
      if (dataLocationList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the dataLocationList for the  " +
            "scripts for which no dataLocation is specified.");
        while (dataLocationList.size() != scriptNames.size())
          dataLocationList.add(" ");
      }
      if (persistenceModeList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"sync\" parameter in the persistenceModeList for" +
            "  the scripts for which no persistence mode is specified.");
        while (persistenceModeList.size() != scriptNames.size())
          persistenceModeList.add("sync");
      }
      if (colocateWithOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"none\" parameter in the colocateWithOptionList " +
            " for the scripts for which no COLOCATE_WITH Option is specified.");
        while (colocateWithOptionList.size() != scriptNames.size())
          colocateWithOptionList.add("none");
      }
      if (partitionByOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the partitionByOptionList for " +
            " the scripts for which no PARTITION_BY option is specified.");
        while (partitionByOptionList.size() != scriptNames.size())
          partitionByOptionList.add(" ");
      }
      if (numPartitionsList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"113\" parameter in the partitionByOptionsList " +
            "for  the scripts for which no BUCKETS option is specified.");
        while (numPartitionsList.size() != scriptNames.size())
          numPartitionsList.add("113");
      }
      if (redundancyOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the redundancyOptionList for " +
            "the  scripts for which no REDUNDANCY option is specified.");
        while (redundancyOptionList.size() != scriptNames.size())
          redundancyOptionList.add(" ");
      }
      if (recoverDelayOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the recoverDelayOptionList for" +
            "  the scripts for which no RECOVER_DELAY option is specified.");
        while (recoverDelayOptionList.size() != scriptNames.size())
          recoverDelayOptionList.add(" ");
      }
      if (maxPartitionSizeList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the maxPartitionSizeList for " +
            "the  scripts for which no MAX_PART_SIZE option is specified.");
        while (maxPartitionSizeList.size() != scriptNames.size())
          maxPartitionSizeList.add(" ");
      }
      if (evictionByOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"LRUHEAPPERCENT\" parameter in the  " +
            "evictionByOptionList for the scripts for which no EVICTION_BY option is" +
            " specified.");
        while (evictionByOptionList.size() != scriptNames.size())
          evictionByOptionList.add("LRUHEAPPERCENT");
      }
      for (int i = 0; i < scriptNames.size(); i++) {
        String userScript = (String) scriptNames.elementAt(i);
        String location = (String) dataLocationList.elementAt(i);
        String persistenceMode = (String) persistenceModeList.elementAt(i);
        String colocateWith = (String) colocateWithOptionList.elementAt(i);
        String partitionBy = (String) partitionByOptionList.elementAt(i);
        String numPartitions = (String) numPartitionsList.elementAt(i);
        String redundancy = (String) redundancyOptionList.elementAt(i);
        String recoverDelay = (String) recoverDelayOptionList.elementAt(i);
        String maxPartitionSize = (String) maxPartitionSizeList.elementAt(i);
        String evictionByOption = (String) evictionByOptionList.elementAt(i);
        String dataLocation = snappyTest.getDataLocation(location);
        String filePath = snappyTest.getScriptLocation(userScript);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator;
        if (SnappyPrms.getLogFileName() != null)
          dest += SnappyPrms.getLogFileName();
        else
          dest += "sqlScriptsResult_" + RemoteTestModule.getCurrentThread().getThreadId() + ".log";
        logFile = new File(dest);
        String primaryLocatorHost = getPrimaryLocatorHost();
        String primaryLocatorPort = getPrimaryLocatorPort();
        ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" +
            filePath, "-param:dataLocation=" + dataLocation,
            "-param:persistenceMode=" + persistenceMode, "-param:colocateWith=" +
            colocateWith, "-param:partitionBy=" + partitionBy,
            "-param:numPartitions=" + numPartitions, "-param:redundancy=" +
            redundancy, "-param:recoverDelay=" + recoverDelay,
            "-param:maxPartitionSize=" + maxPartitionSize, "-param:evictionByOption="
            + evictionByOption, "-client-port=" + primaryLocatorPort,
            "-client-bind-address=" + primaryLocatorHost);
        snappyTest.executeProcess(pb, logFile);
      }
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile " +
          "path  " + log + "\nError Message:" + e.getMessage());
    }
  }

  /**
   * Executes user scripts.
   */
  public static synchronized void HydraTask_executeScripts() {
    Vector scriptNames;
    File log = null, logFile;
    scriptNames = SnappyPrms.getScriptNames();
    if (scriptNames == null) {
      String s = "No Script names provided for executing in the Hydra TASK";
      throw new TestException(s);
    }
    try {

      for (int i = 0; i < scriptNames.size(); i++) {
        String userScript = (String) scriptNames.elementAt(i);
        String filePath = snappyTest.getScriptLocation(userScript);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "scriptResult_" +
            RemoteTestModule.getCurrentThread().getThreadId() + ".log";
        logFile = new File(dest);
        String comma_separated_args_list = StringUtils.join(SnappyPrms.getScriptArgs(), " ");
        String command = null;
        if (isCppTest) {

          String primaryLocatorHost = getPrimaryLocatorHost();
          String primaryLocatorPort = getPrimaryLocatorPort();
          Log.getLogWriter().info("inside cpp test: " + primaryLocatorHost + primaryLocatorPort);
          command = filePath + " " + comma_separated_args_list + " " + primaryLocatorHost + " " + primaryLocatorPort;
        } else command = filePath + " " + comma_separated_args_list;
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        snappyTest.executeProcess(pb, logFile);
      }
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

  protected static String getPrimaryLocatorHost() {
    String primaryLocatorHost = null;
    if (isLongRunningTest) {
      primaryLocatorHost = getDataFromFile("primaryLocatorHost");
      if (primaryLocatorHost == null)
        primaryLocatorHost = (String) SnappyBB.getBB().getSharedMap().get
            ("primaryLocatorHost");
      if (primaryLocatorHost == null && isUserConfTest) {
        primaryLocatorHost = SnappyPrms.getPrimaryLocatorHost();
        snappyTest.writeNodeConfigData("primaryLocatorHost", primaryLocatorHost, false);
      }

    } else
      primaryLocatorHost = (String) SnappyBB.getBB().getSharedMap().get("primaryLocatorHost");
    return primaryLocatorHost;
  }

  protected static String getPrimaryLocatorPort() {
    String primaryLocatorPort = null;
    if (isLongRunningTest) {
      primaryLocatorPort = getDataFromFile("primaryLocatorPort");
      if (primaryLocatorPort == null)
        primaryLocatorPort = (String) SnappyBB.getBB().getSharedMap().get
            ("primaryLocatorPort");
      if (primaryLocatorPort == null && isUserConfTest) {
        primaryLocatorPort = SnappyPrms.getPrimaryLocatorPort();
        snappyTest.writeNodeConfigData("primaryLocatorPort", primaryLocatorPort, false);
      }
    } else
      primaryLocatorPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLocatorPort");
    return primaryLocatorPort;
  }

  public void executeProcess(ProcessBuilder pb, File logFile) {
    Process p = null;
    try {
      if (logFile != null) {
        pb.redirectErrorStream(true);
        pb.redirectError(ProcessBuilder.Redirect.PIPE);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
      }
      p = pb.start();
      if (logFile != null) {
        assert pb.redirectInput() == ProcessBuilder.Redirect.PIPE;
        assert pb.redirectOutput().file() == logFile;
        assert p.getInputStream().read() == -1;
      }
      int rc = p.waitFor();
      if ((rc == 0) || (pb.command().contains("grep") && rc == 1)) {
        Log.getLogWriter().info("Executed successfully");
      } else {
        Log.getLogWriter().info("Failed with exit code: " + rc);
      }
    } catch (IOException e) {
      throw new TestException("Exception occurred while starting the process:" + pb +
          "\nError Message:" + e.getMessage());
    } catch (InterruptedException e) {
      throw new TestException("Exception occurred while waiting for the process execution:"
          + p + "\nError Message:" + e.getMessage());
    }
  }

  public synchronized void recordSnappyProcessIDinNukeRun(String pName) {
    recordSnappyProcessIDinNukeRun(pName, HostHelper.getLocalHost());
  }

  protected synchronized void recordSnappyProcessIDinNukeRun(String pName, String hostName) {
    Process pr = null;
    ProcessBuilder pb;
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "PIDs_" + pName + "_" + hostName + ".log";
      File logFile = new File(dest);
      if (!logFile.exists()) {
        recordProcessIds(pName, hostName, logFile);
      }
    } catch (IOException e) {
      String s = "Problem while starting the process : " + pr;
      throw new TestException(s, e);
    }
  }

  protected static void recordProcessIds(String pName, String hostName, File logFile) {
    String command;
    ProcessBuilder pb;
    Process pr = null;
    try {
      command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostName;
      if (pName.equals("Master"))
        command = "ps ax | grep -w " + pName + " | grep -v grep | awk '{print $1}'";
      else command += " jps | grep " + pName + " | awk '{print $1}'";
      hd = TestConfig.getInstance().getMasterDescription()
          .getVmDescription().getHostDescription();
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
      pr = pb.start();
      pr.waitFor();
      FileInputStream fis = new FileInputStream(logFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String str;
      while ((str = br.readLine()) != null) {
        int pid = Integer.parseInt(str);
        try {
          if (pids.contains(pid)) {
            Log.getLogWriter().info("Pid is already recorded with Master" + pid);
          } else {
            pids.add(pid);
            RemoteTestModule.Master.recordPID(hd, pid);
            SnappyBB.getBB().getSharedMap().put("pid" + "_" + pName + "_" + str, str);
            SnappyBB.getBB().getSharedMap().put("host" + "_" + pid + "_" + hostName, hostName);
            Log.getLogWriter().info("pid successfully recorded with Master: " + hostName + ":" + pid);
          }
        } catch (RemoteException e) {
          String s = "Unable to access master to record PID: " + pid;
          throw new HydraRuntimeException(s, e);
        }
      }
      br.close();
    } catch (IOException e) {
      String s = "Problem while starting the process : " + pr;
      throw new TestException(s, e);
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new TestException(s, e);
    }
  }

  public static String getCurrentDirPath() {
    String currentDir;
    try {
      currentDir = new File(".").getCanonicalPath();
    } catch (IOException e) {
      String s = "Problem while accessing the current dir.";
      throw new TestException(s, e);
    }
    return currentDir;
  }

  /**
   * Task(ENDTASK) for cleaning up snappy processes, because they are not stopped by Hydra in case of Test failure.
   */
  public static void HydraTask_cleanUpSnappyProcessesOnFailure() {
    Process pr = null;
    ProcessBuilder pb = null;
    File logFile = null, log = null, nukeRunOutput = null;
    try {
      Set<String> pidList = new HashSet<>();
      HostDescription hd = TestConfig.getInstance().getMasterDescription()
          .getVmDescription().getHostDescription();
      pidList = snappyTest.getPidList();
      log = new File(".");
      String nukerun = log.getCanonicalPath() + File.separator + "snappyNukeRun.sh";
      logFile = new File(nukerun);
      String nukeRunOutputString = log.getCanonicalPath() + File.separator + "nukeRunOutput.log";
      nukeRunOutput = new File(nukeRunOutputString);
      FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      for (String pidString : pidList) {
        int pid = Integer.parseInt(pidString);
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
      pb = new ProcessBuilder(nukerun);
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(nukeRunOutput));
      pr = pb.start();
      pr.waitFor();
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new TestException(s, e);
    }
  }

  /**
   * Executes snappy Streaming Jobs.
   */
  public static void HydraTask_executeSnappyStreamingJob() {
    snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames(),
        "snappyStreamingJobTaskResult_" + System.currentTimeMillis() + ".log");
  }

  /**
   * Executes Snappy Jobs.
   */
  public static synchronized void HydraTask_executeSnappyJob() {
    int count = 0;
    int currentThread = snappyTest.getMyTid();
    String logFile = "snappyJobResult_thread_" + currentThread + "_" + System.currentTimeMillis() + ".log";
    SnappyBB.getBB().getSharedMap().put("logFilesForJobs_" + currentThread + "_" + System.currentTimeMillis(), logFile);
    snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms
        .getUserAppJar(), jarPath, SnappyPrms.getUserAppName() + "_" + count + "_" + System.currentTimeMillis());
    count++;
  }

  /**
   * Executes snappy Streaming Jobs. Task is specifically written for benchmarking.
   */
  public static void HydraTask_executeSnappyStreamingJob_benchmarking() {
    snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames(),
        "snappyStreamingJobTaskResult_" + System.currentTimeMillis() + ".log");
  }

  /**
   * Executes Spark Jobs in Task.
   */
  public static void HydraTask_executeSparkJob() {
    int currentThread = snappyTest.getMyTid();
    String logFile = "sparkJobTaskResult_thread_" + currentThread + "_" + System.currentTimeMillis() + ".log";
    snappyTest.executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
  }

  /**
   * Executes snappy Streaming Jobs in Task.
   */
  public static void HydraTask_executeSnappyStreamingJobWithFileStream() {
    Runnable fileStreaming = new Runnable() {
      public void run() {
        snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames(), "snappyStreamingJobResult_" + System.currentTimeMillis() + ".log");
      }
    };

    Runnable simulateFileStream = new Runnable() {
      public void run() {
        snappyTest.simulateStream();
      }
    };

    ExecutorService es = Executors.newFixedThreadPool(2);
    es.submit(fileStreaming);
    es.submit(simulateFileStream);
    try {
      Log.getLogWriter().info("Sleeping for " + waitTimeBeforeStreamingJobStatus + "millis before executor service shut down");
      Thread.sleep(waitTimeBeforeStreamingJobStatus);
      es.shutdown();
      es.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new TestException("Exception occurred while waiting for the snappy streaming job process execution." + "\nError Message:" + e.getMessage());
    }
  }

  public static synchronized void createCSVFromTable() {
    int currentThread = snappyTest.getMyTid();
    String logFile = "snappyJobResult_thread_" + currentThread + "_" + System.currentTimeMillis() + ".log";
    SnappyBB.getBB().getSharedMap().put("logFilesForJobs_" + currentThread + "_" + System.currentTimeMillis(), logFile);
    snappyTest.executeSparkJob(SnappyPrms.getSnappyStreamingJobClassNames(),
        "snappyStreamingJobResult_" + System.currentTimeMillis() + ".log");
  }

  public static void streamingActivity() {
    Runnable simulateStreamingActivity = new Runnable() {
      public void run() {
        snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames(),
            "snappyStreamingJobResult_" + System.currentTimeMillis() + ".log");
      }
    };

    ExecutorService es = Executors.newFixedThreadPool(1);
    es.submit(simulateStreamingActivity);
  }

  protected void executeSnappyStreamingJob(Vector jobClassNames, String logFileName) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    String curlCommand1 = null, curlCommand2 = null, curlCommand3 = null, contextName = null, APP_PROPS = null;
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;
    userAppJar = SnappyPrms.getUserAppJar();
    snappyTest.verifyDataForJobExecution(jobClassNames, userAppJar);
    leadHost = getLeadHost();
    String leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    try {
      for (int i = 0; i < jobClassNames.size(); i++) {
        String userJob = (String) jobClassNames.elementAt(i);
        if (SnappyPrms.getCommaSepAPPProps() == null) {
          APP_PROPS = "shufflePartitions=" + SnappyPrms.getShufflePartitions();
        } else {
          APP_PROPS = SnappyPrms.getCommaSepAPPProps() + ",shufflePartitions=" + SnappyPrms.getShufflePartitions();
        }
        contextName = "snappyStreamingContext" + System.currentTimeMillis();
        String contextFactory = "org.apache.spark.sql.streaming.SnappyStreamingContextFactory";
        curlCommand1 = "curl --data-binary @" + snappyTest.getUserAppJarLocation(userAppJar, jarPath) + " " + leadHost + ":" + leadPort + "/jars/myapp";
        curlCommand2 = "curl -d  \"\"" + " '" + leadHost + ":" + leadPort + "/" + "contexts/" + contextName + "?context-factory=" + contextFactory + "'";
        curlCommand3 = "curl -d " + APP_PROPS + " '" + leadHost + ":" + leadPort + "/jobs?appName=myapp&classPath=" + userJob + "&context=" + contextName + "'";
        pb = new ProcessBuilder("/bin/bash", "-c", curlCommand1);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + logFileName;
        logFile = new File(dest);
        snappyTest.executeProcess(pb, logFile);
        pb = new ProcessBuilder("/bin/bash", "-c", curlCommand2);
        snappyTest.executeProcess(pb, logFile);
        pb = new ProcessBuilder("/bin/bash", "-c", curlCommand3);
        snappyTest.executeProcess(pb, logFile);
      }
      snappyTest.getSnappyJobsStatus(snappyJobScript, logFile, leadPort);
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
    }
  }

  protected void executeSnappyStreamingJobUsingJobScript(Vector jobClassNames, String logFileName) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;
    userAppJar = SnappyPrms.getUserAppJar();
    snappyTest.verifyDataForJobExecution(jobClassNames, userAppJar);
    leadHost = getLeadHost();
    String leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    try {
      for (int i = 0; i < jobClassNames.size(); i++) {
        String userJob = (String) jobClassNames.elementAt(i);
        pb = new ProcessBuilder(snappyJobScript, "submit", "--lead", leadHost + ":" + leadPort, "--app-name", "myapp", "--class", userJob, "--app-jar", snappyTest.getUserAppJarLocation(userAppJar, jarPath), "--stream");
        java.util.Map<String, String> env = pb.environment();
        if (SnappyPrms.getCommaSepAPPProps() == null) {
          env.put("APP_PROPS", "shufflePartitions=" + SnappyPrms.getShufflePartitions());
        } else {
          env.put("APP_PROPS", SnappyPrms.getCommaSepAPPProps() + ",shufflePartitions=" + SnappyPrms.getShufflePartitions());
        }
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + logFileName;
        logFile = new File(dest);
        snappyTest.executeProcess(pb, logFile);
      }
      snappyTest.getSnappyJobsStatus(snappyJobScript, logFile, leadPort);
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
    }
  }

  public String getLeadHost() {
    if (isLongRunningTest) {
      leadHost = getDataFromFile("leadHost");
      if (leadHost == null && isUserConfTest) {
        leadHost = SnappyPrms.getLeadHost();
        snappyTest.writeNodeConfigData("leadHost", leadHost, false);
      }
      if (leadHost == null)
        leadHost = getPrimaryLeadHost();
    } else {
      leadHost = getPrimaryLeadHost();
    }
    return leadHost;
  }

  public String getLeadPort() {
    String leadPort = null;
    if (isLongRunningTest) {
      leadPort = getDataFromFile("leadPort");
      if (leadPort == null)
        leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
      if (leadPort == null && isUserConfTest) {
        leadPort = SnappyPrms.getLeadPort();
        snappyTest.writeNodeConfigData("leadPort", leadPort, false);
      }
    } else {
      leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    }
    return leadPort;
  }

  protected String getPrimaryLeadHost() {
    leadHost = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadHost");
    if (leadHost == null || leadHost.length() == 0) {
      retrievePrimaryLeadHost();
      leadHost = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadHost");
      Log.getLogWriter().info("primaryLead Host is: " + leadHost);
    }
    return leadHost;
  }

  protected void verifyDataForJobExecution(Vector jobClassNames, String userAppJar) {
    if (userAppJar == null) {
      String s = "Missing userAppJar parameter.";
      throw new TestException(s);
    }
    if (jobClassNames == null) {
      String s = "Missing JobClassNames parameter for required TASK/CLOSETASK.";
      throw new TestException(s);
    }
  }

  public void executeSnappyJob(Vector jobClassNames, String logFileName, String
      userAppJar, String jarPath, String appName) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    File log = null, logFile = null;
//        userAppJar = SnappyPrms.getUserAppJar();
    if (appName == null) appName = SnappyPrms.getUserAppName() + "_" + System.currentTimeMillis();
    snappyTest.verifyDataForJobExecution(jobClassNames, userAppJar);
    leadHost = getLeadHost();
    String leadPort = getLeadPort();
    Log.getLogWriter().info("Primary Lead host:port =  " + leadHost + ":" + leadPort);
    try {
      for (int i = 0; i < jobClassNames.size(); i++) {
        String userJob = (String) jobClassNames.elementAt(i);
        String APP_PROPS = null;
        if (SnappyPrms.getCommaSepAPPProps() == null) {
          APP_PROPS = "logFileName=" + logFileName;
        } else {
          APP_PROPS = SnappyPrms.getCommaSepAPPProps() + ",logFileName=" + logFileName;
        }
        if (SnappyPrms.useJDBCConnInSnappyJob()) {
          String primaryLocatorHost = getPrimaryLocatorHost();
          String primaryLocatorPort = getPrimaryLocatorPort();
          APP_PROPS = "\"" + APP_PROPS + ",primaryLocatorHost=" + primaryLocatorHost + ",primaryLocatorPort=" + primaryLocatorPort + "\"";
        }
        if (SnappyPrms.isLongRunningJob()) {
          APP_PROPS = "\"" + APP_PROPS + ",maxResultWaitSec=" + maxResultWaitSecs + "\"";
        }
        if (SnappyPrms.hasDynamicAppProps()) {
          String dmlProps = dynamicAppProps.get(getMyTid());
          if (dmlProps == null) {
            dmlProps = dynamicAppProps.get(getMyTid());
            if (dmlProps == null) throw new TestException("Test issue: dml statement for " +
                getMyTid() + " is null");
          }
          APP_PROPS = "\"" + APP_PROPS + "," + dmlProps + "\"";
          Log.getLogWriter().info("APP_PROPS : " + APP_PROPS);
        }
        String curlCommand1 = "curl --data-binary @" + snappyTest.getUserAppJarLocation(userAppJar, jarPath) + " " + leadHost + ":" + leadPort + "/jars/" + appName;
        String curlCommand2 = "curl -d " + APP_PROPS + " '" + leadHost + ":" + leadPort + "/jobs?appName=" + appName + "&classPath=" + userJob + "'";

        Log.getLogWriter().info("jar loading cmd: " + curlCommand1);
        Log.getLogWriter().info("snappy job submit cmd: " + curlCommand2);
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", curlCommand1);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + logFileName;
        logFile = new File(dest);
        snappyTest.executeProcess(pb, logFile);
        pb = new ProcessBuilder("/bin/bash", "-c", curlCommand2);
        snappyTest.executeProcess(pb, logFile);
      }
      boolean retry = snappyTest.getSnappyJobsStatus(snappyJobScript, logFile, leadPort);
      if (retry) {
        if (jobSubmissionCount <= SnappyPrms.getRetryCountForJob()) {
          jobSubmissionCount++;
          Thread.sleep(60000);
          Log.getLogWriter().info("Job failed due to primary lead node failover. Resubmitting" +
              " the job to new primary lead node.....");
          retrievePrimaryLeadHost();
          HydraTask_executeSnappyJob();
        } else {
          throw new TestException("Failed to start Snappy Job. Please check the logs.");
        }
      }
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " +
          log + "\nError Message:" + e.getMessage());
    } catch (InterruptedException e) {
      throw new TestException("Exception occurred while waiting for the snappy job  " +
          "process re-execution." + "\nError Message:" + e.getMessage());
    }
  }

  public String setCDCSparkAppCmds(String userAppArgs, String commonArgs, String snappyJobScript,
                                   String userJob, String masterHost, String masterPort, File logFileName) {
    String appName = SnappyCDCPrms.getAppName();
    if (appName.equals("CDCIngestionApp2")) {
      int BBfinalStart2 = (Integer) SnappyBB.getBB().getSharedMap().get("START_RANGE_APP2");
      int BBfinalEnd2 = (Integer) SnappyBB.getBB().getSharedMap().get("END_RANGE_APP2");
      int finalStart2, finalEnd2;
      if (BBfinalStart2 == 0 || BBfinalEnd2 == 0) {
        finalStart2 = finalStart;
        finalEnd2 = finalEnd;
      } else {
        finalStart2 = BBfinalStart2;
        finalEnd2 = BBfinalEnd2;
      }
      userAppArgs = finalStart2 + " " + finalEnd2 + " " + userAppArgs;
      Log.getLogWriter().info("For CDCIngestionApp2 app New Start range and end range : " + finalStart2 + " & " + finalEnd2 + " and args = " + userAppArgs);
      SnappyBB.getBB().getSharedMap().put("START_RANGE_APP2", finalEnd2 + 1);
      SnappyBB.getBB().getSharedMap().put("END_RANGE_APP2", finalEnd2 + 1000);
    } else if (appName.equals("CDCIngestionApp1")) {
      userAppArgs = finalStart + " " + finalEnd + " " + userAppArgs;
      SnappyBB.getBB().getSharedMap().put("finalStartRange", finalStart);
      SnappyBB.getBB().getSharedMap().put("finalEndRange", finalEnd);
    } else if (appName.equals("BulkDeleteApp")) {
      commonArgs = " --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError" +
          " --conf spark.extraListeners=io.snappydata.hydra.SnappyCustomSparkListener ";
    }
    String command = snappyJobScript + " --class " + userJob +
        " --name " + appName +
        " --master spark://" + masterHost + ":" + masterPort + " " +
        SnappyPrms.getExecutorMemory() + " " +
        SnappyPrms.getSparkSubmitExtraPrms() + " " + commonArgs + " " + snappyTest.getUserAppJarLocation(userAppJar, jarPath) + " " +
        userAppArgs;
    if (SnappyCDCPrms.getIsCDCStream())
      command = "nohup " + command + " > " + logFileName + " & ";
    return command;
  }

  public String getLog4jFile(String dest) {
    String log4jFileName = "";
    try {
      log4jFileName = getCurrentDirPath() + File.separator + "log4j_" + getMyTid() + ".properties";
      Log.getLogWriter().info("log4j path :" + log4jFileName);
      File log4jFile = new File(log4jFileName);
      FileWriter fw = new FileWriter(log4jFile);
      fw.write("log4j.rootCategory=DEBUG, file\n");
      fw.write("log4j.appender.file=org.apache.log4j.RollingFileAppender\n");
      fw.write("log4j.appender.file.append=true\n");
      fw.write("log4j.appender.file.file=" + dest + "\n");
      fw.write("log4j.appender.file.MaxFileSize=1GB\n");
      fw.write("log4j.appender.file.MaxBackupIndex=10000\n");
      fw.write("log4j.appender.file.layout=io.snappydata.log4j.PatternLayout\n");
      fw.write("log4j.appender.file.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss.SSS zzz} %t %p %c{1}: %m%n\n");
      fw.close();
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving log4j file at path " +
          log4jFileName + "\nError Message:" + e.getMessage());
    }
    return log4jFileName;
  }

  public void executeSparkJob(Vector jobClassNames, String logFileName) {
    String snappyJobScript = getScriptLocation("spark-submit");
    boolean isCDCStream = SnappyCDCPrms.getIsCDCStream();
    ProcessBuilder pb = null;
    File log = null, logFile = null;
    userAppJar = SnappyPrms.getUserAppJar();
    snappyTest.verifyDataForJobExecution(jobClassNames, userAppJar);
    boolean isSparkDebug = SnappyPrms.isSparkDebugEnabled();
    try {
      for (int i = 0; i < jobClassNames.size(); i++) {
        String userJob = (String) jobClassNames.elementAt(i);
        String masterHost = getSparkMasterHost();
        String masterPort = MASTER_PORT;
        String command = null;
        String primaryLocatorHost = getPrimaryLocatorHost();
        String primaryLocatorPort = getPrimaryLocatorPort();
        String userAppArgs = SnappyPrms.getUserAppArgs();
        String commonArgs = " --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError" +
            " --conf spark.extraListeners=io.snappydata.hydra.SnappyCustomSparkListener " +
            " --conf snappydata.connection=" + primaryLocatorHost + ":" + primaryLocatorPort;
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + logFileName;
        logFile = new File(dest);
        if (SnappyPrms.hasDynamicAppProps()) {
          String dmlProps = dynamicAppProps.get(getMyTid());
          if (dmlProps == null)
            throw new TestException("dml props for thread " + getMyTid() + " is null)");
          userAppArgs = userAppArgs + " " + dmlProps;
        }
        String log4jFile = (isSparkDebug)?getLog4jFile(dest):"";
        if (SnappyCDCPrms.getIsCDC()) {
          command = setCDCSparkAppCmds(userAppArgs, commonArgs, snappyJobScript, userJob, masterHost, masterPort, logFile);
        } else {
          command = snappyJobScript + " --class " + userJob +
              " --master spark://" + masterHost + ":" + masterPort + " " +
              SnappyPrms.getExecutorMemory() + " " +
              SnappyPrms.getSparkSubmitExtraPrms() + " " + commonArgs + " " +
              ((isSparkDebug)?(" --driver-java-options -Dlog4j.configuration=file://" + log4jFile):"") + " " +
              snappyTest.getUserAppJarLocation(userAppJar, jarPath) + " " +
              userAppArgs + " " + primaryLocatorHost + ":" + primaryLocatorPort;
        }
        Log.getLogWriter().info("spark-submit command is : " + command);
        pb = new ProcessBuilder("/bin/bash", "-c", command);
        snappyTest.executeProcess(pb, logFile);
        Log.getLogWriter().info("CDC stream is : " + SnappyCDCPrms.getIsCDCStream());
        if (SnappyCDCPrms.getIsCDCStream()) {
          //wait for 2 min until the cdc streams starts off.
          try {
            Thread.sleep(120000);
          } catch (InterruptedException ie) {
            Log.getLogWriter().info("Caught exception in cdc thread.sleep " + ie.getMessage());
          }
          Log.getLogWriter().info("Inside getIsCDCStream : " + SnappyCDCPrms.getIsCDCStream());
          return;
        }
        String searchString = "Spark ApplicationEnd: ";
        String expression = "cat " + logFile + " | grep -e Exception -e '" + searchString + "' |" +
            " grep -v java.net.BindException" + " | wc -l)\"";
        String searchCommand = "while [ \"$(" + expression + " -le  0 ] ; do sleep 1 ; done";
        pb = new ProcessBuilder("/bin/bash", "-c", searchCommand);
        Log.getLogWriter().info("spark job " + userJob + " starts at: " + System
            .currentTimeMillis());
        executeProcess(pb, logFile);
        Log.getLogWriter().info("spark job " + userJob + " finishes at:  " + System
            .currentTimeMillis());
      }
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " +
          log + "\nError Message:" + e.getMessage());
    }
  }

  public static void HydraTask_InitializeBB() {
    try {
      Log.getLogWriter().info("InsideHydraTask_InitializeBB ");
      int startR = SnappyCDCPrms.getInitStartRange();
      int endR = SnappyCDCPrms.getInitEndRange();
      SnappyBB.getBB().getSharedMap().put("START_RANGE_APP1", startR);
      SnappyBB.getBB().getSharedMap().put("END_RANGE_APP1", endR);
      SnappyBB.getBB().getSharedMap().put("START_RANGE_APP2", startR + 5000000);
      SnappyBB.getBB().getSharedMap().put("END_RANGE_APP2", 1000 + (startR + 5000000));
      Log.getLogWriter().info("Finished HydraTask_InitializeBB ");
    } catch (Exception e) {
      Log.getLogWriter().info("HydraTask_InitializeBB exception " + e.getMessage());
    }
  }

  protected static String getAbsoluteJarLocation(String jarPath, final String jarName) {
    String absoluteJarPath = null;
    File baseDir = new File(jarPath);
    try {
      IOFileFilter filter = new WildcardFileFilter(jarName);
      List<File> files = (List<File>) FileUtils.listFiles(baseDir, filter, TrueFileFilter.INSTANCE);
      Log.getLogWriter().info("Jar file found: " + Arrays.asList(files));
      for (File file1 : files) {
        if (!file1.getAbsolutePath().contains("/work/"))
          absoluteJarPath = file1.getAbsolutePath();
      }
    } catch (Exception e) {
      Log.getLogWriter().info("Unable to find " + jarName + " jar at " + jarPath + " location.");
    }
    return absoluteJarPath;
  }

  /**
   * Returns the output file containing collective output for all threads executing Snappy job in CLOSETASK.
   */
  public static void HydraTask_getSnappyJobOutputCollectivelyForCloseTask() {
    snappyTest.getSnappyJobOutputCollectively("logFilesForJobs_",
        "snappyJobCollectiveOutputForCloseTask.log");
  }

  /**
   * Returns the output file containing collective output for all threads executing Snappy job in TASK.
   */
  public static void HydraTask_getSnappyJobOutputCollectivelyForTask() {
    snappyTest.getSnappyJobOutputCollectively("logFilesForJobs_",
        "snappyJobCollectiveOutputForTask.log");
  }

  protected void getSnappyJobOutputCollectively(String logFilekey, String fileName) {
    Set<String> snappyJobLogFiles = new LinkedHashSet<String>();
    File fin = null;
    try {
      Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
      for (String key : keys) {
        if (key.startsWith(logFilekey)) {
          String logFilename = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
          snappyJobLogFiles.add(logFilename);
        }
      }
      File dir = new File(".");
      String dest = dir.getCanonicalPath() + File.separator + fileName;
      File file = new File(dest);
      if (!file.exists()) return;
      int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.doneExecution);
      if (num == 1) {
        FileWriter fstream = new FileWriter(dest, true);
        BufferedWriter bw = new BufferedWriter(fstream);
        Iterator<String> itr = snappyJobLogFiles.iterator();
        while (itr.hasNext()) {
          String userScript = itr.next();
          String threadID = userScript.substring(userScript.lastIndexOf("_"), userScript.indexOf("."));
          String threadInfo = "Thread" + threadID + " output:";
          bw.write(threadInfo);
          bw.newLine();
          String fileInput = snappyTest.getLogDir() + File.separator + userScript;
          fin = new File(fileInput);
          FileInputStream fis = new FileInputStream(fin);
          BufferedReader in = new BufferedReader(new InputStreamReader(fis));
          String line = null;
          while ((line = in.readLine()) != null) {
            bw.write(line);
            bw.newLine();
          }
          in.close();
        }
        bw.close();
      }
    } catch (FileNotFoundException e) {
      String s = "Unable to find file: " + fin;
      throw new TestException(s);
    } catch (IOException e) {
      String s = "Problem while writing to the file : " + fin;
      throw new TestException(s, e);
    }
  }

  protected void simulateStream() {
    File logFile = null;
    File log = new File(".");
    try {
      String streamScriptName = snappyTest.getScriptLocation(simulateStreamScriptName);
      ProcessBuilder pb = new ProcessBuilder(streamScriptName, simulateStreamScriptDestinationFolder, productDir);
      String dest = log.getCanonicalPath() + File.separator + "simulateFileStreamResult.log";
      logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

  public boolean getSnappyJobsStatus(String snappyJobScript, File logFile, String leadPort) {
    boolean found = false;
    try {
      String line = null;
      Set<String> jobIds = new LinkedHashSet<String>();
      FileReader freader = new FileReader(logFile);
      BufferedReader inputFile = new BufferedReader(freader);
      while ((line = inputFile.readLine()) != null) {
        if (line.contains("jobId")) {
          String jobID = line.split(":")[1].trim();
          jobID = jobID.substring(1, jobID.length() - 2);
          jobIds.add(jobID);
        }
      }
      if (jobIds == null) {
        Log.getLogWriter().info("Failed to start the snappy job.");
        return true;
      }
      inputFile.close();
      for (String str : jobIds) {
        File log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "jobStatus_" + RemoteTestModule
            .getCurrentThread().getThreadId() + "_" + System.currentTimeMillis() + ".log";
        File commandOutput = new File(dest);
        String expression = snappyJobScript + " status --lead " + leadHost + ":" + leadPort + " " +
            "--job-id " + str + " > " + commandOutput + " 2>&1 ; grep -e '\"status\": " +
            "\"FINISHED\"' -e 'curl:' -e '\"status\": \"ERROR\"' " + commandOutput + " | wc -l)\"";
        String command = "while [ \"$(" + expression + " -le  0 ] ; do rm " +
            commandOutput + " ;  touch " + commandOutput + "   ;  sleep " +
            SnappyPrms.getSleepTimeSecsForJobStatus() + " ; done";
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        Log.getLogWriter().info("job " + str + " starts at: " + System.currentTimeMillis());
        // output is already redirected by script
        executeProcess(pb, null);
        Log.getLogWriter().info("job " + str + " finishes at:  " + System.currentTimeMillis());
        FileInputStream fis = new FileInputStream(commandOutput);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        line = null;
        String connResetErr = "Connection reset by peer";
        String askTimedOutErr = "Ask timed out on ";
        String noSuchJobID ="No such job ID";
        while ((line = br.readLine()) != null && !found) {
          if (line.toLowerCase().contains(connResetErr.toLowerCase()) || line.toLowerCase().contains(askTimedOutErr.toLowerCase())) {
            found = true;
            Log.getLogWriter().info("Job submission failed with : " + line);
          } else if(line.toLowerCase().contains(noSuchJobID.toLowerCase())){
            Log.getLogWriter().info("Job ID " + str + " not found. Sleeping before proceeding test ... ");
            sleepForMs(30);
          }
        }
        br.close();
      }
    } catch (FileNotFoundException e) {
      String s = "Unable to find file: " + logFile;
      throw new TestException(s);
    } catch (IOException e) {
      String s = "Problem while reading the file : " + logFile;
      throw new TestException(s, e);
    }
    return found;
  }

  public synchronized void retrievePrimaryLeadHost() {
    Object[] tmpArr = null;
    String leadPort = null;
    tmpArr = getPrimaryLeadVM(cycleLeadVMTarget);
    List<ClientVmInfo> vmList;
    vmList = (List<ClientVmInfo>) (tmpArr[0]);
    Set<String> myDirList = new LinkedHashSet<String>();
    myDirList = getFileContents("logDir_", myDirList);
    for (int i = 0; i < vmList.size(); i++) {
      ClientVmInfo targetVm = vmList.get(i);
      String clientName = targetVm.getClientName();
      for (String vmDir : myDirList) {
        if (vmDir.contains(clientName)) {
          String[] splitedNodeConfig = vmDir.split("_");
          leadHost = splitedNodeConfig[splitedNodeConfig.length - 1];
          Log.getLogWriter().info("New Primary leadHost is: " + leadHost);
          SnappyBB.getBB().getSharedMap().put("primaryLeadHost", leadHost);
          leadPort = getPrimaryLeadPort(clientName);
          SnappyBB.getBB().getSharedMap().put("primaryLeadPort", leadPort);
          if (isLongRunningTest) writeLeadHostPortInfo();
        }
      }
    }
  }

  /*
  * Returns the log file name.  Autogenerates the directory name at runtime
  * using the same path as the master.  The directory is created if needed.
  *
  * @throws HydraRuntimeException if the directory cannot be created.
  */
  private synchronized String getLogDir() {
    if (this.logFile == null) {
      Vector<String> names = TestConfig.tab().vecAt(ClientPrms.gemfireNames);
      String dirname = generateLogDirName();
//            this.localHost = HostHelper.getLocalHost();
      File dir = new File(dirname);
      String fullname = dir.getAbsolutePath();
      try {
        FileUtil.mkdir(dir);
        try {
          for (String name : names) {
            String[] splitedName = name.split("gemfire");
            String newName = splitedName[0] + splitedName[1];
//                        if (newName.equals(RemoteTestModule.getMyClientName())) {
            RemoteTestModule.Master.recordDir(hd,
                name, fullname);
//                        }
          }
        } catch (RemoteException e) {
          String s = "Unable to access master to record directory: " + dir;
          throw new HydraRuntimeException(s, e);
        }
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Error e) {
        String s = "Unable to create directory: " + dir;
        throw new HydraRuntimeException(s);
      }
      this.logFile = dirname;
      log().info("logFile name is " + this.logFile);
    }
    return this.logFile;
  }

  private String generateLogDirName() {
    String dirname = hd.getUserDir() + File.separator
        + "vm_" + RemoteTestModule.getMyVmid()
        + "_" + RemoteTestModule.getMyClientName()
        + "_" + HostHelper.getLocalHost();
    return dirname;
  }

  public synchronized void generateConfig(String fileName) {
    File file = null;
    try {
      String path = productConfDirPath + sep + fileName;
      log().info("File Path is ::" + path);
      file = new File(path);

      // if file doesnt exists, then create it
      if (!file.exists()) {
        file.createNewFile();
      } else if (file.exists()) {
        if (isStopMode) return;
        file.setWritable(true);
        //file.delete();
        Files.delete(Paths.get(path));
        Log.getLogWriter().info(fileName + " file deleted");
        file.createNewFile();
      }
    } catch (IOException e) {
      String s = "Problem while creating the file : " + file;
      throw new TestException(s, e);
    }
  }

  /**
   * Deletes the snappy config generated spcific to test run after successful test execution.
   */
  public static void HydraTask_deleteSnappyConfig() throws IOException {
    String locatorConf = productConfDirPath + sep + "locators";
    String serverConf = productConfDirPath + sep + "servers";
    String leadConf = productConfDirPath + sep + "leads";
    if (new File(locatorConf).exists()) Files.delete(Paths.get(locatorConf));
    Log.getLogWriter().info("locators file deleted");
    if (new File(serverConf).exists()) Files.delete(Paths.get(serverConf));
    Log.getLogWriter().info("servers file deleted");
    if (new File(leadConf).exists()) Files.delete(Paths.get(leadConf));
    Log.getLogWriter().info("leads file deleted");
    if (useSmartConnectorMode) {
      String slaveConf = productConfDirPath + sep + "slaves";
      String sparkEnvConf = productConfDirPath + sep + "spark-env.sh";
      if (new File(slaveConf).exists()) Files.delete(Paths.get(slaveConf));
      Log.getLogWriter().info("slaves file deleted");
      if (new File(sparkEnvConf).exists()) Files.delete(Paths.get(sparkEnvConf));
      Log.getLogWriter().info("spark-env.sh file deleted");
    }
    if (isLongRunningTest) {
      String locatorList = productConfDirPath + sep + "locatorList";
      String locatorConnInfo = productConfDirPath + sep + "locatorConnInfo";
      String leadHost = productConfDirPath + sep + "leadHost";
      String leadPort = productConfDirPath + sep + "leadPort";
      String masterHost = productConfDirPath + sep + "masterHost";
      String primaryLocatorHost = productConfDirPath + sep + "primaryLocatorHost";
      String primaryLocatorPort = productConfDirPath + sep + "primaryLocatorPort";
      if (new File(locatorList).exists()) Files.delete(Paths.get(locatorList));
      if (new File(locatorConnInfo).exists()) Files.delete(Paths.get(locatorConnInfo));
      if (new File(leadHost).exists()) Files.delete(Paths.get(leadHost));
      if (new File(leadPort).exists()) Files.delete(Paths.get(leadPort));
      if (new File(masterHost).exists()) Files.delete(Paths.get(masterHost));
      if (new File(primaryLocatorHost).exists()) Files.delete(Paths.get(primaryLocatorHost));
      if (new File(primaryLocatorPort).exists()) Files.delete(Paths.get(primaryLocatorPort));
      Log.getLogWriter().info("Long Running Test artifacts deleted.");
    }
    // Removing twitter data directories if exists.
    String twitterdata = dtests + "twitterdata";
    String copiedtwitterdata = dtests + "copiedtwitterdata";
    File file = new File(twitterdata);
    if (file.exists()) {
      file.delete();
      Log.getLogWriter().info("Done removing twitter data directory.");
    }
    file = new File(copiedtwitterdata);
    if (file.exists()) {
      file.delete();
      Log.getLogWriter().info("Done removing copiedtwitterdata data directory.");
    }
  }

  public int getMyTid() {
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    return myTid;
  }


  /**
   * Start snappy cluster using snappy-start-all.sh script.
   */
  public static synchronized void HydraTask_startSnappyCluster() {
    if (forceStart) {
      startSnappyCluster();
    } else {
      int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.snappyClusterStarted);
      if (num == 1) {
        startSnappyCluster();
      }
    }
  }

  protected static void startSnappyCluster() {
    File log = null;
    ProcessBuilder pb = null;
    try {
      pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-start-all.sh"));
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappySystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

  public static synchronized void HydraTask_recordProcessIDWithHost() {
    if (useRowStore) {
      snappyTest.recordSnappyProcessIDinNukeRun("GfxdDistributionLocator");
      snappyTest.recordSnappyProcessIDinNukeRun("GfxdServerLauncher");
    } else {
      snappyTest.recordSnappyProcessIDinNukeRun("LocatorLauncher");
      snappyTest.recordSnappyProcessIDinNukeRun("ServerLauncher");
      snappyTest.recordSnappyProcessIDinNukeRun("LeaderLauncher");
      snappyTest.recordSnappyProcessIDinNukeRun("Worker");
      snappyTest.recordSnappyProcessIDinNukeRun("Master");
    }
  }

  public static synchronized void HydraTask_recordProcessIDWithHostWithUserConfsTest() {
    Vector hostNames = SnappyPrms.getHostNameList();
    if (hostNames.isEmpty()) {
      throw new TestException("List of Host Names " +
          "required for recording the hostnames with hydra master is not specified");
    }

    for (int i = 0; i < hostNames.size(); i++) {
      String hostName = (String) hostNames.elementAt(i);
      if (useRowStore) {
        snappyTest.recordSnappyProcessIDinNukeRun("GfxdDistributionLocator", hostName);
        snappyTest.recordSnappyProcessIDinNukeRun("GfxdServerLauncher", hostName);
      } else {
        snappyTest.recordSnappyProcessIDinNukeRun("LocatorLauncher", hostName);
        snappyTest.recordSnappyProcessIDinNukeRun("ServerLauncher", hostName);
        snappyTest.recordSnappyProcessIDinNukeRun("LeaderLauncher", hostName);
        snappyTest.recordSnappyProcessIDinNukeRun("Worker", hostName);
        snappyTest.recordSnappyProcessIDinNukeRun("Master", hostName);
      }
    }
  }

  /**
   * Create and start snappy locator using snappy-locators.sh script.
   */
  public static synchronized void HydraTask_createAndStartSnappyLocator() {
    int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.locatorsStarted);
    if (num == 1) {
      snappyTest.startSnappyLocator();
    }
  }

  /**
   * Create and start snappy server.
   */
  public static synchronized void HydraTask_createAndStartSnappyServers() {
    int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.serversStarted);
    if (num == 1) {
      snappyTest.startSnappyServer();
    }
  }


  /**
   * Creates and start snappy lead.
   */
  public static synchronized void HydraTask_createAndStartSnappyLeader() {
    int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.leadsStarted);
    if (num == 1) {
      snappyTest.startSnappyLead();
    }
  }


  /**
   * Starts Spark Cluster with the specified number of workers.
   */
  public static synchronized void HydraTask_startSparkCluster() {
    File log = null;
    try {
      int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.sparkClusterStarted);
      if (num == 1) {
        // modifyJobServerConfig();
        ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("start-all.sh"));
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "sparkSystem.log";
        File logFile = new File(dest);
        snappyTest.executeProcess(pb, logFile);
      }
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

  /**
   * Stops Spark Cluster.
   */
  public static synchronized void HydraTask_stopSparkCluster() {
    File log = null;
    try {
      initSnappyArtifacts();
      ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("stop-all.sh"));
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "sparkSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.sparkClusterStarted);
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

  /**
   * Stops snappy lead.
   */
  public static synchronized void HydraTask_stopSnappyLeader() {
    File log = null;
    if(snappyTest==null) snappyTest = new SnappyTest();
    try {
      ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"),
          "stop");
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyLeaderSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.leadsStarted);
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    } catch (Exception e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  /**
   * Stops snappy server/servers.
   */
  public static synchronized void HydraTask_stopSnappyServers() {
    File log = null;
    if(snappyTest==null) snappyTest = new SnappyTest();
    try {
      ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "stop");
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.serversStarted);
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    } catch (Exception e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  /**
   * Stops a snappy locator.
   */
  public static synchronized void HydraTask_stopSnappyLocator() {
    File log = null;
    if(snappyTest==null) snappyTest = new SnappyTest();
    try {
      ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh")
          , "stop");
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyLocatorSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.locatorsStarted);
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    } catch (Exception e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  public static synchronized void HydraTask_stopSnappyCluster() {
    File log = null;
    try {
      initSnappyArtifacts();
      ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-stop-all.sh"));
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappySystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      boolean stopAllFailed = snappyTest.waitForStopAll();
      if (stopAllFailed) {
        // try again
        Thread.sleep(60000);
        stopAllFailed = snappyTest.waitForStopAll();
        if (stopAllFailed) {
          threadDumpForAllServers();
        }
      }
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for snappy-stop-all script execution..";
      throw new TestException(s, e);
    }
  }

  public static void HydraTask_deployJarUsingJDBC() {
    snappyTest.executeDeployJar();
  }

  public void executeDeployJar() {
    Connection conn = null;
    try {
      conn = getLocatorConnection();
    } catch (SQLException se) {
      throw new TestException("Got exception while getting connection", se);
    }
    String userJarPath = snappyTest.getUserAppJarLocation(SnappyPrms.getUserAppJar(), jarPath);
    String deployCmd = "";
    String jarName = SnappyPrms.getJarIdentifier();
    try {
      deployCmd = "deploy jar " + jarName + " '" + userJarPath + "'";
      Log.getLogWriter().info("Executing deploy jar cmd : " + deployCmd);
      conn.createStatement().execute(deployCmd);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing deploy jar:" + deployCmd, se);
    }
    closeConnection(conn);
  }

  /**
   * Concurrently stops a List of snappy store VMs, then restarts them.  Waits for the
   * restart to complete before returning.
   */
  public static void HydraTask_cycleStoreVms() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numVMsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms);
      Long lastCycledTimeForStoreFromBB = (Long) SnappyBB.getBB().getSharedMap().get(LASTCYCLEDTIME);
      snappyTest.cycleVM(numToKill, stopStartVms, "storeVmCycled", lastCycledTimeForStoreFromBB,
          lastCycledTime, "server", false, false, false);
    }
  }

  /**
   * Concurrently stops a List of snappy locator VMs, then restarts them.  Waits for the
   * restart to complete before returning.
   */
  public static void HydraTask_cycleLocatorVms() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numVMsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms);
      Long lastCycledTimeForLocatorFromBB = (Long) SnappyBB.getBB().getSharedMap().get
          (LASTCYCLEDTIMEFORLOCATOR);
      snappyTest.cycleVM(numToKill, stopStartVms, "locatorVmCycled", lastCycledTimeForLocatorFromBB,
          lastCycledTime, "locator", false, false, false);
    }
  }

  /**
   * Stops snappy primary lead member, then restarts it.  Waits for the
   * restart to complete before returning.
   */
  public static synchronized void HydraTask_cycleLeadVM() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numLeadsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartLeadVms);
      Long lastCycledTimeForLeadFromBB = (Long) SnappyBB.getBB().getSharedMap().get(LASTCYCLEDTIMEFORLEAD);
      SnappyBB.getBB().getSharedMap().put("primaryLeadHost", "");
      snappyTest.cycleVM(numToKill, stopStartVms, "leadVmCycled", lastCycledTimeForLeadFromBB,
          lastCycledTimeForLead, "lead", false, false, false);
    }
  }

  protected void cycleVM(int numToKill, int stopStartVMs, String cycledVM, Long lastCycledTimeFromBB, long
      lastCycledTime, String vmName, boolean isDmlOp, boolean restart, boolean rebalance) {
    if (!cycleVms) {
      Log.getLogWriter().warning("cycleVms sets to false, no node will be brought down in the test run");
      return;
    }
    List<ClientVmInfo> vms = null;
    if (stopStartVMs == 1) {
      Object vmCycled = SnappyBB.getBB().getSharedMap().get(cycledVM);
      if (vmCycled == null) {
        while (true) {
          try {
            if (vmName.equalsIgnoreCase("lead"))
              vms = stopStartVMs(numToKill, "lead", isDmlOp, restart, rebalance);
            else if (vmName.equalsIgnoreCase("server")) vms = stopStartVMs(numToKill, "server",
                isDmlOp, restart, rebalance);
            else if (vmName.equalsIgnoreCase("locator")) vms = stopStartVMs(numToKill,
                "locator", isDmlOp, restart, rebalance);
            break;
          } catch (TestException te) {
            throw te;
          }
        }
      } //first time
      else {
        //relaxing a little for HA tests
        //using the BB to track when to kill the next set of vms
        if (lastCycledTimeFromBB == null) {
          int sleepMS = 20000;
          Log.getLogWriter().info("allow  " + sleepMS / 1000 + " seconds before killing others");
          MasterController.sleepForMs(sleepMS); //no vms has been cycled before
        } else if (lastCycledTimeFromBB > lastCycledTime) {
          lastCycledTime = lastCycledTimeFromBB;
          log().info("update last cycled lead vm is set to " + lastCycledTime);
        }

        if (lastCycledTime != 0) {
          long currentTime = System.currentTimeMillis();
          if (currentTime - lastCycledTime < waitTimeBeforeNextCycleVM * THOUSAND) {
            if (vmName.equalsIgnoreCase("lead"))
              SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartLeadVms);
            else if (vmName.equalsIgnoreCase("server")) SnappyBB.getBB().getSharedCounters().zero
                (SnappyBB.stopStartVms);
            else if (vmName.equalsIgnoreCase("locator")) SnappyBB.getBB().getSharedCounters().zero
                (SnappyBB.stopStartLocatorVms);
            return;
          } else {
            if (vmName.equalsIgnoreCase("lead")) log().info("cycle lead vm starts at: " +
                currentTime);
            else if (vmName.equalsIgnoreCase("server")) log().info("cycle store vm starts at: " +
                currentTime);
            else if (vmName.equalsIgnoreCase("locator")) log().info("cycle locator vm starts " +
                "at:" + currentTime);
          }
        }
        vms = stopStartVMs(numToKill, vmName, isDmlOp, restart, rebalance);
      }
      if (vms == null || vms.size() == 0) {
        if (vmName.equalsIgnoreCase("lead")) {
          Log.getLogWriter().info("No lead vm being chosen to be stopped");
          SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartLeadVms);
        } else if (vmName.equalsIgnoreCase("server")) {
          Log.getLogWriter().info("No store vm being chosen to be stopped");
          SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartVms);
        } else if (vmName.equalsIgnoreCase("locator")) {
          Log.getLogWriter().info("No locator vm being chosen to be stopped");
          SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartLocatorVms);
        }
        return;
      }
//            Log.getLogWriter().info("Total number of PR is " + numOfPRs);
//            if (numOfPRs > 0)
//                PRObserver.waitForRebalRecov(vms, 1, numOfPRs, null, null, false);
      long currentTime = System.currentTimeMillis();
      if (vmName.equalsIgnoreCase("lead")) {
        log().info("cycle lead vm finishes at: " + currentTime);
        SnappyBB.getBB().getSharedMap().put(LASTCYCLEDTIMEFORLEAD, currentTime);
        SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartLeadVms);
      } else if (vmName.equalsIgnoreCase("server")) {
        log().info("cycle store vm finishes at: " + currentTime);
        SnappyBB.getBB().getSharedMap().put(LASTCYCLEDTIME, currentTime);
        SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartVms);
      } else if (vmName.equalsIgnoreCase("locator")) {
        log().info("cycle locator vm finishes at: " + currentTime);
        SnappyBB.getBB().getSharedMap().put(LASTCYCLEDTIMEFORLOCATOR, currentTime);
        SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartVms);
      }
      SnappyBB.getBB().getSharedMap().put(cycledVM, "true");
    }
  }

  protected List<ClientVmInfo> stopStartVMs(int numToKill, String vmName, boolean isDmlOp,
                                            boolean restart, boolean rebalance) {
    if (vmName.equalsIgnoreCase("lead")) {
      log().info("stopStartVMs : cycle lead vm starts at: " + System.currentTimeMillis());
      return stopStartVMs(numToKill, cycleLeadVMTarget, vmName, isDmlOp, restart, rebalance);
    } else if (vmName.equalsIgnoreCase("server")) {
      log().info("stopStartVMs : cycle store vm starts at: " + System.currentTimeMillis());
      return stopStartVMs(numToKill, cycleVMTarget, vmName, isDmlOp, restart, rebalance);
    } else if (vmName.equalsIgnoreCase("locator"))
      log().info("stopStartVMs : cycle store vm starts at: " + System.currentTimeMillis());
    return stopStartVMs(numToKill, cycleLocatorVMTarget, vmName, isDmlOp, restart, rebalance);
  }

  protected List<ClientVmInfo> stopStartLeadVM(int numToKill) {
    log().info("cycle lead vm starts at: " + System.currentTimeMillis());
    return stopStartVMs(numToKill, cycleLeadVMTarget, "lead", false, false, false);
  }

  @SuppressWarnings("unchecked")
  protected List<ClientVmInfo> stopStartVMs(int numToKill, String target, String vmName, boolean
      isDmlOp, boolean restart, boolean rebalance) {
    Object[] tmpArr = null;
    if (vmName.equalsIgnoreCase("lead")) tmpArr = snappyTest.getPrimaryLeadVMWithHA(target);
    else tmpArr = StopStartVMs.getOtherVMs(numToKill, target);
    // get the VMs to stop; vmList and stopModeList are parallel lists

    Object vm1 = SnappyBB.getBB().getSharedMap().get("storeVMTarget1");
    Object vm2 = SnappyBB.getBB().getSharedMap().get("storeVMTarget2");
    List<ClientVmInfo> vmList;
    List<String> stopModeList;

    if (vm1 == null && vm2 == null) {
      vmList = (List<ClientVmInfo>) (tmpArr[0]);
      stopModeList = (List<String>) (tmpArr[1]);
      for (ClientVmInfo client : vmList) {
        PRObserver.initialize(client.getVmid());
      } //clear bb info for the vms to be stopped/started
    } else {
      vmList = (List<ClientVmInfo>) (tmpArr[0]);
      stopModeList = (List<String>) (tmpArr[1]);
      for (int i = 0; i < vmList.size(); i++) {
        if (vmList.get(i).getVmid().intValue() == ((ClientVmInfo) vm1).getVmid().intValue()
            || vmList.get(i).getVmid().intValue() == ((ClientVmInfo) vm2).getVmid().intValue()) {
          Log.getLogWriter().info("remove the vm " + vmList.get(i).getVmid() + " from the stop list");
          vmList.remove(i);
        } else PRObserver.initialize(vmList.get(i).getVmid());
      }//clear bb info for the vms to be stopped/started
    }
    if (vmList.size() != 0) {
      stopStartVMs(vmList, stopModeList, vmName, isDmlOp, restart, rebalance);
    }
    return vmList;
  }

  protected void stopStartVMs(List<ClientVmInfo> vmList, List<String> stopModeList, String
      vmName, boolean isDmlOp, boolean restart, boolean rebalance) {
    Set<String> myDirList = new LinkedHashSet<String>();
    myDirList = getFileContents("logDir_", myDirList);
    if (vmList.size() != stopModeList.size()) {
      throw new TestException("Expected targetVmList " + vmList + " and stopModeList " +
          stopModeList + " to be parallel lists of the same size, but they have different sizes");
    }
    Log.getLogWriter().info("In stopStartVMs, vms to stop: " + vmList +
        ", corresponding stop modes: " + stopModeList);
    for (int i = 0; i < vmList.size(); i++) {
      ClientVmInfo targetVm = (ClientVmInfo) (vmList.get(i));
      String stopMode = stopModeList.get(i);
      String clientName = targetVm.getClientName();
      for (String vmDir : myDirList) {
        if (vmDir.contains(clientName)) {
          recycleVM(vmDir, stopMode, clientName, vmName, isDmlOp, restart, rebalance);
        }
      }
    }
  }

  protected void recycleVM(String vmDir, String stopMode, String clientName, String vmName,
                           boolean isDmlOp, boolean restart, boolean rebalance) {
    if (isDmlOp && vmName.equalsIgnoreCase("locator") && !restart) {
      SnappyLocatorHATest.ddlOpDuringLocatorHA(vmDir, clientName, vmName);
    } else if (isDmlOp && vmName.equalsIgnoreCase("locator") && restart) {
      SnappyLocatorHATest.ddlOpAfterLocatorStop_ClusterRestart(vmDir, clientName, vmName);
    } else if (isDmlOp && vmName.equalsIgnoreCase("server") && restart && rebalance) {
      SnappyStartUpTest.serverHAWithRebalance_clusterRestart(vmDir, clientName, vmName);
    } else if (isDmlOp && vmName.equalsIgnoreCase("server") && restart && !rebalance) {
      SnappyStartUpTest.opsDuringServerHA_clusterRestart(vmDir, clientName, vmName);
    } else if (!isDmlOp && !restart && !rebalance && (stopMode.equalsIgnoreCase("NiceKill") ||
        stopMode.equalsIgnoreCase("NICE_KILL"))) {
      killVM(vmDir, clientName, vmName);
      try {
        Thread.sleep(SnappyPrms.getSleepTimeSecsBeforRestart() * 1000);
      } catch (InterruptedException e) {
        String s = "Exception occurred while waiting for the kill " + clientName + "process " +
            "execution..";
        throw new TestException(s, e);
      }
      startVM(vmDir, clientName, vmName);
    }
  }

  protected void killVM(String vmDir, String clientName, String vmName) {
    if (vmName.equalsIgnoreCase("lead")) {
      regenerateConfigData(vmDir, "leads", clientName, vmName);
      HydraTask_stopSnappyLeader();
    } else if (vmName.equalsIgnoreCase("server")) {
      regenerateConfigData(vmDir, "servers", clientName, vmName);
      HydraTask_stopSnappyServers();
    } else if (vmName.equalsIgnoreCase("locator")) {
      regenerateConfigData(vmDir, "locators", clientName, vmName);
      HydraTask_stopSnappyLocator();
    }
    try {
      Thread.sleep(10000);
      boolean serverStopFailed = snappyTest.waitForMemberStop(vmDir, clientName, vmName);
      if (serverStopFailed) {
        // try again
        Thread.sleep(60000);
        serverStopFailed = snappyTest.waitForMemberStop(vmDir, clientName, vmName);
        if (serverStopFailed) {
          // last try
          Thread.sleep(120000);
          serverStopFailed = snappyTest.waitForMemberStop(vmDir, clientName, vmName);
          if (serverStopFailed) {
            threadDumpForAllServers();
          }
        }
      }
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for the kill " + clientName + "process " +
          "execution..";
      throw new TestException(s, e);
    }
    Log.getLogWriter().info(clientName + " stopped successfully...");
  }

  /*
   * Dump stacks for threads in members of snappy cluster
   */

  public static synchronized void HydraTask_dumpStacks() {
    snappyTest = new SnappyTest();
    snappyTest.getClientHostDescription();
    snappyTest.dumpStacks();
  }

  public void dumpStacks() {
    File currDir = new File(".");
    boolean checkErrors = new File(currDir, "errors.txt").exists();
    boolean checkHang = new File(currDir, "hang.txt").exists();
    { // record SparkSubmit processes if left
      Vector<String> hostNamesList = BasePrms.tab().vecAt(HostPrms.hostNames, null);
      if (hostNamesList == null || hostNamesList.isEmpty()) {
        hostNamesList = new Vector<String>();
        hostNamesList.add("localhost");
      }
      for (int i = 0; i < hostNamesList.size(); i++)
        recordSnappyProcessIDinNukeRun("SparkSubmit", hostNamesList.get(i));

    }
    Set pids = getPidList();
    if (checkErrors || checkHang) {
      int dumpItr = SnappyPrms.getNumOfStackDumpItrs();
      Log.getLogWriter().info("Dump stacks for " + dumpItr + " iterations.");
      for (int i = 0; i < dumpItr; i++) {
        Log.getLogWriter().info("Dumping stacks for iteration " + i);
        getThreadDump(pids);
        if (i < (dumpItr - 1)) {
          Log.getLogWriter().info("Sleeping before next thread dump...");
          sleepForMs(SnappyPrms.getSleepBtwnStackDumps());
        }
      }
    } else {
      Log.getLogWriter().info("Test has no failures. Hence no need to take process stack dumps.");
    }
    evaluateThrdTypeCounts(pids);
  }

  protected void getThreadDump(Set<String> pidList) {
    String thrDumpScript = getCurrentDirPath() + File.separator + "threadDump.sh";
    Process pr = null;
    ProcessBuilder pb;
    File logFile = new File(thrDumpScript);
    try {
      if (!logFile.exists()) {
        FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);
        for (String pidString : pidList) {
          int pid = Integer.parseInt(pidString);
          String host = snappyTest.getPidHost(Integer.toString(pid));
          if (host.equalsIgnoreCase("localhost")) {
            bw.write("kill -23 " + pid);
            bw.newLine();
            bw.write("sleep 4;");
            bw.newLine();
            bw.write("kill -3 " + pid);
          } else {
            bw.write("ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " +
                host + " kill -23 " + pid);
            bw.newLine();
            bw.write("sleep 4;");
            bw.newLine();
            bw.write("ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " +
                host + " kill -3 " + pid);
          }
          bw.newLine();
        }
        bw.close();
        fw.close();
        logFile.setExecutable(true);
      }
      String thrDumpLog = getCurrentDirPath() + File.separator + "threadDump.log";
      File thrDumpOutput = new File(thrDumpLog);

      pb = new ProcessBuilder(thrDumpScript);
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(thrDumpOutput));
      pr = pb.start();
      pr.waitFor();
    } catch (IOException ie) {
      throw new TestException("IOException occurred while retriving logFile path.\nError " +
          "Message:" + ie.getMessage());
    } catch (InterruptedException ie) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new TestException(s, ie);
    }
  }

  protected void evaluateThrdTypeCounts(Set<String> pidList) {
    String thrDumpScript = getCurrentDirPath() + File.separator + "jstackDump.sh";
    Process pr = null;
    ProcessBuilder pb;
    File logFile = new File(thrDumpScript);
    try {
      if (!logFile.exists()) {
        FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);
        for (String pidString : pidList) {
          int pid = Integer.parseInt(pidString);
          String host = snappyTest.getPidHost(Integer.toString(pid));
          if (host.equalsIgnoreCase("localhost")) {
            bw.write("jstack " + pid + " > " + pid + "_dump.txt");
          } else {
            bw.write("ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " +
                host + " jstack " + pid + " > " + pid + "_dump.txt");
          }
          bw.newLine();
        }
        bw.close();
        fw.close();
        logFile.setExecutable(true);
      }
      String thrDumpLog = getCurrentDirPath() + File.separator + "jStackDump.log";
      File thrDumpOutput = new File(thrDumpLog);
      pb = new ProcessBuilder(thrDumpScript);
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(thrDumpOutput));
      pr = pb.start();
      pr.waitFor();

      for (String pidString : pidList) {
        Map<String,Integer> thrMap = new HashMap<String, Integer>();
        try {
          String fileName = getCurrentDirPath() + File.separator + pidString + "_dump.txt";
          String line = null;
          String map_id = null;
          BufferedReader br = new BufferedReader(new FileReader(fileName));
          int totalThrdCnt = 0;
          while ((line = br.readLine()) != null) {
            if (line.startsWith("\"")) {
              String thrdName = line.substring(line.indexOf("\"") + 1, line.lastIndexOf("\""));
              if (thrdName.length() < 13) {
                if (thrdName.contains("#")) map_id = thrdName.substring(0, thrdName.indexOf("#"));
                else if (thrdName.length() < 6 || ((!thrdName.contains("-"))))
                  map_id = thrdName;

                else map_id = thrdName.substring(0, 6);
              } else {
                map_id = thrdName.substring(0, 13);
                if (map_id.contains("#"))
                  map_id = map_id.substring(0, map_id.indexOf("#"));
              }
              int cnt = 0;
              if (thrMap.containsKey(map_id))
                cnt = thrMap.get(map_id);
              thrMap.put(map_id, ++cnt);
              totalThrdCnt++;
            }
          }
          StringBuilder aStr = new StringBuilder("The " + totalThrdCnt + " threads in process " +
              pidString + " are :\n");
          for(Map.Entry<String,Integer> entry : thrMap.entrySet()) {
            aStr.append(entry.getKey() + " : " + entry.getValue() + "\n");
          }
          Log.getLogWriter().info(aStr.toString());
        } catch (FileNotFoundException fe) {
          throw new TestException("Could not find file in the specified path. Error Message :"
              + fe.getMessage());
        } catch (IOException ie) {
          throw new TestException("IOException occurred while retriving file contents.Error " +
              "Message:" + ie.getMessage());
        }
      }
    } catch (IOException ie) {
      throw new TestException("IOException occurred while retriving logFile path.\nError " +
          "Message:" + ie.getMessage());
    } catch (InterruptedException ie) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new TestException(s, ie);
    }
  }

  protected static void threadDumpForAllServers() {
    Set<String> pidList;
    pidList = SnappyStartUpTest.getServerPidList();
    pidList.addAll(SnappyStartUpTest.getLeaderPidList());
    snappyTest.getThreadDump(pidList);
  }


  protected boolean waitForMemberStop(String vmDir, String clientName, String vmName) {
    File commandOutput;
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + clientName + "Status_" +
          RemoteTestModule.getCurrentThread().getThreadId() + "_" + System.currentTimeMillis() + ".log";
      commandOutput = new File(dest);
      String expression;
      String scriptName = null;
      if (vmName.equalsIgnoreCase("server")) {
        scriptName = "snappy-server.sh";
      } else if (vmName.equalsIgnoreCase("lead")) {
        scriptName = "snappy-lead.sh";
      } else if (vmName.equalsIgnoreCase("locator")) {
        scriptName = "snappy-locator.sh";
      }
      expression = snappyTest.getScriptLocation(scriptName) + " status " +
          " -dir=" + vmDir + " > " + commandOutput + " 2>&1 ; grep -e 'status: stopped' -e " +
          "'status: waiting' -e 'status: stopping' -e 'java.lang.IllegalStateException' " +
          commandOutput + " | wc -l)\"";
      String command = "while [ \"$(" + expression + " -le  0  ] ; do rm " +
          commandOutput + " ;  touch " + commandOutput + "   ;  sleep " +
          SnappyPrms.getSleepTimeSecsForMemberStatus() + " ; done";
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
      Log.getLogWriter().info("Server Status script for " + clientName + " starts at: " + System
          .currentTimeMillis());
      // output is already redirected by script
      executeProcess(pb, null);
      Log.getLogWriter().info("Server Status script for " + clientName + " finishes at:  " + System
          .currentTimeMillis());
    } catch (IOException e) {
      String s = "Problem while reading the file : " + logFile;
      throw new TestException(s, e);
    }
    return executeStatusTask(commandOutput);
  }

  protected boolean executeStatusTask(File commandOutput) {
    boolean found = false;
    try {
      FileInputStream fis = new FileInputStream(commandOutput);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String line;
      String searchString1 = "java.lang.IllegalStateException";
      String searchString2 = "status: stopping";
      String searchString3 = "status: waiting";
      while ((line = br.readLine()) != null && !found) {
        if (line.toLowerCase().contains(searchString1.toLowerCase()) || line.toLowerCase().contains
            (searchString2.toLowerCase()) || line.toLowerCase().contains(searchString3.toLowerCase
            ())) {
          found = true;
          Log.getLogWriter().info("member did not stop successfully...");
        }
      }
      br.close();
    } catch (FileNotFoundException e) {
      String s = "Unable to find file: " + logFile;
      throw new TestException(s);
    } catch (IOException e) {
      String s = "Problem while reading the file : " + logFile;
      throw new TestException(s, e);
    }
    return found;
  }

  protected boolean waitForStopAll() {
    File commandOutput;
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "stopAllStatus_" +
          RemoteTestModule.getCurrentThread().getThreadId() + "_" + System.currentTimeMillis() + ".log";
      commandOutput = new File(dest);
      String expression = snappyTest.getScriptLocation("snappy-status-all.sh")
          + " > " + commandOutput + " 2>&1 ; grep -e 'status: stopped' -e " +
          "'status: waiting' -e 'status: stopping' -e 'java.lang.IllegalStateException' " +
          commandOutput + " | wc -l)\"";
      String command = "while [ \"$(" + expression + " -le  0  ] ; do sleep " +
          SnappyPrms.getSleepTimeSecsForMemberStatus() + " ; done";
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
      Log.getLogWriter().info("snappy-status-all script starts at: " +
          System.currentTimeMillis());
      // output is already redirected by script
      executeProcess(pb, null);
      Log.getLogWriter().info("snappy-status-all script finishes at:  " + System
          .currentTimeMillis());
    } catch (IOException e) {
      String s = "Problem while reading the file : " + logFile;
      throw new TestException(s, e);
    }
    return executeStatusTask(commandOutput);
  }

  protected void startVM(String vmDir, String clientName, String vmName) {
    if (vmName.equalsIgnoreCase("lead")) {
      //regenerateConfigData(vmDir, "leads", clientName, vmName);
      startSnappyLead();
    } else if (vmName.equalsIgnoreCase("server")) {
      //regenerateConfigData(vmDir, "servers", clientName, vmName);
      startSnappyServer();
    } else if (vmName.equalsIgnoreCase("locator")) {
      //regenerateConfigData(vmDir, "locators", clientName, vmName);
      startSnappyLocator();
    }
    Log.getLogWriter().info(clientName + " restarted successfully...");
  }

  public void regenerateConfigData(String vmDir, String confFileName, String clientName, String
      vmName) {
    generateConfig(confFileName);
    Set<String> fileContent = new LinkedHashSet<String>();
    if (vmName.equalsIgnoreCase("lead")) {
      if (isLongRunningTest) writeLeadHostPortInfo();
      fileContent = snappyTest.getFileContents("leadLogDir", fileContent);
    } else if (vmName.equalsIgnoreCase("server")) {
      fileContent = snappyTest.getFileContents("serverLogDir", fileContent);
    } else if (vmName.equalsIgnoreCase("locator")) {
      fileContent = snappyTest.getFileContents("locatorLogDir", fileContent);
    }
    for (String nodeConfig : fileContent) {
      if (nodeConfig.contains(vmDir)) {
        writeNodeConfigData(confFileName, nodeConfig, true);
      }
    }
  }

  public static Object[] getPrimaryLeadVMWithHA(String clientMatchStr) {
    ArrayList vmList = new ArrayList();
    ArrayList stopModeList = new ArrayList();
    int myVmID = RemoteTestModule.getMyVmid();
    // get VMs that contain the clientMatchStr
    List vmInfoList = StopStartVMs.getAllVMs();
    vmInfoList = StopStartVMs.getMatchVMs(vmInfoList, clientMatchStr);
    // now all vms in vmInfoList match the clientMatchStr
    do {
      Object[] tmpArr = getClientVmInfo(vmInfoList, clientMatchStr);
      ClientVmInfo info = (ClientVmInfo) tmpArr[0];
      int randInt = (int) tmpArr[1];
      if (info.getVmid().intValue() != myVmID) { // info is not the current VM
        getLeadVM(info, vmList, stopModeList);
      }
      vmInfoList.remove(randInt);
    } while (vmList.size() < vmInfoList.size());
    return new Object[]{vmList, stopModeList, vmInfoList};
  }

  public static Object[] getPrimaryLeadVM(String clientMatchStr) {
    ArrayList vmList = new ArrayList();
    ArrayList stopModeList = new ArrayList();
    // get VMs that contain the clientMatchStr
    List vmInfoList = StopStartVMs.getAllVMs();
    vmInfoList = StopStartVMs.getMatchVMs(vmInfoList, clientMatchStr);
    // now all vms in vmInfoList match the clientMatchStr
    do {
      Object[] tmpArr = getClientVmInfo(vmInfoList, clientMatchStr);
      ClientVmInfo info = (ClientVmInfo) tmpArr[0];
      int randInt = (int) tmpArr[1];
      getLeadVM(info, vmList, stopModeList);
      vmInfoList.remove(randInt);
    } while (vmList.size() < vmInfoList.size());
    return new Object[]{vmList, stopModeList, vmInfoList};
  }

  protected static Object[] getClientVmInfo(List vmInfoList, String clientMatchStr) {
    if (vmInfoList.size() == 0) {
      throw new TestException("Unable to find lead node " +
          " vms to stop with client match string " + clientMatchStr +
          "; either a test problem or add StopStartVMs.StopStart_initTask to the test");
    }
    // add a VmId to the list of vms to stop
    int randInt = TestConfig.tab().getRandGen().nextInt(0, vmInfoList.size() - 1);
    ClientVmInfo info = (ClientVmInfo) (vmInfoList.get(randInt));
    return new Object[]{info, randInt};
  }

  protected static void getLeadVM(ClientVmInfo info, ArrayList vmList, ArrayList stopModeList) {
    Set<String> myDirList = new LinkedHashSet<String>();
    myDirList = getFileContents("logDir_", myDirList);
    String vmDir = null;
    String clientName = info.getClientName();
    for (String dir : myDirList) {
      if (dir.contains(clientName)) {
        vmDir = dir;
        break;
      }
    }
    Set<String> fileContent = new LinkedHashSet<String>();
    fileContent = snappyTest.getFileContents("leadLogDir", fileContent);
    boolean found = false;
    String primaryLeadPid = null;
    for (String nodeConfig : fileContent) {
      if (nodeConfig.contains(vmDir)) {
        //check for active lead member dir
        boolean isPrimaryLeadUp = isPrimaryLeadUpAndRunning();
        // try to query the sys.members three times for primary lead node status in case not 'Running' in normal senario
        if (!isPrimaryLeadUp) {
          for (int i = 0; i < 3; i++) {
            if (isPrimaryLeadUp) break;
            sleepForMs(5);
            isPrimaryLeadUp = isPrimaryLeadUpAndRunning();
          }
        }
        if (cycleVms && !isPrimaryLeadUp) {
          for (int i = 0; i < 3; i++) {
            if (isPrimaryLeadUp) break;
            sleepForMs(5);
            isPrimaryLeadUp = isPrimaryLeadUpAndRunning();
          }
        }
        if (!isPrimaryLeadUp) {
          throw new TestException("Primary lead node is not up and running in the cluster.");
        }
        primaryLeadPid = (String) SnappyBB.getBB().getSharedMap().get("PrimaryLeadPID");
        String searchString1 = primaryLeadPid;
        Log.getLogWriter().info("primaryLeadPID: " + primaryLeadPid);
        File dirFile = new File(vmDir);
        for (File srcFile : dirFile.listFiles()) {
          if (srcFile.getAbsolutePath().contains("snappyleader.pid")) {
            try {
              FileInputStream fis = new FileInputStream(srcFile);
              BufferedReader br = new BufferedReader(new InputStreamReader(fis));
              String str = null;
              while ((str = br.readLine()) != null && !found) {
                if (str.toLowerCase().contains(searchString1.toLowerCase())) {
                  found = true;
                }
              }
              br.close();
            } catch (FileNotFoundException e) {
              String s = "Unable to find file: " + srcFile.getAbsolutePath();
              throw new TestException(s);
            } catch (IOException e) {
              String s = "Problem while reading the file : " + srcFile.getAbsolutePath();
              throw new TestException(s, e);
            }
          }
        }
      }
    }
    if (found) {
      vmList.add(info);
      // choose a stopMode
      String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes, "NICE_KILL");
      stopModeList.add(choice);
    }
  }

  protected static String getSparkMasterHost() {
    String masterHost = null;
    if (isLongRunningTest) {
      if (isUserConfTest) {
        masterHost = SnappyPrms.getMasterHost();
        if (masterHost == null)
          throw new TestException("Spark Master hostname configuration parameter not provided.");
        Log.getLogWriter().info("masterHost is : " + masterHost);
        snappyTest.writeNodeConfigData("masterHost", masterHost, false);
      } else {
        masterHost = getDataFromFile("masterHost");
        if (masterHost == null) {
          masterHost = getMasterHost();
          Log.getLogWriter().info("masterHost is : " + masterHost);
          snappyTest.writeNodeConfigData("masterHost", masterHost, false);
        }
      }
    } else masterHost = getMasterHost();
    return masterHost;
  }

  protected static String getMasterHost() {
    String masterHost = (String) SnappyBB.getBB().getSharedMap().get("masterHost");
    if (masterHost == null) {
      try {
        File log = new File(".");
        String dest = log.getCanonicalPath();
        String masterFileName = "spark-*.Master-1-*.out";
        String masterFilePath = snappyTest.getUserAppJarLocation(masterFileName, dest);
        masterHost = masterFilePath.substring(masterFilePath.lastIndexOf("Master-1-") + 9, masterFilePath.lastIndexOf(".out"));
        SnappyBB.getBB().getSharedMap().put("masterHost", masterHost);
        Log.getLogWriter().info("Master host is : " + SnappyBB.getBB().getSharedMap().get("masterHost"));
      } catch (Exception e) {
        String s = "Spark Master host not found";
        throw new HydraRuntimeException(s, e);
      }
    }
    return masterHost;
  }


  private String printStackTrace(Exception e) {
    StringWriter error = new StringWriter();
    e.printStackTrace(new PrintWriter(error));
    return error.toString();
  }

  public List<String> getHostNameFromConf(String nodeName) {
    List<String> hostNames = new ArrayList<>();
    String confFile = snappyTest.getScriptLocation(productConfDirPath + File.separator + nodeName);
    try {
      File file = new File(confFile);
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] data = line.trim().split(" ");
        if (!hostNames.contains(data[0]))
          hostNames.add(data[0]);
      }
      fileReader.close();
    } catch (IOException e) {
      Log.getLogWriter().info(printStackTrace(e));
    }
    return hostNames;
  }


  protected void startSnappyLocator() {
    File log = null;
    ProcessBuilder pb = null;
    List<String> hostNames = getHostNameFromConf("locators");
    try {
      if (useRowStore) {
        Log.getLogWriter().info("Starting locator/s using rowstore option...");
        pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "start", "rowstore");
      } else {
        pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "start");
      }
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyLocatorSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      sleepForMs(30);
      for(int i = 0; i< hostNames.size(); i++)
        recordSnappyProcessIDinNukeRun(hostNames.get(i), "LocatorLauncher");
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  protected void startSnappyServer() {
    File log = null;
    ProcessBuilder pb = null;
    List<String> hostNames = getHostNameFromConf("servers");
    try {
      if (useRowStore) {
        Log.getLogWriter().info("Starting server/s using rowstore option...");
        pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "start", "rowstore");
      } else {
        pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "start");
      }
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      boolean serverStartFailed = snappyTest.executeStatusTask(logFile);
      if (serverStartFailed) {
        snappyTest.threadDumpForAllServers();
        /*Thread.sleep(60000);
        startSnappyServer();*/
      }
      sleepForMs(30);
      for(int i = 0; i< hostNames.size(); i++)
        recordSnappyProcessIDinNukeRun(hostNames.get(i), "ServerLauncher");
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  public static void sleepForMs(int sleepTimeInSec){
    try {
      Thread.sleep(sleepTimeInSec * 1000);
    } catch (InterruptedException ie) {
      throw new TestException("Got exception while thread was sleeping..", ie);
    }
  }

  protected void startSnappyLead() {
    File log = null;
    List<String> hostNames = getHostNameFromConf("leads");
    try {
      ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"),
          "start");
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyLeaderSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
      sleepForMs(30);
      for(int i = 0; i< hostNames.size(); i++)
        recordSnappyProcessIDinNukeRun(hostNames.get(i), "LeaderLauncher");
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  protected LogWriter log() {
    return Log.getLogWriter();
  }

}
