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

package io.snappydata.hydra.adAnalytics;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.BasePrms;
import hydra.HostPrms;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.streaming_sink.StringMessageProducer;
import io.snappydata.hydra.testDMLOps.DerbyTestUtils;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.hydra.testDMLOps.SnappySchemaPrms;
import org.apache.commons.io.FileUtils;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class SnappyAdAnalyticsTest extends SnappyTest {

  public static String kafkaDir = TestConfig.tab().stringAt(SnappyPrms.kafkaDir, null);
  public static String snappyPocJarPath = TestConfig.tab().stringAt(SnappyPrms.snappyPocJarPath, null);
  protected static SnappyAdAnalyticsTest snappyAdAnalyticsTest;
  public static String zookeeperHost = null;
  public static String zookeeperPort = null;
  public static String kafkaLogDir = TestConfig.tab().stringAt(SnappyPrms.kafkaLogDir, null);
  public static int initialBrokerPort = 9092;
  public static int initialBrokerId = 0;
  public static int retryCount = SnappyPrms.getRetryCountForJob();
  public static String[] hostnames;
  public SnappyAdAnalyticsTest() {
  }

  public static String getCurrentDirPath(){
    String currentDir;
    try {
      currentDir = new File(".").getCanonicalPath();
    } catch (IOException e) {
      String s = "Problem while accessing the current dir.";
      throw new TestException(s, e);
    }
    return currentDir;
  }

  public static String[] getNames(Long key) {
    Vector vec = BasePrms.tab().vecAt(key, null);
    if(vec == null) return null;
    String[] strArr = new String[vec.size()];
    for (int i = 0; i < vec.size(); i++) {
      strArr[i] = (String)vec.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }


  public static String[] getHostNames() {

    String[] vmNames = getNames(HostPrms.names);
    String[] vmHostNames = null;
    int numServers=0;
    if(SnappyTest.isUserConfTest) {
      vmHostNames = getNames(SnappyPrms.hostNames);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.numServers);
      SnappyBB.getBB().getSharedCounters().setIfLarger(SnappyBB.numServers,vmHostNames.length);
      Log.getLogWriter().info("The vmHostNames size = " + vmHostNames.length);
    }
    else {
      vmHostNames = getNames(HostPrms.hostNames);
    }
    numServers = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
    Log.getLogWriter().info("Number of servers = " + numServers);
    hostnames = new String[numServers];
    if(vmHostNames==null) {
      for (int j = 0; j<numServers ;  j++)
        hostnames[j] = "localhost";
    } else {
      int j = 0;
      for (int i = 0; i < vmNames.length; i++) {
        if (SnappyTest.isUserConfTest || vmNames[i].startsWith("snappyStore")) {
          hostnames[j] = vmHostNames[i];
          Log.getLogWriter().info("Host name is " + hostnames[j]);
          j++;
        }
      }
    }
    return hostnames;
  }

  public static synchronized void HydraTask_initializeSnappyAdAnalyticsTest() {
    if (snappyAdAnalyticsTest == null)
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
    if (kafkaDir == null) {
      String s = "Did not specify kafka directory.";
      throw new TestException(s);
    }
    if(kafkaLogDir==null)
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    new File(kafkaLogDir).mkdir();
    snappyAdAnalyticsTest.writeSnappyPocToSparkEnv();
  }

  protected void writeSnappyPocToSparkEnv() {
    String filePath = productConfDirPath + sep + "spark-env.sh";
    File file = new File(filePath);
    file.setExecutable(true);
    String fileContent = "SPARK_DIST_CLASSPATH=" + snappyPocJarPath;
    snappyAdAnalyticsTest.writeToFile(fileContent, file,true);
  }

  /**
   * Start kafka zookeeper.
   */

  public static synchronized void HydraTask_StartKafkaZookeeper() {
    snappyAdAnalyticsTest.startZookeeper();
  }

  protected void startZookeeper() {
    ProcessBuilder pb = null;
    try {
      getHostNames();
      String zookeeperLogDirPath = kafkaLogDir + sep + "zookeeper";
      new File(zookeeperLogDirPath).mkdir();
      String dest = zookeeperLogDirPath + sep + "zookeeperSystem.log";
      File logFile = new File(dest);

      String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/zookeeper-server-start.sh");
      String orgPropFilePath = snappyTest.getScriptLocation(kafkaDir + sep + "config/zookeeper.properties");
      File orgPropFile = new File(orgPropFilePath);

      String myPropFilePath = zookeeperLogDirPath + sep + "zookeeper.properties";
      File myPropFile = new File(myPropFilePath);
      //create copy of properties file to modify
      FileUtils.copyFile(orgPropFile, myPropFile);
      // change log dir in properperty file
      zookeeperHost = hostnames[0];
      modifyPropFile(myPropFile, "dataDir=", zookeeperLogDirPath);
      modifyPropFile(myPropFile,"host.name=", zookeeperHost);
      String command = "";
      if(!zookeeperHost.equals("localhost"))
        command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + zookeeperHost;
      command = "nohup " + command + " " + script + " " + myPropFilePath + " > " + logFile + " &";
      Log.getLogWriter().info("Executing command : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      recordSnappyProcessIDinNukeRun("QuorumPeerMain");

      Log.getLogWriter().info("Started Kafka zookeeper");
      //Store zookeeper host and port on blackboard
      updateBlackboard(myPropFile, "clientPort");
    } catch (IOException e) {
      String s = "Problem while copying properties file.";
      throw new TestException(s, e);
    }
  }

  // save zookeeper host and port details on blackboard
  protected void updateBlackboard(File file, String searchString) {
    try {
      SnappyBB.getBB().getSharedMap().put("zookeeperHost", zookeeperHost);
      FileInputStream fis = new FileInputStream(file);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String str = null;
      boolean found = false;
      while (!found && (str = br.readLine()) != null) {
        if (str.contains(searchString)) {
          found = true;
        }
      }
      br.close();
      if (found) {
        zookeeperPort = str.split("=")[1];
        SnappyBB.getBB().getSharedMap().put("zookeeperPort", zookeeperPort);
      } else
        throw new TestException("Didnot find port for zookeeper");
    } catch (IOException e) {
      String s = "Problem while reading the file : " + file;
      throw new TestException(s, e);
    }
  }

  /**
   * Start kafka brokers.
   */
  public static synchronized void HydraTask_StartKafkaBrokers() {
    snappyAdAnalyticsTest.startKafkaBroker();
  }

  protected void startKafkaBroker() {

    int numServers = (int)SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
    Log.getLogWriter().info("Test will start " + numServers + " kafka brokers.");
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-server-start.sh");
    String orgPropFilePath = snappyTest.getScriptLocation(kafkaDir + sep + "config/server.properties");
    File orgPropFile = new File(orgPropFilePath);
    try {
      for (int i = 1; i <= numServers; i++) {
        String command = "";
        String broker = "broker" + i;
        String brokerLogDirPath = kafkaLogDir + sep + broker;
        new File(brokerLogDirPath).mkdir();

        String dest = brokerLogDirPath + sep + "brokerSystem.log";
        File logFile = new File(dest);

        String myPropFilePath = brokerLogDirPath + sep + broker + ".properties";
        File myPropFile = new File(myPropFilePath);
        //create copy of properties file to modify
        FileUtils.copyFile(orgPropFile, myPropFile);
        String hostname = hostnames[i-1];
        //change port and logdir for servers
        modifyPropFile(myPropFile,"log.dirs=", brokerLogDirPath);
        modifyPropFile(myPropFile,"zookeeper.connect=",zookeeperHost+":"+ zookeeperPort);
        modifyPropFile(myPropFile,"host.name=",hostname);
        modifyPropFile(myPropFile,"port=",Integer.toString(initialBrokerPort));
        modifyPropFile(myPropFile,"broker.id=",Integer.toString(initialBrokerId++));
        Log.getLogWriter().info(broker + " properties files is  " + myPropFile);
        if(!hostname.equals("localhost"))
          command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostname;
        command = "nohup " + command + " " + script + " " + myPropFilePath + " > " + logFile + " &";
        Log.getLogWriter().info("Executing command : " + command);
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        snappyTest.executeProcess(pb, logFile);

        Log.getLogWriter().info("Started kafka " + broker);
        if (i == 1)
          SnappyBB.getBB().getSharedMap().put("brokerList", hostname + "--" + initialBrokerPort);
        initialBrokerPort = initialBrokerPort + 2;
      }
    }  catch (IOException e) {
      String s = "Problem while copying properties file.";
      throw new TestException(s, e);
    }
    recordSnappyProcessIDinNukeRun("Kafka");
  }

  //change log directory and port for property file
  protected void modifyPropFile(File file, String searchString, String replaceString) {
    String str = null;
    boolean isReplaced = false;
    ArrayList<String> lines = new ArrayList<String>();
    try {
      FileInputStream fis = new FileInputStream(file);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      while ((str = br.readLine()) != null) {
        if (str.trim().startsWith(searchString)) {
          str = str.replace(str.split("=")[1], replaceString);
          isReplaced = true;
          Log.getLogWriter().info("File str is ::" + str);
        }
        lines.add(str + "\n");
      }
      if(!isReplaced)
        lines.add(searchString + replaceString);
      br.close();
      fis.close();
      FileWriter fw = new FileWriter(file);
      for (String line : lines)
        fw.write(line);
      fw.close();
    } catch (IOException e) {
      String s = "Problem while reading the file : " + file;
      throw new TestException(s, e);
    }
  }

  /**
   * Start kafka topic.
   */

  public static void HydraTask_StartKafkaTopic() {
    snappyAdAnalyticsTest.startKafkaTopic(SnappyPrms.getKafkaTopic());
  }

  protected void startKafkaTopic(Vector topics) {
    ProcessBuilder pb = null;

    zookeeperHost = (String)SnappyBB.getBB().getSharedMap().get("zookeeperHost");
    zookeeperPort = (String)SnappyBB.getBB().getSharedMap().get("zookeeperPort");

    for (int i = 0; i < topics.size(); i++) {
      String topic = (String)topics.elementAt(i);
      String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-topics.sh");
      String command = script + " --create --zookeeper " + zookeeperHost + ":" + zookeeperPort +
          " --partition 8 --topic " + topic + " --replication-factor=1";
      String dest = kafkaLogDir + sep + "startTopic-" + topic + ".log";
      File logFile = new File(dest);
      Log.getLogWriter().info("Executing command : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      Log.getLogWriter().info("Started Kafka topic: " + topic);
    }
  }

  public static void HydraTask_executeSnappyStreamingJob() {
    snappyAdAnalyticsTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames(),
        "snappyStreamingJobResult_" + System.currentTimeMillis() + ".log");
  }

  protected void executeSnappyStreamingJob(Vector jobClassNames, String logFileName) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    String APP_PROPS = "";
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;

    String userJarPath = getUserAppJarLocation(SnappyPrms.getUserAppJar(),jarPath);
    verifyDataForJobExecution(jobClassNames, userJarPath);
    leadHost = getLeadHost();
    String brokerList = null;
    String leadPort = getLeadPort();
    for (int i = 0; i < jobClassNames.size(); i++) {
      String userJob = (String)jobClassNames.elementAt(i);
      if (SnappyPrms.getCommaSepAPPProps() != null) {
        String[] appProps = SnappyPrms.getCommaSepAPPProps().split(",");
        for (int j = 0; j < appProps.length; j++)
          APP_PROPS = APP_PROPS + " --conf " + appProps[j];
      }
      brokerList = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
      APP_PROPS = APP_PROPS + " --conf brokerList=" + brokerList + " --conf tid=" + getMyTid();
      String appName = SnappyPrms.getUserAppName();
      Log.getLogWriter().info("APP PROPS :" + APP_PROPS);
      String snappyJobCommand = snappyJobScript + " submit --lead " + leadHost + ":" + leadPort +
          " --app-name " + appName + " --class " + userJob + " --app-jar " + userJarPath +
          APP_PROPS;
      //if(!useStreamingSink)
      //  snappyJobCommand += " --stream ";
      String dest = getCurrentDirPath() + File.separator + logFileName;
      Log.getLogWriter().info("Executing command:" + snappyJobCommand);
      logFile = new File(dest);
      pb = new ProcessBuilder("/bin/bash", "-c", snappyJobCommand);
      snappyTest.executeProcess(pb, logFile);
      String jobID = getJobID(logFile);
      if (jobID == null) {
        if (retryCount > 0) {
          retryCount--;
          retrievePrimaryLeadHost();
          HydraTask_executeSnappyStreamingJob();
        } else
          throw new TestException("Failed to start the streaming job. Please check the logs.");
      } else {
        Log.getLogWriter().info("JobID is : " + jobID);
        for (int j = 0; j < 3; j++) {
          if (!getJobStatus(jobID)) {
            throw new TestException("Got Exception while executing streaming job. Please check " +
                "the job status output.");
          }
          SnappyBB.getBB().getSharedMap().put(appName, jobID);
        }
      }
    }
  }

  public static void HydraTask_verifyResults(){
    if(DerbyTestUtils.hasDerbyServer)
      SnappyDMLOpsUtil.HydraTask_verifyResults();
    else snappyAdAnalyticsTest.verifyResults();
  }

  public void verifyResults(){
    String query = "select * from ";
    try {
      String queryResultDirPath;
      try {
        queryResultDirPath = new File(".").getCanonicalPath() + File.separator + "queryResults";
        File queryResultDir = new File(queryResultDirPath);
        if (!queryResultDir.exists())
          queryResultDir.mkdirs();
      } catch (IOException ie) {
        throw new TestException ("Got exception while creating directory for query results.");
      }
      Connection conn = getLocatorConnection();
      ResultSet rs_tmp = null;
      ResultSet rs = conn.createStatement().executeQuery(query + " persoon");
      StructTypeImpl snappySti = ResultSetHelper.getStructType(rs);
      List<Struct> snappyList = ResultSetHelper.asList(rs, snappySti, false);
      int stream_tableCnt = snappyList.size();
      String streamTableFile = queryResultDirPath + File.separator + "persoon_" + getMyTid() + ".out";
      SnappyDMLOpsUtil.listToFile(snappyList, streamTableFile);
      snappyList.clear();
      rs.close();

      if(SnappySchemaPrms.isAggregate()) {
        String aggType = SnappySchemaPrms.getAggregateType();
        switch (aggType.toUpperCase()) {
          case "JOIN":
            query = "select tp.*, pd.languages from temp_persoon tp, persoon_details pd where tp.id=pd.id";
            break;
          case "AVG":
            query = "select id, avg(age) as avg_age, avg(numChild) as avg_numchild from temp_persoon group by id";
            break;
          case "SUM":
            query = "select id, sum(age) as sum_age, sum(numChild) as sum_numchild from temp_persoon group by id";
            break;
          case "COUNT":
            query = "select age, count(*) from temp_persoon group by age";
            break;
        }
      } else {
        query = query + "temp_persoon";
      }
      rs_tmp = conn.createStatement().executeQuery(query);
      StructTypeImpl rs_tmpSti = ResultSetHelper.getStructType(rs_tmp);
      List<Struct> rsTmpList = ResultSetHelper.asList(rs_tmp, rs_tmpSti, false);
      int snappyTableCnt = rsTmpList.size();
      String tmpTableFile = queryResultDirPath + File.separator + "tmp_tab" + getMyTid() + ".out";
      SnappyDMLOpsUtil.listToFile(rsTmpList, tmpTableFile);
      if(stream_tableCnt != snappyTableCnt)
        throw new TestException("Number of rows in snappy and streaming table different. Please " +
            "check the result at :" + queryResultDirPath);
      rsTmpList.clear();
      rs.close();
      SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
      String errMsg = testInstance.compareFiles(queryResultDirPath, streamTableFile, tmpTableFile,
          false, "streaming");
      if(errMsg.length()> 0 ){
        throw new TestException("Got exception while validating results : " + errMsg);
      }
    } catch (SQLException se) {
      Log.getLogWriter().info("Got exception while verifying results");
      throw new TestException("Got Exception while verifying results.", se);
    }
  }


  public static void HydraTask_executeSnappyStreamingApp() {
    snappyAdAnalyticsTest.executeSnappyStreamingApp(SnappyPrms.getSnappyStreamingJobClassNames(),
        "snappyStreamingAppResult_" + System.currentTimeMillis() + ".log");
  }

  protected void executeSnappyStreamingApp(Vector jobClassNames, String logFileName) {
    String snappyJobScript = getScriptLocation("spark-submit");
    String APP_PROPS = "";
    ProcessBuilder pb = null;
    File logFile = null;
    String userJarPath = SnappyPrms.getUserAppJar();
    verifyDataForJobExecution(jobClassNames, userJarPath);
    String brokerList = null;

    String masterHost = getSparkMasterHost();
    String masterPort = MASTER_PORT;
    String command = null;
    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    String userAppArgs = SnappyPrms.getUserAppArgs();
    userAppJar = SnappyPrms.getUserAppJar();
    String commonArgs = " --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError" +
        " --conf spark.extraListeners=io.snappydata.hydra.SnappyCustomSparkListener " +
        " --conf snappydata.connection=" + primaryLocatorHost + ":" + primaryLocatorPort;
    for (int i = 0; i < jobClassNames.size(); i++) {
      String userJob = (String)jobClassNames.elementAt(i);
      brokerList = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
      String appName = SnappyPrms.getUserAppName();
      Log.getLogWriter().info("APP PROPS :" + APP_PROPS);
      command = snappyJobScript + " --class " + userJob +
          " --master spark://" + masterHost + ":" + masterPort + " " +
          "--name " + appName + " " +
          SnappyPrms.getExecutorMemory() + " " +
          SnappyPrms.getSparkSubmitExtraPrms() + " " + commonArgs + " " +
          snappyTest.getUserAppJarLocation(userAppJar, jarPath) +
          " " + getMyTid() + " " + brokerList + " " + userAppArgs + " ";
      command = "nohup " + command + " > " + logFileName + " & ";
      String dest = getCurrentDirPath() + File.separator + logFileName;
      logFile = new File(dest);
      Log.getLogWriter().info("spark-submit command is : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      //wait for 2 min until the streaming query starts.
      sleepForMs(120);
      return;
    }
  }

  public String getJobID(File logFile) {
    String line = null;
    String jobID = null;
    try {
      BufferedReader inputFile = new BufferedReader(new FileReader(logFile));
      while ((line = inputFile.readLine()) != null) {
        if (line.contains("jobId")) {
          jobID = line.split(":")[1].trim();
          jobID = jobID.substring(1, jobID.length() - 2);
          break;
        }
      }
      inputFile.close();
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " +
          logFile.getAbsolutePath() + "\nError Message:" + e.getMessage());
    }
    return jobID;
  }

  public static void HydraTask_executeSQLScriptsWithSleep() {
    try { Thread.sleep(60000); } catch (InterruptedException ie) {}
    HydraTask_executeSQLScripts();
  }

  public static void HydraTask_restartStreaming() {
    HydraTask_stopStreamingJob();
    try { Thread.sleep(60000); } catch (InterruptedException ie) {}
    HydraTask_executeSnappyStreamingJob();
    try { Thread.sleep(30000); } catch (InterruptedException ie) {}
  }

  public static void HydraTask_restartLeadVMWithStreaming(){
    HydraTask_cycleLeadVM();
    try { Thread.sleep(60000); } catch (InterruptedException ie) {}
    HydraTask_executeSnappyStreamingJob();
  }

  public static void HydraTask_restartSnappyClusterForStreaming(){
    HydraTask_stopSnappyCluster();
    HydraTask_startSnappyCluster();
    HydraTask_executeSnappyStreamingJob();
  }

  /*
  Task for restarting the streaming in smart connector mode.
   */
  public static void HydraTask_restartStreamingApp() {
    HydraTask_closeStreamingApp();
    try { Thread.sleep(60000); } catch (InterruptedException ie) {}
    HydraTask_executeSnappyStreamingApp();
    try { Thread.sleep(30000); } catch (InterruptedException ie) {}
  }

  /*
  Task for restarting the leadVM
   */
  public static void HydraTask_restartLeadVMWithStreamingApp(){
    HydraTask_cycleLeadVM();
  }

  public static void HydraTask_restartSnappyClusterForStreamingApp(){
    HydraTask_stopSnappyCluster();
    HydraTask_startSnappyCluster();
    HydraTask_executeSnappyStreamingApp();
  }

  public boolean getJobStatus(String jobID){
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    leadHost = getLeadHost();
    String leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    try {
      String dest = getCurrentDirPath() + File.separator + "jobStatus_" + RemoteTestModule
          .getCurrentThread().getThreadId() + "_" + System.currentTimeMillis() + ".log";
      File commandOutput = new File(dest);
      String command = snappyJobScript + " status --lead " + leadHost + ":" + leadPort + " " +
          "--job-id " + jobID + " > " + commandOutput;
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
      executeProcess(pb, null);
      String line = null;
      BufferedReader inputFile = new BufferedReader(new FileReader(commandOutput));
      while ((line = inputFile.readLine()) != null) {
        if(line.contains("status") ){
          if (line.contains("ERROR"))
            return false;
          break;
        }
      } try { Thread.sleep(10*1000);} catch(InterruptedException ie) { }
    } catch (IOException ie){
      Log.getLogWriter().info("Got exception while accessing current dir");
    }
    return true;
  }

  /* Generator and Publisher for StringMessageProducer
  */
  public static void HydraTask_generateAndPublishMethod() {
    String[] appProps = null;
    if (SnappyPrms.getCommaSepAPPProps() != null) {
      appProps = SnappyPrms.getCommaSepAPPProps().split(",");
    }
    String[] appProps1 = Arrays.copyOf(appProps, appProps.length + 3);
    appProps1[appProps.length] = "" + TestConfig.tasktab().booleanAt(SnappyPrms.isConflationTest,
        false);
    appProps1[appProps.length + 1] = "" + DerbyTestUtils.hasDerbyServer;
    appProps1[appProps.length + 2] = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
    StringMessageProducer.generateAndPublish(appProps1);
  }

  /**
   * Start generating and publishing logs to Kafka
   */
  public static void HydraTask_generateAndPublish() {
    snappyAdAnalyticsTest.generateAndPublish(SnappyPrms.getSnappyStreamingJobClassNames());
  }

  protected void generateAndPublish(Vector generatorAndPublisher) {
    ProcessBuilder pb = null;
    String APP_PROPS = "";
    String userJarPath = getUserAppJarLocation(SnappyPrms.getUserAppJar(),jarPath);
    if (SnappyPrms.getCommaSepAPPProps() != null) {
      String[] appProps = SnappyPrms.getCommaSepAPPProps().split(",");
      for (int j = 0; j < appProps.length; j++)
        APP_PROPS = APP_PROPS + " " + appProps[j];
    }
    boolean hasDerby = false;
    String brokerList = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
    APP_PROPS += " " + hasDerby + " " + brokerList;
    for (int i = 0; i < generatorAndPublisher.size(); i++) {
      String dest = kafkaLogDir + sep + "generatorAndPublisher" + getMyTid() + "_" + i + ".log";
      File logFile = new File(dest);
      String processName= (String)generatorAndPublisher.elementAt(i);
      String command = "java -cp .:" + userJarPath + ":" + getStoreTestsJar() + ":" + productDir +
          "jars/* " +  processName + " " + APP_PROPS + " > " + logFile;
      if(SnappyPrms.executeInBackGround())
        command = "nohup " + command + " & ";
      Log.getLogWriter().info("Executing command : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      recordSnappyProcessIDinNukeRun(processName);
    }
  }

  public static void HydraTask_calculateStreamingTime(){
    List<String> tableName = SnappyPrms.getTableList();
    List<String> expectedRows = SnappyPrms.getNumRowsList();
    snappyAdAnalyticsTest.calculateStreamingTime(tableName.get(0),Integer.parseInt(expectedRows.get(0)));
  }

  public void calculateStreamingTime(String tableName, int expectedRows){
    Long startTime = System.currentTimeMillis();
    Long endTime = 0L;
    Connection conn = null;
    boolean allConsumed = false;
    int numRows = 0;
    try {
      conn = getLocatorConnection();
    } catch(SQLException se) {
      throw new TestException("Got exception while getting connection.",se);
    }
    try {
      while (!allConsumed) {
        ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + tableName);
        while (rs.next()) {
          numRows = rs.getInt(1);
        }
        Log.getLogWriter().info("Number of records returned are : " + numRows);
        if (numRows == expectedRows) {
          allConsumed = true;
          endTime = System.currentTimeMillis();
        }
      }
      Log.getLogWriter().info("Total time for consuming streaming data is :" + (endTime -
          startTime)/1000);
    } catch(SQLException se) {
      throw new TestException("Got exception while quering the table.",se);
    }

  }
  public static void HydraTask_assertStreaming(){
    List<String> tableName = SnappyPrms.getTableList();
    List<String> expectedRows = SnappyPrms.getNumRowsList();
    snappyAdAnalyticsTest.assertStreaming(tableName.get(0),Integer.parseInt(expectedRows.get(0)));
  }

  public void assertStreaming(String tableName, int expectedRows){
    Connection conn = null;
    int numRows = 0;
    try {
      conn = getLocatorConnection();
    } catch(SQLException se) {
      throw new TestException("Got exception while getting connection.",se);
    }
    try {
      ResultSet rs= conn.createStatement().executeQuery("select count(*) from " + tableName);
      while(rs.next()) {
        numRows = rs.getInt(1);
      }
      if(numRows != expectedRows){
        throw new TestException("Streaming app didnot consume all the data for " + tableName +
            " from the stream. Expected " + expectedRows + " rows to be consumed, but got only "
            + numRows + "rows.");
      }
    } catch(SQLException se) {
      throw new TestException("Got exception while quering the table.",se);
    }

  }

  /**
   * Stop kafka brokers.
   */
  public static synchronized void HydraTask_StopKafkaBrokers() {
    if(snappyAdAnalyticsTest == null) {
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    }
    snappyAdAnalyticsTest.stopKafkaBroker();
  }

  protected void stopKafkaBroker() {
    getHostNames();
    File log = null;
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-server-stop.sh");
    int numServers = (int)SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
    for (int i = 1; i <= numServers; i++) {
      String dest = kafkaLogDir + sep + "broker" + i + sep + "stopKafkaServer.log";
      File logFile = new File(dest);
      String hostname = hostnames[i-1];
      String command = "";
      if(!hostname.equals("localhost"))
        command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostname;
      command = command + "  " + script;
      Log.getLogWriter().info("Executing command : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
    }
    Log.getLogWriter().info("Stopped Kafka servers");
  }

  /**
   * Stop kafka zookeeper.
   */

  public static synchronized void HydraTask_StopKafkaZookeeper() {
    if(snappyAdAnalyticsTest == null) {
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    }
    snappyAdAnalyticsTest.stopKafkaZookeeper();
  }

  protected void stopKafkaZookeeper() {
    File log = null;
    String command = "";
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/zookeeper-server-stop.sh");
    String dest = kafkaLogDir + sep + "stopZookeeper.log";
    File logFile = new File(dest);
    String hostname = (String)SnappyBB.getBB().getSharedMap().get("zookeeperHost");
    if(!hostname.equals("localhost"))
      command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostname;
    command = command + " " + script;
    Log.getLogWriter().info("Executing command : " + command);
    pb = new ProcessBuilder("/bin/bash", "-c", command);
    snappyTest.executeProcess(pb, logFile);
    Log.getLogWriter().info("Stopped Kafka zookeeper");
  }

  public static void HydraTask_stopStreamingJob() {
    snappyAdAnalyticsTest.stopSnappyStreamingJob();
  }

  protected void stopSnappyStreamingJob() {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    ProcessBuilder pb = null;
    File logFile = null;
    leadHost = getLeadHost();
    String appName = SnappyPrms.getUserAppName();
    Log.getLogWriter().info("User App Name is : " + appName);
    String leadPort = (String)SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    String jobID = (String) SnappyBB.getBB().getSharedMap().get(appName);
    String snappyCmd = snappyJobScript + " stop --job-id " + jobID + " --lead " + leadHost + ":" + leadPort;
    Log.getLogWriter().info("Executing command :" + snappyCmd);
    String dest = getCurrentDirPath() + File.separator + "stopStreamingJobResult_" + jobID + ".log";
    logFile = new File(dest);
    pb = new ProcessBuilder("/bin/bash", "-c", snappyCmd);
    snappyTest.executeProcess(pb, logFile);
    //check status after stop
    snappyCmd = snappyJobScript + " status --job-id " + jobID + " --lead " + leadHost + ":" + leadPort;
    dest = getCurrentDirPath() + File.separator + "statusStreamingJob_" + jobID +".log";
    logFile = new File(dest);
    pb = new ProcessBuilder("/bin/bash", "-c", snappyCmd);
    snappyTest.executeProcess(pb, logFile);
  }

  public static void HydraTask_closeStreamingApp() {
    String curlCmd = null;
    ProcessBuilder pb = null;
    String appName = SnappyPrms.getUserAppName();
    String logFileName = "sparkStreamingStopResult_" + System.currentTimeMillis() + ".log";
    File log = null;
    File logFile = null;
    try {
      String hostName = getSparkMasterHost();
      curlCmd = "curl -d \"name=" + appName + "&terminate=true\" -X POST http://" + hostName + ":8080/app/killByName/";
      Log.getLogWriter().info("The curlCmd  is " + curlCmd);
      pb = new ProcessBuilder("/bin/bash", "-c", curlCmd);
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + logFileName;
      logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
    } catch (Exception ex) {
      Log.getLogWriter().info("Exception in HydraTask_closeStreamingJob() " + ex.getMessage());
    }
  }

}
