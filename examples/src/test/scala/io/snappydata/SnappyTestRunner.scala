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
package io.snappydata

import java.io._

import com.gemstone.gemfire.internal.AvailablePort
import org.apache.commons.io.FileUtils
import org.apache.commons.io.output.TeeOutputStream
import org.apache.spark.Logging
import org.scalatest.{BeforeAndAfterAll, FunSuite, Retries}

import scala.language.postfixOps
import scala.sys.process._
import scala.util.parsing.json.JSON

/**
 * Extensible Abstract test suite to test different shell based commands like submit jobs, snappy shell, spark shell etc.
 * The output of each of the processes are captured and validated.
 *
 * Class extending can mix match different methods like SnappyShell, Job to create a test case.
 */
abstract class SnappyTestRunner extends FunSuite
with BeforeAndAfterAll
with Serializable
with Logging with Retries {

  var snappyHome = ""
  var localHostName = ""
  var currWorkingDir = ""

  //One can ovveride this method to pass other parameters like heap size.
  def servers = s"$localHostName\n$localHostName"

  def snappyShell = s"$snappyHome/bin/snappy-shell"

  def sparkShell = s"$snappyHome/bin/spark-shell"

  private val availablePort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)
  private  var locatorDirPath = ""

  implicit class X(in: Seq[String]) {
    def pipe(cmd: String) =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  override def beforeAll(): Unit = {
    snappyHome = System.getenv("SNAPPY_HOME")
    if (snappyHome == null) {
      throw new Exception("SNAPPY_HOME should be set as an environment variable")
    }
    localHostName = java.net.InetAddress.getLocalHost().getHostName()
    currWorkingDir = System.getProperty("user.dir")
    val workDir = new File(s"$snappyHome/work")
    if (workDir.exists) {
      FileUtils.deleteDirectory(workDir)
    }
    startupCluster
  }

  override def afterAll(): Unit = {
    stopCluster
  }

  def stopCluster(): Unit = {
    executeProcess("snappyCluster", s"$snappyHome/sbin/snappy-stop-all.sh")
    new File(s"$snappyHome/conf/servers").deleteOnExit()
    executeProcess("sparkCluster", s"$snappyHome/sbin/stop-all.sh")
  }

  def startupCluster(): Unit = {
    new PrintWriter(s"$snappyHome/conf/servers") {
      write(servers)
      close
    }
    val (out, err) = executeProcess("snappyCluster", s"$snappyHome/sbin/snappy-start-all.sh")

    if (!out.contains("Distributed system now has 4 members")) {
      throw new Exception(s"Failed to start Snappy cluster")
    }
    val (out1, err1) = executeProcess("sparkCluster", s"$snappyHome/sbin/start-all.sh")
  }

  def executeProcess(name: String , command: String): (String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream

    val teeOut = new TeeOutputStream(stdout, new BufferedOutputStream(stdoutStream))
    val teeErr = new TeeOutputStream(stderr, new BufferedOutputStream(stderrStream))

    val stdoutWriter = new PrintStream(teeOut, true)
    val stderrWriter = new PrintStream(teeErr, true)

    val workDir = new File(s"$currWorkingDir/$name")
    if (!workDir.exists) {
      workDir.mkdir()
    }

    Process(command, workDir, "SNAPPY_HOME" -> snappyHome,
      "PYTHONPATH" -> s"${snappyHome}/python/lib/py4j-0.10.3-src.zip:${snappyHome}/python") !
      ProcessLogger(stdoutWriter.println, stderrWriter.println)
    (stdoutStream.toString, stderrStream.toString)
  }


  def SnappyShell(name: String, sqlCommand: Seq[String]): Unit = {
    sqlCommand pipe snappyShell foreach (s => {
      println(s)
      if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
        throw new Exception(s"Failed to run Query")
      }
    })
  }

  def Job(jobClass: String, lead: String, jarPath: String, confs: Seq[String] = Seq.empty[String]): Unit = {

    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""

    val jobSubmit = s"$snappyHome/bin/snappy-job.sh submit --lead $lead --app-jar $jarPath"

    val jobStatus = s"$snappyHome/bin/snappy-job.sh status --lead $lead --job-id "

    val jobCommand = s"$jobSubmit --app-name ${jobClass}_${System.currentTimeMillis()} --class $jobClass $confStr"

    val (out, err) = executeProcess("snappyCluster", jobCommand)

    val jobSubmitStr = out

    val jsonStr = if (jobSubmitStr.charAt(2) == '{')
      jobSubmitStr.substring(2)
    else jobSubmitStr.substring(4)

    def json = JSON.parseFull(jsonStr)
    val jobID = json match {
      case Some(map: Map[String, Any]) =>
        map.get("result").get.asInstanceOf[Map[String, Any]].get("jobId").get
      case other => throw new Exception(s"bad result : $jsonStr")
    }
    println("jobID " + jobID)


    var status = "RUNNING"
    while (status == "RUNNING") {
      Thread.sleep(3000)
      val statusCommand = s"$jobStatus $jobID"
      val (out, err) = executeProcess("snappyCluster", statusCommand)

      val jobSubmitStatus = out

      def statusjson = JSON.parseFull(jobSubmitStatus)
      statusjson match {
        case Some(map: Map[String, Any]) => {
          val v = map.get("status").get
          println("Current status of job: " + v)
          status = v.toString
        }
        case other => "bad Result"
      }
    }

    println(s" Job $jobClass finished with status $status")
    if (status == "ERROR") {
      throw new Exception(s"Failed to Execute job $jobClass")
    }
  }

  def SparkSubmit(name: String, appClass: String,
                  master: Option[String],
                  confs: Seq[String] = Seq.empty[String],
                  appJar: String): Unit = {

    val masterStr = master.getOrElse(s"spark://$localHostName:7077")
    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    val classStr = if (appClass.isEmpty) "" else s"--class  $appClass"
    val sparkSubmit = s"$snappyHome/bin/spark-submit $classStr --master $masterStr $confStr $appJar"
    val (out, err) = executeProcess(name, sparkSubmit)

    if (out.toLowerCase().contains("exception")) {
      throw new Exception(s"Failed to submit $appClass")
    }
  }

  def RunExample(name: String, exampleClas: String,
                 args: Seq[String] = Seq.empty[String]): Unit = {
    val argsStr = args.mkString(" ")
    val runExample = s"$snappyHome/bin/run-example $exampleClas $argsStr"
    val (out, err) = executeProcess(name, runExample)

    if (out.toLowerCase().contains("exception")) {
      throw new Exception(s"Failed to run $exampleClas")
    }
  }

  def SparkShell(confs: Seq[String], options: String, scriptFile : String): Unit = {
    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    val shell = s"$sparkShell $confStr $options -i $scriptFile"
    val (out, err) = executeProcess("snappyCluster", shell)
    if (out.toLowerCase().contains("exception")) {
      throw new Exception(s"Failed to run $shell")
    }
  }

  def SparkShell(confs: Seq[String], options: String, scalaStatements: Seq[String]): Unit ={
    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    scalaStatements pipe s"$snappyShell $confStr $options" foreach (s => {
      println(s)
      if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
        throw new Exception(s"Failed to run Scala statement")
      }
    })
  }

/*  def withExceptionHandling[T](function: => T): T = {
    try {
      function
    } catch {
      case e: Exception => throw e
  }*/
}
