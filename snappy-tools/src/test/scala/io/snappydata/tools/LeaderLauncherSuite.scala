package io.snappydata.tools

import java.io.{PrintStream, ByteArrayOutputStream}

import scala.util.matching.Regex
import scala.util.{Success, Failure, Try}

import com.gemstone.gemfire.internal.cache.CacheServerLauncher
import com.gemstone.gemfire.internal.{DistributionLocator, AvailablePort}
import com.pivotal.gemfirexd.tools.GfxdDistributionLocator
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Created by soubhikc on 6/10/15.
 */
class LeaderLauncherSuite extends SnappyFunSuite with BeforeAndAfterAll {

  private val availablePort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)

  override def beforeAll(): Unit = {
    val f = new java.io.File("tests-snappy-loc-dir");
    f.mkdir()

    CacheServerLauncher.DONT_EXIT_AFTER_LAUNCH = true
    GfxdDistributionLocator.main(Array(
      "start",
      "-dir=" + f.getAbsolutePath,
      s"-peer-discovery-address=localhost",
      s"-peer-discovery-port=${availablePort}"
    ))
  }

  override def afterAll(): Unit = {
    GfxdDistributionLocator.main(Array(
      "stop",
      "-dir=tests-snappy-loc-dir"
    ))
    deleteDir(new java.io.File("tests-snappy-loc-dir"))
    CacheServerLauncher.DONT_EXIT_AFTER_LAUNCH = false
  }

  test("simple leader launch") {
    val dirname = createDir("tests-snappy-leader")
    val stream = new ByteArrayOutputStream()

    val currentOut = System.out

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + dirname,
        s"-locators=localhost[${availablePort}]"
      ))
    }

    try {
      start transform( { _ =>
        Try {
          System.setOut(new PrintStream(stream))
          LeaderLauncher.main(Array(
            "status",
            "-dir=" + dirname))
        } map { _ =>
          val outputLines = stream.toString
          assert(outputLines.replaceAll("\n", "").matches(
            "SnappyData Leader pid: [0-9]+ status: running" +
              "  Distributed system now has [0-9]+ members." +
              "  Other members: .*([0-9]+:.*)<.*>:[0-9]+".r), outputLines)

        }
      }, {
        throw _
      }) match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      System.setOut(currentOut)
      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + dirname
      ))
    }

  }

  test("leader standby") {

    def verifyStatus(workingDir: String, expectedOutput: String) = {
      val stream = new ByteArrayOutputStream()
      Try {
        System.setOut(new PrintStream(stream))
        LeaderLauncher.main(Array(
          "status",
          "-dir=" + workingDir))
      } map { _ =>
        val outputLines = stream.toString
        assert(outputLines.replaceAll("\n", "").matches(expectedOutput),
          workingDir + " returned with: \n" + outputLines)
      }
    }

    val leader1 = createDir("tests-snappy-leader-1")
    val leader2 = createDir("tests-snappy-leader-2")
    val currentOut = System.out

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader1,
        s"-locators=localhost[${availablePort}]"
      ))
    } transform(_ => Try {

      verifyStatus(leader1, "SnappyData Leader pid: [0-9]+ status: running.*").get

      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader2,
        s"-locators=localhost[${availablePort}]"
      ))
    }, {
      throw _
    })

    var isLeader1NotStopped = true
    try {
      val checkStandby = start transform(_ => {
        verifyStatus(leader2, "SnappyData Leader pid: [0-9]+ status: standby.*")
      }, throw _)


      val leader2TakeOver = checkStandby match {
        case Success(v) =>
          Try {
            LeaderLauncher.main(Array(
              "stop",
              "-dir=" + leader1))
            isLeader1NotStopped = false
          } transform(_ => {
            verifyStatus(leader2, "SnappyData Leader pid: [0-9]+ status: running.*")
          }, throw _)

        case Failure(t) => throw t
      }

      leader2TakeOver match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      System.setOut(currentOut)
      if (isLeader1NotStopped)
        LeaderLauncher.main(Array(
          "stop",
          "-dir=" + leader1
        ))

      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + leader2
      ))
    }


  }
}
