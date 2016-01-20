package io.snappydata.dunit.streaming

import java.sql.{Connection, DriverManager}

import dunit.AvailablePortHelper
import io.snappydata.dunit.cluster.ClusterManagerTestBase

/**
  * Created by ymahajan on 23/12/15.
  */
class StreamingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  override def tearDown2(): Unit = {
    super.tearDown2()
  }

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testStreamingAdhocSQL(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("streaming init 2")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '***REMOVED***', " +
        "consumerSecret '***REMOVED***', " +
        "accessToken '***REMOVED***', " +
        "accessTokenSecret '***REMOVED***', " +
        "rowConverter 'io.snappydata.dunit.streaming.TweetToRowsConverter')")
    s.execute("streaming start")

    s.execute("create topk table topkTweets on tweetsTable options(" +
        s"key 'hashtag', epoch '${System.currentTimeMillis()}', " +
        "timeInterval '2000ms', size '10')")
    for (a <- 1 to 5) {
      Thread.sleep(2000)
      s.execute("select text, fullName from tweetsTable where text like '%e%'")
      var rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
      s.execute("select * from topkTweets")
      println("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        println(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getObject(3)}")
        numResults += 1
      }
      println(s"Num results=$numResults")
      assert(numResults <= 10)
    }
    s.execute("streaming stop")
    s.execute("drop table tweetsTable")
    s.execute("drop table topktweets")

    conn.close()
  }

  def testSnappyStreamingContextStartStop(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("streaming stop")
    s.execute("streaming init 2")
    s.execute("streaming init 4")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '***REMOVED***', " +
        "consumerSecret '***REMOVED***', " +
        "accessToken '***REMOVED***', " +
        "accessTokenSecret '***REMOVED***', " +
        "rowConverter 'io.snappydata.dunit.streaming.TweetToRowsConverter')")
    s.execute("streaming start")
    s.execute("streaming start")
    s.execute("streaming stop")
    s.execute("streaming stop")
    s.execute("drop table tweetsTable")
    conn.close()
  }
}

