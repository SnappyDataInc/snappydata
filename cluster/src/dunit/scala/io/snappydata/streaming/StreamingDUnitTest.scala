package io.snappydata.streaming

import java.sql.{Connection, DriverManager}

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

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
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.streaming.TweetToRowsConverter')")
    s.execute("streaming start")
    s.execute("streaming start")
    s.execute("streaming stop")
    s.execute("streaming stop")
    s.execute("drop table tweetsTable")
    conn.close()
  }
}
