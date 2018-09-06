/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import hydra.TestConfig;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.Vector;

import static hydra.Prms.totalTaskTimeSec;

public class SnappyConcurrencyTest extends SnappyTest {

  public static void runPointLookUpQueries() throws SQLException {
    Vector<String> queryVect = SnappyPrms.getPointLookUpQueryList();
    long totalTaskTime = TestConfig.tab().longAt(totalTaskTimeSec);
    String query = null;
    Connection conn = getLocatorConnection();
    ResultSet rs;
    long startTime = System.currentTimeMillis();
    long endTime = startTime + 300000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing pointLookUp query:" + query, se);
      }
    }
    endTime = startTime + totalTaskTime * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numPointLookUpQueriesExecuted);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing pointLookUp query:" + query, se);
      }
    }
    /*StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> queryResult = ResultSetHelper.asList(rs, sti, false);
    Log.getLogWriter().info("SS - Result for query : " + query + "\n" + queryResult.toString());*/
    closeConnection(conn);
  }

  public static void runAnalyticalQueries() throws SQLException {
    Connection conn = getLocatorConnection();
    Vector<String> queryVect = SnappyPrms.getAnalyticalQueryList();
    long totalTaskTime = TestConfig.tab().longAt(totalTaskTimeSec);
    Log.getLogWriter().info("SS - totalTaskTime : " + totalTaskTime);
    String query = null;
    ResultSet rs;
    long startTime = System.currentTimeMillis();
    long endTime = startTime + 300000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = (String) queryVect.elementAt(queryNum);
        if (query.contains("create temporary view revenue")) {
          // Changes specific to TPCH Q15 execution as the query has dependancy on temporary view revenue
          rs = conn.createStatement().executeQuery(query);
          rs = conn.createStatement().executeQuery("select s_suppkey, s_name, s_address, s_phone, total_revenue " +
              "from SUPPLIER, revenue where s_suppkey = supplier_no and total_revenue = (select max(total_revenue) " +
              "from  revenue ) order by s_suppkey");
        } else rs = conn.createStatement().executeQuery(query);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing Analytical query:" + query, se);
      }
    }
    startTime = System.currentTimeMillis();
    endTime = startTime + totalTaskTime * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = (String) queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numAggregationQueriesExecuted);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing Analytical query:" + query, se);
      }
    }
    /*StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> queryResult = ResultSetHelper.asList(rs, sti, false);
    Log.getLogWriter().info("SS - Result for query : " + query + "\n" + queryResult.toString());*/
    closeConnection(conn);
  }

  public static void validateNumQueriesExecuted() throws SQLException {
    int numQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numQueriesExecuted);
    int numpointLookUpQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB
        .numPointLookUpQueriesExecuted);
    int numAggregationQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numAggregationQueriesExecuted);
    Log.getLogWriter().info("Total number of queries executed : " + numQueriesExecuted);
    Log.getLogWriter().info("Total number of pointLookUp queries executed : " +
        numpointLookUpQueriesExecuted);
    Log.getLogWriter().info("Total number of analytical queries executed : " + numAggregationQueriesExecuted);
  }

}
