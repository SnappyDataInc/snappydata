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

package io.snappydata.examples;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.SnappyJobInvalid;
import org.apache.spark.sql.SnappyJobValid;
import org.apache.spark.sql.SnappyJobValidation;
import org.apache.spark.sql.SnappySQLJob;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Creates and loads Airline data from parquet files in row and column
 * tables. Also samples the data and stores it in a column table.
 */
public class JavaCreateAndLoadAirlineDataJob extends SnappySQLJob {


  private String airlinefilePath = null;
  private String airlinereftablefilePath = null;
  private static final String colTable = "AIRLINE";
  private static final String rowTable = "AIRLINEREF";
  private static final String sampleTable = "AIRLINE_SAMPLE";
  private static final String stagingAirline = "STAGING_AIRLINE";

  @Override
  public Object runSnappyJob(SnappyContext snc, Config jobConfig) {
    PrintWriter pw = null;
    String currentDirectory = null;
    boolean success = false;

    try {
      currentDirectory = new java.io.File(".").getCanonicalPath();
      pw = new PrintWriter("JavaCreateAndLoadAirlineDataJob.out");
      // Drop tables if already exists
      snc.dropTable(sampleTable, true);
      snc.dropTable(colTable, true);
      snc.dropTable(rowTable, true);
      snc.dropTable(stagingAirline, true);

      pw.println("****** JavaCreateAndLoadAirlineDataJob ******");

      // Create a DF from the parquet data file and make it a table
      Map<String, String> props = new HashMap<>();
      props.put("path", airlinefilePath);
      DataFrame airlineDF = snc.createExternalTable(stagingAirline, "parquet", props);
      StructType updatedSchema = replaceReservedWords(airlineDF.schema());

      // Create a table in snappy store
      Map<String, String> columnTableProps = new HashMap<>();
      columnTableProps.put("buckets", "11");
      snc.createTable(colTable, "column",
          updatedSchema, columnTableProps, false);

      // Populate the table in snappy store
      airlineDF.write().mode(SaveMode.Append).saveAsTable(colTable);
      pw.println("Created and imported data in $colTable table.");

      // Create a DF from the airline ref data file
      DataFrame airlinerefDF = snc.read().load(airlinereftablefilePath);

      // Create a table in snappy store
      snc.createTable(rowTable, "row",
          airlinerefDF.schema(), Collections.<String, String>emptyMap(), false);

      // Populate the table in snappy store
      airlinerefDF.write().mode(SaveMode.Append).saveAsTable(rowTable);

      pw.println("Created and imported data in $rowTable table");

      // Create a sample table sampling parameters.

      Map<String, String> sampleTableProps = new HashMap<>();
      sampleTableProps.put("buckets", "7");
      sampleTableProps.put("qcs", "UniqueCarrier, Year_, Month_");
      sampleTableProps.put("fraction", "0.03");
      sampleTableProps.put("strataReservoirSize", "50");

      snc.createSampleTable(sampleTable, "Airline", sampleTableProps, false);

      // Initiate the sampling from base table to sample table.
      snc.table(colTable).write().mode(SaveMode.Append).saveAsTable(sampleTable);

      pw.println("Created and imported data in $sampleTable table.");

      pw.println("****** Job finished ******");
      success = true;

    } catch (IOException e) {
      pw.close();
    } finally {
      if (success) {
        String returnValue = String.format("See %s/JavaCreateAndLoadAirlineDataJob.out", currentDirectory);
        return returnValue;
      }
      pw.close();
    }
    return null;
  }

  @Override
  public SnappyJobValidation isValidJob(SnappyContext snc, Config config) {

    if (config.hasPath("airline_file")) {
      airlinefilePath = config.getString("airline_file");
    } else {
      airlinefilePath = "../../quickstart/data/airlineParquetData";
    }

    if (!(new File(airlinefilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline path. " +
          "Specify airline_file property in APP_PROPS");
    }

    if (config.hasPath("airlineref_file")) {
      airlinereftablefilePath = config.getString("airlineref_file");
    } else {
      airlinereftablefilePath = "../../quickstart/data/airportcodeParquetData";
    }
    if (!(new File(airlinereftablefilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline ref path. " +
          "Specify airlineref_file property in APP_PROPS");
    }

    return new SnappyJobValid();
  }

  private static StructType replaceReservedWords(StructType airlineSchema) {
    StructField[] fields = airlineSchema.fields();
    StructField[] newFields = new StructField[fields.length];
    for (StructField s : fields) {
      StructField newField = null;
      if (s.name().equals("Year")) {
        newField = new StructField("Year_", s.dataType(), s.nullable(), s.metadata());
      } else if (s.name().equals("Month")) {
        newField = new StructField("Month_", s.dataType(), s.nullable(), s.metadata());
      } else {
        newField = s;
      }
      newFields[airlineSchema.indexOf(s)] = newField;
    }
    return new StructType(newFields);
  }

}
