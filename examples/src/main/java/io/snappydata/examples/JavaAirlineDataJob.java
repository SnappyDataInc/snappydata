package io.snappydata.examples;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Creates and loads Airline data from parquet files in row and column
 * tables. Also samples the data and stores it in a column table.
 */
public class JavaAirlineDataJob {

  private static String airlinefilePath = "examples/quickstart/data/airlineParquetData";
  private static String airlinereftablefilePath = "examples/quickstart/data/airportcodeParquetData";
  private static final String colTable = "AIRLINE";
  private static final String rowTable = "AIRLINEREF";
  private static final String sampleTable = "AIRLINE_SAMPLE";
  private static final String stagingAirline = "STAGING_AIRLINE";

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    SnappyContext snc = new SnappyContext(jsc.sc());

    // Drop tables if already exists
    snc.dropTable(colTable, true);
    snc.dropTable(rowTable, true);
    snc.dropTable(sampleTable, true);
    snc.dropTable(stagingAirline, true);

    System.out.println("****** CreateAndLoadAirlineDataJob ******");

    // Create a DF from the parquet data file and make it a table

    Map<String, String> options = new HashMap<>();
    options.put("path", airlinefilePath);
    DataFrame airlineDF = snc.createExternalTable(stagingAirline, "parquet", options);

    StructType updatedSchema = replaceReservedWords(airlineDF.schema());

    // Create a table in snappy store
    options.clear();
    options.put("buckets", "11");
    snc.createTable(colTable, "column", updatedSchema, options, false);

    // Populate the table in snappy store
    airlineDF.write().mode(SaveMode.Append).saveAsTable(colTable);
    System.out.println("Created and imported data in $colTable table.");

    // Create a DF from the airline ref data file
    DataFrame airlinerefDF = snc.read().load(airlinereftablefilePath);

    // Create a table in snappy store
    snc.createTable(rowTable, "row",
        airlinerefDF.schema(), Collections.<String, String>emptyMap(), false);

    // Populate the table in snappy store
    airlinerefDF.write().mode(SaveMode.Append).saveAsTable(rowTable);

    System.out.println(String.format("Created and imported data in %s table", rowTable));

    // Create a sample table sampling parameters.
    options.clear();
    options.put("buckets", "7");
    options.put("qcs", "UniqueCarrier, Year_, Month_");
    options.put("fraction", "0.03");
    options.put("strataReservoirSize", "50");
    options.put("basetable", "Airline");

    snc.createSampleTable(sampleTable, options, false);


    // Initiate the sampling from base table to sample table.
    snc.table(colTable).write().mode(SaveMode.Append).saveAsTable(sampleTable);

    System.out.println(String.format("Created and imported data in %s table.", sampleTable));

    System.out.println("****** Job finished ******");
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
