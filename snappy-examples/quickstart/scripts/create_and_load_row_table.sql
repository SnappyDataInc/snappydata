----- CREATE TEMP TABLE -----
CREATE TABLE IF NOT EXISTS AIRLINEREF_PARQUET_SOURCE
 (
   CODE VARCHAR(25),
   DESCRIPTION VARCHAR(25)
 ) 
 USING parquet OPTIONS(path '../../quickstart/data/airportcodeParquetData');

----- CREATE ROW TABLE -----

CREATE TABLE IF NOT EXISTS AIRLINEREF USING row OPTIONS() AS (SELECT * FROM AIRLINEREF_PARQUET_SOURCE);
