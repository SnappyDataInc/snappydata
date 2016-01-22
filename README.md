## Table of Contents
* [Introduction](#introduction)
* [Download binary distribution](#download-binary-distribution)
* [Community Support](#community-support)
* [Link with SnappyData distribution](#link-with-snappydata-distribution)
* [Working with SnappyData Source Code](#working-with-snappydata-source-code)
    * [Building SnappyData from source](#building-snappydata-from-source)
* [Key Features](#key-features)
* [Getting started](#getting-started)
  * [Objectives](#objectives)
  * [SnappyData Cluster](#snappydata-cluster)
    * [Step 1 - Start the SnappyData cluster](#step-1---start-the-snappydata-cluster)
  * [Interacting with SnappyData](#interacting-with-snappydata)
  * [Getting Started with SQL](#getting-started-with-sql)
    * [Column and Row tables](#column-and-row-tables)
    * [Step 2 - Create column table, row table and load data](#step-2---create-column-table-row-table-and-load-data)
    * [OLAP and OLTP queries](#olap-and-oltp-queries)
    * [Step 3 - Run OLAP and OLTP queries](#step-3---run-olap-and-oltp-queries)
    * [Approximate query processing (AQP)](#approximate-query-processing-aqp)
    * [Step 4 - Create, Load and Query Sample Table](#step-4---create-load-and-query-sample-table)
    * [Stream analytics using SQL and Spark Streaming](#stream-analytics-using-sql-and-spark-streaming)
    * [Top-K Elements in a Stream](#top-k-elements-in-a-stream)
    * [Step 5 - Create and Query Stream Table and Top-K Declaratively](#step-5---create-and-query-stream-table-and-top-k-declaratively)
  * [Getting Started with Spark API](#getting-started-with-spark-api)
    * [Column and Row tables](#column-and-row-tables-1)
    * [Step 2 - Create column table, row table and load data](#step-2---create-column-table-row-table-and-load-data-1)
    * [OLAP and OLTP Store](#olap-and-oltp-store)
    * [Step 3 - Run OLAP and OLTP queries](#step-3---run-olap-and-oltp-queries-1)
    * [Approximate query processing (AQP)](#approximate-query-processing-aqp-1)
    * [Step 4 - Create, Load and Query Sample Table](#step-4---create-load-and-query-sample-table-1)
    * [Stream analytics using Spark Streaming](#stream-analytics-using-spark-streaming)
    * [Top-K Elements in a Stream](#top-k-elements-in-a-stream-1)
    * [Step 5 - Create and Query Stream Table and Top-K](#step-5---create-and-query-stream-table-and-top-k-1)
    * [Working with Spark shell and spark-submit](#working-with-spark-shell-and-spark-submit)
    * [Step 6 - Submit a Spark App that interacts with SnappyData](#step-6---submit-a-spark-app-that-interacts-with-snappydata)
  * [Final Step - Stop the SnappyData Cluster](#final-step---stop-the-snappydata-cluster)

## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP(online transaction processing) and OLAP(online analytical processing) in a single integrated cluster**. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD(as an in- memory transactional store with scale-out SQL semantics). 

![SnappyDataOverview](https://prismic-io.s3.amazonaws.com/snappyblog/c6658eccdaf158546930376296cd7c3d33cff544_jags_resize.png)

## Download binary distribution
You can download the latest version of SnappyData from [here][2]. SnappyData has been tested on Linux (mention kernel version) and Mac (OS X 10.9 and 10.10?). If not already installed, you will need to download scala 2.10 and [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).  (this info should also be in the download page on our web site) [Skip to Getting Started](#getting-started)

## Community Support

We monitor channels listed below for comments/questions. We prefer using Stackoverflow. 

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        Gitter ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [IRC](http://webchat.freenode.net/?randomnick=1&channels=%23snappydata&uio=d4) ![IRC](http://i.imgur.com/vbH3Zdx.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          JIRA ![JIRA](http://i.imgur.com/E92zntA.png)

## Link with SnappyData distribution
SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:
```
groupId: io.snappydata
artifactId: snappydata_2.10
version: 0.1_preview
```

## Working with SnappyData Source Code
(Info for our download page?)
If you are interested in working with the latest code or contributing to SnappyData development, you can also check out the master branch from Git:
```
Master development branch
git clone https://github.com/SnappyDataInc/snappydata.git

###### 0.1 preview release branch with stability fixes ######
git clone https://github.com/SnappyDataInc/snappydata.git -b 0.1_preview (??)
```

#### Building SnappyData from source
You will find the instructions for building, layout of the code, integration with IDEs using Gradle, etc, [here](docs/build-instructions.md)
> #### NOTE:
> SnappyData is built using Spark 1.6 (build xx) which is packaged as part of SnappyData. While you can build your application using Apache Spark 1.5, you will need to link to Snappy-spark to make  use of the SnappyData extensions. Gradle build tasks are packaged.  


## Key Features
- **100% compatible with Spark**: Use SnappyData as a database as well as use any of the Spark APIs - ML, Graph, etc. on the same data
- **In-memory row and column store**: Run the store collocated in Spark executors (i.e. a single compute and data cluster) or in its own process space (i.e. separate compute and data cluster)
- **SQL standard compliance**: Spark SQL + several SQL extensions: DML, DDL, indexing, constraints.
- **SQL based extensions for streaming processing**: Use native Spark streaming, Dataframe APIs or declaratively specify your streams and how you want it processed. No need to learn Spark APIs to get going with stream processing or its subtleties when processing in parallel.
- **Interactive analytics using Approximate query processing(AQP)**: We introduce multiple synopses techniques through data structures like count-min-sketch and stratified sampling to dramatically reduce the in-memory space requirements and provide true interactive speeds for analytic queries. These structures can be created and managed by developers with little to no statistical background and can be completely transparent to the SQL developer running queries. Error estimators are also integrated with simple mechanisms to get to the errors through built-in SQL functions. 
- **Mutate, transact on data in Spark**: Use SQL to insert, update, delete data in tables(something that you cannot do in Spark). We also provide extensions to Spark’s context so you can mutate data in your spark programs. Any tables in SnappyData is visible as DataFrames without having to maintain multiples copies of your data: cached RDDs in Spark and then separately in your data store. 
- **Optimizations**: Use indexes to improve query performance in the row store (the GemFire SQL optimizer automatically uses in-memory indexes when available) 
- **High availability not just Fault tolerance**: Data is instantly replicated (one at a time or batch at a time) to other nodes in the cluster and is deeply integrated with a membership based distributed system to detect and handle failures instantaneously providing applications with continuous HA.
- **Durability and recovery:** Data can also be managed on disk and automatically recovered. Utilities for backup and restore are bundled. 

Read SnappyData [docs](complete docs) for a more detailed list of all features and semantics. 

## Getting started

###Objectives

- **In-memory Column and Row tables**: Illustrate both SQL syntax and Spark API to create and manage column tables for large data and how row tables can be used for reference data and can be replicated to each node in the cluster. 
- **OLAP, OLTP operations**: We run analytic class SQL queries (full scan with aggregations) on column tables and fully distributed join queries and observe the space requirements as well as the performance of these queries. For OLTP, we run simple update queries - you can note the Spark API extensions to support mutations in Spark. 
- **AQP**: We run the same analytic queries by creating adjunct stratified samples to note the performance difference - can we get close to interactive query performance speeds?
- **Streaming with SQL**: We ingest twitter streams into both a probabilistic data structure for TopK time series analytics and the entire stream (full data set) into a row table. We run both ad-hoc queries on these streams (modeled as tables) as well as showcase our first preview for continuous querying support. The SnappyData value add demonstrated here is simpler, SQL centric abstractions on top of Spark streaming. And, of course, ingestion into the built-in store.

In this document, we discuss the features mentioned above and ask you to take steps to run the scripts that demonstrate these features. 

### SnappyData Cluster
SnappyData, a database server cluster, has three main components - Locator, Server and Lead. 

- **Locator**: Provides discovery service for the cluster. Informs a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.
- **Lead Node**: Acts as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.
- **Data Servers**: Hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node, or pass it to the lead node for execution by Spark SQL.

![ClusterArchitecture](docs/GettingStarted_Architecture.png)

Details of about the architecture can be found [here](./docs/architecture.md). SnappyData also has multiple deployment options which can be found [here](./docs/deployment.md).

#### Step 1 - Start the SnappyData cluster 

> ##### Note
> The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS) tracks the on-time performance of domestic flights operated by large air carriers. 
Summary information on the number of on-time, delayed, canceled and diverted flights is available for the last 20 years. We use this data set in the examples below. You can learn more on this schema [here](http://www.transtats.bts.gov/Fields.asp?Table_ID=236).
> Default airline data shipped with product is of 15 MB compressed size. To download the larger data set run this command from the shell:
>> $ ./download_full_airlinedata.sh full_dataset_folder

>In case you are running Getting Started with full dataset, configure snappy to start two servers with max heap size as 4G each. 
```bash
$ cat conf/servers
# Two servers with total of 8G.
yourhostName -J-Xmx4g
yourhostName -J-Xmx4g 
```

>##### Passwordless ssh
>The quick start scripts use ssh to start up various processes. By default, this requires a password. To be able to log on to the localhost and run the script without being prompted for the password, please enable passwordless ssh.


The following script starts up a minimal set of essential components to form the cluster - A locator, one data server and one lead node. All nodes are started locally. To spin up remote nodes simply rename/copy the files without the template suffix and add the hostnames. The [article](./docs/configuration.md) discusses the custom configuration and startup options.
```
$ sbin/snappy-start-all.sh 
  (Roughly can take upto a minute. Associated logs are in the ‘work’ sub-directory)
This would output something like this ...
localhost: Starting SnappyData Locator using peer discovery on: 0.0.0.0[10334]
...
localhost: SnappyData Locator pid: 56703 status: running

localhost: Starting SnappyData Server using locators for peer discovery: jramnara-mbpro[10334]   (port used for members to form a p2p cluster)
localhost: SnappyData Server pid: 56819 status: running
localhost:   Distributed system now has 2 members.

localhost: Starting SnappyData Leader using locators for peer discovery: jramnara-mbpro[10334]
localhost: SnappyData Leader pid: 56932 status: running
localhost:   Distributed system now has 3 members.

localhost:   Other members: jramnara-mbpro(56703:locator)<v0>:54414, jramnara-mbpro(56819:datastore)<v1>:39737

``` 

At this point, the SnappyData cluster is up and running and is ready to accept Snappy jobs and SQL requests via JDBC/ODBC. You can also check the details of the embedded Spark driver which by default can be seen at http://hostnameOfLead:4040. You can explore the Spark SQL query plan, the job execution stages and storage details of column tables.

<img src="docs/ExternalBlockStoreSize.png" width="800">

<img src="docs/queryPlan.png" height="800">

### Interacting with SnappyData

> We assume some familiarity with [core Spark, Spark SQL and Spark Streaming concepts](http://spark.apache.org/docs/latest/). 
> And, you can try out the Spark [Quick Start](http://spark.apache.org/docs/latest/quick-start.html). All the commands and programs
> listed in the Spark guides will work in SnappyData also.

To interact with SnappyData, we provide interfaces for developers familiar with Spark programming as well as SQL. JDBC can be used to connect to SnappyData cluster and interact using SQL. On the other hand, users comfortable with Spark programming paradigm can write Snappy jobs to interact with SnappyData. Snappy jobs can be like a self contained Spark application or can share state with other jobs using SnappyData store. 

Unlike Apache Spark, which is primarily a computational engine, SnappyData cluster holds mutable database state in its JVMs and requires all submitted Spark jobs/queries to share the same state (of course, with schema isolation and security as expected in a database). This required extending Spark in two fundamental ways.

1. __Long running executors__: Executors are running within the Snappy store JVMs and form a p2p cluster.  Unlike Spark, the application Job is decoupled from the executors - submission of a job does not trigger launching of new executors. 
2. __Driver runs in HA configuration__: Assignment of tasks to these executors are managed by the Spark Driver.  When a driver fails, this can result in the executors getting shutdown, taking down all cached state with it. Instead, we leverage the [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver) to manage Jobs and queries within a "lead" node.  Multiple such leads can be started and provide HA (they automatically participate in the SnappyData cluster enabling HA). 
Read [docs](docs) for details of the architecture.
 
In this document, we showcase mostly the same set of features via Spark API or using SQL. You can skip the SQL part if you are familiar with Scala and Spark and go directly to [Getting Started with Spark API](#getting-started-with-spark-api).

### Getting Started with SQL

For SQL, the SnappyData SQL Shell (_snappy-shell_) provides a simple way to inspect the catalog,  run admin operations,  manage the schema and run interactive queries. You can also use your favorite SQL tool like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster).

```sql
// Run from the SnappyData base directory
$ ./bin/snappy-shell
Version 2.0-SNAPSHOT.1
snappy> 

-- Connect to the cluster ..
snappy> connect client 'localhost:1527';
snappy> show connections; 

-- Check the cluster status
this will list each cluster member and its status
snappy> show members;
```
#### Column and Row tables 

[Column tables](columnTables) organize and manage data in memory in compressed columnar form such that modern day CPUs can traverse and run computations like a sum or a average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.
```sql
-- DDL to create a column table
CREATE TABLE AIRLINE (<column definitions>) USING column OPTIONS(buckets '5') ;
```
[Row tables](rowTables), unlike column tables are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location determined by a hash function and hence very fast for point lookups or updates.  
_create table_ DDL for Row and Column tables allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk , overflow to disk, be replicated for HA, etc.  Read our preliminary [docs](./docs/rowAndColumnTables.md) for the details.
```sql
-- DDL to create a row table
CREATE TABLE AIRLINEREF (<column definitions>) USING row OPTIONS() ;
```

#### Step 2 - Create column table, row table and load data

> To run the scripts with full airline data set, change the 'create_and_load_column_table.sql' script to point at the data set that you had downloaded in Step 1.


SQL scripts to create and load column and row tables.
```sql
-- Loads parquet formatted data into a temporary spark table 
-- then saves it in  column table called Airline.
snappy> run './quickstart/scripts/create_and_load_column_table.sql';

-- Creates the airline code table. Row tables can be replicated to each node 
-- so join processing with other tables can completely avoid shuffling 
snappy> run './quickstart/scripts/create_and_load_row_table.sql';

-- See the status of system
snappy> run './quickstart/scripts/status_queries.sql'
```
You can see the size of the column tables on Spark UI which by default can be seen at http://hostNameOfLead:4040. 

#### OLAP and OLTP queries
SQL client connections (via JDBC or ODBC) are routed to the appropriate data server via the locator (Physical connections are automatically created in the driver and are transparently swizzled in case of failures also). When queries are executed they are parsed initially by the SnappyData server to determine if it is a OLAP class or a OLTP class query.  Currently, all column table queries are considered OLAP.  Such queries are routed to the __lead__ node where a __ Spark SQLContext__ is managed for each connection. The Query is planned using Spark's Catalyst engine and scheduled to be executed on the data servers. The number of partitions determine the number of concurrent tasks used across the data servers to parallel run the query. In this case, our column table was created using _5 partitions(buckets)_ and hence will use 5 concurrent tasks. 

```sql
---- Which Airlines Arrive On Schedule? JOIN with reference table ----
snappy> select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier 
  from airline_sample, airlineref
  where airline_sample.UniqueCarrier = airlineref.Code 
  group by UniqueCarrier, description 
  order by arrivalDelay;
```
For low latency OLTP queries, the engine won't route it to the lead and instead execute it immediately without any scheduling overhead. Quite often, this may mean simply fetching a row by hashing a key (in nanoseconds).
 
```sql
--- Suppose a particular Airline company say 'Delta Air Lines Inc.' re-brands itself as 'Delta America'
--- the airline code can be updated in the row table
UPDATE AIRLINEREF SET DESCRIPTION='Delta America' WHERE CAST(CODE AS VARCHAR(25))='DL';
```
Spark SQL can cache DataFrames as temporary tables and the data set is immutable. SnappyData SQL is compatible with the SQL standard with support for transactions and DML (insert, update, delete) on tables. [Link to Snappy Store SQL reference](http://gemfirexd.docs.pivotal.io/1.3.0/userguide/index.html#reference/sql-language-reference.html).  As we show later, any table in Snappy is also visible as Spark DataFrame. 

#### Step 3 - Run OLAP and OLTP queries
 
```sql
-- Simply run the script or copy/paste one query at a time if you want to explore the query execution on the Spark console. 
snappy> run './quickstart/scripts/olap_queries.sql';

---- Which Airlines Arrive On Schedule? JOIN with reference table ----
select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier 
  from airline_sample, airlineref
  where airline_sample.UniqueCarrier = airlineref.Code 
  group by UniqueCarrier, description 
  order by arrivalDelay;
```
You can explore the Spark SQL query plan on Spark UI which by default can be seen at http://hostnameOfLead:4040. Each query is executed as a Job and you can explore the different stages of the query execution.

```sql
-- Run a simple update SQL statement on the replicated row table.
snappy> run './quickstart/scripts/oltp_queries.sql';
```
You can now re-run olap_queries.sql to see the updated join result set.

> **Note**
> In the current implementation we only support appending to Column tables. Future releases will support all DML operations. 
> You can execute transactions using commands _autocommit off_ and _commit_.  

#### Approximate query processing (AQP)
OLAP queries are expensive as they require traversing through large data sets and shuffling data across nodes. While the in-memory queries above executed in less than a second the response times typically would be much higher with very large data sets. On top of this, concurrent execution for multiple users would also slow things down. Achieving interactive query speed in most analytic environments requires drastic new approaches like AQP.
Similar to how indexes provide performance benefits in traditional databases, SnappyData provides APIs and DDL to specify one or more curated [stratified samples](http://stratifiedsamples) on large tables. 

> #### Note
> We recommend downloading the _onTime airline_ data for 2009-2015 which is about 52 million records. With the above data set (1 million rows) only about third of the time is spent in query execution engine and  sampling is unlikely to show much of any difference in speed.


The following DDL creates a sample that is 3% of the full data set and stratified on 3 columns. The commonly used dimensions in your _Group by_ and _Where_ make us the _Query Column Set_ (strata columns). Multiple samples can be created and queries executed on the base table are analyzed for appropriate sample selection. 

```sql
CREATE SAMPLE TABLE AIRLINE_SAMPLE
   OPTIONS(
    buckets '5',                          -- Number of partitions 
    qcs 'UniqueCarrier, Year_, Month_',   -- QueryColumnSet(qcs): The strata - 3% of each combination of Carrier, 
                                          -- Year and Month are stored as sample
    fraction '0.03',                      -- How big should the sample be
    strataReservoirSize '50',             -- Reservoir sampling to support streaming inserts
    basetable 'Airline')                  -- The parent base table
```
You can run queries directly on the sample table (stored in columnar format) or on the base table. For base table queries you have to specify the _With Error_ constraint indicating to the SnappyData Query processor that a sample can be substituted for the full data set. 

```sql
-- What is the average arrival delay for all airlines for each month?;
snappy> select avg(ArrDelay), Month_ from Airline where ArrDelay >0 
    group by Month_
    with error .05 ;
-- The above query will consult the sample and return an answer if the estimated answer 
-- is at least 95% accurate (here, by default we use a 95% confidence interval). Read [docs](docs) for more details.

-- You can also access the error using built-in functions. 
snappy> select avg(ArrDelay) avgDelay, absolute_error(avgDelay), Month_ 
    from Airline where ArrDelay >0 
    group by Month_
    with error .05 ;
-- TWO PROBLEMS IN CURRENT IMPL: (1) PRESENTED ERROR IS NOT WITHIN RANGE (2) ERROR CONSTRAINT NOT MET .. BUT QUERY RETURNS VALUE
-- The correct answer is within +/- 'error'
-- Consult the docs for access to other related functions like relative_error(), 
--  lower and upper bounds for the error returned. 
```
#### Step 4 - Create, Load and Query Sample Table

```sql
--- Creates and then samples a table from the Airline table 
snappy> run 'create_and_load_sample_table.sql';
```
You can now re-run the previous OLAP queries with an error constraint and compare the results.  You should notice a 10X or larger difference in query execution latency while the results remain nearly accurate. As a reminder, we recommend downloading the larger data set for this exercise.

```sql
-- re-run olap queries with error constraint to automatically use sampling
snappy> run 'olap_approx_queries.sql';
```
#### Stream analytics using SQL and Spark Streaming

SnappyData extends Spark streaming so stream definitions can be declaratively done using SQL and you can analyze these streams using SQL. You can also dynamically run SQL queries on these streams. There is no need to learn Spark streaming APIs or statically define all the rules to be executed on these streams.

The commands below consume tweets, models the stream as a table (so it can be queried) and we then run ad-hoc SQL from remote clients on the current state of the stream. 
```sql
--- Inits the Streaming Context 
snappy> STREAMING INIT 2
--- create a stream table
snappy> CREATE STREAM TABLE HASHTAG_FILESTREAMTABLE
              (hashtag string)
            USING file_stream
            OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2',
              rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow',
              directory '/tmp/copiedtwitterdata')
--- Start streaming context 
snappy> STREAMING START
--- Adhoc sql on the stream table to query the current batch
--- Get top 10 popular hashtags ----
snappy> SELECT hashtag, count(*) as tagcount
        FROM HASHTAG_FILESTREAMTABLE
        GROUP BY hashtag
        ORDER BY tagcount DESC limit 10;
```
Later, in the Spark code section we further enhance to showcase "continuous queries" (CQ). Dynamic registration of CQs (from remote clients) will be available in the next release.

#### Top-K Elements in a Stream 

Continuously finding the _k_ most popular elements in a data stream is a common analytic query. SnappyData provides SQL extensions to Spark to maintain top-k approximate structures on streams. Also, SnappyData adds temporal component (i.e. data can be queried based on time interval) to these structures and enables transparent querying using Spark SQL. More details about SnappyData's implementation of top-k can be found [here.](./docs/aqp.md)

SnappyData provides DDL extensions to create Top-k structure. And, if a stream table is specified as base table, the Top-k structure is automatically populated from it as the data arrives. The Top-k structures can be queried using SQL queries. 

```sql
--- Create a topk table from a stream table
CREATE TOPK TABLE filestream_topktable ON HASHTAG_FILESTREAMTABLE OPTIONS
(key 'hashtag', timeInterval '2000ms', size '10' );
--- Query a topk table 
SELECT hashtag, COUNT(hashtag) AS TopKCount
FROM filestream_topktable
GROUP BY hashtag ORDER BY TopKCount limit 10;
```
 
#### Step 5 - Create and Query Stream Table and Top-K Declaratively 

Ideally, we would like you to try this example using live twitter stream. Alternatively, you can use use file stream scripts that simulate twitter stream by copying pre-loaded tweets in a tmp folder. 

##### Steps to work with live Twitter stream

You would have to generate authorization keys and secrets on [twitter apps](https://apps.twitter.com/) and update SNAPPY_HOME/quickstart/scripts/create_and_start_twitter_streaming.sql with the keys and secrets.
```sql
--- Run the create and start script that has keys and secrets to fetch live twitter stream
--- Note: Currently, we do not encrypt the keys. 
-- This also creates Topk structures
snappy> run './quickstart/scripts/create_and_start_twitter_streaming.sql';

snappy> run './quickstart/scripts/twitter_streaming_query.sql';
```

##### Steps to work with simulated Twitter stream

Create a file stream table that listens on a folder and then start the streaming context. 
```sql
snappy> run './quickstart/scripts/create_and_start_file_streaming.sql';
```
Run the following utility in another terminal that simulates a twitter stream by copying tweets in the folder on which file stream table is listening.
```bash 
$ quickstart/scripts/simulateTwitterStream 
```
Now query the current batch of the stream using the following script. This also creates Topk structures. simulateTwitterStream script runs for only for a minute or so. Since our script is querying the current window, it will return no results after the streaming is over. 
```sql
snappy> run './quickstart/scripts/file_streaming_query.sql';
```

### Getting Started with Spark API 

SnappyContext is the main entry point for SnappyData extensions to Spark. A SnappyContext extends Spark's [SQLContext](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.SQLContext) to work with Row and Column tables. Any DataFrame can be managed as SnappyData table and any table can be accessed as a DataFrame. This is similar to [HiveContext](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.hive.HiveContext) and it integrates the SQLContext functionality with the Snappy store. Similarly, SnappyStreamingContext is an entry point for SnappyData extensions to Spark streaming and it extends Spark's [Streaming Context](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.streaming.StreamingContext). 

Applications typically submit Jobs to SnappyData and do not explicitly create a SnappyContext or SnappyStreamingContext. These jobs are the primary mechanism to interact with SnappyData using Spark API. A job implements either SnappySQLJob or SnappyStreamingJob (for streaming applications) trait. 

```scala
class SnappySampleJob implements SnappySQLJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runJob(snc: SnappyContext, jobConfig: Config): Any

  /** SnappyData calls this function to validate the job input and reject invalid job requests **/
  def validate(sc: SnappyContext, config: Config): SparkJobValidation
}
```
The implementation of _runJob_ function of SnappySQLJob uses SnappyContext to interact with SnappyData store to process and store tables. The implementation of runJob of SnappyStreamingJob uses SnappyStreamingContext to create streams and manage the streaming context. The jobs are submitted to lead node of SnappyData over REST API using a _spark-submit_ like utility. See more details about jobs [here](./docs/jobs.md)

#### Column and Row tables 

[Column tables](columnTables) organize and manage data in memory in compressed columnar form such that modern day CPUs can traverse and run computations like a sum or a average really fast (as the values are available in contiguous memory). Column table follows the Spark DataSource access model.

```scala
// creating a column table in Snappy job
snappyContext.createTable("AIRLINE", "column", schema, Map("buckets" -> "5"))
```
[Row tables](rowTables), unlike column tables are laid out one row at a time in contiguous memory. Rows are typically accessed using keys and its location determined by a hash function and hence very fast for point lookups or updates.  
_create table_ DDL allows tables to be partitioned on primary keys, custom partitioned, replicated, carry indexes in memory, persist to disk , overflow to disk, be replicated for HA, etc.  Read our preliminary [docs](docs) for the details.
```scala
// creating a row table in Snappy job
val airlineCodeDF = snappyContext.createTable("AIRLINEREF", "row", schema, Map())
```
#### Step 2 - Create column table, row table and load data

> To run the scripts with full airline data set, set the following config parameter to point at the data set that you had downloaded in Step 1.
>> export APP_PROPS="airline_file=full_dataset_folder"

Submit CreateAndLoadAirlineDataJob over REST API to create row and column tables. See more details about jobs and job submission [here.](./docs/jobs.md). 

```bash
$ bin/snappy-job.sh submit --lead hostNameOfLead:8090 --app-name airlineApp --class  io.snappydata.examples.CreateAndLoadAirlineDataJob --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar
{"status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  } }

# A JSON with jobId of the submitted job is returned. Use job ID can be used to query the status of the running job. 
$ bin/snappy-job.sh status --lead hostNameOfLead:8090 --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
{ "duration": "17.53 secs",
  "classPath": "io.snappydata.examples.CreateAndLoadAirlineDataJob",
  "startTime": "2016-01-12T16:59:14.746+05:30",
  "context": "snappyContext1452598154529305363",
  "result": "See /snappy/work/localhost-lead-1/CreateAndLoadAirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
}
# Tables are created
```
The output of the job can be found in CreateAndLoadAirlineDataJob.out in the lead directory which by default is SNAPPY_HOME/work/localhost-lead-*/. You can see the size of the column tables on Spark UI which by default can be seen at http://hostNameOfLead:4040. 

#### OLAP and OLTP Store

SnappyContext extends SQLContext and adds functionality to work with row and column tables. When queries inside jobs are executed they are parsed initially by the SnappyData server to determine if it is a OLAP class or a OLTP class query.  Currently, all column table queries are considered OLAP. Such queries are planned using Spark's Catalyst engine and scheduled to be executed on the data servers. 
```scala
val resultDF = airlineDF.join(airlineCodeDF,
        airlineDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
        groupBy(airlineDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
```
For low latency OLTP queries in jobs, SnappyData won't schedule these queries instead execute them immediately on SnappyData servers without any scheduling overhead. Quite often, this may mean simply fetching or updating a row by hashing a key (in nanoseconds). 
```scala
// Suppose a particular Airline company say 'Delta Air Lines Inc.' re-brands itself as 'Delta America'. Update the row table.
val filterExpr: String = " CODE ='DL'"
val newColumnValues: Row = Row("Delta America")
val updateColumns = "DESCRIPTION"
snappyContext.update(rowTableName, filterExpr, newColumnValues, updateColumns)
```

#### Step 3 - Run OLAP and OLTP queries

AirlineDataJob.scala runs OLAP and OLTP queries on Snappy tables. Also, it caches the same airline data in Spark cache and runs the same OLAP query on the Spark cache. With airline data set, we have seen both Spark cache and snappy store table to have more and less the same performance.  

```bash
# Submit AirlineDataJob to SnappyData
$ bin/snappy-job.sh submit --lead hostNameOfLead:8090 --app-name airlineApp  --class  io.snappydata.examples.AirlineDataJob --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar
{ "status": "STARTED",
  "result": {
    "jobId": "1b0d2e50-42da-4fdd-9ea2-69e29ab92de2",
    "context": "snappyContext1453196176725064822"
 } }
# A JSON with jobId of the submitted job is returned. Use job ID can be used to query the status of the running job. 
$ bin/snappy-job.sh status --lead localhost:8090  --job-id 1b0d2e50-42da-4fdd-9ea2-69e29ab92de2 
{ "duration": "6.617 secs",
  "classPath": "io.snappydata.examples.AirlineDataJob",
  "startTime": "2016-01-19T15:06:16.771+05:30",
  "context": "snappyContext1453196176725064822",
  "result": "See /snappy/work/localhost-lead-1/AirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "1b0d2e50-42da-4fdd-9ea2-69e29ab92de2"
}
```
The output of the job can be found in AirlineDataJob.out in the lead directory which by default is SNAPPY_HOME/work/localhost-lead-*/. You can explore the Spark SQL query plan on Spark UI which by default can be seen at http://hostNameOfLead:4040.

#### Approximate query processing (AQP)
OLAP jobs are expensive as they require traversing through large data sets and shuffling data across nodes. While the in-memory jobs above executed in less than a second the response times typically would be much higher with very large data sets. On top of this, concurrent execution for multiple users would also slow things down. Achieving interactive query speed in most analytic environments requires drastic new approaches like AQP.
Similar to how indexes provide performance benefits in traditional databases, SnappyData provides APIs to specify one or more curated [stratified samples](http://stratifiedsamples) on large tables. 

> #### Note
> We recommend downloading the _onTime airline_ data for 2009-2015 which is about 50 million records. With the above data set (1 million rows) only about third of the time is spent in query execution engine and  sampling is unlikely to show much of any difference in speed.

The following scala code creates a sample that is 3% of the full data set and stratified on 3 columns. The commonly used dimensions in your _Group by_ and _Where_ make us the _Query Column Set_ (strata columns). Multiple samples can be created and queries executed on the base table are analyzed for appropriate sample selection. 

```scala
val sampleDF = snappyContext.createTable(sampleTable, 
        "column_sample", // DataSource provider for sample tables
        updatedSchema, Map("buckets" -> "5",
          "qcs" -> "UniqueCarrier, Year_, Month_",
          "fraction" -> "0.03",
          "strataReservoirSize" -> "50",
          "basetable" -> "Airline"
        ))
```
You can run queries directly on the sample table (stored in columnar format) or on the base table. For base table queries you have to specify the _With Error_ constraint indicating to the SnappyData Query processor that a sample can be substituted for the full data set. 

```scala
// Query Snappy Store's Sample table :Which Airlines arrive On schedule? JOIN with reference table
sampleResult = sampleDF.join(airlineCodeDF,
        sampleDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
          groupBy(sampleDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
          agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")

 // Query Snappy Store's Airline table with error clause.
airlineDF.groupBy(airlineDF("Month_"))
  .agg("ArrDelay" -> "avg")
  .orderBy("Month_").withError(0.05,0.95)
```

#### Step 4 - Create, Load and Query Sample Table

CreateAndLoadAirlineDataJob and AirlineDataJob executed in the previous sections created the sample tables and executed OLAP queries over them.

#### Stream analytics using Spark Streaming

SnappyData extends Spark streaming so stream definitions can be declaratively done using SQL and you can analyze these streams using SQL. Also, SnappyData introduces "continuous queries" (CQ) on the stream. One can define a continous query as a SQL query on the stream with window and slide extensions which is returned as SchemaDStream i.e. DStream with schema. SnappyData's extensions provide functionality to insert a SchemaDStream into snappy store. 

Dynamic registration of CQs (from remote clients) will be available in the next release.

```scala
// create a stream table declaratively 
snsc.sql("CREATE STREAM TABLE RETWEETTABLE (retweetId long, " +
    "retweetCnt int, retweetTxt string) USING file_stream " +
    "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
    "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow'," +
    "directory '/tmp/copiedtwitterdata')");

// Register a continous query on the stream table with window and slide parameters
val retweetStream: SchemaDStream = snsc.registerCQ("SELECT retweetId, retweetCnt FROM RETWEETTABLE " +
    "window (duration '2' seconds, slide '2' seconds)")

// Create a row table to hold the retweets based on their id 
snsc.snappyContext.sql(s"CREATE TABLE $tableName (retweetId bigint PRIMARY KEY, " +
    s"retweetCnt int, retweetTxt string) USING row OPTIONS ()")

// Iterate over the stream and insert it into snappy store
retweetStream.foreachDataFrame(df => {
    df.write.mode(SaveMode.Append).saveAsTable(tableName)
})
```
#### Top-K Elements in a Stream 

Continuously finding the _k_ most popular elements in a data stream is a common analytic query. SnappyData provides extensions to Spark to maintain top-k approximate structures on streams. Also, SnappyData adds temporal component (i.e. data can be queried based on time interval) to these structures. More details about SnappyData's implementation of top-k can be found [here.](./docs/aqp.md)

SnappyData provides API in SnappyContext to create Top-k structure. And, if a stream table is specified as base table, the Top-k structure is automatically populated from it as the data arrives. The Top-k structures can be queried using another API. 

```scala
--- Create a topk table from a stream table
snappyContext.createApproxTSTopK("topktable", "hashtag",
    Some(schema), Map(
      "epoch" -> System.currentTimeMillis().toString,
      "timeInterval" -> "2000ms",
      "size" -> "10",
      "basetable" -> "HASHTAGTABLE"
    ))
--- Query a topk table for the last two seconds
val topKDF = snappyContext.queryApproxTSTopK("topktable",
                System.currentTimeMillis - 2000,
                System.currentTimeMillis)
```
#### Step 5 -  Create and Query Stream Table and Top-K

Ideally, we would like you to try this example using live twitter stream. For that, you would have to generate authorization keys and secrets on twitter apps. Alternatively, you can use use file stream scripts that simulate twitter stream by copying pre-loaded tweets in a tmp folder.

##### Steps to work with live Twitter stream

```bash
# Set the keys and secrets to fetch live twitter stream
# Note: Currently, we do not encrypt the keys. 
$ export APP_PROPS="consumerKey=<consumerKey>,consumerSecret=<consumerSecret>,accessToken=<accessToken>,accessTokenSecret=<accessTokenSecret>"

# submit the TwitterPopularTagsJob that declares a stream table, creates and populates a topk -structure, registers CQ on it and stores the result in a snappy store table 
# This job runs streaming for two minutes. 
$ /bin/snappy-job.sh submit --lead hostNameOfLead:8090 --app-name TwitterPopularTagsJob --class io.snappydata.examples.TwitterPopularTagsJob --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar --stream

```

##### Steps to work with simulated Twitter stream

Submit the TwitterPopularTagsJob that declares a stream table, creates and populates a topk -structure, registers CQ on it and stores the result in a gemxd table. It starts the streaming and waits for two minutes. 
 
```bash
# Submit the TwitterPopularTagsJob 
$ ./bin/snappy-job.sh submit --lead hostNameOfLead:8090 --app-name TwitterPopularTagsJob --class io.snappydata.examples.TwitterPopularTagsJob --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar --stream

# Run the following utility in another terminal to simulate a twitter stream by copying tweets in the folder on which file stream table is listening.
$ quickstart/scripts/simulateTwitterStream 

```
The output of the job can be found in TwitterPopularTagsJob_timestamp.out in the lead directory which by default is SNAPPY_HOME/work/localhost-lead-*/. 

#### Working with Spark shell and spark-submit

SnappyData, out-of-the-box, collocates Spark executors and the data store for efficient data intensive computations. But, it may desirable to isolate the computational cluster for other reasons - for instance, a  computationally intensive Map-reduce machine learning algorithm that needs to iterate for a  cache data set repeatedly. To support such scenarios it is also possible to run native Spark jobs that accesses a SnappyData cluster as a storage layer in a parallel fashion. 

```bash
# Start the spark shell in local mode. Pass Snappy locator’s host:port as a conf parameter.
# Change the UI port because the default port 4040 is being used by Snappy’s lead. 
$ bin/spark-shell  --master local[*] --conf snappydata.store.locators=locatorhost:port --conf spark.ui.port=4041
scala>
Try few commands on the spark-shell 

# fetch the tables and using sqlContext which is going to be an instance of SnappyContext in this case
scala> val airlinerefDF = sqlContext.table("airlineref").show
scala> val airlineDF = sqlContext.table("airline").show

# you can now work with the dataframes to fetch the data.
```
#### Step 6 - Submit a Spark App that interacts with SnappyData 

```bash
# Start the Spark standalone cluster.
$ sbin/start-all.sh 
# Submit AirlineDataSparkApp to Spark Cluster with snappydata's locator host port.
$ bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp --master spark://masterhost:7077 --conf snappydata.store.locators=locatorhost:port --conf spark.ui.port=4041 $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar

# The results can be seen on the command line. 
```

### Final Step - Stop the SnappyData Cluster

```bash
$ sbin/snappy-stop-all.sh 
localhost: The SnappyData Leader has stopped.
localhost: The SnappyData Server has stopped.
localhost: The SnappyData Locator has stopped.
```

-----


