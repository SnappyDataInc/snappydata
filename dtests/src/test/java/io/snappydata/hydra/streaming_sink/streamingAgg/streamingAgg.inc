TASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod = HydraTask_generateAndPublishMethod
    //io.snappydata.hydra.cluster.SnappyPrms-streamingJobClassNames = "io.snappydata.hydra.streaming_sink.StringMessageProducer"
io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer = "400000,streamingSinkTopic,500000,0"
io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
threadGroups = leadThread
maxTimesToRun = 1;

TASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSQLScripts
io.snappydata.hydra.cluster.SnappyPrms-logFileName = sqlScriptResults_0.log
io.snappydata.hydra.cluster.SnappyPrms-sqlScriptNames = streaming_sink_queries.sql
threadGroups = snappyStoreThreads
maxThreads = 1;

CLOSETASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_stopStreamingJob
io.snappydata.hydra.cluster.SnappyPrms-userAppName = streamingSinkApp
threadGroups = snappyThreads;

CLOSETASK   taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod = HydraTask_verifyResults
threadGroups = snappyThreads;

CLOSETASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSQLScripts
io.snappydata.hydra.cluster.SnappyPrms-logFileName = internal_query_execution.log
io.snappydata.hydra.cluster.SnappyPrms-sqlScriptNames = query_internal_table.sql
threadGroups = snappyThreads;

INCLUDE $JTESTS/io/snappydata/hydra/streaming_sink/stopClusters.inc;

hydra.Prms-alwaysDoEndTasks = true;
hydra.Prms-totalTaskTimeSec = 900;
hydra.Prms-maxResultWaitSec = 1200;

hydra.VmPrms-extraVMArgs   += fcn "hydra.TestConfigFcns.duplicate
(\"-Xms1512m -Xmx2g \", ${${A}Hosts}, true)"
ncf
,
fcn "hydra.TestConfigFcns.duplicate
(\"-Xms1512m -Xmx2g \", ${${B}Hosts}, true)"
ncf;

io.snappydata.hydra.cluster.SnappyPrms-locatorMemory = 1024m;
io.snappydata.hydra.cluster.SnappyPrms-serverMemory = 8g;
io.snappydata.hydra.cluster.SnappyPrms-leadMemory = 4g;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-largeDataSet = true;

//INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/startDerby.inc;
INCLUDE $JTESTS/io/snappydata/hydra/streaming_sink/schema.inc;