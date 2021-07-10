INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_4.inc;

hydra.GemFirePrms-names = gemfire1;
hydra.ClientPrms-gemfireNames = gemfire1;
hydra.GemFirePrms-distributedSystem = ds;
io.snappydata.hydra.cluster.SnappyPrms-kafkaDir="${kafkaDir}";

THREADGROUP snappyStoreThreads
totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) -1 " ncf
totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
${${A}Hosts}, true)" ncf;

THREADGROUP leadThreads
totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) -1 " ncf
totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
${${B}Hosts}, true)" ncf;

THREADGROUP locatorThreads
totalThreads = fcn "(${${C}Hosts} * ${${C}VMsPerHost} *  ${${C}ThreadsPerVM}) " ncf
totalVMs     = fcn "(${${C}Hosts} * ${${C}VMsPerHost})" ncf
clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${C}\",
${${C}Hosts}, true)" ncf;

THREADGROUP snappyThreads
totalThreads = 1
totalVMs     = 1
clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",${${A}Hosts}, true)"  ncf;

THREADGROUP leadThread
totalThreads = 1
totalVMs     = 1
clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",${${B}Hosts}, true)" ncf;

THREADGROUP workerThreads
totalThreads = fcn "(${${D}Hosts} * ${${D}VMsPerHost} *  ${${D}ThreadsPerVM}) " ncf
totalVMs     = fcn "(${${D}Hosts} * ${${D}VMsPerHost})" ncf
clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${D}\",${${D}Hosts}, true)" ncf;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_initializeSnappyTest
runMode = always
threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads,leadThread,workerThreads;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads,leadThread,workerThreads;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_initializeSnappyAdAnalyticsTest
runMode = always
threadGroups = snappyThreads,leadThread;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyLocatorConfig
runMode = always
threadGroups = locatorThreads;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLocatorConfigData
runMode = always
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyServerConfig
runMode = always
threadGroups = snappyStoreThreads,snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeServerConfigData
runMode = always
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSnappyLeadConfig
runMode = always
threadGroups = leadThreads, leadThread;

INITTASK     taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeLeadConfigData
runMode = always
threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_generateSparkWorkerConfig
runMode = always
threadGroups = workerThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_writeWorkerConfigData
runMode = always
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StartKafkaZookeeper
runMode = always
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StartKafkaBrokers
runMode = always
threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSnappyCluster
runMode = always
threadGroups = snappyThreads;

INITTASK    taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_startSparkCluster
runMode = always
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil taskMethod  = HydraTask_initialize
threadGroups = snappyThreads;

INITTASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_executeSQLScripts
io.snappydata.hydra.cluster.SnappyPrms-logFileName = create_temp_table.log
io.snappydata.hydra.cluster.SnappyPrms-sqlScriptNames = createTempTableForStreaming.sql
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_StartKafkaTopic
io.snappydata.hydra.cluster.SnappyPrms-kafkaTopic="streamingSinkTopic"
runMode = always
threadGroups = snappyThreads;

INITTASK     taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod = HydraTask_generateAndPublishMethod
io.snappydata.hydra.cluster.SnappyPrms-appPropsForJobServer = "400000,streamingSinkTopic,0,0"
io.snappydata.hydra.cluster.SnappyPrms-userAppJar = snappydata-store-scala-tests*tests.jar
threadGroups = snappyThreads;

TASK  taskClass   = io.snappydata.hydra.adAnalytics.SnappyAdAnalyticsTest taskMethod  = HydraTask_executeSQLScriptsWithSleep
    io.snappydata.hydra.cluster.SnappyPrms-logFileName = sqlScriptResults_0.log
    io.snappydata.hydra.cluster.SnappyPrms-sqlScriptNames = query_snappyTemp_table.sql
    threadGroups = snappyStoreThreads
    maxThreads = 1;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-isAggregate=true;