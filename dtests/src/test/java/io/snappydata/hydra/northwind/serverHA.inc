io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 120; //how long to wait before nodes will be cycled.
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

hydra.Prms-totalTaskTimeSec           = 240;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;

INITTASK    taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
            threadGroups = snappyThreads, locatorThreads, snappyStoreThreads, leadThreads;

TASK       taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleStoreVms
           maxThreads = 1
           startInterval = 60
           //weight = 40
           threadGroups = snappyThreads
           maxTimesToRun = 3
           ;

ENDTASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
         clientNames = locator1;
