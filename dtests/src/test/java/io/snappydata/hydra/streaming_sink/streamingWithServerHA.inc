io.snappydata.hydra.cluster.SnappyPrms-waitTimeBeforeNextCycleVM = 120; //how long to wait before nodes will be cycled.
io.snappydata.hydra.cluster.SnappyPrms-cycleVms = true;

util.StopStartPrms-stopModes = NICE_KILL;
util.StopStartPrms-numVMsToStop = 1;

TASK       taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = HydraTask_cycleStoreVms
           startInterval = 60
           threadGroups = snappyThreads
           io.snappydata.hydra.cluster.SnappyPrms-sleepTimeSecsBeforeRestart = 60
           maxTimesToRun = 1;

ENDTASK  taskClass   = io.snappydata.hydra.cluster.SnappyTest taskMethod  = restoreServerConfigData
         clientNames = locator1;
