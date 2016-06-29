package io.snappydata.hydra.cluster;

import hydra.BasePrms;

import java.util.Vector;


/**
 * Created by swati on 11/3/16.
 */
public class SnappyPrms extends BasePrms {
    /**
     * Parameter used to get the user specified script names for INITTASK.
     * (VectosetValues of Strings) A list of values for script Names to execute.
     */
    public static Long sqlScriptNamesForInitTask;

    /**
     * Parameter used to get the user specified param List for INITTASK.
     * (VectosetValues of Strings) A list of values for parameters to be replaced in the sql scripts.
     * If no parameter is required for sql script then expected value to be provided for param is : Empty String : " " in case if user don't want to maintain the sequence.
     * Or else provide the script that does not require any parameter at the end in list of sqlScriptNamesForInitTask parameter.
     * Framework will treat its corresponding parameter as " " string in this case.
     */
    public static Long sqlScriptParamsForInitTask;

    /**
     * Parameter used to get the user specified script names for TASK.
     * (VectosetValuesr of Strings) A list of values for script Names to execute.
     */
    public static Long sqlScriptNamesForTask;

    /**
     * Parameter used to get the user specified snappy job class names for CLOSETASK.
     * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long jobClassNamesForCloseTask;

    /**
     * Parameter used to get the user specified snappy job class names for TASK.
     * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long jobClassNamesForTask;

    /**
     * Parameter used to get the user specified spark job class names for TASK.
     * (VectosetValues of Strings) A list of values for spark-job Names to execute.
     */
    public static Long sparkJobClassNamesForTask;

    /**
     * Parameter used to get the user specified snappy streaming job class names for TASK.
     * (VectosetValues of Strings) A list of values for snappy-job Names to execute.
     */
    public static Long streamingJobClassNamesForTask;

    /**
     * (boolean) for testing HA
     */
    public static Long cycleVms;

    /**
     * (String) cycleVMTarget - which node to be cycled "store, lead" etc
     */
    public static Long cycleVMTarget;

    /**
     * (String) e.g. simulateFileStream
     */
    public static Long simulateStreamScriptName;

    /**
     * (String) - destination folder to copy the streaming data. e.g. /home/swati
     */
    public static Long simulateStreamScriptDestinationFolder;

    /**
     * (boolean) - whether snappy servers and locators needs to be started using rowstore option.
     */
    public static Long useRowStore;

    /**
     * (boolean) - whether split mode cluster needs to be started.
     */
    public static Long useSplitMode;

    /**
     * (boolean) - whether stop mode needs to be checked before deleting the config data if already exists.
     * This is required in case user wants to start the cluster and then stop the same later on using different script.
     * In this case, test should not delete the existing configuration data created by previous test.
     */
    public static Long isStopMode;

    /**
     * (boolean) - whether created tables to be replicated or partitioned. snappy hydra already sets the gemfirexd.table-default-partitioned to false.
     */
    public static Long tableDefaultPartitioned;

    /**
     * (boolean) - whether to enable time statistics. snappy hydra already sets the enable-time-statistics to true.
     */
    public static Long enableTimeStatistics;


    /**
     * (String) userAppJar containing the user snappy job class
     */
    public static Long userAppJar;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status in Task
     */
    public static Long jobExecutionTimeInMillisForTask;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status in Task
     */
    public static Long streamingJobExecutionTimeInMillisForTask;

    /**
     * (int) how long (milliseconds) it should wait for getting the job status in Close Task
     */
    public static Long jobExecutionTimeInMillisForCloseTask;

    /**
     * (int) how long (milliseconds) it should wait before Cycle VMs again
     */
    public static Long waitTimeBeforeNextCycleVM;

    /** (int) The number of VMs to stop (then restart) at a time.
     */
    public static Long numVMsToStop;

    /** (int) The number of lead VMs to stop (then restart).
     */
    public static Long numLeadsToStop;

    /**
     * Parameter used to get the user APP_PROPS for snappy job.
     * (VectosetValues of Strings) A list of values for snappy-job.
     */
    public static Long appPropsForJobServer;

    /**
     * (int) number of executor cores to be used in test
     */
    public static Long executorCores;

    /**
     * (String) Maximun Result Size for Driver. Defaults to 1GB if not provided.
     */
    public static Long driverMaxResultSize;

    /**
     * (String) Local Memory. Defaults to 1GB if not provided.
     */
    public static Long locatorMemory;

    /**
     * (String) Memory to be used while starting the Server process. Defaults to 1GB if not provided.
     */
    public static Long serverMemory;

    /**
     * (String) Memory to be used while starting the Lead process. Defaults to 1GB if not provided.
     */
    public static Long leadMemory;

    /**
     * (String) sparkSchedulerMode. Defaults to 'FAIR' if not provided.
     */
    public static Long sparkSchedulerMode;

    /**
     * (int) sparkSqlBroadcastJoinThreshold
     */
    public static Long sparkSqlBroadcastJoinThreshold;

    /**
     * (boolean) - whether in-memory Columnar store needs to be compressed . Defaults to false if not provided.
     */
    public static Long compressedInMemoryColumnarStorage;

    /**
     * (boolean) - whether to use conserveSockets. Defaults to false if not provided.
     */
    public static Long conserveSockets;

    /**
     * (int) number of shuffle partitions to be used in test
     */
    public static Long shufflePartitions;

    public static int getExecutorCores() {
        Long key = executorCores;
        return tasktab().intAt(key, tab().intAt(key, 1));
    }

    public static String getDriverMaxResultSize() {
        Long key = driverMaxResultSize;
        return tab().stringAt(key, "1g").toLowerCase();
    }

    public static String getLocatorMemory() {
        Long key = locatorMemory;
        return tab().stringAt(key, "1G");
    }

    public static String getServerMemory() {
        Long key = serverMemory;
        return tab().stringAt(key, "4G");
    }

    public static String getLeadMemory() {
        Long key = leadMemory;
        return tab().stringAt(key, "1G");
    }

    public static String getSparkSchedulerMode() {
        Long key = sparkSchedulerMode;
        return tab().stringAt(key, "FAIR");
    }

    public static int getSparkSqlBroadcastJoinThreshold() {
        Long key = sparkSqlBroadcastJoinThreshold;
        return tasktab().intAt(key, tab().intAt(key, -1));
    }

    public static boolean getCompressedInMemoryColumnarStorage() {
        Long key = compressedInMemoryColumnarStorage;
        return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    public static boolean getConserveSockets() {
        Long key = conserveSockets;
        return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    public static int getShufflePartitions() {
        Long key = shufflePartitions;
        return tasktab().intAt(key, tab().intAt(key, 1));
    }

    public static String getCommaSepAPPProps() {
        Long key = appPropsForJobServer;
        return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
    }

    public static boolean getTableDefaultDataPolicy() {
        Long key = tableDefaultPartitioned;
        return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    public static boolean getTimeStatistics() {
        Long key = enableTimeStatistics;
        return tasktab().booleanAt(key, tab().booleanAt(key, true));
    }

    public static Vector getSQLScriptNamesForInitTask() {
        Long key = sqlScriptNamesForInitTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSQLScriptParamsForInitTask() {
        Long key = sqlScriptParamsForInitTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSQLScriptNamesForTask() {
        Long key = sqlScriptNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyJobClassNamesForTask() {
        Long key = jobClassNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSparkJobClassNamesForTask() {
        Long key = sparkJobClassNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyStreamingJobClassNamesForTask() {
        Long key = streamingJobClassNamesForTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    public static Vector getSnappyJobClassNamesForCloseTask() {
        Long key = jobClassNamesForCloseTask;
        return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    }

    static {
        BasePrms.setValues(SnappyPrms.class);
    }

    public static void main(String args[]) {
        BasePrms.dumpKeys();
    }
}
