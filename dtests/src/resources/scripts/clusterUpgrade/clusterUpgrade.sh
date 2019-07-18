#!/usr/bin/env bash

source clusterUpgrade.conf
cnt=1
tableArr=(colTable rowPartitionedTable rowReplicatedTable)

if [ $# -lt 2 ]; then
   echo "ERROR: incorrect argument specified: " "$@"
   echo "The script should have atleast 2 builds for comparison"
   echo "Usage:./clusterUpgrade.sh <snappyBuildPath1> <snappyBuildPath2> ... <snappyBuildPathN>"
   exit 1
fi

totalNoBuilds=$(echo $#)
echo "The total no. of builds to be tested are $totalNoBuilds"

#Do clean up
rm -rf $resultDir/*
rm -rf $snappydataTestDir/tempConf/*

if [ ! -d "$resultDir" ]; then
   mkdir $resultDir
fi

for i in "$@";
  do
    if [ $cnt -gt 1 ]; then
      echo "========= Copying the previous conf from tempConf to $i/conf ==========="
      cp -r $snappydataTestDir/tempConf/* $i/conf/
    fi

    echo -e "\n\n============ Starting Snappy cluster =========="
    echo "For build $i cluster start status is in $resultDir/clusterStartStatus.log"
    echo -e "\n====== Build = $i ============" >> $resultDir/clusterStartStatus.log
    sh $i/sbin/snappy-start-all.sh >> $resultDir/clusterStartStatus.log
    sh $i/sbin/snappy-status-all.sh >> $resultDir/clusterStartStatus.log
    sleep 30
    echo "===================================================================="
    grep 'Exception\|Error\|WARN' ${resultDir}/clusterStartStatus.log
    if [ $? -eq 0 ]; then
       echo "Cluster start up encountered an Exception/Error/WARN"
       echo "Please see the logs"
       exit 1
    fi

    echo -e "\n\n============ Starting Spark cluster =========="
    sh $i/sbin/start-all.sh

    #execute create table script.
    if [ $cnt -eq 1 ]; then
      echo -e "\n\n=========== Loading table for the first time ========="
      sh $i/bin/snappy run -file=$createTableScript -client-bind-address=localhost -client-port=1527
      echo -e "\n=========== Finished loading tables ==========="
      sh $i/bin/snappy run -file=$dmlScript -client-bind-address=localhost -client-port=1527 > $resultDir/dmlOutput.log
    fi

    echo -e "\n ========== Executing snappy job ============"
   # if [ $cnt -eq 1 ]; then
    #  $i/bin/snappy-job.sh submit --lead localhost:8090 --app-name myApp --class $snappyJobClassName --app-jar $jarFile --conf queryFile=$dmlScript > $resultDir/jobRun.txt
    #else
      $i/bin/snappy-job.sh submit --lead localhost:8090 --app-name myApp --class $snappyJobClassName --app-jar $jarFile --conf queryFile=$validationScript > $resultDir/jobRun.txt
    #fi
    jobId=$(grep -r 'jobId' $resultDir/jobRun.txt|cut -d'"' -f4)
    $i/bin/snappy-job.sh status --lead localhost:8090 --job-id $jobId > $resultDir/jobStatus.txt
    while ! grep 'FINISHED\|ERROR' $resultDir/jobStatus.txt > /dev/null
    do
      sleep 5
      echo -e "\n Waiting for the job to finish"
      $i/bin/snappy-job.sh status --lead localhost:8090 --job-id $jobId > $resultDir/jobStatus.txt
    done
    cat $resultDir/jobStatus.txt


    echo -e "\n\n========= Execute Spark job ==========="
   # if [ $cnt -eq 1 ]; then
    #  sh $i/bin/spark-submit --class $sparkJobClassName --master spark://$HOSTNAME:7077  --executor-memory 1280m --conf snappydata.connection=localhost:1527 $jarFile $dmlScript > "$resultDir/sparkJobOutPut_$cnt.log"
    #else
      sh $i/bin/spark-submit --class $sparkJobClassName --master spark://$HOSTNAME:7077  --executor-memory 1280m --conf snappydata.connection=localhost:1527 $jarFile $validationScript > "$resultDir/sparkJobOutPut_$cnt.log"
    #fi
    #echo "Validation for $i build " >> $resultDir/output_$cnt.log
    sh $i/bin/snappy run -file=$validationScript -client-bind-address=localhost -client-port=1527 >> $resultDir/output_$cnt.log


    echo -e "\n\n=========== Stopping Snappy cluster =========="
    sh $i/sbin/snappy-stop-all.sh >> $resultDir/clusterStopStatus.log
    sh $i/sbin/snappy-status-all.sh
    sleep 30
    echo -e "\n\n=========== Stopping Spark cluster =========="
    sh $i/sbin/stop-all.sh

    echo -e "\n========= Copying the present conf to $snappydataTestDir/tempConf ========"
    if [ ! -d "$snappydataTestDir/tempConf" ]; then
      mkdir $snappydataTestDir/tempConf
    fi
    cp -r $i/conf/* $snappydataTestDir/tempConf
    ((cnt++))
  done

  validate() {
    tableName=$1
    for ((i=1;i<=$totalNoBuilds;i++)); do
      cntVal[$i]=$(grep -A3 $tableName $resultDir/output_$i.log |tail -n 1)
      echo "COUNT(*) = ${cntVal[$i]}" >> $resultDir/${tableName}_OutPut.log
    done
  }

  echo -e "\n============Starting Validation ============================"
 # for i in "${tableArr[@]}"; do
  #  echo -e "\n=============Validation results for $i is in $resultDir/${i}_OutPut.log ================"
   # validate $i
  #done
  
  cd $resultDir
  for ((i=1;i<=$totalNoBuilds;i++)); do
    echo "Comparing output_$i.log with output_1.log"
    sort output_$i.log > output_sorted_$i.log
    diff output_sorted_1.log output_sorted_$i.log
  done



  echo -e "\n\n=============Finished Cluster Upgradation test =================="

