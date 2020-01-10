#!/bin/bash

# Batch Layer
echo "Rebuilding the Batch Layer"
# NOTE: THESE TWO COMMANDS WILL FULLY REBUILD THE DATASET FROM THE SOURCE
# THEY HAVE BEEN COMMENTED OUT SINCE THEY ARE TIME CONSUMING, BUT CAN EASILY BE REENABLED BY UNCOMMENTING
# echo "Downloading Master Dataset"
# ./zepelin_batch/pull_data.sh
# NOTE: LOADING DATA HAS ALSO BEEN COMMENTED OUT AS THE CLUSTER FILESYSTEM IS VERY FULL
# DATA WILL NEED TO BE REDOWNLOADED WITH THE "pull_data.sh" ABOVE IN ORDER FOR "ingest_data.sh" TO FUNCTION AS EXPECTED
# echo "Loading into HDFS"
# ./zepelin_batch/ingest_data.sh
echo "Loading crime data into Hive"
hive -f zepelin_batch/zepelin-crime.hql
echo "Loading weather data into Hive"
hive -f zepelin_batch/zepelin-weather.hql
echo "Joining datasets and computing views"
hive -f zepelin_batch/zepelin-joined.hql

# Cleaning
echo "Cleanup from former run"
hive -f zepelin_serving/clean_hive.hql
hbase shell zepelin_serving/hbase_setup

# Serving Layer
echo "Building the Serving Layer"
echo "Loading precomputed data into HBase tables"
hive -f zepelin_serving/load_hbase.hql

# Speed Layer
echo "Building the Speed Layer"
# NOTE: While this script is capable of rebuilding almost the entire job from scratch,
# it does not try to recreate the Kafka Topic it uses: 'zepelin-incident'
echo "Creating Speed Layer Hbase Tables"
hive -f zepelin_speed/zepelin-speed-tables.hql
echo "Initializing HBase Incremental Counters"
hbase shell zepelin_speed/zepelin-hbase-init
echo "Submitting Streaming Job"
spark-submit --class StreamIncident /home/zepelin/zepelin_final/zepelin_speed/uber-speed-layer-0.0.1-SNAPSHOT.jar mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667
