#!/bin/bash

hdfs dfs -rm -f /user/zepelin/final_project/inputs/crime/crime_data.csv
hdfs dfs -rm -f /user/zepelin/final_project/inputs/weather/weatherdata.csv

# Load data into hdfs
# NOTE: use tail command to get rid of header row
tail -n +2 /home/zepelin/final_project/inputs/crime/chicrime.csv | hdfs dfs -put -f - /user/zepelin/final_project/inputs/crime/crime_data.csv
tail -n +2 /home/zepelin/final_project/inputs/weather/chiweather.csv | hdfs dfs -put -f - /user/zepelin/final_project/inputs/weather/weatherdata.csv