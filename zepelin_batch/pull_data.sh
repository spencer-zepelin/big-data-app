#!/bin/bash

# Run this script
rm -f /home/zepelin/final_project/inputs/crime/chicrime.csv 
rm -f /home/zepelin/final_project/inputs/weather/chiweather.csv

# Download crime data from Chicago Data Portal API
wget -O /home/zepelin/final_project/inputs/crime/chicrime.csv -q "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv"

# Download only necessary weather data from noaa
wget -O /home/zepelin/final_project/inputs/weather/chiweather.csv -q "https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&format=csv&stations=USW00014819&startDate=2001-01-01T00:00:00&endDate=2019-12-31T23:09:59&dataTypes=AWND,FMTM,PGTM,PRCP,TMAX,TMIN"
