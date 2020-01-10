-- for repeated runs, drop table before recreating
DROP TABLE IF EXISTS zepelin_weather_csv;
DROP TABLE IF EXISTS zepelin_weather;

-- create hive table for the weather data
create external table zepelin_weather_csv(
  Station string,
  WeatherDate string,
  AWND string,
  FMTM string,
  PGTM string,
  PRCP string,
  TMAX string,
  TMIN string)

  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/user/zepelin/final_project/inputs/weather';

-- create an ORC table into which we will process this data
create table zepelin_weather(
  Year smallint,
  Month tinyint,
  Day tinyint,
  AvgWindSpeed float,
  Precipitation float,
  MaxTemp float,
  MinTemp float
  )
  stored as orc;

-- write data from csv hive table into ORC table
insert overwrite table zepelin_weather
select
cast(split(weatherdate, "[-]")[0] as smallint), 
cast(split(weatherdate, "[-]")[1] as tinyint), 
cast(split(weatherdate, "[-]")[2] as tinyint), 
if(AWND is not NULL, cast(AWND as int) * 0.223694, 9.40),
if(PRCP is not NULL, cast(PRCP as int) * 2.54, 0.0),
if(TMAX is not NULL, (cast(TMAX as int) * 0.18) + 32, 62.1),
if(TMIN is not NULL, (cast(TMIN as int) * 0.18) + 32, 45.0)
from zepelin_weather_csv;