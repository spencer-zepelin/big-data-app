# for repeated runs, drop table before recreating
DROP TABLE IF EXISTS zepelin_crime_csv;
DROP TABLE IF EXISTS zepelin_crime;

# create hive table for the crime data
# note: treating both date fields as strings for the time being
# will need to convert into usable date for join
create external table zepelin_crime_csv(
  CID int,
  CaseNumber string,
  EventDate timestamp,
  Block string,
  IUCR string,
  PrimaryCrimeType string,
  CrimeDescription string,
  LocationDescription string,
  Arrest boolean,
  Domestic boolean,
  Beat string,
  District string,
  Ward tinyint,
  CommunityArea tinyint,
  FBICode string,
  XCoord int,
  YCoord int,
  Year smallint,
  UpdatedOn timestamp, 
  Latitude string,
  Longitude string,
  Location string)

  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/user/zepelin/final_project/inputs/crime';


# create an ORC table into which we will process this data
create table zepelin_crime(
  CID int,
  CaseNumber string,
  Month tinyint,
  Day tinyint,
  Year smallint,
  Block string,
  IUCR string,
  PrimaryCrimeType string,
  CrimeDescription string,
  LocationDescription string,
  Arrest boolean,
  Domestic boolean,
  Beat string,
  District string,
  Ward tinyint,
  CommunityArea tinyint,
  FBICode string
  )
  stored as orc;

# write data from csv hive table into ORC table
insert overwrite table zepelin_crime
select
CID, 
CaseNumber, 
cast(split(eventdate, "[/]")[0] as tinyint), 
cast(split(eventdate, "[/]")[1] as tinyint), 
Year, 
Block, 
IUCR, 
PrimaryCrimeType,
CrimeDescription,
LocationDescription,
Arrest,
Domestic,
Beat,
District,
Ward,
CommunityArea,
FBICode
from zepelin_crime_csv
where EventDate is not null 
and Year is not null
and PrimaryCrimeType is not null 
and Ward is not null;
