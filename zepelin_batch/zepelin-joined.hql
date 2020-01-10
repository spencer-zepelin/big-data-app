DROP TABLE IF EXISTS zepelin_chi_crime_and_weather;
DROP TABLE IF EXISTS zepelin_incidents_by_weathercombos;
DROP TABLE IF EXISTS zepelin_chi_crimes_by_day;

create table zepelin_chi_crime_and_weather(
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
  FBICode string,
  AvgWindSpeed float,
  Precipitation float,
  MaxTemp float,
  MinTemp float,
  LowWind boolean,
  ModerateWind boolean,
  HighWind boolean,
  LowTemp boolean,
  ModerateTemp boolean,
  HighTemp boolean,
  LowPrecipitation boolean,
  HighPrecipitation boolean
);

create table zepelin_yearly_incidents_by_weathercombos(
year int,
totalincidents bigint,
LWLTLP bigint,
LWLTHP bigint,
LWMTLP bigint,
LWMTHP bigint,
LWHTLP bigint,
LWHTHP bigint,
MWLTLP bigint,
MWLTHP bigint,
MWMTLP bigint,
MWMTHP bigint,
MWHTLP bigint,
MWHTHP bigint,
HWLTLP bigint,
HWLTHP bigint,
HWMTLP bigint,
HWMTHP bigint,
HWHTLP bigint,
HWHTHP bigint
);

create table zepelin_chi_crimes_by_day(
  Year smallint,
  Month tinyint,
  Day tinyint,
  LowWind boolean,
  ModerateWind boolean,
  HighWind boolean,
  LowTemp boolean,
  ModerateTemp boolean,
  HighTemp boolean,
  LowPrecipitation boolean,
  HighPrecipitation boolean,
  IncidentCount bigint,
  ArrestCount bigint
);

insert overwrite table zepelin_chi_crime_and_weather
select
c.CID,
c.CaseNumber,
c.Month,
c.Day,
c.Year,
c.Block,
c.IUCR,
c.PrimaryCrimeType,
c.CrimeDescription,
c.LocationDescription,
c.Arrest,
c.Domestic,
c.Beat,
c.District,
c.Ward,
c.CommunityArea,
c.FBICode,
w.AvgWindSpeed,
w.Precipitation,
w.MaxTemp,
w.MinTemp,
if(w.AvgWindSpeed < 7.2, 1, 0),
if(w.AvgWindSpeed < 11.9 AND w.AvgWindSpeed >= 7.2, 1, 0),
if(w.AvgWindSpeed >= 11.9, 1, 0),
if(w.MaxTemp < 43, 1, 0),
if(w.MaxTemp < 79 AND w.MaxTemp >= 43, 1, 0),
if(w.MaxTemp >= 79, 1, 0),
if(w.Precipitation < 0.04, 1, 0),
if(w.Precipitation >= 0.04, 1, 0)
from zepelin_crime c
join zepelin_weather w
on c.year = w.year and c.month = w.month and c.day = w.day;

insert overwrite table zepelin_yearly_incidents_by_weathercombos
select
year,
count(1), 
count(if(LowWind AND LowTemp AND LowPrecipitation, 1, NULL)),
count(if(LowWind AND LowTemp AND HighPrecipitation, 1, NULL)),
count(if(LowWind AND ModerateTemp AND LowPrecipitation, 1, NULL)),
count(if(LowWind AND ModerateTemp AND HighPrecipitation, 1, NULL)),
count(if(LowWind AND HighTemp AND LowPrecipitation, 1, NULL)),
count(if(LowWind AND HighTemp AND HighPrecipitation, 1, NULL)),
count(if(ModerateWind AND LowTemp AND LowPrecipitation, 1, NULL)),
count(if(ModerateWind AND LowTemp AND HighPrecipitation, 1, NULL)),
count(if(ModerateWind AND ModerateTemp AND LowPrecipitation, 1, NULL)),
count(if(ModerateWind AND ModerateTemp AND HighPrecipitation, 1, NULL)),
count(if(ModerateWind AND HighTemp AND LowPrecipitation, 1, NULL)),
count(if(ModerateWind AND HighTemp AND HighPrecipitation, 1, NULL)),
count(if(HighWind AND LowTemp AND LowPrecipitation, 1, NULL)),
count(if(HighWind AND LowTemp AND HighPrecipitation, 1, NULL)),
count(if(HighWind AND ModerateTemp AND LowPrecipitation, 1, NULL)),
count(if(HighWind AND ModerateTemp AND HighPrecipitation, 1, NULL)),
count(if(HighWind AND HighTemp AND LowPrecipitation, 1, NULL)),
count(if(HighWind AND HighTemp AND HighPrecipitation, 1, NULL))
from zepelin_chi_crime_and_weather
group by year;

insert overwrite table zepelin_chi_crimes_by_day
select
w.year,
w.month,
w.day,
if(w.AvgWindSpeed < 7.2, 1, 0),
if(w.AvgWindSpeed < 11.9 AND w.AvgWindSpeed >= 7.2, 1, 0),
if(w.AvgWindSpeed >= 11.9, 1, 0),
if(w.MaxTemp < 43, 1, 0),
if(w.MaxTemp < 79 AND w.MaxTemp >= 43, 1, 0),
if(w.MaxTemp >= 79, 1, 0),
if(w.Precipitation < 0.04, 1, 0),
if(w.Precipitation >= 0.04, 1, 0),
c.incidents,
c.arrests
from zepelin_weather w
join 
(select year, month, day, count(CaseNumber) as incidents, sum(if(Arrest, 1, 0)) as arrests from zepelin_crime group by year, month, day) c
on c.year = w.year and c.month = w.month and c.day = w.day;