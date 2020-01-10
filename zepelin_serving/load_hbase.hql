create external table zepelin_hbase_yearly_combos (
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
HWHTHP bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,year:totalincidents,year:LWLTLP,year:LWLTHP,year:LWMTLP,year:LWMTHP,year:LWHTLP,year:LWHTHP,year:MWLTLP,year:MWLTHP,year:MWMTLP,year:MWMTHP,year:MWHTLP,year:MWHTHP,year:HWLTLP,year:HWLTHP,year:HWMTLP,year:HWMTHP,year:HWHTLP,year:HWHTHP,')
TBLPROPERTIES ('hbase.table.name' = 'zepelin_hbase_yearly_combos');

create external table zepelin_hbase_crimes_by_day (
year int,
alldays int,
allincidents bigint,
allarrests bigint,
lowwinddays int,
lowwindincidents bigint,
lowwindarrests bigint,
moderatewinddays int,
moderatewindincidents bigint,
moderatewindarrests bigint,
highwinddays int,
highwindincidents bigint,
highwindarrests bigint,
lowtempdays int,
lowtempincidents bigint,
lowtemparrests bigint,
moderatetempdays int,
moderatetempincidents bigint,
moderatetemparrests bigint,
hightempdays int,
hightempincidents bigint,
hightemparrests bigint,
lowprecipdays int,
lowprecipincidents bigint,
lowpreciparrests bigint,
highprecipdays int,
highprecipincidents bigint,
highpreciparrests bigint
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,year:alldays,year:allincidents,year:allarrests,year:lowwinddays,year:lowwindincidents,year:lowwindarrests,year:moderatewinddays,year:moderatewindincidents,year:moderatewindarrests,year:highwinddays,year:highwindincidents,year:highwindarrests,year:lowtempdays,year:lowtempincidents,year:lowtemparrests,year:moderatetempdays,year:moderatetempincidents,year:moderatetemparrests,year:hightempdays,year:hightempincidents,year:hightemparrests,year:lowprecipdays,year:lowprecipincidents,year:lowpreciparrests,year:highprecipdays,year:highprecipincidents,year:highpreciparrests,')
TBLPROPERTIES ('hbase.table.name' = 'zepelin_hbase_crimes_by_day');

insert overwrite table zepelin_hbase_yearly_combos
select 
year,
totalincidents,
LWLTLP,
LWLTHP,
LWMTLP,
LWMTHP,
LWHTLP,
LWHTHP,
MWLTLP,
MWLTHP,
MWMTLP,
MWMTHP,
MWHTLP,
MWHTHP,
HWLTLP,
HWLTHP,
HWMTLP,
HWMTHP,
HWHTLP,
HWHTHP
  from zepelin_yearly_incidents_by_weathercombos;

insert overwrite table zepelin_hbase_crimes_by_day
select 
year,
count(1),
sum(IncidentCount),
sum(ArrestCount),
sum(if(LowWind, 1, 0)),
sum(if(LowWind, IncidentCount, 0)),
sum(if(LowWind, ArrestCount, 0)),
sum(if(ModerateWind, 1, 0)),
sum(if(ModerateWind, IncidentCount, 0)),
sum(if(ModerateWind, ArrestCount, 0)),
sum(if(HighWind, 1, 0)),
sum(if(HighWind, IncidentCount, 0)),
sum(if(HighWind, ArrestCount, 0)),
sum(if(LowTemp, 1, 0)),
sum(if(LowTemp, IncidentCount, 0)),
sum(if(LowTemp, ArrestCount, 0)),
sum(if(ModerateTemp, 1, 0)),
sum(if(ModerateTemp, IncidentCount, 0)),
sum(if(ModerateTemp, ArrestCount, 0)),
sum(if(HighTemp, 1, 0)),
sum(if(HighTemp, IncidentCount, 0)),
sum(if(HighTemp, ArrestCount, 0)),
sum(if(LowPrecipitation, 1, 0)),
sum(if(LowPrecipitation, IncidentCount, 0)),
sum(if(LowPrecipitation, ArrestCount, 0)),
sum(if(HighPrecipitation, 1, 0)),
sum(if(HighPrecipitation, IncidentCount, 0)),
sum(if(HighPrecipitation, ArrestCount, 0))
from zepelin_chi_crimes_by_day
group by year;