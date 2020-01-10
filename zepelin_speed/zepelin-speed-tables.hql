create external table zepelin_hbase_yearly_combos_speed (
  year string,
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
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,number:totalincidents#b,number:LWLTLP#b,number:LWLTHP#b,number:LWMTLP#b,number:LWMTHP#b,number:LWHTLP#b,number:LWHTHP#b,number:MWLTLP#b,number:MWLTHP#b,number:MWMTLP#b,number:MWMTHP#b,number:MWHTLP#b,number:MWHTHP#b,number:HWLTLP#b,number:HWLTHP#b,number:HWMTLP#b,number:HWMTHP#b,number:HWHTLP#b,number:HWHTHP#b,')
TBLPROPERTIES ('hbase.table.name' = 'zepelin_hbase_yearly_combos_speed');

create external table zepelin_hbase_crimes_by_day_speed (
year string,
allincidents bigint,
allarrests bigint,
lowwindincidents bigint,
lowwindarrests bigint,
moderatewindincidents bigint,
moderatewindarrests bigint,
highwindincidents bigint,
highwindarrests bigint,
lowtempincidents bigint,
lowtemparrests bigint,
moderatetempincidents bigint,
moderatetemparrests bigint,
hightempincidents bigint,
hightemparrests bigint,
lowprecipincidents bigint,
lowpreciparrests bigint,
highprecipincidents bigint,
highpreciparrests bigint
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,year:allincidents#b,year:allarrests#b,year:lowwindincidents#b,year:lowwindarrests#b,year:moderatewindincidents#b,year:moderatewindarrests#b,year:highwindincidents#b,year:highwindarrests#b,year:lowtempincidents#b,year:lowtemparrests#b,year:moderatetempincidents#b,year:moderatetemparrests#b,year:hightempincidents#b,year:hightemparrests#b,year:lowprecipincidents#b,year:lowpreciparrests#b,year:highprecipincidents#b,year:highpreciparrests#b,')
TBLPROPERTIES ('hbase.table.name' = 'zepelin_hbase_crimes_by_day_speed');