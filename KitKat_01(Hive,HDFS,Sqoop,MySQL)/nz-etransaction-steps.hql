1) Create Table in Database in MYSQL:
Create Table E_NY_TRAN_DATA(
Series_reference varchar(50),
Period varchar(10),
Data_value double(16,2) DEFAULT 0.00,
Suppressed varchar(3) DEFAULT 'N',
STATUS varchar(3) DEFAULT 'NA',
UNITS varchar(10),
Magnitude INT,
Subject varchar(100),
Group_ varchar(150),
Series_title_1 varchar(100),
Series_title_2 varchar(300)
);

-- Table Created

2) Upload the File to FTP 

2.1) Ensure Access is proper
ls -lrt
cd <path where file resides>
chmod 775 <filename>

3) 
LOAD DATA LOCAL INFILE  
'/mnt/home/.../kitkat/NZ-electronic-card-transactions-may-2021-csv-tables-lower-level.csv'
INTO TABLE E_NY_TRAN_DATA  
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
3.1) Check loaded data:
Select * from e_ny_tran_data limit 1;

4) Create Table in Hive

CREATE EXTERNAL TABLE `kitkat_db.e_ny_tran_data`(
  `series_reference` string COMMENT 'from deserializer', 
  `period` string COMMENT 'from deserializer', 
  `data_value` string COMMENT 'from deserializer', 
  `suppressed` string COMMENT 'from deserializer', 
  `status` string COMMENT 'from deserializer', 
  `units` string COMMENT 'from deserializer', 
  `magnitude` string COMMENT 'from deserializer', 
  `subject` string COMMENT 'from deserializer', 
  `group_` string COMMENT 'from deserializer', 
  `series_title_1` string COMMENT 'from deserializer', 
  `series_title_2` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'quoteChar'='"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/../kitkat01'

5) Import using Sqoop:
sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" --connect jdbc:mysql://<hostname>/mihirdh93 --username mihirdh93 -P --table E_NY_TRAN_DATA --delete-target-dir  --target-dir kitkat01 --columns Series_reference,Period,Data_val ue,Suppressed,STATUS,UNITS,Magnitude,Subject,Group_,Series_title_1,Series_title_2 --split-by Series_reference

6) KPI 1: SQL  ; Keep this SQL in .hql file and export using "hive -f <filname>.hql > output.txt"
SELECT  
PRD AS TIMELINE, 
SERIES_TITLE_2 AS CATEGORY,
CEIL(TOT_VAL) AS TOTAL_AMOUNT
FROM
(SELECT SPLIT(PERIOD,'[.]')[0] AS PRD, 
SERIES_TITLE_2,
SUM(DATA_VALUE) TOT_VAL,
ROW_NUMBER() OVER(PARTITION BY SPLIT(PERIOD,'[.]')[0] ORDER BY SUM(DATA_VALUE) DESC ) AS RNUM  
FROM KITKAT_DB.E_NY_TRAN_DATA
GROUP BY SPLIT(PERIOD,'[.]')[0], 
SERIES_TITLE_2
)TB
WHERE RNUM = 1
ORDER BY TIMELINE DESC;

7) KPI 2: SQL

select ceil(avg(data_value)) as avg_spend,series_title_2 as category 
from kitkat_db.e_ny_tran_data group by series_title_2
order by category ;