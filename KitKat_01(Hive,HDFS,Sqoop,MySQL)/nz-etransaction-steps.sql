1) Create Table in Database in MYSQL:
Create Table E_NY_TRAN_DATA(
Series_reference varchar(50),
Period varchar(10),
Data_value double(10,2),
Suppressed varchar(3),
STATUS varchar(3),
UNITS varchar(10),
Magnitude INT,
Subject varchar(100),
Group_ varchar(150),
Series_title_1 varchar(100),
Series_title_2 varchar(300)
);

-- Table Created

2) Upload the File to FTP 

3) 
LOAD DATA LOCAL INFILE  
'/mnt/home/yvkrvamsigmail/kitkat/NZ-electronic-card-transactions-may-2021-csv-tables-lower-level.csv'
INTO TABLE E_NY_TRAN_DATA  
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
(field_1,field_2 , field_3);


Create Table kitkat_db.E_NY_TRAN_DATA(
Series_reference string,
Period string,
Data_value string,
Suppressed string,
STATUS string,
UNITS string,
Magnitude string,
Subject string,
Group_ string,
Series_title_1 string,
Series_title_2 string
)row format delimited fields terminated by ',' stored as ORC;


select
split(period,'[.]')[0] As Y_Month 
,ceil(sum(data_value)) as total_transaction_val
,series_title_2
from
kitkat_db.E_NY_TRAN_DATA --limit 3
group by split(period,'[.]')[0]
,series_title_2
sort by total_transaction_val desc
limit 10