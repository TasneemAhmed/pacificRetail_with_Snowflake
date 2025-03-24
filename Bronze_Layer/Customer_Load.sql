use PACIFICRETAIL_DB.BRONZE;

create or replace file format my_csv_format
type = 'csv'
skip_header = 1
field_delimiter = ','
record_delimiter = '\n'
compression = 'auto' -- detect the files which compressed by gzip and extracted or loaded
null_if = ('NULL', 'null', '') -- When loading data, Snowflake replaces these values in the data load source with SQL NULL.
empty_field_as_null = True -- When loading data, specifies whether to insert SQL NULL for empty fields in an input file, which are represented by two successive delimiters (e.g. ,,)

-- verify if the csv files readed
select 
    $1, $2, $3, $4, $5, $6, $7,$8, $9,  metadata$filename, metadata$file_row_number
from @my_s3_landing_stage/Customer/

CREATE TABLE IF NOT EXISTS raw_customer (
    customer_id INT,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
	age INT,
    gender STRING,
    total_purchases INT,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- create task run automatically every data at 2 AM to select from stage and load into raw_customer table
CREATE or replace TASK load_customer_data_task
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
    AS
      copy into raw_customer (
        customer_id,
        name,
        email,
        country,
        customer_type,
        registration_date,
    	age,
        gender,
        total_purchases,
        source_file_name,
        source_file_row_number
      )
      from (
      select 
        $1, 
        $2, 
        $3, 
        $4, 
        $5, 
        $6, 
        $7,
        $8, 
        $9,  
        metadata$filename, 
        metadata$file_row_number
from @my_s3_landing_stage/Customer/)
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
on_error = 'CONTINUE'
PATTERN='.*[.]csv' -- to load from any csv file

--start task
alter task load_customer_data_task resume;

--Verify if task executed and data loaded
select * from raw_customer;
