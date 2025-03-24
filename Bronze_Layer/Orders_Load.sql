use PACIFICRETAIL_DB.BRONZE; 

CREATE OR REPLACE FILE FORMAT my_parquet_format
TYPE = 'parquet';

-- verify file format
select
    $1, $1:payment_method
from  @my_s3_landing_stage/Order/transaction.snappy.parquet
(FILE_FORMAT => 'my_parquet_format');

-- Create table
CREATE OR REPLACE TABLE raw_order (
  customer_id INT,
  payment_method STRING,
  product_id INT,
  quantity INT,
  store_type STRING,
  total_amount DOUBLE,
  transaction_date DATE,
  transaction_id STRING,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- create task run automatically every data at 5 AM to select from stage and load into raw_order table
CREATE or replace TASK load_orders_data_task
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 5 * * * America/New_York'
    AS
      copy into raw_order (
        CUSTOMER_ID, 
        PAYMENT_METHOD, 
        PRODUCT_ID, 
        QUANTITY, 
        STORE_TYPE, 
        TOTAL_AMOUNT, 
        TRANSACTION_DATE, 
        TRANSACTION_ID, 
        SOURCE_FILE_NAME, 
        SOURCE_FILE_ROW_NUMBER
      )
      from (
     select
    $1:customer_id::int,
    $1:payment_method::string,
    $1:product_id::int,
    $1:quantity::int,
    $1:store_type::string,
    $1:total_amount::double,
    $1:transaction_date::date,
    $1:transaction_id::string,
    metadata$filename::string, 
    metadata$file_row_number::int
from  @my_s3_landing_stage/Order/transaction.snappy.parquet)
file_format = (format_name = 'my_parquet_format')
on_error = 'CONTINUE'
PATTERN='.*[.]parquet' -- to load from any parquet file

--start task
alter task load_orders_data_task resume;

-- Verify the task executed and the loaded into raw_order
select * from raw_order;

