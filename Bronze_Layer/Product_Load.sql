use PACIFICRETAIL_DB.BRONZE;

CREATE OR REPLACE FILE FORMAT my_json_format 
TYPE = 'json'
STRIP_OUTER_ARRAY = True -- Boolean that instructs the JSON parser to remove outer brackets (i.e. [ ]).
STRIP_NULL_VALUES = TRUE --Boolean that instructs the JSON parser to remove object fields or array elements containing null values.
;
-- verify
select
    $1:product_id,
    $1:name,
    $1:category,
    $1:brand,
    $1:price,
    $1:stock_quantity,
    $1:rating::float,
    $1:is_active::boolean,
    metadata$filename::string, 
    metadata$file_row_number::int
from  @my_s3_landing_stage/Product/products.json
(file_format => 'my_json_format')

-- create raw_product table which have raw data
CREATE TABLE IF NOT EXISTS raw_product (
    product_id INT,
    name STRING,
    category STRING,
	brand STRING,
    price FLOAT,
	stock_quantity INT,
    rating FLOAT,
    is_active BOOLEAN,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- create task run automatically every data at 3 AM to select from stage and load into raw_product table
CREATE or replace TASK load_product_data_task
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 3 * * * America/New_York'
    AS
      copy into raw_product (
        PRODUCT_ID, 
        NAME,  
        CATEGORY, 
        BRAND, 
        PRICE, 
        STOCK_QUANTITY, 
        RATING, 
        IS_ACTIVE, 
        SOURCE_FILE_NAME, 
        SOURCE_FILE_ROW_NUMBER
      )
      from (
     select
    $1:product_id::int,
    $1:name::string,
    $1:category::string,
    $1:brand::string,
    $1:price::float,
    $1:stock_quantity::int,
    $1:rating::float,
    $1:is_active::boolean,
    metadata$filename::string, 
    metadata$file_row_number::int
from  @my_s3_landing_stage/Product/products.json)
file_format = (format_name = 'my_json_format')
on_error = 'CONTINUE'
PATTERN='.*[.]json' -- to load from any json file

--start task
alter task load_product_data_task resume;

