
/*
A storage integration is reuable and securable Snowflake object which can be applied
across stages and is recommended to avoid having expicit snsetive information for each stage definiation

- I have added STORAGE_AWS_EXTERNAL_ID static as every time recreate integration with same ID
as this ID added in trusted_policy and if change snowflake will not able to access the bucket

- Creating Stage 'my_s3_landing_stage' in Bronze schema which have files of format CSV, JSON, Parquet
- Test and select the files
- add your storage_aws_role_arn
*/

create or replace storage integration aws_pacificretail_integration
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = ''
    enabled = true
    STORAGE_AWS_EXTERNAL_ID = 'NO16077_SFCRole=3_g1dLXubuldCYoHuoEAZxO4gRwu0='
    storage_allowed_locations = ( 's3://datalake4snowflake/')
    ;

desc integration aws_pacificretail_integration;

USE pacificretail_db.bronze;

CREATE or replace STAGE my_s3_landing_stage
  STORAGE_INTEGRATION = aws_pacificretail_integration
  URL = 's3://datalake4snowflake/';

ls @my_s3_landing_stage

select 
    metadata$filename, metadata$file_row_number
from @my_s3_landing_stage;
select 
    $1, $2, $3, $5
from @my_s3_landing_stage/Customer/customer.csv

CREATE OR REPLACE FILE FORMAT my_json_format TYPE = 'json';
select
    value
from  @my_s3_landing_stage/Product/products.json
(file_format => 'my_json_format'),
lateral flatten (input =>$1) -- flatten the objects in array to make every object in one row

CREATE OR REPLACE FILE FORMAT my_parquet_format TYPE = 'parquet';
select
    $1, $1:payment_method
from  @my_s3_landing_stage/Order/transaction.snappy.parquet
(FILE_FORMAT => 'my_parquet_format');
