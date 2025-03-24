use database PACIFICRETAIL_DB;
create or replace schema GOLD;

-- Verify there is data loaded in Bronze layer tables
select * from PACIFICRETAIL_DB.BRONZE.RAW_CUSTOMER;
select * from PACIFICRETAIL_DB.BRONZE.RAW_PRODUCT;
select * from PACIFICRETAIL_DB.BRONZE.RAW_ORDER;

use PACIFICRETAIL_DB.BRONZE;
show tasks;
-- to run tasks immediately not wait the schedule of task
execute task LOAD_CUSTOMER_DATA_TASK;
execute task LOAD_PRODUCT_DATA_TASK;
execute task LOAD_ORDERS_DATA_TASK;


-- Verify there is data loaded in Bronze layer tables
select * from PACIFICRETAIL_DB.SILVER.CUSTOMER;

use PACIFICRETAIL_DB.BRONZE;
show streams;

select * from CUSTOMER_CHANGES_STREAM;
select * from PRODUCT_CHANGES_STREAM;
select * from ORDERS_CHANGES_STREAM;

-- Verify silver layer tables
use PACIFICRETAIL_DB.SILVER;
select * from PACIFICRETAIL_DB.SILVER.CUSTOMER;
select * from PACIFICRETAIL_DB.SILVER.PRODUCT;
select * from PACIFICRETAIL_DB.SILVER.orders;

show tasks;
execute task SILVER_CUSTOMER_MERGE_TASK;
execute task ORDERS_SILVER_MERGE_TASK;
execute task PRODUCT_SILVER_MERGE_TASK;

-- the difference will be 50 rows beacause email is null, and in silver layer table need rows which email is not null
select CUSTOMER_ID from PACIFICRETAIL_DB.BRONZE.RAW_CUSTOMER
except 
select CUSTOMER_ID from PACIFICRETAIL_DB.SILVER.CUSTOMER

select count(*) from PACIFICRETAIL_DB.BRONZE.RAW_CUSTOMER
where email is not null;

