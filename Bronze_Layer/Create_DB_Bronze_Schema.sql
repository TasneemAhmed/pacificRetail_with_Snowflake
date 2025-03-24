/*
The bronze layer in Medallion architecture includes raw data without any processing on it
- The layer will have 3 tables raw_customer, raw_product, raw_orders
- The data gets from the External stage which connects to AWS S3
*/
create database if not exists pacificretail_db;
create schema if not exists bronze;
