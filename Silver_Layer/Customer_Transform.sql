use PACIFICRETAIL_DB.SILVER;
/*
The process_customer_changes procedure is designed to process and integrate customer data changes from a staging layer (bronze.customer_changes_stream) into a refined layer (silver.customer). It follows data validation, transformation, and standardization rules before updating or inserting records.

📌 Key Operations Performed
Merge Customer Data into the Silver Layer (silver.customer)

Uses a MERGE INTO statement to update existing records and insert new records from bronze.customer_changes_stream.
Ensures that customer_id and email are not NULL before processing.
Data Standardization & Validation

1. customer_type Standardization:
    - Converts variations like 'REG', 'R' → 'Regular'
    - Converts variations like 'PREM', 'P' → 'Premium'
    - Defaults to 'Unknown' if the value is unrecognized.
2. age Validation:
    - Only allows ages between 18 and 120.
    - Invalid ages are set to NULL.
3. gender Standardization:
    - Converts variations of 'Male' ('M', 'MALE') → 'Male'
    - Converts variations of 'Female' ('F', 'FEMALE') → 'Female'
    - All other values default to 'Other'.
4. total_purchases Validation:
    Ensures total_purchases is never negative (sets it to 0 if negative).
5.Timestamps:
    - Assigns current_timestamp() to last_updated_timestamp.
6. Merge Logic (MERGE INTO)

    If customer_id exists (WHEN MATCHED), update the record with cleaned data.
    If customer_id doesn’t exist (WHEN NOT MATCHED), insert a new record.
    
Returns a Summary Message
*/
CREATE OR REPLACE PROCEDURE process_customer_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  rows_inserted INT;
  rows_updated INT;
BEGIN
  -- Merge changes into silver layer
  MERGE INTO silver.customer AS target
  USING (
    SELECT
       customer_id,
       name,
      
      email,
      
      country,
      
      -- Customer type standardization
      CASE
        WHEN TRIM(UPPER(customer_type)) IN ('REGULAR', 'REG', 'R') THEN 'Regular'
        WHEN TRIM(UPPER(customer_type)) IN ('PREMIUM', 'PREM', 'P') THEN 'Premium'
        ELSE 'Unknown'
      END AS customer_type,
      
      registration_date,
      
      -- Age validation
      CASE
        WHEN age BETWEEN 18 AND 120 THEN age
        ELSE NULL
      END AS age,
      
      -- Gender standardization
      CASE
        WHEN TRIM(UPPER(gender)) IN ('M', 'MALE') THEN 'Male'
        WHEN TRIM(UPPER(gender)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'Other'
      END AS gender,
      
      -- Total purchases validation
      CASE
        WHEN total_purchases >= 0 THEN total_purchases
        ELSE 0
      END AS total_purchases,
      
      current_timestamp() AS last_updated_timestamp
    FROM bronze.customer_changes_stream
    WHERE  customer_id IS NOT NULL and email is not null -- Basic data quality rule
    
  ) AS source
  ON target.customer_id = source.customer_id
  WHEN MATCHED THEN
    UPDATE SET
      name = source.name,
      email = source.email,
      country = source.country,
      customer_type = source.customer_type,
      registration_date = source.registration_date,
      age = source.age,
      gender = source.gender,
      total_purchases = source.total_purchases,
      last_updated_timestamp = source.last_updated_timestamp
  WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, country, customer_type, registration_date, age, gender, total_purchases, last_updated_timestamp)
    VALUES (source.customer_id, source.name, source.email, source.country, source.customer_type, source.registration_date, source.age, source.gender, source.total_purchases, source.last_updated_timestamp);


  -- Return summary of operations
  RETURN 'Customers processed';
END;
$$;

-- create task to automatically run stored procedure
create or replace task silver_customer_merge_task
    warehouse = 'compute_wh'
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
AS
    call PROCESS_CUSTOMER_CHANGES();

-- start task    
alter task silver_customer_merge_task resume;
