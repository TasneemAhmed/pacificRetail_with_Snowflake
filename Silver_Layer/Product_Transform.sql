use PACIFICRETAIL_DB.SILVER;
/*
process_product_changes
📖 Purpose
The process_product_changes stored procedure is designed to merge product changes from the bronze layer into the silver layer in a retail data pipeline. It ensures:

Data standardization and validation before insertion or updates.
Tracking the number of inserted and updated rows after execution.
🔍 Procedure Logic
1️⃣ Data Source & Target Tables
    Source: PACIFICRETAIL_DB.BRONZE.PRODUCT_CHANGES_STREAM
    Target: PACIFICRETAIL_DB.SILVER.PRODUCT
2️⃣ Data Processing Steps
1. Merge Data (MERGE INTO)

    - Matches PRODUCT_ID between the source and target tables.
    - Updates existing records if a match is found.
    - Inserts new records if no match is found.
2. Data Standardization & Validation:
    - price → If price is ≤ 0, set it to 0.
    - stock_quantity → If STOCK_QUANTITY is negative, set it to 0.
    - rating → Keep only values between 1 and 5, else set to NULL.
    - last_updated_timestamp → Set to CURRENT_TIMESTAMP().
3. Row Count Tracking (QUERY_HISTORY)
    - Fetches updated & inserted row counts using INFORMATION_SCHEMA.QUERY_HISTORY().
    - Stores results in variables rows_updated and rows_inserted.
4. Return Summary Message
    Returns a string summarizing the number of rows inserted and updated.
*/
create or replace procedure process_product_changes ()
returns string
language SQL
AS
$$
DECLARE 
    rows_updated INT;
    rows_inserted INT;
BEGIN
    merge into PACIFICRETAIL_DB.SILVER.PRODUCT as target
    using (
        select
            product_id,
            name as name,
            category,
            brand,

            case 
                when price >0 then price 
                else 0
            end as price,

            case 
                when STOCK_QUANTITY >= 0 then STOCK_QUANTITY
                else 0
            end as stock_quantity,

            case 
                when rating between 1 and 5 then rating
                else NULL
            end as rating,

            is_active,
            CURRENT_TIMESTAMP() as last_updated_timestamp
            
        from 
            PACIFICRETAIL_DB.BRONZE.PRODUCT_CHANGES_STREAM
        
    ) as source
    on source.PRODUCT_ID = target.PRODUCT_ID
    when matched then
        update set
          NAME = source.name,
          CATEGORY = source.category, 
          BRAND = source.brand, 
          PRICE = source.price, 
          STOCK_QUANTITY = source.stock_quantity, 
          RATING = source.rating, 
          IS_ACTIVE = source.is_active, 
          LAST_UPDATED_TIMESTAMP = source.last_updated_timestamp

    when not matched then
        insert (PRODUCT_ID, NAME, CATEGORY, BRAND, PRICE, STOCK_QUANTITY, RATING, IS_ACTIVE, LAST_UPDATED_TIMESTAMP)
        values(source.product_id, source.name, source.category, source.brand, source.price, source.stock_quantity, source.rating, source.is_active, source.last_updated_timestamp);

    -- Use RESULT_SCAN() to track affected rows
    SELECT rows_updated, rows_inserted
    INTO rows_updated, rows_inserted
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Return message
    RETURN 'Updated ' || rows_updated || ' rows; Inserted ' || rows_inserted || ' rows.';
END;
$$;

create or replace task product_silver_merge_task
    warehouse = 'compute_wh'
    schedule = 'USING CRON 15 */4 * * * America/New_York'
AS
    call process_product_changes();

--start task
alter task product_silver_merge_task resume;
