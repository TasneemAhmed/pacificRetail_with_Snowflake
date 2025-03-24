use PACIFICRETAIL_DB.SILVER;

create or replace procedure process_orders_changes ()
returns string
language SQL
AS
$$
DECLARE 
    rows_updated INT;
    rows_inserted INT;
BEGIN
    merge into PACIFICRETAIL_DB.SILVER.ORDERS as target
    using (
        select
          transaction_id,
          customer_id,
          product_id,
          quantity,
          store_type,
          total_amount,
          transaction_date,
          payment_method,
         
          CURRENT_TIMESTAMP() AS last_updated_timestamp
            
        from 
            PACIFICRETAIL_DB.BRONZE.ORDERS_CHANGES_STREAM
        
    ) as source
    on source.transaction_id = target.transaction_id
    when matched then
        update set
          customer_id = source.customer_id,
          product_id = source.product_id,
          quantity = source.quantity,
          store_type = source.store_type,
          total_amount = source.total_amount,
          transaction_date = source.transaction_date,
          payment_method = source.payment_method,
          
          
          last_updated_timestamp = source.last_updated_timestamp

    when not matched then
        INSERT (transaction_id, customer_id, product_id, quantity, store_type, total_amount, transaction_date, payment_method, last_updated_timestamp)
    VALUES (source.transaction_id, source.customer_id, source.product_id, source.quantity, source.store_type, source.total_amount, source.transaction_date, source.payment_method, source.last_updated_timestamp);


    -- Use RESULT_SCAN() to track affected rows
    SELECT rows_updated, rows_inserted
    INTO rows_updated, rows_inserted
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Return message
    RETURN 'Updated ' || rows_updated || ' rows; Inserted ' || rows_inserted || ' rows.';
END;
$$;

-- create task to run monthly
create or replace task orders_silver_merge_task
    warehouse = 'compute_wh'
    schedule = 'USING CRON 30 */2 * * * America/New_York'
AS
    call process_orders_changes();

--start task
alter task orders_silver_merge_task resume;
