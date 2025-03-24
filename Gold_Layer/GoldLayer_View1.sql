USE PACIFICRETAIL_DB.GOLD;
/*
NULLIF : handle division by zero
 NULLIF(SUM(o.quantity), 0):
 in this example if SUM(o.quantity) = 0 the the output will be NULL
*/
create or replace view VW_DAILY_SALES_ANALYSIS 
AS
    select
        o.TRANSACTION_DATE,
        c.customer_id,
        c.customer_type,
        p.product_id,
        p.name as product_name,
        p.category as product_category,
        SUM(o.quantity) AS total_quantity,
        SUM(o.total_amount) AS total_sales,
        COUNT(DISTINCT o.transaction_id) AS num_transactions,
        SUM(o.total_amount) / NULLIF(SUM(o.quantity), 0) AS avg_price_per_unit,
        SUM(o.total_amount) / NULLIF(COUNT(DISTINCT o.transaction_id), 0) AS avg_transaction_value
        
    from PACIFICRETAIL_DB.SILVER.ORDERS o
    join PACIFICRETAIL_DB.SILVER.CUSTOMER c on o.customer_id = c.customer_id
    join PACIFICRETAIL_DB.SILVER.PRODUCT p on o.product_id = p.product_id

    GROUP BY
        o.TRANSACTION_DATE,
        c.customer_id,
        c.customer_type,
        p.product_id,
        product_name,
        product_category;

select * from VW_DAILY_SALES_ANALYSIS;
