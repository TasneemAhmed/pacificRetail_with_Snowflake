USE PACIFICRETAIL_DB.GOLD;
/*
- This view to track the customer purchase behaviour
*/
create or replace view VW_CUSTOMER_PRODUCT_AFFINITY 
AS
    select
        date_trunc('month', o.TRANSACTION_DATE) as puchase_month,
        c.customer_id,
        c.customer_type,
        p.product_id,
        p.name as product_name,
        p.category as product_category,
        COUNT(DISTINCT o.transaction_id) AS purchase_count,
        SUM(o.quantity) AS total_quantity,
        SUM(o.total_amount) AS total_spent,
        AVG(o.total_amount) AS avg_purchase_amount,
        DATEDIFF('DAY', MAX(o.TRANSACTION_DATE), MIN(o.TRANSACTION_DATE)) AS days_between_first_last_purchase
        
    from PACIFICRETAIL_DB.SILVER.ORDERS o
    join PACIFICRETAIL_DB.SILVER.CUSTOMER c on o.customer_id = c.customer_id
    join PACIFICRETAIL_DB.SILVER.PRODUCT p on o.product_id = p.product_id

    GROUP BY
        puchase_month,
        c.customer_id,
        c.customer_type,
        p.product_id,
        product_name,
        product_category
    order by puchase_month;

select * from VW_CUSTOMER_PRODUCT_AFFINITY;
