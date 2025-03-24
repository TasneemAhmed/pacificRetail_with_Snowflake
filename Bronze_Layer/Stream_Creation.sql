/*
 A stream records data manipulation language (DML) changes made to a table.
- append_only = True: Specifies whether this is an append-only stream. Append-only streams track row inserts only. Update and delete operations (including table truncates) are not recorded. 
*/

use PACIFICRETAIL_DB.BRONZE;

-- First stream created wich track the insertion on RAW_CUSTOMER table in Bronze layer
create or replace stream customer_changes_stream on table RAW_CUSTOMER
    append_only = True;


-- Second stream created wich track the insertion on RAW_PRODUCT table in Bronze layer
create or replace stream product_changes_stream on table RAW_PRODUCT
    append_only = True;

-- Third stream created wich track the insertion on RAW_ORDER table in Bronze layer
create or replace stream orders_changes_stream on table RAW_ORDER
    append_only = True;

-- Verify that streams created    
show streams;
