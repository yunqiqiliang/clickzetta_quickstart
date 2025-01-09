--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
USE VCLUSTER SCD_VC;
USE SCHEMA SCD_SCH;

MERGE INTO customer AS c 
USING customer_raw AS cr
   ON c.customer_id = cr.customer_id
WHEN MATCHED AND (c.first_name  <> cr.first_name  OR
                  c.last_name   <> cr.last_name   OR
                  c.email       <> cr.email       OR
                  c.street      <> cr.street      OR
                  c.city        <> cr.city        OR
                  c.state       <> cr.state       OR
                  c.country     <> cr.country) THEN 
    UPDATE SET 
        c.first_name = cr.first_name,
        c.last_name = cr.last_name,
        c.email = cr.email,
        c.street = cr.street,
        c.city = cr.city,
        c.state = cr.state,
        c.country = cr.country,
        c.update_timestamp = current_timestamp()
WHEN NOT MATCHED THEN 
    INSERT (customer_id, first_name, last_name, email, street, city, state, country)
    VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);

select count(*) from customer;


