MERGE INTO customers AS a
    USING stream_customers AS b
    ON a.customer_id = b.customer_id
    WHEN NOT MATCHED AND b.__change_type = 'INSERT' THEN
        INSERT (customer_id, customer_age, region_density)
        VALUES (b.customer_id, b.customer_age, b.region_density);