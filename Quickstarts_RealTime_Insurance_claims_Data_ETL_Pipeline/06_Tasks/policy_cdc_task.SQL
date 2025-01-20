MERGE INTO policy AS a
    USING stream_policy AS b
    ON a.policy_id = b.policy_id
    WHEN NOT MATCHED AND b.__change_type = 'INSERT' THEN
        INSERT (policy_id, subscription_length, region_code, segment)
        VALUES (b.policy_id, b.subscription_length, b.region_code, b.segment);