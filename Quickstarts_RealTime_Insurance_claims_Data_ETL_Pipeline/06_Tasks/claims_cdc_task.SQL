MERGE INTO claims AS a
    USING stream_claims AS b
    ON a.claim_id = b.claim_id
    WHEN NOT MATCHED AND b.__change_type = 'INSERT' THEN
        INSERT (claim_id, claim_status)
        VALUES (b.claim_id, b.claim_status);