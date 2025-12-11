CREATE OR REPLACE PROCEDURE insert_if_different(
    target_table STRING,
    new_record OBJECT
)
RETURNS STRING
LANGUAGE SQL
AS $$
    DECLARE
        old_hash STRING DEFAULT NULL;
        new_hash STRING DEFAULT NULL;
        id_col STRING DEFAULT NULL;

    BEGIN
        SELECT MD5(:new_record)) INTO new_hash;
        SELECT primary_key_columns INTO id_col FROM config WHERE table_name = :target_table;

        SELECT row_hash INTO old_hash
        FROM IDENTIFIER(:target_table)
        WHERE SALE_DETAIL_ID = :new_record:"SALE_DETAIL_ID"::STRING;

        IF (old_hash IS NULL OR old_hash != new_hash) THEN
            INSERT INTO IDENTIFIER(:target_table) 
            SELECT (:new_record);
            UPDATE IDENTIFIER(:target_table)
            SET row_hash = :new_hash
            WHERE SALE_DETAIL_ID = :new_record:"SALE_DETAIL_ID"::STRING;
        END IF;
    END;
$$;