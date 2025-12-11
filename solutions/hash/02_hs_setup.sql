CREATE OR REPLACE PROCEDURE setup_hash_tables()
RETURNS STRING
LANGUAGE SQL
AS $$
    DECLARE
        table_nr STRING;
        cursor CURSOR FOR SELECT table_name FROM config;
    BEGIN
        FOR record IN cursor DO
            table_nr := record.table_name;
            
            ALTER TABLE IDENTIFIER(:table_nr)
                ADD COLUMN IF NOT EXISTS row_hash STRING;

            UPDATE IDENTIFIER(:table_nr)
                SET row_hash = MD5(* EXCLUDE row_hash);
        END FOR;
        
        RETURN 'ROW_HASH kolom toegevoegd aan elke tabel en gevuld met MD5 serialization.';
    END;
$$;

CALL setup_hash_tables();