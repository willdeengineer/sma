-- Simpele procedure om aantal duplicates te vinden in staging en deze te mergen naar de public tabel
CREATE OR REPLACE PROCEDURE merge_non_duplicates()
RETURNS NUMBER
LANGUAGE SQL
AS $$
    DECLARE
        merged_count NUMBER;
        insert_sql STRING;
    BEGIN
        MERGE INTO PUBLIC.EmployeeSalary AS target
        USING (
            SELECT *
            FROM STAGING.EmployeeSalary s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1
        ) AS source
        ON target.id = source.id
        WHEN NOT MATCHED THEN
            INSERT (id, salary, name, country)
            VALUES (source.id, source.salary, source.name, source.country);
        
        merged_count := SQLROWCOUNT;
        RETURN merged_count;
    END;
$$;
