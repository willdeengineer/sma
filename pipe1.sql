CREATE OR REPLACE PROCEDURE find_duplicates()
RETURNS TABLE (id VARCHAR, count_value NUMBER)
LANGUAGE SQL
AS $$
    SELECT id, COUNT(*)
    FROM test_table
    GROUP BY id
    HAVING COUNT(*) > 1;
$$;


