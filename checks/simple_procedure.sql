-- Simpele procedure om aantal duplicates te vinden
CREATE OR REPLACE PROCEDURE find_duplicates()
RETURNS NUMBER
LANGUAGE SQL
AS $$
    DECLARE
        result NUMBER;
    BEGIN
        SELECT COUNT(*) INTO result
        FROM (SELECT sale_detail_id FROM sales_detail GROUP BY sale_detail_id HAVING COUNT(*) > 1);
        RETURN result;
    END;
$$;
