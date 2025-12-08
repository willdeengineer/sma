-- Check for duplicates across all tables in current database
SELECT 
    table_catalog,
    table_schema,
    table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT *) as unique_rows,
    COUNT(*) - COUNT(DISTINCT *) as duplicate_count
FROM information_schema.tables t
WHERE table_type = 'BASE TABLE'
QUALIFY COUNT(*) - COUNT(DISTINCT *) > 0
ORDER BY duplicate_count DESC;