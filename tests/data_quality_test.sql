-- Differences between STAGING.S_Employee and TARGET.T_Employee (business columns only)
SELECT 
    'STAGING' AS source,
    COUNT(*) AS row_count
FROM STAGING.S_EMPLOYEE
UNION ALL
SELECT 
    'TARGET' AS source,
    COUNT(*) AS row_count
FROM TARGET.T_EMPLOYEE
WHERE IS_ACTIVE = TRUE
UNION ALL
SELECT 
    'DIFFERENCES' AS source,
    COUNT(*) AS row_count
FROM (
    SELECT 
        EMPLOYEE_ID,
        SALARY,
        MANAGER,
        DEPARTMENT
    FROM STAGING.S_EMPLOYEE
    MINUS
    SELECT 
        EMPLOYEE_ID,
        SALARY,
        MANAGER,
        DEPARTMENT
    FROM TARGET.T_EMPLOYEE
    WHERE IS_ACTIVE = TRUE
)