-- -------------------------------------------------
-- CDC regressietests (SQL variant)
--
-- Dit script valideert het CDC-proces met 4 scenario's:
-- 1) Inserts
-- 2) Updates met HISTORY strategie
-- 3) Soft deletes
-- 4) Data quality errors (duplicate insert/update + missende PK)
-- 5) Failed run bij ongeldige configuratie
-- 6) Updates met OVERWRITE strategie
-- 7) Deletes met HARD strategie
--
-- Verwacht:
-- - De procedures uit solutions/sql moeten al zijn aangemaakt.
-- - De database CDC_SQL_DB moet bestaan en gevuld zijn met de CDC objecten.
-- -------------------------------------------------

USE DATABASE CDC_PYTHON_DB;
USE SCHEMA CDC;

CREATE OR REPLACE TEMP TABLE TEST_RESULTS (
    TEST_NAME STRING,
    STATUS STRING,
    DETAILS STRING
);

-- -------------------------------------------------
-- Scenario 1: Inserts
-- -------------------------------------------------
TRUNCATE TABLE STAGING.S_EMPLOYEE;
TRUNCATE TABLE TARGET.T_EMPLOYEE;
TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;
DELETE FROM CDC.CDC_CONFIG;

INSERT INTO CDC.CDC_CONFIG (
    ENTITY_NAME,
    SOURCE_TABLE,
    TARGET_TABLE,
    PRIMARY_KEY_COLUMN,
    DELETE_STRATEGY,
    ERROR_STRATEGY,
    UPDATE_STRATEGY,
    IS_ACTIVE
)
VALUES (
    'Employee',
    'STAGING.S_EMPLOYEE',
    'TARGET.T_EMPLOYEE',
    'EMPLOYEE_ID',
    'SOFT',
    'CONTINUE',
    'HISTORY',
    TRUE
);

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E001', 5000, 'MGR_A', 'IT'),
    ('E002', 5200, 'MGR_A', 'IT'),
    ('E003', 4800, 'MGR_B', 'HR');

CALL CDC.CDC_RUN();

INSERT INTO TEST_RESULTS
SELECT
    'T01 - inserts: 3 actieve rijen in target',
    IFF(COUNT(*) = 3, 'PASS', 'FAIL'),
    'active_target_rows=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE IS_ACTIVE = TRUE;

INSERT INTO TEST_RESULTS
SELECT
    'T02 - inserts: run status is completed',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'completed_runs=' || COUNT(*)
FROM LOGGING.RUN_LOG
WHERE STATUS = 'COMPLETED';

INSERT INTO TEST_RESULTS
SELECT
    'T03 - inserts: rows_inserted in run log = 3',
    IFF(MAX(ROWS_INSERTED) = 3, 'PASS', 'FAIL'),
    'rows_inserted=' || COALESCE(MAX(ROWS_INSERTED), 0)
FROM LOGGING.RUN_LOG;

-- -------------------------------------------------
-- Scenario 2: Updates met HISTORY
-- -------------------------------------------------
TRUNCATE TABLE STAGING.S_EMPLOYEE;

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E001', 6000, 'MGR_A', 'IT'), -- gewijzigd
    ('E002', 5200, 'MGR_A', 'IT'), -- ongewijzigd
    ('E003', 4800, 'MGR_B', 'HR'); -- ongewijzigd

CALL CDC.CDC_RUN();

INSERT INTO TEST_RESULTS
SELECT
    'T04 - history update: 1 actieve record voor E001 met cdc_operation U en geupdate waarde',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'active_u_rows_for_E001=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE EMPLOYEE_ID = 'E001'
  AND IS_ACTIVE = TRUE
  AND SALARY = 6000
  AND CDC_OPERATION = 'U';

INSERT INTO TEST_RESULTS
SELECT
    'T05 - history update: totaal rows voor E001 is 2 (oude versie + nieuwe versie)',
    IFF(COUNT(*) = 2, 'PASS', 'FAIL'),
    'history_rows_for_E001=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE EMPLOYEE_ID = 'E001';

INSERT INTO TEST_RESULTS
SELECT
    'T06 - history update: rows_updated in laatste run = 1',
    IFF(ROWS_UPDATED = 1, 'PASS', 'FAIL'),
    'rows_updated=' || ROWS_UPDATED
FROM (
    SELECT ROWS_UPDATED
    FROM LOGGING.RUN_LOG
    QUALIFY ROW_NUMBER() OVER (ORDER BY RUN_ID DESC) = 1
);

-- -------------------------------------------------
-- Scenario 3: Soft delete
-- -------------------------------------------------
TRUNCATE TABLE STAGING.S_EMPLOYEE;

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E001', 6000, 'MGR_A', 'IT'),
    ('E003', 4800, 'MGR_B', 'HR');

CALL CDC.CDC_RUN();

INSERT INTO TEST_RESULTS
SELECT
    'T07 - soft delete: E002 als D en inactief en einddatum gezet',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'deleted_rows_for_E002=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE EMPLOYEE_ID = 'E002'
  AND IS_ACTIVE = FALSE
  AND END_TS IS NOT NULL
  AND CDC_OPERATION = 'D';

INSERT INTO TEST_RESULTS
SELECT
    'T08 - soft delete: rows_deleted in laatste run = 1',
    IFF(ROWS_DELETED = 1, 'PASS', 'FAIL'),
    'rows_deleted=' || ROWS_DELETED
FROM (
    SELECT ROWS_DELETED
    FROM LOGGING.RUN_LOG
    QUALIFY ROW_NUMBER() OVER (ORDER BY RUN_ID DESC) = 1
);

-- -------------------------------------------------
-- Scenario 4: Data quality errors
-- -------------------------------------------------
TRUNCATE TABLE STAGING.S_EMPLOYEE;
TRUNCATE TABLE TARGET.T_EMPLOYEE;
TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;
DELETE FROM CDC.CDC_CONFIG;

INSERT INTO CDC.CDC_CONFIG (
    ENTITY_NAME,
    SOURCE_TABLE,
    TARGET_TABLE,
    PRIMARY_KEY_COLUMN,
    DELETE_STRATEGY,
    ERROR_STRATEGY,
    UPDATE_STRATEGY,
    IS_ACTIVE
)
VALUES (
    'Employee',
    'STAGING.S_EMPLOYEE',
    'TARGET.T_EMPLOYEE',
    'EMPLOYEE_ID',
    'SOFT',
    'CONTINUE',
    'HISTORY',
    TRUE
);

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E100', 5000, 'MGR_A', 'IT'),
    ('E100', 5000, 'MGR_A', 'IT'), -- duplicate insert
    ('E200', 4000, 'MGR_B', 'HR'),
    ('E200', 4500, 'MGR_B', 'HR'), -- duplicate update
    (NULL, 3500, 'MGR_C', 'OPS'), -- missing PK
    ('', 3600, 'MGR_C', 'OPS'),   -- missing PK
    ('E300', 3700, 'MGR_D', 'FIN');

CALL CDC.CDC_RUN();

INSERT INTO TEST_RESULTS
SELECT
    'T09 - quality: duplicate_insert errors >= 1',
    IFF(COUNT(*) >= 1, 'PASS', 'FAIL'),
    'duplicate_insert_errors=' || COUNT(*)
FROM LOGGING.RUN_ERROR_LOG
WHERE ERROR_CODE = 'DUPLICATE_INSERT';

INSERT INTO TEST_RESULTS
SELECT
    'T10 - quality: duplicate_update errors >= 1',
    IFF(COUNT(*) >= 1, 'PASS', 'FAIL'),
    'duplicate_update_errors=' || COUNT(*)
FROM LOGGING.RUN_ERROR_LOG
WHERE ERROR_CODE = 'DUPLICATE_UPDATE';

INSERT INTO TEST_RESULTS
SELECT
    'T11 - quality: primary_key_error = 2',
    IFF(COUNT(*) = 2, 'PASS', 'FAIL'),
    'primary_key_errors=' || COUNT(*)
FROM LOGGING.RUN_ERROR_LOG
WHERE ERROR_CODE = 'PRIMARY_KEY_ERROR';

INSERT INTO TEST_RESULTS
SELECT
    'T12 - quality: alleen geldige unieke key inserted (E300)',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'active_target_rows=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE IS_ACTIVE = TRUE;

-- -------------------------------------------------
-- Scenario 5: Failed run
-- -------------------------------------------------
TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;

INSERT INTO LOGGING.RUN_LOG (RUN_ID, START_TS, STATUS)
VALUES (999001, CURRENT_TIMESTAMP(), 'RUNNING');

CALL CDC.CDC_PROCESS(-1, 999001);

INSERT INTO TEST_RESULTS
SELECT
    'T13 - failed run: status is FAILED',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'failed_runs=' || COUNT(*)
FROM LOGGING.RUN_LOG
WHERE RUN_ID = 999001
  AND STATUS = 'FAILED';

INSERT INTO TEST_RESULTS
SELECT
    'T14 - failed run: geen entity log rows',
    IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
    'entity_log_rows=' || COUNT(*)
FROM LOGGING.RUN_ENTITY_LOG
WHERE RUN_ID = 999001;

-- -------------------------------------------------
-- Scenario 6: Overwrite update
-- -------------------------------------------------
TRUNCATE TABLE STAGING.S_EMPLOYEE;
TRUNCATE TABLE TARGET.T_EMPLOYEE;
TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;
DELETE FROM CDC.CDC_CONFIG;

INSERT INTO CDC.CDC_CONFIG (
    ENTITY_NAME,
    SOURCE_TABLE,
    TARGET_TABLE,
    PRIMARY_KEY_COLUMN,
    DELETE_STRATEGY,
    ERROR_STRATEGY,
    UPDATE_STRATEGY,
    IS_ACTIVE
)
VALUES (
    'Employee',
    'STAGING.S_EMPLOYEE',
    'TARGET.T_EMPLOYEE',
    'EMPLOYEE_ID',
    'SOFT',
    'CONTINUE',
    'OVERWRITE',
    TRUE
);

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E500', 5000, 'MGR_X', 'IT');

CALL CDC.CDC_RUN();

TRUNCATE TABLE STAGING.S_EMPLOYEE;

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E500', 6500, 'MGR_Y', 'FIN');

CALL CDC.CDC_RUN();

INSERT INTO TEST_RESULTS
SELECT
    'T15 - overwrite: nog maar 1 row voor E500',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'rows_for_E500=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE EMPLOYEE_ID = 'E500';

INSERT INTO TEST_RESULTS
SELECT
    'T16 - overwrite: actieve row bevat nieuwe waarden',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'matching_rows=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE EMPLOYEE_ID = 'E500'
  AND IS_ACTIVE = TRUE
  AND SALARY = 6500
  AND MANAGER = 'MGR_Y'
  AND DEPARTMENT = 'FIN'
  AND CDC_OPERATION = 'U';

INSERT INTO TEST_RESULTS
SELECT
    'T17 - overwrite: rows_updated in laatste run = 1 (oude waarde overschreven)',
    IFF(ROWS_UPDATED = 1, 'PASS', 'FAIL'),
    'rows_updated=' || ROWS_UPDATED
FROM (
    SELECT ROWS_UPDATED
    FROM LOGGING.RUN_LOG
    QUALIFY ROW_NUMBER() OVER (ORDER BY RUN_ID DESC) = 1
);

-- -------------------------------------------------
-- Scenario 7: Hard delete
-- -------------------------------------------------
TRUNCATE TABLE STAGING.S_EMPLOYEE;
TRUNCATE TABLE TARGET.T_EMPLOYEE;
TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;
DELETE FROM CDC.CDC_CONFIG;

INSERT INTO CDC.CDC_CONFIG (
    ENTITY_NAME,
    SOURCE_TABLE,
    TARGET_TABLE,
    PRIMARY_KEY_COLUMN,
    DELETE_STRATEGY,
    ERROR_STRATEGY,
    UPDATE_STRATEGY,
    IS_ACTIVE
)
VALUES (
    'Employee',
    'STAGING.S_EMPLOYEE',
    'TARGET.T_EMPLOYEE',
    'EMPLOYEE_ID',
    'HARD',
    'CONTINUE',
    'HISTORY',
    TRUE
);

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E601', 4200, 'MGR_H', 'OPS'),
    ('E602', 4300, 'MGR_H', 'OPS');

CALL CDC.CDC_RUN();

TRUNCATE TABLE STAGING.S_EMPLOYEE;

INSERT INTO STAGING.S_EMPLOYEE (EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
VALUES
    ('E601', 4200, 'MGR_H', 'OPS');

CALL CDC.CDC_RUN();

INSERT INTO TEST_RESULTS
SELECT
    'T18 - hard delete: E602 bestaat niet meer in target',
    IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
    'rows_for_E602=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE EMPLOYEE_ID = 'E602';

INSERT INTO TEST_RESULTS
SELECT
    'T19 - hard delete: rows_deleted in laatste run = 1',
    IFF(ROWS_DELETED = 1, 'PASS', 'FAIL'),
    'rows_deleted=' || ROWS_DELETED
FROM (
    SELECT ROWS_DELETED
    FROM LOGGING.RUN_LOG
    QUALIFY ROW_NUMBER() OVER (ORDER BY RUN_ID DESC) = 1
);

INSERT INTO TEST_RESULTS
SELECT
    'T20 - hard delete: 1 actieve row blijft over (E602 is hard deleted)',
    IFF(COUNT(*) = 1, 'PASS', 'FAIL'),
    'active_target_rows=' || COUNT(*)
FROM TARGET.T_EMPLOYEE
WHERE IS_ACTIVE = TRUE;

-- Resultaten
SELECT *
FROM TEST_RESULTS
ORDER BY TEST_NAME;
