USE DATABASE DUP_DB;
USE SCHEMA TEST;

--
-- Reset
--
CREATE OR REPLACE SEQUENCE RUN_SEQ START WITH 1 INCREMENT BY 1 ORDER;
TRUNCATE TABLE STAGE_ENTITY;
TRUNCATE TABLE TARGET_ENTITY;
TRUNCATE TABLE ERROR_LOG;
TRUNCATE TABLE RUN_LOG;


-------------------------------------------------
-- Dataset 1 eerste run (met inserts en duplicaten)
-------------------------------------------------

SET run_id = RUN_SEQ.NEXTVAL;
SELECT $run_id;

INSERT INTO STAGE_ENTITY (RUN_ID, ENTITY_ID, NAME, SALARY, ROW_HASH)
SELECT $run_id, 'E00000001', 'John Doe', 5000, SHA2(UPPER(CONCAT_WS('|', 'E00000001', 'John Doe', '5000')), 256) UNION ALL
SELECT $run_id, 'E00000001', 'John Doe', 5000, SHA2(UPPER(CONCAT_WS('|', 'E00000001', 'John Doe', '5000')), 256) UNION ALL
SELECT $run_id, 'E00000001', 'John Doe', 5000, SHA2(UPPER(CONCAT_WS('|', 'E00000001', 'John Doe', '5000')), 256) UNION ALL
SELECT $run_id, 'E00000002', 'Jane Smith', 6000, SHA2(UPPER(CONCAT_WS('|', 'E00000002', 'Jane Smith', '6000')), 256) UNION ALL
SELECT $run_id, 'E00000003', 'Bob Johnson', 4500, SHA2(UPPER(CONCAT_WS('|', 'E00000003', 'Bob Johnson', '4500')), 256) UNION ALL
SELECT $run_id, 'E00000004', 'Alice Brown', 7000, SHA2(UPPER(CONCAT_WS('|', 'E00000004', 'Alice Brown', '7000')), 256) UNION ALL
SELECT $run_id, 'E00000005', 'Charlie Davis', 5500, SHA2(UPPER(CONCAT_WS('|', 'E00000005', 'Charlie Davis', '5500')), 256) UNION ALL
SELECT $run_id, 'E00000006', 'Diana Wilson', 6200, SHA2(UPPER(CONCAT_WS('|', 'E00000006', 'Diana Wilson', '6200')), 256) UNION ALL
SELECT $run_id, 'E00000007', 'Frank Miller', 4800, SHA2(UPPER(CONCAT_WS('|', 'E00000007', 'Frank Miller', '4800')), 256) UNION ALL
SELECT $run_id, 'E00000008', 'Grace Lee', 5300, SHA2(UPPER(CONCAT_WS('|', 'E00000008', 'Grace Lee', '5300')), 256) UNION ALL
SELECT $run_id, 'E00000009', 'Henry Taylor', 6800, SHA2(UPPER(CONCAT_WS('|', 'E00000009', 'Henry Taylor', '6800')), 256) UNION ALL
SELECT $run_id, 'E00000010', 'Ivy Martinez', 5100, SHA2(UPPER(CONCAT_WS('|', 'E00000010', 'Ivy Martinez', '5100')), 256) UNION ALL
SELECT $run_id, 'E00000011', 'Jack Anderson', 4700, SHA2(UPPER(CONCAT_WS('|', 'E00000011', 'Jack Anderson', '4700')), 256) UNION ALL
SELECT $run_id, 'E00000012', 'Kate Thomas', 6500, SHA2(UPPER(CONCAT_WS('|', 'E00000012', 'Kate Thomas', '6500')), 256) UNION ALL
SELECT $run_id, 'E00000013', 'Leo White', 5900, SHA2(UPPER(CONCAT_WS('|', 'E00000013', 'Leo White', '5900')), 256) UNION ALL
SELECT $run_id, 'E00000014', 'Mia Harris', 4900, SHA2(UPPER(CONCAT_WS('|', 'E00000014', 'Mia Harris', '4900')), 256) UNION ALL
SELECT $run_id, 'E00000014', 'Mia Harris', 5100, SHA2(UPPER(CONCAT_WS('|', 'E00000014', 'Mia Harris', '5100')), 256) UNION ALL
SELECT $run_id, 'E00000014', 'Mia Harris', 5100, SHA2(UPPER(CONCAT_WS('|', 'E00000014', 'Mia Harris', '5300')), 256) UNION ALL
SELECT $run_id, 'E00000015', 'Noah Clark', 7200, SHA2(UPPER(CONCAT_WS('|', 'E00000015', 'Noah Clark', '7200')), 256);

SELECT * FROM STAGE_ENTITY ORDER BY ENTITY_ID, SALARY;

-------------------------------------------------
-- DATASET 2 tweede run (met inserts, updates, deletes))
-------------------------------------------------
TRUNCATE TABLE STAGE_ENTITY;
SET run_id = RUN_SEQ.NEXTVAL;

INSERT INTO STAGE_ENTITY (RUN_ID, ENTITY_ID, NAME, SALARY, ROW_HASH)
    -- Ongewijzigde rijen
SELECT $run_id, 'E00000001', 'John Doe', 5000, SHA2(UPPER(CONCAT_WS('|', 'E00000001', 'John Doe', '5000')), 256) UNION ALL
SELECT $run_id, 'E00000002', 'Jane Smith', 6000, SHA2(UPPER(CONCAT_WS('|', 'E00000002', 'Jane Smith', '6000')), 256) UNION ALL
SELECT $run_id, 'E00000002', 'Jane Smith', 6000, SHA2(UPPER(CONCAT_WS('|', 'E00000002', 'Jane Smith', '6000')), 256) UNION ALL -- Duplicaat
    -- Gewijzigde rijen (salary veranderd)
SELECT $run_id, 'E00000003', 'Bob Johnson', 5500, SHA2(UPPER(CONCAT_WS('|', 'E00000003', 'Bob Johnson', '5500')), 256) UNION ALL
SELECT $run_id, 'E00000004', 'Alice Brown', 7500, SHA2(UPPER(CONCAT_WS('|', 'E00000004', 'Alice Brown', '7500')), 256) UNION ALL
SELECT $run_id, 'E00000006', 'Diana Wilson', 6700, SHA2(UPPER(CONCAT_WS('|', 'E00000006', 'Diana Wilson', '6700')), 256) UNION ALL
SELECT $run_id, 'E00000008', 'Grace Lee', 5800, SHA2(UPPER(CONCAT_WS('|', 'E00000008', 'Grace Lee', '5800')), 256) UNION ALL
    -- E00000005, E00000007 zijn verwijderd (niet meer aanwezig)
    -- Ongewijzigde rijen
SELECT $run_id, 'E00000009', 'Henry Taylor', 6800, SHA2(UPPER(CONCAT_WS('|', 'E00000009', 'Henry Taylor', '6800')), 256) UNION ALL
SELECT $run_id, 'E00000010', 'Ivy Martinez', 5100, SHA2(UPPER(CONCAT_WS('|', 'E00000010', 'Ivy Martinez', '5100')), 256) UNION ALL
SELECT $run_id, 'E00000012', 'Kate Thomas', 6500, SHA2(UPPER(CONCAT_WS('|', 'E00000012', 'Kate Thomas', '6500')), 256) UNION ALL
SELECT $run_id, 'E00000013', 'Leo White', 5900, SHA2(UPPER(CONCAT_WS('|', 'E00000013', 'Leo White', '5900')), 256) UNION ALL
    -- E00000011, E00000014 zijn verwijderd
SELECT $run_id, 'E00000015', 'Noah Clark', 7200, SHA2(UPPER(CONCAT_WS('|', 'E00000015', 'Noah Clark', '7200')), 256) UNION ALL
    -- Nieuwe rijen
SELECT $run_id, 'E00000016', 'Olivia Lewis', 5400, SHA2(UPPER(CONCAT_WS('|', 'E00000016', 'Olivia Lewis', '5400')), 256) UNION ALL
SELECT $run_id, 'E00000017', 'Paul Walker', 6100, SHA2(UPPER(CONCAT_WS('|', 'E00000017', 'Paul Walker', '6100')), 256) UNION ALL
SELECT $run_id, 'E00000017', 'Paul Walker', 6100, SHA2(UPPER(CONCAT_WS('|', 'E00000017', 'Paul Walker', '6100')), 256); -- Duplicaat