USE DATABASE CDC_TEST_DB;

USE SCHEMA STAGING;

TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE STAGING.S_EMPLOYEE;
TRUNCATE TABLE STAGING.S_CUSTOMER;
TRUNCATE TABLE STAGING.S_ORDER;
TRUNCATE TABLE TARGET.T_EMPLOYEE;
TRUNCATE TABLE TARGET.T_CUSTOMER;
TRUNCATE TABLE TARGET.T_ORDER;

-----------------------------------------------
-- Run 1
-----------------------------------------------
INSERT INTO S_EMPLOYEE (ROW_HASH, EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
SELECT SHA2('E001' || 5000 || 'A' || 'HR', 256), 'E001', 5000, 'A', 'HR'
UNION ALL SELECT SHA2('E002' || 6000 || 'B' || 'IT', 256), 'E002', 6000, 'B', 'IT'
UNION ALL SELECT SHA2('E003' || 5500 || 'C' || 'SALES', 256), 'E003', 5500, 'C', 'SALES'
UNION ALL SELECT SHA2('E004' || 7000 || 'A' || 'HR', 256), 'E004', 7000, 'A', 'HR'
UNION ALL SELECT SHA2('E005' || 6500 || 'B' || 'IT', 256), 'E005', 6500, 'B', 'IT'
UNION ALL SELECT SHA2('E006' || 7200 || 'C' || 'SALES', 256), 'E006', 7200, 'C', 'SALES'
UNION ALL SELECT SHA2('' || 5000 || 'A' || 'HR', 256), NULL, 5000, 'A', 'HR'
UNION ALL SELECT SHA2('' || 5100 || 'B' || 'IT', 256), NULL, 5100, 'B', 'IT'
-- Duplicaat in staging met zelfde PK en zelfde data/hash
UNION ALL SELECT SHA2('E007' || 4800 || 'A' || 'HR', 256), 'E007', 4800, 'A', 'HR'
UNION ALL SELECT SHA2('E007' || 4800 || 'A' || 'HR', 256), 'E007', 4800, 'A', 'HR'
-- Duplicaat in staging met zelfde PK maar verschillende data/hash
UNION ALL SELECT SHA2('E008' || 5150 || 'B' || 'IT', 256), 'E008', 5150, 'B', 'IT'
UNION ALL SELECT SHA2('E008' || 5200 || 'B' || 'IT', 256), 'E008', 5200, 'B', 'IT';

INSERT INTO S_CUSTOMER (ROW_HASH, CUSTOMER_ID, CUSTOMER_NAME, EMAIL, COUNTRY, REGISTRATION_DATE)
SELECT SHA2('C001' || 'John Doe' || 'john@example.com' || 'US' || '2023-01-15', 256), 'C001', 'John Doe', 'john@example.com', 'US', '2023-01-15'
UNION ALL SELECT SHA2('C002' || 'Jane Smith' || 'jane@example.com' || 'UK' || '2023-02-20', 256), 'C002', 'Jane Smith', 'jane@example.com', 'UK', '2023-02-20'
UNION ALL SELECT SHA2('C003' || 'Bob Wilson' || 'bob@example.com' || 'DE' || '2023-03-10', 256), 'C003', 'Bob Wilson', 'bob@example.com', 'DE', '2023-03-10'
UNION ALL SELECT SHA2('C004' || 'Alice Brown' || 'alice@example.com' || 'FR' || '2023-04-05', 256), 'C004', 'Alice Brown', 'alice@example.com', 'FR', '2023-04-05'
-- Lege PK waarde
UNION ALL SELECT SHA2('' || 'Charlie Davis' || 'charlie@example.com' || 'NL' || '2023-05-12', 256), 'C005', 'Charlie Davis', 'charlie@example.com', 'NL', '2023-05-12'
UNION ALL SELECT SHA2('C006' || 'Diana Evans' || 'diana@example.com' || 'US' || '2023-06-18', 256), 'C006', 'Diana Evans', 'diana@example.com', 'US', '2023-06-18'
UNION ALL SELECT SHA2('C007' || 'Edward Foster' || 'edward@example.com' || 'UK' || '2023-07-22', 256), 'C007', 'Edward Foster', 'edward@example.com', 'UK', '2023-07-22'
UNION ALL SELECT SHA2('C008' || 'Fiona Garcia' || 'fiona@example.com' || 'ES' || '2023-08-14', 256), 'C008', 'Fiona Garcia', 'fiona@example.com', 'ES', '2023-08-14'
-- Duplicaat insert in staging
UNION ALL SELECT SHA2('C009' || 'George Harris' || 'george@example.com' || 'IT' || '2023-09-25', 256), 'C009', 'George Harris', 'george@example.com', 'IT', '2023-09-25'
UNION ALL SELECT SHA2('C009' || 'George Harris' || 'george@example.com' || 'IT' || '2023-09-25', 256), 'C009', 'George Harris', 'george@example.com', 'IT', '2023-09-25'
UNION ALL SELECT SHA2('C010' || 'Helen Johnson' || 'helen@example.com' || 'BE' || '2023-10-30', 256), 'C010', 'Helen Johnson', 'helen@example.com', 'BE', '2023-10-30';

INSERT INTO S_Order (ROW_HASH, ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS)
SELECT SHA2('O001' || 'C001' || '2023-11-01' || 250.00 || 'Delivered', 256), 'O001', 'C001', '2023-11-01', 250.00, 'Delivered'
UNION ALL SELECT SHA2('O002' || 'C002' || '2023-11-02' || 500.50 || 'Pending', 256), 'O002', 'C002', '2023-11-02', 500.50, 'Pending'
-- Duplicaat insert in staging
UNION ALL SELECT SHA2('O003' || 'C003' || '2023-11-03' || 175.25 || 'Delivered', 256), 'O003', 'C003', '2023-11-03', 175.25, 'Delivered'
UNION ALL SELECT SHA2('O003' || 'C003' || '2023-11-03' || 175.25 || 'Delivered', 256), 'O003', 'C003', '2023-11-03', 175.25, 'Delivered'
UNION ALL SELECT SHA2('O004' || 'C004' || '2023-11-04' || 620.75 || 'Shipped', 256), 'O004', 'C004', '2023-11-04', 620.75, 'Shipped'
UNION ALL SELECT SHA2('O005' || 'C005' || '2023-11-05' || 450.00 || 'Delivered', 256), 'O005', 'C005', '2023-11-05', 450.00, 'Delivered'
UNION ALL SELECT SHA2('O006' || 'C006' || '2023-11-06' || 325.50 || 'Pending', 256), 'O006', 'C006', '2023-11-06', 325.50, 'Pending'
UNION ALL SELECT SHA2('O007' || 'C007' || '2023-11-07' || 800.00 || 'Shipped', 256), 'O007', 'C007', '2023-11-07', 800.00, 'Shipped'
UNION ALL SELECT SHA2('O008' || 'C008' || '2023-11-08' || 225.75 || 'Delivered', 256), 'O008', 'C008', '2023-11-08', 225.75, 'Delivered'
UNION ALL SELECT SHA2('O009' || 'C009' || '2023-11-09' || 550.25 || 'Pending', 256), 'O009', 'C009', '2023-11-09', 550.25, 'Pending'
UNION ALL SELECT SHA2('O010' || 'C010' || '2023-11-10' || 675.50 || 'Delivered', 256), 'O010', 'C010', '2023-11-10', 675.50, 'Delivered';

CALL CDC.CDC_RUN();

-----------------------------------------------
-- Run 2 - S_EMPLOYEE
-----------------------------------------------
INSERT INTO S_EMPLOYEE (ROW_HASH, EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
SELECT SHA2('E014' || 5300 || 'D' || 'FIN', 256), 'E014', 5300, 'D', 'FIN'
UNION ALL SELECT SHA2('E015' || 5400 || 'E' || 'OPS', 256), 'E015', 5400, 'E', 'OPS'
UNION ALL SELECT SHA2('E016' || 5500 || 'F' || 'MKT', 256), 'E016', 5500, 'F', 'MKT'
UNION ALL SELECT SHA2('E017' || 5600 || 'A' || 'HR', 256), 'E017', 5600, 'A', 'HR'
UNION ALL SELECT SHA2('E018' || 5700 || 'B' || 'IT', 256), 'E018', 5700, 'B', 'IT'
UNION ALL SELECT SHA2('' || 5800 || 'C' || 'SALES', 256), NULL, 5800, 'C', 'SALES'
UNION ALL SELECT SHA2('' || 5900 || 'D' || 'FIN', 256), NULL, 5900, 'D', 'FIN'
UNION ALL SELECT SHA2('' || 6000 || 'E' || 'OPS', 256), NULL, 6000, 'E', 'OPS'
UNION ALL SELECT SHA2('' || 6100 || 'F' || 'MKT', 256), NULL, 6100, 'F', 'MKT'
UNION ALL SELECT SHA2('E019' || 6200 || 'A' || 'HR', 256), 'E019', 6200, 'A', 'HR'
UNION ALL SELECT SHA2('E020' || 6300 || 'B' || 'IT', 256), 'E020', 6300, 'B', 'IT'
UNION ALL SELECT SHA2('E020' || 6300 || 'B' || 'IT', 256), 'E020', 6300, 'B', 'IT'
UNION ALL SELECT SHA2('E021' || 6400 || 'C' || 'SALES', 256), 'E021', 6400, 'C', 'SALES'
UNION ALL SELECT SHA2('E021' || 6400 || 'C' || 'SALES', 256), 'E021', 6400, 'C', 'SALES'
UNION ALL SELECT SHA2('E022' || 6500 || 'D' || 'FIN', 256), 'E022', 6500, 'D', 'FIN'
UNION ALL SELECT SHA2('E022' || 6500 || 'D' || 'FIN', 256), 'E022', 6500, 'D', 'FIN';

UPDATE S_EMPLOYEE SET SALARY = 5350, ROW_HASH = SHA2('E014' || 5350 || 'D' || 'FIN', 256) WHERE EMPLOYEE_ID = 'E014';
UPDATE S_EMPLOYEE SET SALARY = 5400, ROW_HASH = SHA2('E014' || 5400 || 'D' || 'FIN', 256) WHERE EMPLOYEE_ID = 'E014';
UPDATE S_EMPLOYEE SET MANAGER = 'F', ROW_HASH = SHA2('E015' || 5400 || 'F' || 'OPS', 256) WHERE EMPLOYEE_ID = 'E015';
UPDATE S_EMPLOYEE SET MANAGER = 'G', ROW_HASH = SHA2('E015' || 5400 || 'G' || 'OPS', 256) WHERE EMPLOYEE_ID = 'E015';

DELETE FROM S_EMPLOYEE WHERE EMPLOYEE_ID IN ('E001','E002');

-----------------------------------------------
-- Run 2 - S_CUSTOMER
-----------------------------------------------
INSERT INTO S_CUSTOMER (ROW_HASH, CUSTOMER_ID, CUSTOMER_NAME, EMAIL, COUNTRY, REGISTRATION_DATE)
SELECT SHA2('C011' || 'Isaac Brown' || 'isaac@example.com' || 'SE' || '2023-11-15', 256), 'C011', 'Isaac Brown', 'isaac@example.com', 'SE', '2023-11-15'
UNION ALL SELECT SHA2('C012' || 'Julia Clark' || 'julia@example.com' || 'NO' || '2023-12-01', 256), 'C012', 'Julia Clark', 'julia@example.com', 'NO', '2023-12-01'
UNION ALL SELECT SHA2('C013' || 'Kevin Lee' || 'kevin@example.com' || 'DK' || '2023-12-10', 256), 'C013', 'Kevin Lee', 'kevin@example.com', 'DK', '2023-12-10'
UNION ALL SELECT SHA2('C014' || 'Laura Martin' || 'laura@example.com' || 'PL' || '2024-01-05', 256), 'C014', 'Laura Martin', 'laura@example.com', 'PL', '2024-01-05'
UNION ALL SELECT SHA2('C015' || 'Michael Scott' || 'michael@example.com' || 'CZ' || '2024-01-12', 256), 'C015', 'Michael Scott', 'michael@example.com', 'CZ', '2024-01-12'
UNION ALL SELECT SHA2('' || 'Nancy White' || 'nancy@example.com' || 'HU' || '2024-01-20', 256), NULL, 'Nancy White', 'nancy@example.com', 'HU', '2024-01-20'
UNION ALL SELECT SHA2('' || 'Oliver Green' || 'oliver@example.com' || 'RO' || '2024-02-01', 256), NULL, 'Oliver Green', 'oliver@example.com', 'RO', '2024-02-01'
UNION ALL SELECT SHA2('' || 'Patricia Hall' || 'patricia@example.com' || 'BG' || '2024-02-15', 256), NULL, 'Patricia Hall', 'patricia@example.com', 'BG', '2024-02-15'
UNION ALL SELECT SHA2('' || 'Quentin Young' || 'quentin@example.com' || 'HR' || '2024-03-01', 256), NULL, 'Quentin Young', 'quentin@example.com', 'HR', '2024-03-01'
UNION ALL SELECT SHA2('' || 'Rachel King' || 'rachel@example.com' || 'SI' || '2024-03-10', 256), NULL, 'Rachel King', 'rachel@example.com', 'SI', '2024-03-10'
UNION ALL SELECT SHA2('C016' || 'Samuel Wright' || 'samuel@example.com' || 'SK' || '2024-03-20', 256), 'C016', 'Samuel Wright', 'samuel@example.com', 'SK', '2024-03-20'
UNION ALL SELECT SHA2('C017' || 'Tina Lopez' || 'tina@example.com' || 'LV' || '2024-04-01', 256), 'C017', 'Tina Lopez', 'tina@example.com', 'LV', '2024-04-01'
UNION ALL SELECT SHA2('C017' || 'Tina Lopez' || 'tina@example.com' || 'LV' || '2024-04-01', 256), 'C017', 'Tina Lopez', 'tina@example.com', 'LV', '2024-04-01'
UNION ALL SELECT SHA2('C018' || 'Uma Patel' || 'uma@example.com' || 'LT' || '2024-04-15', 256), 'C018', 'Uma Patel', 'uma@example.com', 'LT', '2024-04-15'
UNION ALL SELECT SHA2('C018' || 'Uma Patel' || 'uma@example.com' || 'LT' || '2024-04-15', 256), 'C018', 'Uma Patel', 'uma@example.com', 'LT', '2024-04-15';

UPDATE S_CUSTOMER SET COUNTRY = 'SE', ROW_HASH = SHA2('C011' || 'Isaac Brown' || 'isaac@example.com' || 'SE' || '2023-11-15', 256) WHERE CUSTOMER_ID = 'C011';
UPDATE S_CUSTOMER SET COUNTRY = 'FI', ROW_HASH = SHA2('C011' || 'Isaac Brown' || 'isaac@example.com' || 'FI' || '2023-11-15', 256) WHERE CUSTOMER_ID = 'C011';
UPDATE S_CUSTOMER SET CUSTOMER_NAME = 'Julia C Clark', ROW_HASH = SHA2('C012' || 'Julia C Clark' || 'julia@example.com' || 'NO' || '2023-12-01', 256) WHERE CUSTOMER_ID = 'C012';

DELETE FROM S_CUSTOMER WHERE CUSTOMER_ID IN ('C001','C002','C003');

-----------------------------------------------
-- Run 2 - S_ORDER
-----------------------------------------------
INSERT INTO S_ORDER (ROW_HASH, ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS)
SELECT SHA2('O011' || 'C011' || '2024-04-20' || 320.00 || 'Delivered', 256), 'O011', 'C011', '2024-04-20', 320.00, 'Delivered'
UNION ALL SELECT SHA2('O012' || 'C012' || '2024-04-25' || 450.75 || 'Pending', 256), 'O012', 'C012', '2024-04-25', 450.75, 'Pending'
UNION ALL SELECT SHA2('O013' || 'C013' || '2024-05-01' || 275.50 || 'Shipped', 256), 'O013', 'C013', '2024-05-01', 275.50, 'Shipped'
UNION ALL SELECT SHA2('O014' || 'C014' || '2024-05-05' || 550.25 || 'Delivered', 256), 'O014', 'C014', '2024-05-05', 550.25, 'Delivered'
UNION ALL SELECT SHA2('O015' || 'C015' || '2024-05-10' || 680.00 || 'Pending', 256), 'O015', 'C015', '2024-05-10', 680.00, 'Pending'
UNION ALL SELECT SHA2('' || 'C016' || '2024-05-15' || 420.50 || 'Shipped', 256), NULL, 'C016', '2024-05-15', 420.50, 'Shipped'
UNION ALL SELECT SHA2('' || 'C017' || '2024-05-20' || 375.00 || 'Delivered', 256), NULL, 'C017', '2024-05-20', 375.00, 'Delivered'
UNION ALL SELECT SHA2('' || 'C018' || '2024-05-25' || 525.75 || 'Pending', 256), NULL, 'C018', '2024-05-25', 525.75, 'Pending'
UNION ALL SELECT SHA2('O016' || 'C011' || '2024-06-01' || 300.25 || 'Shipped', 256), 'O016', 'C011', '2024-06-01', 300.25, 'Shipped'
UNION ALL SELECT SHA2('O017' || 'C012' || '2024-06-05' || 625.50 || 'Delivered', 256), 'O017', 'C012', '2024-06-05', 625.50, 'Delivered'
UNION ALL SELECT SHA2('O018' || 'C013' || '2024-06-10' || 450.00 || 'Pending', 256), 'O018', 'C013', '2024-06-10', 450.00, 'Pending'
UNION ALL SELECT SHA2('O019' || 'C014' || '2024-06-15' || 575.25 || 'Shipped', 256), 'O019', 'C014', '2024-06-15', 575.25, 'Shipped'
UNION ALL SELECT SHA2('O019' || 'C014' || '2024-06-15' || 575.25 || 'Shipped', 256), 'O019', 'C014', '2024-06-15', 575.25, 'Shipped'
UNION ALL SELECT SHA2('O020' || 'C015' || '2024-06-20' || 725.50 || 'Delivered', 256), 'O020', 'C015', '2024-06-20', 725.50, 'Delivered'
UNION ALL SELECT SHA2('O020' || 'C015' || '2024-06-20' || 725.50 || 'Delivered', 256), 'O020', 'C015', '2024-06-20', 725.50, 'Delivered';

UPDATE S_ORDER SET STATUS = 'Shipped', ROW_HASH = SHA2('O011' || 'C011' || '2024-04-20' || 320.00 || 'Shipped', 256) WHERE ORDER_ID = 'O011';
UPDATE S_ORDER SET TOTAL_AMOUNT = 330.00, ROW_HASH = SHA2('O011' || 'C011' || '2024-04-20' || 330.00 || 'Shipped', 256) WHERE ORDER_ID = 'O011';
UPDATE S_ORDER SET STATUS = 'Delivered', ROW_HASH = SHA2('O012' || 'C012' || '2024-04-25' || 450.75 || 'Delivered', 256) WHERE ORDER_ID = 'O012';

DELETE FROM S_ORDER WHERE ORDER_ID IN ('O001','O002');

CALL CDC.CDC_RUN();
-----------------------------------------------
-- Run 3 - All Tables
-----------------------------------------------
INSERT INTO S_EMPLOYEE (ROW_HASH, EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
SELECT SHA2('E023' || 5500 || 'G' || 'RND', 256), 'E023', 5500, 'G', 'RND'
UNION ALL SELECT SHA2('E024' || 5600 || 'H' || 'ADMIN', 256), 'E024', 5600, 'H', 'ADMIN'
UNION ALL SELECT SHA2('' || 5700 || 'A' || 'HR', 256), NULL, 5700, 'A', 'HR'
UNION ALL SELECT SHA2('E025' || 5800 || 'B' || 'IT', 256), 'E025', 5800, 'B', 'IT'
UNION ALL SELECT SHA2('E025' || 5800 || 'B' || 'IT', 256), 'E025', 5800, 'B', 'IT';

UPDATE S_EMPLOYEE SET SALARY = 5550, ROW_HASH = SHA2('E023' || 5550 || 'G' || 'RND', 256) WHERE EMPLOYEE_ID = 'E023';

DELETE FROM S_EMPLOYEE WHERE EMPLOYEE_ID = 'E014';

INSERT INTO S_CUSTOMER (ROW_HASH, CUSTOMER_ID, CUSTOMER_NAME, EMAIL, COUNTRY, REGISTRATION_DATE)
SELECT SHA2('C019' || 'Victor Anderson' || 'victor@example.com' || 'MT' || '2024-07-01', 256), 'C019', 'Victor Anderson', 'victor@example.com', 'MT', '2024-07-01'
UNION ALL SELECT SHA2('C020' || 'Wendy Taylor' || 'wendy@example.com' || 'GR' || '2024-07-10', 256), 'C020', 'Wendy Taylor', 'wendy@example.com', 'GR', '2024-07-10'
UNION ALL SELECT SHA2('C021' || 'Xavier Thomas' || 'xavier@example.com' || 'PT' || '2024-07-15', 256), 'C021', 'Xavier Thomas', 'xavier@example.com', 'PT', '2024-07-15'
UNION ALL SELECT SHA2('' || 'Yara Miller' || 'yara@example.com' || 'IE' || '2024-07-20', 256), NULL, 'Yara Miller', 'yara@example.com', 'IE', '2024-07-20'
UNION ALL SELECT SHA2('C022' || 'Zoe Wilson' || 'zoe@example.com' || 'AT' || '2024-07-25', 256), 'C022', 'Zoe Wilson', 'zoe@example.com', 'AT', '2024-07-25'
UNION ALL SELECT SHA2('C022' || 'Zoe Wilson' || 'zoe@example.com' || 'AT' || '2024-07-25', 256), 'C022', 'Zoe Wilson', 'zoe@example.com', 'AT', '2024-07-25'
UNION ALL SELECT SHA2('C023' || 'Aaron Davis' || 'aaron@example.com' || 'CH' || '2024-08-01', 256), 'C023', 'Aaron Davis', 'aaron@example.com', 'CH', '2024-08-01';

UPDATE S_CUSTOMER SET COUNTRY = 'GR', ROW_HASH = SHA2('C020' || 'Wendy Taylor' || 'wendy@example.com' || 'GR' || '2024-07-10', 256) WHERE CUSTOMER_ID = 'C020';
UPDATE S_CUSTOMER SET CUSTOMER_NAME = 'Wendy T Taylor', ROW_HASH = SHA2('C020' || 'Wendy T Taylor' || 'wendy@example.com' || 'GR' || '2024-07-10', 256) WHERE CUSTOMER_ID = 'C020';

DELETE FROM S_CUSTOMER WHERE CUSTOMER_ID IN ('C011','C012');

INSERT INTO S_ORDER (ROW_HASH, ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS)
SELECT SHA2('O021' || 'C019' || '2024-08-05' || 400.00 || 'Pending', 256), 'O021', 'C019', '2024-08-05', 400.00, 'Pending'
UNION ALL SELECT SHA2('O022' || 'C020' || '2024-08-10' || 550.25 || 'Shipped', 256), 'O022', 'C020', '2024-08-10', 550.25, 'Shipped'
UNION ALL SELECT SHA2('' || 'C021' || '2024-08-15' || 325.50 || 'Delivered', 256), NULL, 'C021', '2024-08-15', 325.50, 'Delivered'
UNION ALL SELECT SHA2('O023' || 'C022' || '2024-08-20' || 675.00 || 'Pending', 256), 'O023', 'C022', '2024-08-20', 675.00, 'Pending'
UNION ALL SELECT SHA2('O023' || 'C022' || '2024-08-20' || 675.00 || 'Pending', 256), 'O023', 'C022', '2024-08-20', 675.00, 'Pending';

UPDATE S_ORDER SET TOTAL_AMOUNT = 410.00, ROW_HASH = SHA2('O021' || 'C019' || '2024-08-05' || 410.00 || 'Pending', 256) WHERE ORDER_ID = 'O021';
UPDATE S_ORDER SET STATUS = 'Delivered', ROW_HASH = SHA2('O021' || 'C019' || '2024-08-05' || 410.00 || 'Delivered', 256) WHERE ORDER_ID = 'O021';

DELETE FROM S_ORDER WHERE ORDER_ID = 'O011';

CALL CDC.CDC_RUN();
-----------------------------------------------
-- Run 4 - All Tables
-----------------------------------------------
INSERT INTO S_EMPLOYEE (ROW_HASH, EMPLOYEE_ID, SALARY, MANAGER, DEPARTMENT)
SELECT SHA2('E026' || 5900 || 'I' || 'LEGAL', 256), 'E026', 5900, 'I', 'LEGAL'
UNION ALL SELECT SHA2('' || 6000 || 'J' || 'EXEC', 256), NULL, 6000, 'J', 'EXEC'
UNION ALL SELECT SHA2('E027' || 6100 || 'A' || 'HR', 256), 'E027', 6100, 'A', 'HR'
UNION ALL SELECT SHA2('E028' || 6200 || 'B' || 'IT', 256), 'E028', 6200, 'B', 'IT'
UNION ALL SELECT SHA2('E028' || 6200 || 'B' || 'IT', 256), 'E028', 6200, 'B', 'IT'
UNION ALL SELECT SHA2('E029' || 6300 || 'C' || 'SALES', 256), 'E029', 6300, 'C', 'SALES';

UPDATE S_EMPLOYEE SET SALARY = 6050, ROW_HASH = SHA2('E026' || 6050 || 'I' || 'LEGAL', 256) WHERE EMPLOYEE_ID = 'E026';
UPDATE S_EMPLOYEE SET MANAGER = 'J', ROW_HASH = SHA2('E026' || 6050 || 'J' || 'LEGAL', 256) WHERE EMPLOYEE_ID = 'E026';
UPDATE S_EMPLOYEE SET DEPARTMENT = 'EXEC', ROW_HASH = SHA2('E027' || 6100 || 'A' || 'EXEC', 256) WHERE EMPLOYEE_ID = 'E027';

DELETE FROM S_EMPLOYEE WHERE EMPLOYEE_ID IN ('E023','E024','E025');

INSERT INTO S_CUSTOMER (ROW_HASH, CUSTOMER_ID, CUSTOMER_NAME, EMAIL, COUNTRY, REGISTRATION_DATE)
SELECT SHA2('C024' || 'Bella Rodriguez' || 'bella@example.com' || 'CY' || '2024-08-30', 256), 'C024', 'Bella Rodriguez', 'bella@example.com', 'CY', '2024-08-30'
UNION ALL SELECT SHA2('' || 'Carlos Martinez' || 'carlos@example.com' || 'LU' || '2024-09-05', 256), NULL, 'Carlos Martinez', 'carlos@example.com', 'LU', '2024-09-05'
UNION ALL SELECT SHA2('C025' || 'Diana Garcia' || 'diana@example.com' || 'EE' || '2024-09-10', 256), 'C025', 'Diana Garcia', 'diana@example.com', 'EE', '2024-09-10'
UNION ALL SELECT SHA2('C026' || 'Ethan Brown' || 'ethan@example.com' || 'UA' || '2024-09-15', 256), 'C026', 'Ethan Brown', 'ethan@example.com', 'UA', '2024-09-15'
UNION ALL SELECT SHA2('C026' || 'Ethan Brown' || 'ethan@example.com' || 'UA' || '2024-09-15', 256), 'C026', 'Ethan Brown', 'ethan@example.com', 'UA', '2024-09-15'
UNION ALL SELECT SHA2('C027' || 'Fiona Lopez' || 'fiona@example.com' || 'BY' || '2024-09-20', 256), 'C027', 'Fiona Lopez', 'fiona@example.com', 'BY', '2024-09-20'
UNION ALL SELECT SHA2('C027' || 'Fiona Lopez' || 'fiona@example.com' || 'BY' || '2024-09-20', 256), 'C027', 'Fiona Lopez', 'fiona@example.com', 'BY', '2024-09-20';

UPDATE S_CUSTOMER SET COUNTRY = 'CY', ROW_HASH = SHA2('C024' || 'Bella Rodriguez' || 'bella@example.com' || 'CY' || '2024-08-30', 256) WHERE CUSTOMER_ID = 'C024';
UPDATE S_CUSTOMER SET EMAIL = 'bella.r@example.com', ROW_HASH = SHA2('C024' || 'Bella Rodriguez' || 'bella.r@example.com' || 'CY' || '2024-08-30', 256) WHERE CUSTOMER_ID = 'C024';
UPDATE S_CUSTOMER SET CUSTOMER_NAME = 'Ethan J Brown', ROW_HASH = SHA2('C026' || 'Ethan J Brown' || 'ethan@example.com' || 'UA' || '2024-09-15', 256) WHERE CUSTOMER_ID = 'C026';

DELETE FROM S_CUSTOMER WHERE CUSTOMER_ID IN ('C019','C020','C021');

INSERT INTO S_ORDER (ROW_HASH, ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS)
SELECT SHA2('O024' || 'C024' || '2024-09-25' || 500.00 || 'Delivered', 256), 'O024', 'C024', '2024-09-25', 500.00, 'Delivered'
UNION ALL SELECT SHA2('' || 'C025' || '2024-10-01' || 425.50 || 'Pending', 256), NULL, 'C025', '2024-10-01', 425.50, 'Pending'
UNION ALL SELECT SHA2('O025' || 'C026' || '2024-10-05' || 625.75 || 'Shipped', 256), 'O025', 'C026', '2024-10-05', 625.75, 'Shipped'
UNION ALL SELECT SHA2('O026' || 'C027' || '2024-10-10' || 750.00 || 'Delivered', 256), 'O026', 'C027', '2024-10-10', 750.00, 'Delivered'
UNION ALL SELECT SHA2('O026' || 'C027' || '2024-10-10' || 750.00 || 'Delivered', 256), 'O026', 'C027', '2024-10-10', 750.00, 'Delivered'
UNION ALL SELECT SHA2('O027' || 'C024' || '2024-10-15' || 350.25 || 'Pending', 256), 'O027', 'C024', '2024-10-15', 350.25, 'Pending'
UNION ALL SELECT SHA2('O027' || 'C024' || '2024-10-15' || 350.25 || 'Pending', 256), 'O027', 'C024', '2024-10-15', 350.25, 'Pending';

UPDATE S_ORDER SET STATUS = 'Shipped', ROW_HASH = SHA2('O024' || 'C024' || '2024-09-25' || 500.00 || 'Shipped', 256) WHERE ORDER_ID = 'O024';
UPDATE S_ORDER SET TOTAL_AMOUNT = 510.00, ROW_HASH = SHA2('O024' || 'C024' || '2024-09-25' || 510.00 || 'Shipped', 256) WHERE ORDER_ID = 'O024';
UPDATE S_ORDER SET STATUS = 'Delivered', ROW_HASH = SHA2('O025' || 'C026' || '2024-10-05' || 625.75 || 'Delivered', 256) WHERE ORDER_ID = 'O025';

DELETE FROM S_ORDER WHERE ORDER_ID IN ('O021','O022');

CALL CDC.CDC_RUN();