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
INSERT INTO STAGING.S_EMPLOYEE VALUES
('abc123', 'E001', 5000, 'A', 'HR'),
('def456', 'E002', 6000, 'B', 'IT'),
('ghi789', 'E003', 5500, 'A', 'SALES'),
('jkl012', 'E004', 7000, 'C', 'IT'),
('mno345', 'E005', 5200, 'B', 'HR');

INSERT INTO STAGING.S_CUSTOMER VALUES
('cust001', 'C001', 'John Doe', 'john@example.com', 'USA', '2023-01-15'),
('cust002', 'C002', 'Jane Smith', 'jane@example.com', 'UK', '2023-02-20'),
('cust003', 'C003', 'Bob Johnson', 'bob@example.com', 'USA', '2023-03-10'),
('cust004', 'C004', 'Alice Brown', 'alice@example.com', 'Canada', '2023-04-05'),
('cust005', 'C005', 'Charlie White', 'charlie@example.com', 'USA', '2023-05-12');

INSERT INTO STAGING.S_ORDER VALUES
('ord001', 'O001', 'C001', '2023-06-01', 1500.00, 'Completed'),
('ord002', 'O002', 'C002', '2023-06-05', 2000.00, 'Pending'),
('ord003', 'O003', 'C001', '2023-06-10', 1200.00, 'Completed'),
('ord004', 'O004', 'C003', '2023-06-15', 3000.00, 'Shipped'),
('ord005', 'O005', 'C004', '2023-06-20', 2500.00, 'Pending');

-----------------------------------------------
-- Run 2
-----------------------------------------------
INSERT INTO STAGING.S_EMPLOYEE VALUES
('abc123', 'E006', 3000, 'C', 'SALES'),
('def456', 'E007', 7000, 'A', 'IT'),
('ghi789', 'E008', 3500, 'B', 'SALES'),
('jkl012', 'E009', 4000, 'A', 'HR'),
('mno345', 'E010', 6200, 'C', 'HR');