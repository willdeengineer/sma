USE DATABASE CDC_TEST_DB;
USE SCHEMA CDC;

-- Hybrid tables zijn voor CONFIG tabellen en LOG tabellen best practice, alleen deze zijn niet toegankelijk in een trial account. Daarom gebruiken we gewone tabellen.
-- -------------------------------------------------
-- Config tabel
-- -------------------------------------------------
CREATE OR REPLACE TABLE CDC_CONFIG (
  CONFIG_ID NUMBER AUTOINCREMENT, -- unieke config identifier
  ENTITY_NAME STRING NOT NULL, -- bv. 'ENTITY', 'EMPLOYEE', 'ORDER'
  SOURCE_TABLE STRING NOT NULL, -- bv. 'S_Employee'
  TARGET_TABLE STRING NOT NULL, -- bv. 'TARGET_ENTITY'
  PRIMARY_KEY_COLUMN STRING NOT NULL, -- bv. 'ENTITY_ID' of 'EMPLOYEE_ID'
  DELETE_STRATEGY STRING DEFAULT 'SOFT', -- 'SOFT' (update IS_ACTIVE) of 'HARD' (fysieke delete)
  ERROR_STRATEGY STRING DEFAULT 'CONTINUE', -- 'CONTINUE' (log en ga door) of 'STOP' (stop proces bij fout)
  UPDATE_STRATEGY STRING DEFAULT 'HISTORY', -- 'OVERWRITE' (overschrijf bestaande waarde) of 'HISTORY' (oude rij afsluiten, nieuwe toevoegen)
  IS_ACTIVE BOOLEAN DEFAULT TRUE, -- of deze config actief is
  PRIMARY KEY (CONFIG_ID)
);

-------------------------------------------------
-- Log tabellen
-------------------------------------------------
USE SCHEMA LOGGING;

CREATE OR REPLACE TABLE RUN_LOG (
  RUN_ID NUMBER NOT NULL, -- bv. 1
  START_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- TIMESTAMP van run start
  END_TS TIMESTAMP_NTZ, -- TIMESTAMP van run END'
  ROWS_INSERTED NUMBER DEFAULT 0, -- aantal inserts van een run
  ROWS_UPDATED NUMBER DEFAULT 0,  -- aantal updates van een run
  ROWS_DELETED NUMBER DEFAULT 0,  -- aantal deletes van een run
  ROWS_UNCHANGED NUMBER DEFAULT 0,  -- hoeveel rijen zijn ongewijzigd
  DUPLICATE_INSERTS NUMBER DEFAULT 0,  -- duplicaat gevonden tijdens insert nieuwe waarde
  DUPLICATE_UPDATES NUMBER DEFAULT 0,  -- duplicaat gevonden tijdens update nieuwe waarde
  KEY_ERRORS NUMBER DEFAULT 0, -- aantal key errors (bv. null key)
  STATUS STRING DEFAULT 'FAILED', -- 'RUNNING', 'COMPLETED', 'FAILED'
    PRIMARY KEY (RUN_ID)
);

CREATE OR REPLACE TABLE RUN_ENTITY_LOG (
    RUN_ID INT,
    START_TS TIMESTAMP_NTZ,
    END_TS TIMESTAMP_NTZ,
    ENTITY_NAME STRING,
    ROWS_INSERTED INT,
    ROWS_UPDATED INT,
    ROWS_DELETED INT,
    ROWS_UNCHANGED INT,
    DUPLICATE_INSERTS INT,
    DUPLICATE_UPDATES INT,
    KEY_ERRORS INT
);

CREATE OR REPLACE TABLE RUN_ERROR_LOG (
  ERROR_ID NUMBER AUTOINCREMENT, -- unieke error identifier
  RUN_ID NUMBER NOT NULL, -- bv. 1
  ENTITY_NAME STRING NOT NULL, -- bv. 'Employee'
  ERROR_CODE STRING NOT NULL, -- bv. 'DUPLICATE_KEY', 'DUPLICATE_INSERT', 'DUPLICATE_UPDATE'
  ERROR_ROW VARIANT, -- volledige rij met fout
  OCCURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- TIMESTAMP van fout
  PRIMARY KEY (ERROR_ID)
);

-------------------------------------------------
-- Stage tabel (hier wordt brondata ingeladen)
-------------------------------------------------
USE SCHEMA STAGING;
CREATE OR REPLACE TABLE S_Employee (
  ROW_HASH STRING, -- bv. 'abc123def456'
  -- Velden
  EMPLOYEE_ID STRING, -- bv. 'R001'
  SALARY NUMBER, -- bv. 5000
  MANAGER STRING, -- bv. 'A', 'B', 'C'
  DEPARTMENT STRING -- bv. 'HR', 'IT', 'SALES'
);

CREATE OR REPLACE TABLE S_Customer (
  ROW_HASH STRING,
  CUSTOMER_ID STRING,
  CUSTOMER_NAME STRING,
  EMAIL STRING,
  COUNTRY STRING,
  REGISTRATION_DATE DATE
);

CREATE OR REPLACE TABLE S_Order (
  ROW_HASH STRING,
  ORDER_ID STRING,
  CUSTOMER_ID STRING,
  ORDER_DATE DATE,
  TOTAL_AMOUNT NUMBER,
  STATUS STRING
);


-------------------------------------------------
-- Target tabel
------------------------------------------------- 
USE SCHEMA TARGET;

CREATE OR REPLACE TABLE T_Employee (
  ROW_HASH STRING NOT NULL, -- bv. 'abc123def456'
  START_TS TIMESTAMP_NTZ  NOT NULL, -- bv. '2025-12-13 10:00:00'
  END_TS TIMESTAMP_NTZ, -- bv. NULL of '2025-12-14 12:00:00'
  IS_ACTIVE BOOLEAN DEFAULT TRUE, -- bv. FALSE of TRUE
  CDC_OPERATION STRING NOT NULL, -- 'I (insert)','U (update)','D (delete)'
  -- Velden
  EMPLOYEE_ID STRING, -- bv. 'R001'
  SALARY NUMBER, -- bv. 5000
  MANAGER STRING, -- bv. 'A', 'B', 'C'
  DEPARTMENT STRING, -- bv. 'HR', 'IT', 'SALES'
  PRIMARY KEY (EMPLOYEE_ID, START_TS) 
);

CREATE OR REPLACE TABLE T_Customer (
  ROW_HASH STRING NOT NULL,
  START_TS TIMESTAMP_NTZ NOT NULL,
  END_TS TIMESTAMP_NTZ,
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  CDC_OPERATION STRING NOT NULL,
  CUSTOMER_ID STRING,
  CUSTOMER_NAME STRING,
  EMAIL STRING,
  COUNTRY STRING,
  REGISTRATION_DATE DATE,
  PRIMARY KEY (CUSTOMER_ID, START_TS)
);

CREATE OR REPLACE TABLE T_Order (
  ROW_HASH STRING NOT NULL,
  START_TS TIMESTAMP_NTZ NOT NULL,
  END_TS TIMESTAMP_NTZ,
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  CDC_OPERATION STRING NOT NULL,
  ORDER_ID STRING,
  CUSTOMER_ID STRING,
  ORDER_DATE DATE,
  TOTAL_AMOUNT NUMBER,
  STATUS STRING,
  PRIMARY KEY (ORDER_ID, START_TS)
);

-------------------------------------------------
-- Clustering voor performance
-------------------------------------------------
-- ALTER TABLE TARGET_ENTITY CLUSTER BY (EMPLOYEE_ID, ROW_HASH);
