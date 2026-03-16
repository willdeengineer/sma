-- -------------------------------------------------
-- Setup SQL voor CDC proces
-- Hie  rmee worden de benodigde database, schemas en tabellen aangemaakt.
--
-- Notes:
-- - Hybrid tables zijn voor CONFIG tabellen en LOG tabellen best practice, alleen deze zijn niet toegankelijk in een trial account.
-- -------------------------------------------------

-- -------------------------------------------------
-- 1. Database en schemas aanmaken
-- Hier worden de database en schemas aangemaakt die we nodig hebben voor het CDC proces.
-- -------------------------------------------------
CREATE DATABASE IF NOT EXISTS CDC_STREAMS_DB;

USE DATABASE CDC_STREAMS_DB;
CREATE SCHEMA IF NOT EXISTS CDC;
CREATE SCHEMA IF NOT EXISTS LOGGING;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS TARGET;

-- -------------------------------------------------
-- 2. Config tabel aanmaken in schema CDC
-- Hier wordt de CDC_CONFIG tabel aangemaakt waarin de configuratie voor het CDC proces wordt opgeslagen.
-- De config bevat informatie over een entiteit: de naam van bron en doeltabel, de primaire sleutel en de strategieen voor deletes, errors en updates.
-- -------------------------------------------------
USE SCHEMA CDC;

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

-- -------------------------------------------------
-- 3. Log tabellen aanmaken
-- Hier worden de tabellen in het LOGGING schema aangemaakt waarin we de resultaten van het CDC proces loggen (aantal inserts, updates, deletes, errors, etc.)..
-- -------------------------------------------------
USE SCHEMA LOGGING;

-- Logt per run het aantal inserts, updates, deletes, etc.
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

-- Logt per run en per entity het aantal inserts, updates, deletes, etc.
CREATE OR REPLACE TABLE RUN_ENTITY_LOG (
    RUN_ID INT, -- bv. 1
    START_TS TIMESTAMP_NTZ, -- TIMESTAMP van start verwerking entity
    END_TS TIMESTAMP_NTZ, -- TIMESTAMP van einde verwerking entity
    ENTITY_NAME STRING, -- bv. 'Employee'
    ROWS_INSERTED INT, -- aantal inserts van een entity
    ROWS_UPDATED INT, -- aantal updates van een entity
    ROWS_DELETED INT, -- aantal deletes van een entity
    ROWS_UNCHANGED INT, -- hoeveel rijen zijn ongewijzigd
    DUPLICATE_INSERTS INT, -- duplicaat gevonden tijdens insert nieuwe waarde
    DUPLICATE_UPDATES INT, -- dubbele update gevonden tijdens update nieuwe waarde
    KEY_ERRORS INT, -- aantal key errors (bv. null key of lege key)
    PRIMARY KEY (RUN_ID)
);

-- Logt details van fouten die optreden tijdens het CDC proces
CREATE OR REPLACE TABLE RUN_ERROR_LOG (
  ERROR_ID NUMBER AUTOINCREMENT, -- unieke error identifier
  RUN_ID NUMBER NOT NULL, -- bv. 1
  ENTITY_NAME STRING NOT NULL, -- bv. 'Employee'
  ERROR_CODE STRING NOT NULL, -- bv. 'DUPLICATE_KEY', 'DUPLICATE_INSERT', 'DUPLICATE_UPDATE'
  ERROR_ROW VARIANT, -- volledige rij met fout
  OCCURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- TIMESTAMP van fout
  PRIMARY KEY (ERROR_ID)
);

-- -------------------------------------------------
-- 4. Stage schema 
-- Hier wordt brondata ingeladen.
-- Voor testen maken we hier 3 tabellen aan: S_Employee, S_Customer, S_Order.
-- -------------------------------------------------
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE S_Employee (
  -- ROW_HASH STRING, -- bv. 'abc123def456'
  -- Velden
  EMPLOYEE_ID STRING, -- bv. 'R001'
  SALARY NUMBER, -- bv. 5000
  MANAGER STRING, -- bv. 'A', 'B', 'C'
  DEPARTMENT STRING -- bv. 'HR', 'IT', 'SALES'
);

CREATE OR REPLACE TABLE S_Customer (
  -- ROW_HASH STRING, -- bv. 'abc123def456'
  -- Velden
  CUSTOMER_ID STRING, -- bv. 'C001'
  CUSTOMER_NAME STRING, -- bv. 'John Doe'
  EMAIL STRING, -- bv. 'john.doe@example.com'
  COUNTRY STRING, -- bv. 'NL', 'BE', 'DE'
  REGISTRATION_DATE DATE -- bv. '1-1-2026'
);

CREATE OR REPLACE TABLE S_Order (
  -- ROW_HASH STRING, -- bv. 'abc123def456'
  -- Velden
  ORDER_ID STRING, -- bv. 'O001'
  CUSTOMER_ID STRING, -- bv. 'C001'
  ORDER_DATE DATE, -- bv. '1-1-2026'
  TOTAL_AMOUNT NUMBER, -- bv. 100.50
  STATUS STRING -- bv. 'PENDING', 'COMPLETED', 'CANCELLED'
);

-- -------------------------------------------------
-- 5. Target schema
-- Hier komt de data in wanneer het CDC proces voltooid is.
-- Voor testen maken we hier 3 tabellen aan: T_Employee, T_Customer, T_Order.
-- ------------------------------------------------- 
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
  ROW_HASH STRING NOT NULL, -- bv. 'abc123def456'
  START_TS TIMESTAMP_NTZ NOT NULL, -- bv. '2025-12-13 10:00:00'
  END_TS TIMESTAMP_NTZ, -- bv. NULL of '2025-12-14 12:00:00'
  IS_ACTIVE BOOLEAN DEFAULT TRUE, -- bv. FALSE of TRUE
  CDC_OPERATION STRING NOT NULL, -- 'I (insert)','U (update)','D (delete)'
  -- Velden
  CUSTOMER_ID STRING, -- bv. 'C001'
  CUSTOMER_NAME STRING, -- bv. 'John Doe'
  EMAIL STRING, -- bv. 'john.doe@example.com'
  COUNTRY STRING, -- bv. 'NL', 'BE', 'DE'
  REGISTRATION_DATE DATE, -- bv. '1-1-2026'
  PRIMARY KEY (CUSTOMER_ID, START_TS) 
);

CREATE OR REPLACE TABLE T_Order (
  ROW_HASH STRING NOT NULL, -- bv. 'abc123def456'
  START_TS TIMESTAMP_NTZ NOT NULL, -- bv. '2025-12-13 10:00:00'
  END_TS TIMESTAMP_NTZ, -- bv. NULL of '2025-12-14 12:00:00'
  IS_ACTIVE BOOLEAN DEFAULT TRUE, -- bv. FALSE of TRUE
  CDC_OPERATION STRING NOT NULL, -- 'I (insert)','U (update)','D (delete)'
  -- Velden
  ORDER_ID STRING, -- bv. 'O001'
  CUSTOMER_ID STRING, -- bv. 'C001'
  ORDER_DATE DATE, -- bv. '1-1-2026'
  TOTAL_AMOUNT NUMBER, -- bv. 100.50
  STATUS STRING, -- bv. 'PENDING', 'COMPLETED', 'CANCELLED'
  PRIMARY KEY (ORDER_ID, START_TS)
);

-- -------------------------------------------------
-- Clustering voor performance
-- -------------------------------------------------
-- ALTER TABLE TARGET_ENTITY CLUSTER BY (EMPLOYEE_ID, ROW_HASH);
