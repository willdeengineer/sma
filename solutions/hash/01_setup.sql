USE SCHEMA TEST;

CREATE SEQUENCE IF NOT EXISTS run_seq START = 1 INCREMENT = 1; -- voor identificatie en traceerbaarheid van een run
CREATE OR REPLACE SEQUENCE RUN_SEQ START WITH 1 INCREMENT BY 1 ORDER;

-- Log tabellen
CREATE OR REPLACE TABLE RUN_LOG (
  RUN_ID NUMBER NOT NULL, -- bv. 1
  RUN_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- TIMESTAMP van run start
  ENTITY_NAME STRING NOT NULL, -- bv. 'Entity'
  INSERTS NUMBER DEFAULT 0, -- aantal inserts van een run
  UPDATES NUMBER DEFAULT 0,  -- aantal updates van een run
  DELETES NUMBER DEFAULT 0,  -- aantal deletes van een run
  DUP_INSERT NUMBER DEFAULT 0,  -- duplicaat gevonden tijdens insert nieuwe waarde
  DUP_UPDATE NUMBER DEFAULT 0,  -- duplicaat gevonden tijdens update nieuwe waarde
  DUP_NO_CHANGE NUMBER DEFAULT 0,  -- identiek aan target
  ERRORS NUMBER DEFAULT 0, -- aantal errors tijdens een run
);

CREATE OR REPLACE TABLE ERROR_LOG (
  RUN_ID NUMBER NOT NULL, -- bv. 1
  ENTITY_NAME STRING NOT NULL, -- bv. 'Entity'
  ERROR_CODE STRING NOT NULL, -- bv. 'DUPLICATE_KEY'
  ERROR_ROW VARIANT, -- volledige rij met fout
  ERROR_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- TIMESTAMP van fout
);

-- Stage tabel (hier wordt brondata ingeladen)
CREATE OR REPLACE TABLE STAGE_ENTITY (
  RUN_ID NUMBER NOT NULL, -- bv. 1
  ENTITY_ID STRING NOT NULL, -- bv. 'E99'
  ROW_HASH STRING NOT NULL, -- bv. 'abc123def456'
  -- Velden
  NAME STRING, -- bv. "John"
  SALARY NUMBER -- bv. 3750
);

-- Target tabel slowly changing dimension 
CREATE OR REPLACE TABLE TARGET_ENTITY (
  ENTITY_ID STRING NOT NULL, -- bv. 'E99'
  ROW_HASH STRING NOT NULL, -- bv. 'abc123def456'
  START_TS TIMESTAMP_NTZ  NOT NULL, -- bv. '2025-12-13 10:00:00'
  END_TS TIMESTAMP_NTZ, -- bv. NULL of '2025-12-14 12:00:00'
  IS_ACTIVE BOOLEAN DEFAULT TRUE, -- bv. FALSE of TRUE
  CDC_OPERATION STRING NOT NULL, -- 'I (insert)','U (update)','D (delete)'
  -- Velden
  NAME STRING, -- bv. "John"
  SALARY NUMBER, -- bv. 3750
  CONSTRAINT PK_TARGET_ENTITY PRIMARY KEY (ENTITY_ID, START_TS) -- surogate key voor uniciteit binnen tabel
);

-- Clustering voor performance
-- ALTER TABLE TARGET_ENTITY CLUSTER BY (ENTITY_ID, ROW_HASH);
