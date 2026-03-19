
-- -------------------------------------------------
-- Procedure om het CDC proces te runnen.
-- Deze procedure kan worden aangeroepen met CALL CDC_RUN() en voert het CDC proces uit voor alle entiteiten die voorkomen in CDC_CONFIG.
-- -------------------------------------------------

-- -------------------------------------------------
-- 1. Database en schema gebruiken
-- -------------------------------------------------
USE DATABASE CDC_SQL_DB;
USE SCHEMA CDC;


-- -------------------------------------------------
-- 2. CDC_RUN procedure aanmaken
-- Hier wordt de procedure CDC_PROCESS aangemaakt die het daadwerkelijke CDC proces uitvoert.
-- Deze procedure wordt per entiteit uitgevoerd en verwerkt de brondata volgens de configuratie in CDC_CONFIG.
-- -------------------------------------------------
CREATE OR REPLACE PROCEDURE CDC_RUN()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    c1 CURSOR FOR SELECT CONFIG_ID FROM CDC_CONFIG WHERE IS_ACTIVE = TRUE;
    c2 CURSOR FOR SELECT CONFIG_ID FROM CDC_CONFIG WHERE IS_ACTIVE = TRUE;

    v_config_id INT;
    v_run_id INT;
    v_source_table STRING;
      v_run_status STRING;
    v_columns STRING;

    v_sql STRING;
BEGIN
    -- Run ID bepalen (max RUN_ID + 1) en run log initialiseren
    SELECT COALESCE(MAX(RUN_ID), 0) + 1 INTO :v_run_id FROM LOGGING.RUN_LOG;

    INSERT INTO LOGGING.RUN_LOG (RUN_ID, START_TS, STATUS)
    VALUES (:v_run_id, CURRENT_TIMESTAMP(), 'RUNNING');

    
    FOR record IN c1 DO
        v_config_id := record.CONFIG_ID;

        SELECT SOURCE_TABLE
        INTO :v_source_table
        FROM CDC_CONFIG
        WHERE CONFIG_ID = :v_config_id;

        -- kolom toevoegen als die niet bestaat
        v_sql := 'ALTER TABLE ' || v_source_table || ' ADD COLUMN IF NOT EXISTS ROW_HASH STRING';
        EXECUTE IMMEDIATE :v_sql;

        -- Hash berekenen (SHA256) op basis van alle kolommen in de brondata.
        v_sql := 'UPDATE ' || v_source_table || ' SET ROW_HASH = SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(* EXCLUDE ROW_HASH)),256)';
        EXECUTE IMMEDIATE :v_sql;

    END FOR;
    
    -- Voor elke actieve config in het CDC_CONFIG wordt CDC_PROCESS procedure aangeroepen die het CDC proces uitvoert voor die entiteit.
    FOR record IN c2 DO
        v_config_id := record.CONFIG_ID;
        CALL CDC_PROCESS(:v_config_id, :v_run_id);

            SELECT STATUS
            INTO :v_run_status
            FROM LOGGING.RUN_LOG
            WHERE RUN_ID = :v_run_id;

            IF (v_run_status = 'FAILED') THEN
                  RETURN 'Run met id ' || v_run_id || ' is mislukt.';
            END IF;
    END FOR;

  -- Na het uitvoeren van CDC_PROCESS voor alle entiteiten worden de totalen van inserts, updates, deletes, etc. in RUN_LOG bijgewerkt op basis van de gegevens in RUN_ENTITY_LOG.
  UPDATE LOGGING.RUN_LOG
  SET
    ROWS_INSERTED = (
          SELECT COALESCE(SUM(ROWS_INSERTED),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    ROWS_UPDATED = (
          SELECT COALESCE(SUM(ROWS_UPDATED),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    ROWS_DELETED = (
          SELECT COALESCE(SUM(ROWS_DELETED),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    ROWS_UNCHANGED = (
          SELECT COALESCE(SUM(ROWS_UNCHANGED),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    DUPLICATE_INSERTS = (
          SELECT COALESCE(SUM(DUPLICATE_INSERTS),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    DUPLICATE_UPDATES = (
          SELECT COALESCE(SUM(DUPLICATE_UPDATES),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    KEY_ERRORS = (
          SELECT COALESCE(SUM(KEY_ERRORS),0)
          FROM LOGGING.RUN_ENTITY_LOG
          WHERE RUN_ID = :v_run_id
    ),
    END_TS = CURRENT_TIMESTAMP(),
    STATUS = 'COMPLETED'
  WHERE RUN_ID = :v_run_id;

  RETURN 'Run met id ' || v_run_id || ' is klaar.';
END;
$$;