USE DATABASE CDC_TEST_DB;
USE SCHEMA CDC;

CREATE OR REPLACE PROCEDURE CDC_PROCESS(config_id INT, run_id INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  -- Config
  v_runid := run_id;
  v_entity STRING;
  v_pk STRING;
  v_source STRING;
  v_target STRING;
  v_delete_strategy STRING;
  v_error_strategy STRING;
  v_update_strategy STRING;
  v_business_columns STRING;

  -- Statistieken
  v_unchanged INT := 0;
  v_inserts INT := 0;
  v_updates INT := 0;
  v_deletes INT := 0;
  v_duplicate_inserts INT := 0;
  v_duplicate_updates INT := 0;
  v_key_errors INT := 0;

  v_start_ts TIMESTAMP := CURRENT_TIMESTAMP();

  v_sql STRING;
BEGIN
  ---------------------------------------------
  -- 1. Run initialiseren
  ---------------------------------------------

  -- Config ophalen
  SELECT ENTITY_NAME, PRIMARY_KEY_COLUMN, SOURCE_TABLE, TARGET_TABLE, DELETE_STRATEGY, ERROR_STRATEGY, UPDATE_STRATEGY
  INTO v_entity, v_pk, v_source, v_target, v_delete_strategy, v_error_strategy, v_update_strategy
  FROM CDC.CDC_CONFIG
  WHERE CONFIG_ID = :config_id
  AND IS_ACTIVE = TRUE;
   IF (v_entity IS NULL) THEN
    UPDATE LOGGING.RUN_LOG 
    SET END_TS = CURRENT_TIMESTAMP(), STATUS = 'FAILED'
    WHERE RUN_ID = :v_runid;
    RETURN 'Fout: Config met ID ' || config_id || ' niet gevonden of niet actief.';
  END IF;

  SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ') 
  INTO :v_business_columns
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'STAGING'
    AND TABLE_NAME = UPPER(REGEXP_SUBSTR(:v_source, '[^.]+$'))
    AND COLUMN_NAME NOT IN ('ROW_HASH', 'START_TS', 'END_TS', 'IS_ACTIVE', 'CDC_OPERATION')
    AND COLUMN_NAME != :v_pk;

  ---------------------------------------------
  -- 2. Errors detecteren
  ---------------------------------------------
  v_sql := 'INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
    SELECT ' || v_runid || ', ''' || v_entity || ''', ''DUPLICATE_INSERT'', OBJECT_CONSTRUCT(*)
    FROM (
      SELECT s.*, COUNT(*) OVER (PARTITION BY s.' || v_pk || ', s.ROW_HASH) AS CNT
      FROM ' || v_source || ' s
      WHERE s.' || v_pk || ' IS NOT NULL
        AND s.ROW_HASH IS NOT NULL
    ) t
    WHERE t.CNT > 1';
  EXECUTE IMMEDIATE v_sql;
  v_duplicate_inserts := SQLROWCOUNT;

  v_sql := 'INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
    SELECT ' || v_runid || ', ''' || v_entity || ''', ''DUPLICATE_UPDATE'', OBJECT_CONSTRUCT(*)
    FROM ' || v_source || ' s
    WHERE s.' || v_pk || ' IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM ' || v_source || ' s2
        WHERE s2.' || v_pk || ' = s.' || v_pk || '
          AND s2.ROW_HASH <> s.ROW_HASH
          AND s2.ROW_HASH IS NOT NULL
      )';
  EXECUTE IMMEDIATE v_sql;
  v_duplicate_updates := SQLROWCOUNT;

  v_sql := 'INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
    SELECT ' || v_runid || ', ''' || v_entity || ''', ''PRIMARY_KEY_ERROR'', OBJECT_CONSTRUCT(*)
    FROM ' || v_source || ' s
    WHERE s.' || v_pk || ' IS NULL OR s.' || v_pk || ' = ''''';
  EXECUTE IMMEDIATE v_sql;
  v_key_errors := SQLROWCOUNT;

  ---------------------------------------------
  -- 3. Inserts uitvoeren
  ---------------------------------------------
  v_sql := 'INSERT INTO ' || v_target || ' (
      ROW_HASH, START_TS, IS_ACTIVE, CDC_OPERATION,
      ' || v_pk || ', ' || v_business_columns || ')
      SELECT s.ROW_HASH, CURRENT_TIMESTAMP(), TRUE, ''I'', s.' || v_pk || ', ' ||
      's.' || REPLACE(:v_business_columns, ', ', ', s.') || '
      FROM ' || v_source || ' s
      LEFT JOIN ' || v_target || ' t
      ON t.' || v_pk || ' = s.' || v_pk || ' AND t.IS_ACTIVE = TRUE
      WHERE t.' || v_pk || ' IS NULL
        AND s.' || v_pk || ' IS NOT NULL
        AND (SELECT COUNT(*) FROM ' || v_source || ' s2
             WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1';
  EXECUTE IMMEDIATE v_sql;
  v_inserts := SQLROWCOUNT;

  ---------------------------------------------
  -- 4. Updates uitvoeren
  ---------------------------------------------
  IF (v_update_strategy = 'HISTORY') THEN
    v_sql := 'UPDATE ' || v_target || ' t
      SET IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP()
      WHERE t.IS_ACTIVE = TRUE
      AND EXISTS (
        SELECT 1
        FROM ' || v_source || ' s
        WHERE s.' || v_pk || ' = t.' || v_pk || '
          AND s.ROW_HASH <> t.ROW_HASH
          AND (SELECT COUNT(*) FROM ' || v_source || ' s2
               WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1
      )';
    EXECUTE IMMEDIATE v_sql;

    v_sql := 'INSERT INTO ' || v_target || ' (ROW_HASH, START_TS, IS_ACTIVE, CDC_OPERATION, ' || v_pk || ', ' || v_business_columns || ')
      SELECT s.ROW_HASH, CURRENT_TIMESTAMP(), TRUE, ''U'', s.' || v_pk || ', ' ||
      's.' || REPLACE(:v_business_columns, ', ', ', s.') || '
      FROM ' || v_source || ' s
      WHERE s.' || v_pk || ' IS NOT NULL
        AND (SELECT COUNT(*) FROM ' || v_source || ' s2
             WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1
        AND NOT EXISTS (
          SELECT 1
          FROM ' || v_target || ' t
          WHERE t.' || v_pk || ' = s.' || v_pk || '
            AND t.IS_ACTIVE = TRUE
            AND t.ROW_HASH = s.ROW_HASH
        )';
  ELSE
    v_sql := 'UPDATE ' || v_target || ' t
      SET ROW_HASH = s.ROW_HASH, START_TS = CURRENT_TIMESTAMP(), IS_ACTIVE = TRUE, CDC_OPERATION = ''U'',
          ' || v_pk || ' = s.' || v_pk || ', ' ||
          's.' || REPLACE(:v_business_columns, ', ', ', s.') || '
      FROM ' || v_source || ' s
      WHERE t.' || v_pk || ' = s.' || v_pk || '
        AND t.IS_ACTIVE = TRUE
        AND t.ROW_HASH <> s.ROW_HASH
        AND (SELECT COUNT(*) FROM ' || v_source || ' s2
             WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1';
  END IF;
  EXECUTE IMMEDIATE v_sql;
  v_updates := SQLROWCOUNT;

  ---------------------------------------------
  -- 5. Deletes
  ---------------------------------------------
  IF (v_delete_strategy = 'SOFT') THEN
    v_sql := 'UPDATE ' || v_target || ' t
      SET IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP(), CDC_OPERATION = ''D''
      WHERE t.IS_ACTIVE = TRUE
        AND NOT EXISTS (
          SELECT 1
          FROM ' || v_source || ' s
          WHERE s.' || v_pk || ' = t.' || v_pk || '
        )';
  ELSE
    v_sql := 'DELETE FROM ' || v_target || ' t
      WHERE t.IS_ACTIVE = TRUE
        AND NOT EXISTS (
          SELECT 1
          FROM ' || v_source || ' s
          WHERE s.' || v_pk || ' = t.' || v_pk || '
        )';
  END IF;
  EXECUTE IMMEDIATE v_sql;
  v_deletes := SQLROWCOUNT;

  ---------------------------------------------
  -- 6. Run voltooien
  ---------------------------------------------
  INSERT INTO LOGGING.RUN_ENTITY_LOG
  VALUES (
      :run_id,
      :v_start_ts,
      CURRENT_TIMESTAMP(),
      :v_entity,
      :v_inserts,
      :v_updates,
      :v_deletes,
      :v_unchanged,
      :v_duplicate_inserts,
      :v_duplicate_updates,
      :v_key_errors
  );

  RETURN 'Entity ' || v_entity || ' verwerkt.';

END;
$$;