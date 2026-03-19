-- -------------------------------------------------
-- Procedure om wijzigingen van bron naar target te verwerken volgens de CDC logica beschreven in het functioneel en technisch ontwerp.
-- Deze procedure wordt aangeroepen vanuit CDC_RUN().
-- De procedure verwerkt de brondata volgens de configuratie in CDC_CONFIG en werkt de doeltabel bij met inserts, updates en deletes.
-- Daarnaast worden er 'fouten' gedetecteerd (duplicate inserts, duplicate updates, key errors) en gelogd in RUN_ERROR_LOG.
-- Na het verwerken van de brondata worden de waardes van de run per entiteit (aantal inserts, updates, deletes, etc.) gelogd in RUN_ENTITY_LOG.
-- -------------------------------------------------

----------------------------------------------------
-- 1. Database en schema gebruiken
----------------------------------------------------
USE DATABASE CDC_SQL_DB;
USE SCHEMA CDC;

--------------------------------------------------
-- 2. CDC_PROCESS procedure aanmaken
-- Hier wordt de procedure CDC_PROCESS aangemaakt die het daadwerkelijke CDC proces uitvoert.
-- Deze procedure wordt per entiteit uitgevoerd en verwerkt de brondata volgens de configuratie in CDC_CONFIG.
--------------------------------------------------
CREATE OR REPLACE PROCEDURE CDC_PROCESS(config_id INT, run_id INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  -- Config variabelen
  v_runid := run_id;
  v_entity STRING;
  v_pk STRING;
  v_source STRING;
  v_target STRING;
  v_delete_strategy STRING;
  v_error_strategy STRING;
  v_update_strategy STRING;
  v_business_columns STRING;
  v_business_update_assignments STRING;

  -- Statistiek variabelen
  v_unchanged INT := 0;
  v_inserts INT := 0;
  v_updates INT := 0;
  v_deletes INT := 0;
  v_duplicate_inserts INT := 0;
  v_duplicate_updates INT := 0;
  v_key_errors INT := 0;

  -- Start timestamp van de run
  v_start_ts TIMESTAMP := CURRENT_TIMESTAMP();

  -- Dynamische SQL variabele (wordt gebruikt om SQL statements te maken en uitvoeren die afhankelijk zijn van de configuratie en de structuur van de brondata)
  v_sql STRING;
BEGIN
  ---------------------------------------------
  -- 3. Run initialiseren
  ---------------------------------------------
  -- Config van de entiteit ophalen
  SELECT ENTITY_NAME, PRIMARY_KEY_COLUMN, SOURCE_TABLE, TARGET_TABLE, DELETE_STRATEGY, ERROR_STRATEGY, UPDATE_STRATEGY
  INTO v_entity, v_pk, v_source, v_target, v_delete_strategy, v_error_strategy, v_update_strategy
  FROM CDC.CDC_CONFIG
  WHERE CONFIG_ID = :config_id
    AND IS_ACTIVE = TRUE;

  -- Business kolommen van de entiteit ophalen (alle kolommen behalve kolommen die we gebruiken voor CDC logica: ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION en de primary key).
  SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ') 
  INTO :v_business_columns
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'STAGING'
    AND TABLE_NAME = UPPER(SPLIT_PART(:v_source, '.', -1))
    AND COLUMN_NAME NOT IN ('ROW_HASH', 'START_TS', 'END_TS', 'IS_ACTIVE', 'CDC_OPERATION')
    AND COLUMN_NAME != :v_pk;

  -- Business kolommen maken voor de update statement in geval van 'OVERWRITE' update strategie
  SELECT LISTAGG('"' || COLUMN_NAME || '" = s."' || COLUMN_NAME || '"', ', ')
  INTO :v_business_columns_update
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'STAGING'
    AND TABLE_NAME = UPPER(SPLIT_PART(:v_source, '.', -1))
    AND COLUMN_NAME NOT IN ('ROW_HASH', 'START_TS', 'END_TS', 'IS_ACTIVE', 'CDC_OPERATION')
    AND COLUMN_NAME != :v_pk;

  ---------------------------------------------
  -- 4. Errors detecteren
  ---------------------------------------------
  -- Duplicate inserts in staging detecteren (zelfde PK, zelfde hash)
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

  -- Duplicate updates in staging detecteren (zelfde PK, verschillende hash)
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

  -- Key errors detecteren (null of lege waarde in primary key)
  v_sql := 'INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
            SELECT ' || v_runid || ', ''' || v_entity || ''', ''PRIMARY_KEY_ERROR'', OBJECT_CONSTRUCT(*)
            FROM ' || v_source || ' s
            WHERE s.' || v_pk || ' IS NULL OR s.' || v_pk || ' = ''''';
  EXECUTE IMMEDIATE v_sql;
  v_key_errors := SQLROWCOUNT;

  ---------------------------------------------
  -- Transactie beginnen van het CDC proces met rollback mogelijkheid bij fouten.
  ---------------------------------------------
  BEGIN TRANSACTION;
  ---------------------------------------------
  -- 5. Inserts uitvoeren
  -- Inserts worden alleen uitgevoerd voor rijen zonder errors.
  ---------------------------------------------
  v_sql := 'INSERT INTO ' || v_target || ' (ROW_HASH, START_TS, IS_ACTIVE, CDC_OPERATION, ' || v_pk || ', ' || v_business_columns || ')
            SELECT s.ROW_HASH, CURRENT_TIMESTAMP(), TRUE, ''I'', s.' || v_pk || ', ' || 's.' || REPLACE(:v_business_columns, ', ', ', s.') || '
            FROM ' || v_source || ' s
            LEFT JOIN ' || v_target || ' t
            ON t.' || v_pk || ' = s.' || v_pk || ' AND t.IS_ACTIVE = TRUE
            WHERE t.' || v_pk || ' IS NULL
              AND s.' || v_pk || ' IS NOT NULL
              AND s.' || v_pk || ' != ''''
              AND (SELECT COUNT(*) FROM ' || v_source || ' s2
              WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1';
  EXECUTE IMMEDIATE v_sql;
  v_inserts := SQLROWCOUNT;

  ---------------------------------------------
  -- 6. Updates uitvoeren
  -- Updates worden uitgevoerd afhankelijk van de update strategie.
  ---------------------------------------------
  -- Bij 'HISTORY' worden oude versies van rijen in de target op non actief gezet (IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP()) en wordt een nieuwe rij met de nieuwe waarde, IS_ACTIVE = TRUE en START_TS = CURRENT_TIMESTAMP() toegevoegd.
  IF (v_update_strategy = 'HISTORY') THEN
    v_sql := 'UPDATE ' || v_target || ' t
              SET IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP()
              WHERE t.IS_ACTIVE = TRUE
              AND EXISTS (
                SELECT 1
                FROM ' || v_source || ' s
                WHERE s.' || v_pk || ' = t.' || v_pk || '
                  AND s.' || v_pk || ' IS NOT NULL
                  AND s.' || v_pk || ' != ''''
                  AND s.ROW_HASH <> t.ROW_HASH
                  AND (SELECT COUNT(*) FROM ' || v_source || ' s2
                      WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1
              )';
    EXECUTE IMMEDIATE v_sql; 

    v_sql := 'INSERT INTO ' || v_target || ' (ROW_HASH, START_TS, IS_ACTIVE, CDC_OPERATION, ' || v_pk || ', ' || v_business_columns || ')
              SELECT s.ROW_HASH, CURRENT_TIMESTAMP(), TRUE, ''U'', s.' || v_pk || ', ' || 's.' || REPLACE(:v_business_columns, ', ', ', s.') || '
              FROM ' || v_source || ' s
              WHERE s.' || v_pk || ' IS NOT NULL
                AND s.' || v_pk || ' != ''''
                AND (SELECT COUNT(*) FROM ' || v_source || ' s2
                WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1
                AND NOT EXISTS (
                  SELECT 1
                  FROM ' || v_target || ' t
                  WHERE t.' || v_pk || ' = s.' || v_pk || '
                    AND t.IS_ACTIVE = TRUE
                    AND t.ROW_HASH = s.ROW_HASH
                )';
  -- Bij 'OVERWRITE' worden bestaande rijen in de target geupdate met de nieuwe waarde, IS_ACTIVE blijft TRUE en START_TS wordt bijgewerkt naar CURRENT_TIMESTAMP().
  ELSE
    v_sql := 'UPDATE ' || v_target || ' t
              SET ROW_HASH = s.ROW_HASH, START_TS = CURRENT_TIMESTAMP(), IS_ACTIVE = TRUE, CDC_OPERATION = ''U'', ' || v_pk || ' = s.' || v_pk || ', ' || :v_business_columns_update || '
              FROM ' || v_source || ' s
              WHERE t.' || v_pk || ' = s.' || v_pk || '
                AND s.' || v_pk || ' IS NOT NULL
                AND s.' || v_pk || ' != ''''
                AND t.IS_ACTIVE = TRUE
                AND t.ROW_HASH <> s.ROW_HASH
                AND (SELECT COUNT(*) FROM ' || v_source || ' s2
                    WHERE s2.' || v_pk || ' = s.' || v_pk || ') = 1';
  END IF;
  EXECUTE IMMEDIATE v_sql;
  v_updates := SQLROWCOUNT;

  ---------------------------------------------
  -- 7. Deletes
  -- Deletes worden uitgevoerd afhankelijk van de delete strategie.
  ---------------------------------------------
  -- Bij 'SOFT' worden rijen in de target op non actief gezet (IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP()).
  IF (v_delete_strategy = 'SOFT') THEN
      v_sql := 'UPDATE ' || v_target || ' t
      SET IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP(), CDC_OPERATION = ''D''
      WHERE t.IS_ACTIVE = TRUE
        AND NOT EXISTS (
          SELECT 1
          FROM ' || v_source || ' s
          WHERE s.' || v_pk || ' = t.' || v_pk || '
        )';
  -- Bij 'HARD' worden rijen fysiek verwijderd uit de target.
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
  -- 8. Run voltooien
  -- De log van de entiteit wordt bijgewerkt met het aantal inserts, updates, deletes, etc.
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

  COMMIT;
  RETURN 'Entity ' || v_entity || ' verwerkt.';

  EXCEPTION
    WHEN OTHER THEN
      ROLLBACK;
      UPDATE LOGGING.RUN_LOG 
      SET END_TS = CURRENT_TIMESTAMP(), STATUS = 'FAILED'
      WHERE RUN_ID = :v_runid;
      RETURN 'Fout tijdens verwerken van entiteit met naam ' || v_entity || ': ' || SQLERRM;

END;
$$;