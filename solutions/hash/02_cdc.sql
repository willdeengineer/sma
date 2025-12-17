USE SCHEMA TEST;

DECLARE
  run_id NUMBER;
  entity_name STRING := 'ENTITY';
  inserts_count NUMBER := 0;
  updates_count NUMBER := 0;
  deletes_count NUMBER := 0;
  dup_insert_count NUMBER := 0;
  dup_update_count NUMBER := 0;
  dup_no_change_count NUMBER := 0;
  error_count NUMBER := 0;
BEGIN

SELECT MAX(RUN_ID) INTO :run_id FROM STAGE_ENTITY;

--
-- No change detecteren
--
  SELECT COUNT(*) INTO :dup_no_change_count
  FROM STAGE_ENTITY s
  WHERE s.RUN_ID = run_id
    AND EXISTS (
      SELECT 1
      FROM TARGET_ENTITY t
      WHERE t.ENTITY_ID = s.ENTITY_ID
        AND t.IS_ACTIVE = TRUE
        AND t.ROW_HASH = s.ROW_HASH
    );

--
-- Duplicaat insert
--
  INSERT INTO ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
  SELECT
    :run_id,
    :entity_name,
    'DUPLICATE_INSERT',
    OBJECT_CONSTRUCT(
      'ENTITY_ID', ENTITY_ID,
      'ROW_HASH', ROW_HASH,
      'NAME', NAME,
      'SALARY', SALARY,
      'DUPLICATE_COUNT', cnt
    )
  FROM (
    SELECT
      s.ENTITY_ID,
      s.ROW_HASH,
      s.NAME,
      s.SALARY,
      COUNT(*) as cnt
    FROM STAGE_ENTITY s
    WHERE s.RUN_ID = run_id
      AND NOT EXISTS (
        SELECT 1
        FROM TARGET_ENTITY t
        WHERE t.ENTITY_ID = s.ENTITY_ID
          AND t.IS_ACTIVE = TRUE
      )
    GROUP BY s.ENTITY_ID, s.ROW_HASH, s.NAME, s.SALARY
    HAVING COUNT(*) > 1
  );

  dup_insert_count := SQLROWCOUNT;

--
-- Inserts
--
  INSERT INTO TARGET_ENTITY (
    ENTITY_ID, ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION,
    NAME, SALARY
  )
  SELECT DISTINCT
    s.ENTITY_ID,
    s.ROW_HASH,
    CURRENT_TIMESTAMP(), NULL, TRUE, 'I',
    s.NAME, s.SALARY
  FROM STAGE_ENTITY s
  WHERE s.RUN_ID = run_id
    AND NOT EXISTS (
      SELECT 1
      FROM TARGET_ENTITY t
      WHERE t.ENTITY_ID = s.ENTITY_ID
        AND t.IS_ACTIVE = TRUE
    )
    AND (
      SELECT COUNT(*)
      FROM STAGE_ENTITY s2
      WHERE s2.RUN_ID = s.RUN_ID
        AND s2.ENTITY_ID = s.ENTITY_ID
    ) = 1;

  inserts_count := SQLROWCOUNT;

--
-- Updates
--

-- Duplicaat update in
  INSERT INTO ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
  SELECT
    :run_id,
    :entity_name,
    'DUPLICATE_UPDATE',
    OBJECT_CONSTRUCT(
      'ENTITY_ID', s.ENTITY_ID,
      'ROW_HASH', s.ROW_HASH,
      'NAME', s.NAME,
      'SALARY', s.SALARY
    )
  FROM STAGE_ENTITY s
  WHERE s.RUN_ID = run_id
    AND EXISTS (
      SELECT 1
      FROM TARGET_ENTITY t
      WHERE t.ENTITY_ID = s.ENTITY_ID
        AND t.IS_ACTIVE = TRUE
    )
    AND (
      SELECT COUNT(DISTINCT ROW_HASH)
      FROM STAGE_ENTITY s2
      WHERE s2.RUN_ID = s.RUN_ID
        AND s2.ENTITY_ID = s.ENTITY_ID
    ) > 1;

  dup_update_count := SQLROWCOUNT;

-- Update de oude versie
  UPDATE TARGET_ENTITY t
  SET
    END_TS = CURRENT_TIMESTAMP(),
    IS_ACTIVE = FALSE
  WHERE t.IS_ACTIVE = TRUE
    AND EXISTS (
      SELECT 1
      FROM STAGE_ENTITY s
      WHERE s.RUN_ID    = run_id
        AND s.ENTITY_ID = t.ENTITY_ID
        AND s.ROW_HASH <> t.ROW_HASH
    );

-- Insert geupdate versie
  INSERT INTO TARGET_ENTITY (
    ENTITY_ID, ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION,
    NAME, SALARY
  )
  SELECT DISTINCT
    s.ENTITY_ID,
    s.ROW_HASH,
    CURRENT_TIMESTAMP(), NULL, TRUE, 'U',
    s.NAME, s.SALARY
  FROM STAGE_ENTITY s
  WHERE s.RUN_ID = run_id
    AND EXISTS (
      SELECT 1
      FROM TARGET_ENTITY t
      WHERE t.ENTITY_ID = s.ENTITY_ID
        AND t.IS_ACTIVE = FALSE
        AND t.ROW_HASH <> s.ROW_HASH
    )
    AND NOT EXISTS (
      SELECT 1
      FROM TARGET_ENTITY t2
      WHERE t2.ENTITY_ID = s.ENTITY_ID
        AND t2.IS_ACTIVE = TRUE
        AND t2.ROW_HASH = s.ROW_HASH
    )
    AND (
      SELECT COUNT(*)
      FROM STAGE_ENTITY s2
      WHERE s2.RUN_ID = s.RUN_ID
        AND s2.ENTITY_ID = s.ENTITY_ID
        AND EXISTS (
          SELECT 1
          FROM TARGET_ENTITY t3
          WHERE t3.ENTITY_ID = s2.ENTITY_ID
            AND t3.IS_ACTIVE = FALSE
            AND t3.ROW_HASH <> s2.ROW_HASH
        )
    ) = 1;

  updates_count := SQLROWCOUNT;

--
-- Deletes
--
  UPDATE TARGET_ENTITY t
  SET
    END_TS        = CURRENT_TIMESTAMP(),
    IS_ACTIVE     = FALSE,
    CDC_OPERATION = 'D'
  WHERE t.IS_ACTIVE = TRUE
    AND NOT EXISTS (
      SELECT 1
      FROM STAGE_ENTITY s
      WHERE s.RUN_ID    = run_id
        AND s.ENTITY_ID = t.ENTITY_ID
    );

  deletes_count := SQLROWCOUNT;

--
-- Run loggen
--
error_count := dup_insert_count + dup_update_count;

  INSERT INTO RUN_LOG (
    RUN_ID, ENTITY_NAME, INSERTS, UPDATES, DELETES, 
    DUP_INSERT, DUP_UPDATE, DUP_NO_CHANGE, ERRORS, RUN_TS
  )
  VALUES (
    :run_id,
    'ENTITY',
    :inserts_count,
    :updates_count,
    :deletes_count,
    :dup_insert_count,
    :dup_update_count,
    :dup_no_change_count,
    :error_count,
    CURRENT_TIMESTAMP()
  );

  RETURN OBJECT_CONSTRUCT(
      'RUN_ID', :run_id,
      'INSERTS', :inserts_count,
      'UPDATES', :updates_count,
      'DELETES', :deletes_count,
      'DUP_INSERT', :dup_insert_count,
      'DUP_UPDATE', :dup_update_count,
      'DUP_NO_CHANGE', :dup_no_change_count,
      'ERRORS', :error_count
  );

END;
