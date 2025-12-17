USE SCHEMA TEST;

SET run_id = 138;

--           --
-- Uitvoeren --
--           --

BEGIN;
-- Insert uitvoeren
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
WHERE s.RUN_ID = $run_id
    AND NOT EXISTS (
        SELECT 1
        FROM TARGET_ENTITY t
        WHERE t.ENTITY_ID = s.ENTITY_ID
        AND t.IS_ACTIVE = TRUE
    );
-- Insert uitvoeren klaar

-- Update uitvoeren
UPDATE TARGET_ENTITY t
SET 
    t.END_TS   = CURRENT_TIMESTAMP(),
    t.IS_ACTIVE     = FALSE
WHERE t.IS_ACTIVE = TRUE
  AND EXISTS (
        SELECT 1
        FROM STAGE_ENTITY s
        WHERE s.RUN_ID     = $run_id
          AND s.ENTITY_ID  = t.ENTITY_ID
          AND s.ROW_HASH  <> t.ROW_HASH
  );

INSERT INTO TARGET_ENTITY (
    ENTITY_ID, ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION, NAME, SALARY
)
SELECT DISTINCT
    s.ENTITY_ID,
    s.ROW_HASH,
    CURRENT_TIMESTAMP(), NULL, TRUE, 'U',
    s.NAME, s.SALARY
FROM STAGE_ENTITY s
WHERE s.RUN_ID = $run_id
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
    );
-- Update uitvoeren klaar

-- Delete uitvoeren
UPDATE TARGET_ENTITY
SET 
    END_TS = CURRENT_TIMESTAMP(),
    IS_ACTIVE = FALSE,
    CDC_OPERATION = 'D'
WHERE IS_ACTIVE = TRUE
    AND NOT EXISTS (
        SELECT 1
        FROM STAGE_ENTITY s
        WHERE s.RUN_ID = $run_id
            AND s.ENTITY_ID = TARGET_ENTITY.ENTITY_ID
    );
-- Delete uitvoeren klaar

-- Log naar RUN_LOG tabel
INSERT INTO RUN_LOG (
    RUN_ID,
    RUN_TS,
    ENTITY_NAME,
    INSERTS,
    UPDATES,
    DELETES,
    SUMMARY_JSON
)
SELECT
    $run_id,
    CURRENT_TIMESTAMP(),
    'ENTITY',
    (SELECT COUNT(*) FROM TARGET_ENTITY WHERE CDC_OPERATION = 'I' AND START_TS >= (SELECT MAX(RUN_TS) FROM RUN_LOG WHERE RUN_ID < $run_id)),
    (SELECT COUNT(*) FROM TARGET_ENTITY WHERE CDC_OPERATION = 'U' AND START_TS >= (SELECT MAX(RUN_TS) FROM RUN_LOG WHERE RUN_ID < $run_id)),
    (SELECT COUNT(*) FROM TARGET_ENTITY WHERE CDC_OPERATION = 'D' AND END_TS >= (SELECT MAX(RUN_TS) FROM RUN_LOG WHERE RUN_ID < $run_id)),
    OBJECT_CONSTRUCT(
        'run_id', $run_id,
        'timestamp', CURRENT_TIMESTAMP(),
        'operations', ARRAY_CONSTRUCT('INSERT', 'UPDATE', 'DELETE')
    );
COMMIT;


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
    );

  inserts_count := SQLROWCOUNT;

--
-- Updates
--

-- Update de oude versie
  UPDATE TARGET_ENTITY t
  SET
    END_TS        = CURRENT_TIMESTAMP(),
    IS_ACTIVE     = FALSE,
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
    );

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
  INSERT INTO RUN_LOG (
    RUN_ID, ENTITY_NAME, INSERTS, UPDATES, DELETES, RUN_TS
  )
  VALUES (
    :run_id,
    'ENTITY',
    :inserts_count,
    :updates_count,
    :deletes_count,
    CURRENT_TIMESTAMP()
  );

  RETURN OBJECT_CONSTRUCT(
      'RUN_ID', :run_id,
      'INSERTS', :inserts_count,
      'UPDATES', :updates_count,
      'DELETES', :deletes_count
  );

END;




