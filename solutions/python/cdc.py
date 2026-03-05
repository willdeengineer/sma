import snowflake.connector
from datetime import datetime

def process_cdc(conn):
    """Simple CDC process"""
    cursor = conn.cursor()
    
    # Setup
    cursor.execute("USE DATABASE DUP_DB")
    cursor.execute("USE SCHEMA SMA")
    
    # Get run ID
    cursor.execute("SELECT run_seq.NEXTVAL")
    run_id = cursor.fetchone()[0]
    
    # Log start
    cursor.execute(
        "INSERT INTO RUN_LOG (RUN_ID, RUN_START_TS, STATUS) VALUES (?, ?, 'RUNNING')",
        (run_id, datetime.now())
    )
    
    # Detect no changes
    cursor.execute("SELECT COUNT(*) FROM STAGING.STAGE_ENTITY s WHERE s.RUN_ID = ? AND EXISTS (SELECT 1 FROM TARGET.TARGET_ENTITY t WHERE t.KLANT_ID = s.KLANT_ID AND t.IS_ACTIVE = TRUE AND t.ROW_HASH = s.ROW_HASH)", (run_id,))
    no_change_count = cursor.fetchone()[0]
    
    # Insert new records
    cursor.execute("""
        INSERT INTO TARGET.TARGET_ENTITY (KLANT_ID, ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION, BSN, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, BIRTH_PLACE, NATIONALITY, MARITAL_STATUS, EMAIL, PHONE_MOBILE)
        SELECT DISTINCT s.KLANT_ID, s.ROW_HASH, ?, NULL, TRUE, 'I', s.BSN, s.FIRST_NAME, s.LAST_NAME, s.DATE_OF_BIRTH, s.BIRTH_PLACE, s.NATIONALITY, s.MARITAL_STATUS, s.EMAIL, s.PHONE_MOBILE
        FROM STAGING.STAGE_ENTITY s
        WHERE s.RUN_ID = ? AND NOT EXISTS (SELECT 1 FROM TARGET.TARGET_ENTITY t WHERE t.KLANT_ID = s.KLANT_ID AND t.IS_ACTIVE = TRUE)
    """, (datetime.now(), run_id))
    inserts_count = cursor.rowcount
    
    # Close old versions
    cursor.execute("""
        UPDATE TARGET.TARGET_ENTITY t
        SET END_TS = ?, IS_ACTIVE = FALSE
        WHERE t.IS_ACTIVE = TRUE AND EXISTS (SELECT 1 FROM STAGING.STAGE_ENTITY s WHERE s.RUN_ID = ? AND s.KLANT_ID = t.KLANT_ID AND s.ROW_HASH <> t.ROW_HASH)
    """, (datetime.now(), run_id))
    
    # Insert updates
    cursor.execute("""
        INSERT INTO TARGET.TARGET_ENTITY (KLANT_ID, ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION, BSN, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, BIRTH_PLACE, NATIONALITY, MARITAL_STATUS, EMAIL, PHONE_MOBILE)
        SELECT DISTINCT s.KLANT_ID, s.ROW_HASH, ?, NULL, TRUE, 'U', s.BSN, s.FIRST_NAME, s.LAST_NAME, s.DATE_OF_BIRTH, s.BIRTH_PLACE, s.NATIONALITY, s.MARITAL_STATUS, s.EMAIL, s.PHONE_MOBILE
        FROM STAGING.STAGE_ENTITY s
        WHERE s.RUN_ID = ? AND NOT EXISTS (SELECT 1 FROM TARGET.TARGET_ENTITY t WHERE t.KLANT_ID = s.KLANT_ID AND t.IS_ACTIVE = TRUE AND t.ROW_HASH = s.ROW_HASH)
    """, (datetime.now(), run_id))
    updates_count = cursor.rowcount
    
    # Soft delete
    cursor.execute("""
        UPDATE TARGET.TARGET_ENTITY t
        SET END_TS = ?, IS_ACTIVE = FALSE, CDC_OPERATION = 'D'
        WHERE t.IS_ACTIVE = TRUE AND NOT EXISTS (SELECT 1 FROM STAGING.STAGE_ENTITY s WHERE s.RUN_ID = ? AND s.KLANT_ID = t.KLANT_ID)
    """, (datetime.now(), run_id))
    deletes_count = cursor.rowcount
    
    # Log completion
    cursor.execute("""
        UPDATE RUN_LOG
        SET RUN_END_TS = ?, INSERTS = ?, UPDATES = ?, DELETES = ?, NO_CHANGE = ?, STATUS = 'COMPLETED'
        WHERE RUN_ID = ?
    """, (datetime.now(), inserts_count, updates_count, deletes_count, no_change_count, run_id))
    
    conn.commit()
    
    return {
        'RUN_ID': run_id,
        'INSERTS': inserts_count,
        'UPDATES': updates_count,
        'DELETES': deletes_count,
        'NO_CHANGES': no_change_count
    }