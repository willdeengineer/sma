"""
-------------------------------------------------
CDC_RUN - Change Data Capture Runner voor Snowflake
-------------------------------------------------

Procedure om het CDC proces te runnen.

Deze procedure kan worden aangeroepen en voert het CDC proces uit 
voor alle entiteiten die voorkomen in CDC_CONFIG.

Voor elke actieve config in het CDC_CONFIG wordt CDC_PROCESS procedure 
aangeroepen die het CDC proces uitvoert voor die entiteit.
"""

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from datetime import datetime
from typing import Dict, Optional
import logging

from cdc_process import CDCProcess

# Logging configureren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCRun:
    """
    Procedure om het CDC proces te runnen voor alle actieve entiteiten.
    """
    
    def __init__(self, conn, database: str = 'CDC_TEST_DB'):
        """
        Initialiseer CDC Run met Snowflake connectie
        
        Args:
            conn: Snowflake connector verbinding
            database: Databasenaam (default: CDC_TEST_DB)
        """
        self.conn = conn
        self.cursor = conn.cursor()
        self.database = database
        self.cursor.execute(f"USE DATABASE {database}")
        self.cursor.execute("USE SCHEMA CDC")
    
    def _get_next_run_id(self) -> int:
        """
        Bepaal volgende RUN_ID (CDC_RUN stap 1)
        """
        self.cursor.execute("USE SCHEMA LOGGING")
        self.cursor.execute("SELECT COALESCE(MAX(RUN_ID), 0) + 1 FROM RUN_LOG")
        result = self.cursor.fetchone()
        return result[0] if result else 1
    
    def _add_row_hash_to_all_sources(self, run_id: int):
        """
        Voeg ROW_HASH kolom toe aan alle brontabellen (CDC_RUN stap 2-3)
        """
        logger.info("ROW_HASH kolommen toevoegen aan alle brontabellen")
        
        self.cursor.execute("USE SCHEMA CDC")
        self.cursor.execute("SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_CONFIG WHERE IS_ACTIVE = TRUE")
        configs = self.cursor.fetchall()
        
        for config_row in configs:
            source_table = config_row[1]
            logger.info(f"ROW_HASH toevoegen aan {source_table}")
            
            try:
                # -------------------------------------------------
                # Kolom toevoegen als die niet bestaat
                # -------------------------------------------------
                self.cursor.execute(f"ALTER TABLE {source_table} ADD COLUMN IF NOT EXISTS ROW_HASH STRING")
                
                # -------------------------------------------------
                # Hash berekenen
                # -------------------------------------------------
                sql = f"""
                UPDATE {source_table}
                SET ROW_HASH = SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)
                WHERE ROW_HASH IS NULL
                """
                self.cursor.execute(sql)
                
            except ProgrammingError as e:
                logger.error(f"Fout bij toevoegen ROW_HASH: {e}")
                raise
    
    def execute(self) -> Dict:
        """
        Voer het complete CDC proces uit voor alle actieve entiteiten (CDC_RUN)
        
        -------------------------------------------------
        Procedure om het CDC proces te runnen.
        Deze procedure voert het CDC proces uit 
        voor alle entiteiten die voorkomen in CDC_CONFIG.
        -------------------------------------------------
        
        Returns:
            Dictionary met run statistieken
        """
        logger.info("CDC_RUN starten")
        
        run_id = None
        try:
            # -------------------------------------------------
            # Run ID bepalen en run log initialiseren
            # -------------------------------------------------
            run_id = self._get_next_run_id()
            logger.info(f"Run ID: {run_id}")
            
            self.cursor.execute("USE SCHEMA LOGGING")
            self.cursor.execute(
                "INSERT INTO RUN_LOG (RUN_ID, START_TS, STATUS) VALUES (%s, %s, 'RUNNING')",
                (run_id, datetime.now())
            )
            self.conn.commit()
            
            # -------------------------------------------------
            # Voor alle actieve configs ROW_HASH kolom toevoegen
            # en hash berekenen
            # -------------------------------------------------
            self._add_row_hash_to_all_sources(run_id)
            
            # -------------------------------------------------
            # Voor elke actieve config in het CDC_CONFIG wordt CDC_PROCESS 
            # aangeroepen die het CDC proces uitvoert voor die entiteit.
            # -------------------------------------------------
            self.cursor.execute("USE SCHEMA CDC")
            self.cursor.execute("SELECT CONFIG_ID FROM CDC_CONFIG WHERE IS_ACTIVE = TRUE")
            configs = self.cursor.fetchall()
            
            cdc_process = CDCProcess(self.conn, self.database)
            
            for config_row in configs:
                config_id = config_row[0]
                success, msg = cdc_process.execute(config_id, run_id)
                if not success:
                    logger.warning(f"CDC_PROCESS mislukt voor config_id {config_id}: {msg}")
            
            cdc_process.close()
            
            # -------------------------------------------------
            # Na het uitvoeren van CDC_PROCESS voor alle entiteiten worden 
            # de totalen van inserts, updates, deletes, etc. in RUN_LOG 
            # bijgewerkt op basis van de gegevens in RUN_ENTITY_LOG.
            # -------------------------------------------------
            self.cursor.execute("USE SCHEMA LOGGING")
            self.cursor.execute(f"""
            UPDATE RUN_LOG
            SET
                ROWS_INSERTED = (SELECT COALESCE(SUM(ROWS_INSERTED), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                ROWS_UPDATED = (SELECT COALESCE(SUM(ROWS_UPDATED), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                ROWS_DELETED = (SELECT COALESCE(SUM(ROWS_DELETED), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                ROWS_UNCHANGED = (SELECT COALESCE(SUM(ROWS_UNCHANGED), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                DUPLICATE_INSERTS = (SELECT COALESCE(SUM(DUPLICATE_INSERTS), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                DUPLICATE_UPDATES = (SELECT COALESCE(SUM(DUPLICATE_UPDATES), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                KEY_ERRORS = (SELECT COALESCE(SUM(KEY_ERRORS), 0) FROM RUN_ENTITY_LOG WHERE RUN_ID = {run_id}),
                END_TS = CURRENT_TIMESTAMP(),
                STATUS = 'COMPLETED'
            WHERE RUN_ID = {run_id}
            """)
            
            self.conn.commit()
            
            # Haal final statistieken op
            self.cursor.execute(f"""
            SELECT ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNCHANGED, 
                   DUPLICATE_INSERTS, DUPLICATE_UPDATES, KEY_ERRORS
            FROM RUN_LOG WHERE RUN_ID = {run_id}
            """)
            
            result = self.cursor.fetchone()
            
            logger.info(f"CDC_RUN {run_id} succesvol voltooid")
            
            return {
                'RUN_ID': run_id,
                'ROWS_INSERTED': result[0],
                'ROWS_UPDATED': result[1],
                'ROWS_DELETED': result[2],
                'ROWS_UNCHANGED': result[3],
                'DUPLICATE_INSERTS': result[4],
                'DUPLICATE_UPDATES': result[5],
                'KEY_ERRORS': result[6],
                'STATUS': 'COMPLETED'
            }
            
        except Exception as e:
            logger.error(f"Fout tijdens CDC_RUN: {e}")
            # Update run status naar FAILED
            if run_id:
                try:
                    self.cursor.execute("USE SCHEMA LOGGING")
                    self.cursor.execute(f"UPDATE RUN_LOG SET STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP() WHERE RUN_ID = {run_id}")
                    self.conn.commit()
                except:
                    pass
            
            return {
                'STATUS': 'FAILED',
                'ERROR': str(e)
            }
    
    def close(self):
        """Sluit database verbinding"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Verbinding gesloten")


def main():
    """
    Ingang voor het CDC_RUN proces
    
    Voorbeeld:
        import snowflake.connector
        from cdc_run import CDCRun
        
        conn = snowflake.connector.connect(
            user='uw_username',
            password='uw_password',
            account='uw_account',
            warehouse='uw_warehouse'
        )
        
        cdc_run = CDCRun(conn)
        # Veronderstelt dat database en tabellen al zijn aangemaakt via setup.sql
        result = cdc_run.execute()
        print(result)
        cdc_run.close()
    """
    pass


if __name__ == '__main__':
    main()
