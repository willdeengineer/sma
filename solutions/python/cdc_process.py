"""
-------------------------------------------------
CDC_PROCESS - Change Data Capture Processor voor Snowflake
-------------------------------------------------

Deze procedure verwerkt wijzigingen van bron naar target 
volgens de CDC logica.

De procedure wordt aangeroepen vanuit CDC_RUN().
De procedure verwerkt de brondata volgens de configuratie in CDC_CONFIG 
en werkt de target bij met inserts, updates en deletes.

Daarnaast worden er specifieke errors gedetecteerd 
(duplicate inserts, duplicate updates, key errors) 
en gelogd in RUN_ERROR_LOG.

Na het verwerken van de brondata worden de statistieken van de run 
per entiteit (aantal inserts, updates, deletes, etc.) 
gelogd in RUN_ENTITY_LOG.
"""

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from datetime import datetime
from typing import Dict, Tuple, List, Optional
import logging

# Logging configureren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCProcess:
    """
    Procedure om wijzigingen van bron naar target te verwerken volgens de CDC logica.
    """
    
    def __init__(self, conn, database: str = 'CDC_TEST_DB'):
        """
        Initialiseer CDC Process met Snowflake connectie
        
        Args:
            conn: Snowflake connector verbinding
            database: Databasenaam (default: CDC_TEST_DB)
        """
        self.conn = conn
        self.cursor = conn.cursor()
        self.database = database
        self.cursor.execute(f"USE DATABASE {database}")
        self.cursor.execute("USE SCHEMA CDC")
    
    def _get_config(self, config_id: int) -> Optional[Dict]:
        """
        Haal configuratie voor een entiteit op (CDC_PROCESS stap 1)
        
        Config ophalen
        Config bevat informatie over een entiteit: de naam van bron en doeltabel, 
        de primaire sleutel en de strategieen voor deletes, errors en updates.
        """
        self.cursor.execute("USE SCHEMA CDC")
        sql = """
        SELECT ENTITY_NAME, PRIMARY_KEY_COLUMN, SOURCE_TABLE, TARGET_TABLE, 
               DELETE_STRATEGY, ERROR_STRATEGY, UPDATE_STRATEGY
        FROM CDC_CONFIG
        WHERE CONFIG_ID = %s AND IS_ACTIVE = TRUE
        """
        self.cursor.execute(sql, (config_id,))
        result = self.cursor.fetchone()
        
        if result:
            return {
                'entity_name': result[0],
                'pk_column': result[1],
                'source_table': result[2],
                'target_table': result[3],
                'delete_strategy': result[4],
                'error_strategy': result[5],
                'update_strategy': result[6]
            }
        return None
    
    def _get_business_columns(self, source_table: str, pk_column: str) -> List[str]:
        """
        Business kolommen van de entiteit ophalen (CDC_PROCESS stap 2)
        
        Alle kolommen behalve kolommen die we gebruiken voor CDC logica: 
        ROW_HASH, START_TS, END_TS, IS_ACTIVE, CDC_OPERATION en de primary key
        """
        self.cursor.execute("USE SCHEMA STAGING")
        
        # Parse schema en tabelnaam
        parts = source_table.split('.')
        table_name = parts[-1].upper() if parts else source_table.upper()
        
        sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
          AND COLUMN_NAME NOT IN ('ROW_HASH', 'START_TS', 'END_TS', 'IS_ACTIVE', 'CDC_OPERATION')
          AND COLUMN_NAME != %s
        ORDER BY ORDINAL_POSITION
        """
        self.cursor.execute(sql, (table_name, pk_column))
        results = self.cursor.fetchall()
        return [row[0] for row in results]
    
    def _add_row_hash_column(self, source_table: str):
        """
        Voeg ROW_HASH kolom toe aan brontabel indien deze niet bestaat
        
        ROW_HASH wordt gebruikt om te detecteren of een rij is gewijzigd.
        """
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
    
    def _detect_duplicate_inserts(self, config: Dict, run_id: int) -> int:
        """
        Duplicate inserts in staging detecteren (CDC_PROCESS stap 3.1)
        
        Duplicate inserts: zelfde PK, zelfde hash
        """
        logger.info(f"Duplicate inserts detecteren voor {config['entity_name']}")
        
        # -------------------------------------------------
        # Duplicate inserts in staging detecteren
        # (zelfde PK, zelfde hash)
        # -------------------------------------------------
        sql = f"""
        INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
        SELECT {run_id}, '{config['entity_name']}', 'DUPLICATE_INSERT', OBJECT_CONSTRUCT(*)
        FROM (
            SELECT s.*, COUNT(*) OVER (PARTITION BY s.{config['pk_column']}, s.ROW_HASH) AS CNT
            FROM {config['source_table']} s
            WHERE s.{config['pk_column']} IS NOT NULL AND s.ROW_HASH IS NOT NULL
        ) t
        WHERE t.CNT > 1
        """
        
        try:
            self.cursor.execute(sql)
            count = self.cursor.rowcount
            logger.info(f"Gevonden: {count} duplicate inserts")
            return count
        except ProgrammingError as e:
            logger.error(f"Fout bij detecteren duplicate inserts: {e}")
            return 0
    
    def _detect_duplicate_updates(self, config: Dict, run_id: int) -> int:
        """
        Duplicate updates in staging detecteren (CDC_PROCESS stap 3.2)
        
        Duplicate updates: zelfde PK, verschillende hash
        """
        logger.info(f"Duplicate updates detecteren voor {config['entity_name']}")
        
        # -------------------------------------------------
        # Duplicate updates in staging detecteren
        # (zelfde PK, verschillende hash)
        # -------------------------------------------------
        sql = f"""
        INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
        SELECT {run_id}, '{config['entity_name']}', 'DUPLICATE_UPDATE', OBJECT_CONSTRUCT(*)
        FROM {config['source_table']} s
        WHERE s.{config['pk_column']} IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM {config['source_table']} s2
              WHERE s2.{config['pk_column']} = s.{config['pk_column']}
                AND s2.ROW_HASH <> s.ROW_HASH
                AND s2.ROW_HASH IS NOT NULL
          )
        """
        
        try:
            self.cursor.execute(sql)
            count = self.cursor.rowcount
            logger.info(f"Gevonden: {count} duplicate updates")
            return count
        except ProgrammingError as e:
            logger.error(f"Fout bij detecteren duplicate updates: {e}")
            return 0
    
    def _detect_key_errors(self, config: Dict, run_id: int) -> int:
        """
        Key errors detecteren (CDC_PROCESS stap 3.3)
        
        Key errors: null of lege waarde in primary key
        """
        logger.info(f"Key errors detecteren voor {config['entity_name']}")
        
        # -------------------------------------------------
        # Key errors detecteren
        # (null of lege waarde in primary key)
        # -------------------------------------------------
        sql = f"""
        INSERT INTO LOGGING.RUN_ERROR_LOG (RUN_ID, ENTITY_NAME, ERROR_CODE, ERROR_ROW)
        SELECT {run_id}, '{config['entity_name']}', 'PRIMARY_KEY_ERROR', OBJECT_CONSTRUCT(*)
        FROM {config['source_table']} s
        WHERE s.{config['pk_column']} IS NULL OR s.{config['pk_column']} = ''
        """
        
        try:
            self.cursor.execute(sql)
            count = self.cursor.rowcount
            logger.info(f"Gevonden: {count} key errors")
            return count
        except ProgrammingError as e:
            logger.error(f"Fout bij detecteren key errors: {e}")
            return 0
    
    def _process_inserts(self, config: Dict, business_cols: List[str]) -> int:
        """
        Inserts uitvoeren (CDC_PROCESS stap 4)
        
        Inserts worden alleen uitgevoerd voor rijen zonder errors.
        """
        logger.info(f"Inserts verwerken voor {config['entity_name']}")
        
        cols_str = ', '.join([f'"{col}"' for col in business_cols])
        cols_select = ', '.join([f's."{col}"' for col in business_cols])
        
        # -------------------------------------------------
        # Inserts uitvoeren
        # Inserts worden alleen uitgevoerd voor rijen zonder errors.
        # -------------------------------------------------
        sql = f"""
        INSERT INTO {config['target_table']} (
            ROW_HASH, START_TS, IS_ACTIVE, CDC_OPERATION, {config['pk_column']}, {cols_str}
        )
        SELECT s.ROW_HASH, CURRENT_TIMESTAMP(), TRUE, 'I', s.{config['pk_column']}, {cols_select}
        FROM {config['source_table']} s
        LEFT JOIN {config['target_table']} t
            ON t.{config['pk_column']} = s.{config['pk_column']} AND t.IS_ACTIVE = TRUE
        WHERE t.{config['pk_column']} IS NULL
          AND s.{config['pk_column']} IS NOT NULL
          AND s.{config['pk_column']} != ''
          AND (SELECT COUNT(*) FROM {config['source_table']} s2
               WHERE s2.{config['pk_column']} = s.{config['pk_column']}) = 1
        """
        
        try:
            self.cursor.execute(sql)
            count = self.cursor.rowcount
            logger.info(f"Ingevoegd: {count} rijen")
            return count
        except ProgrammingError as e:
            logger.error(f"Fout bij verwerken inserts: {e}")
            return 0
    
    def _process_updates(self, config: Dict, business_cols: List[str]) -> int:
        """
        Updates uitvoeren (CDC_PROCESS stap 5)
        
        Updates worden uitgevoerd afhankelijk van de update strategie.
        - HISTORY: oude versies afsluiten, nieuwe versie toevoegen
        - OVERWRITE: bestaande rijen overschrijven
        """
        logger.info(f"Updates verwerken voor {config['entity_name']} met strategie: {config['update_strategy']}")
        
        cols_str = ', '.join([f'"{col}"' for col in business_cols])
        cols_select = ', '.join([f's."{col}"' for col in business_cols])
        
        updates_count = 0
        
        # -------------------------------------------------
        # Updates uitvoeren
        # Updates worden uitgevoerd afhankelijk van de update strategie.
        # -------------------------------------------------
        if config['update_strategy'] == 'HISTORY':
            # -------------------------------------------------
            # HISTORY strategie: oude versies afsluiten, nieuwe versie toevoegen
            # Bij 'HISTORY' worden oude versies van rijen in de target op non actief 
            # gezet (IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP()) en wordt een 
            # nieuwe rij met de nieuwe waarde, IS_ACTIVE = TRUE en 
            # START_TS = CURRENT_TIMESTAMP() toegevoegd.
            # -------------------------------------------------
            
            # Oude versies afsluiten
            sql = f"""
            UPDATE {config['target_table']} t
            SET IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP()
            WHERE t.IS_ACTIVE = TRUE
              AND EXISTS (
                  SELECT 1
                  FROM {config['source_table']} s
                  WHERE s.{config['pk_column']} = t.{config['pk_column']}
                    AND s.{config['pk_column']} IS NOT NULL
                    AND s.{config['pk_column']} != ''
                    AND s.ROW_HASH <> t.ROW_HASH
                    AND (SELECT COUNT(*) FROM {config['source_table']} s2
                         WHERE s2.{config['pk_column']} = s.{config['pk_column']}) = 1
              )
            """
            
            try:
                self.cursor.execute(sql)
            except ProgrammingError as e:
                logger.error(f"Fout bij afsluiten oude versies: {e}")
            
            # Nieuwe versies toevoegen
            sql = f"""
            INSERT INTO {config['target_table']} (
                ROW_HASH, START_TS, IS_ACTIVE, CDC_OPERATION, {config['pk_column']}, {cols_str}
            )
            SELECT s.ROW_HASH, CURRENT_TIMESTAMP(), TRUE, 'U', s.{config['pk_column']}, {cols_select}
            FROM {config['source_table']} s
            WHERE s.{config['pk_column']} IS NOT NULL
              AND s.{config['pk_column']} != ''
              AND (SELECT COUNT(*) FROM {config['source_table']} s2
                   WHERE s2.{config['pk_column']} = s.{config['pk_column']}) = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM {config['target_table']} t
                  WHERE t.{config['pk_column']} = s.{config['pk_column']}
                    AND t.IS_ACTIVE = TRUE
                    AND t.ROW_HASH = s.ROW_HASH
              )
            """
            
        else:
            # -------------------------------------------------
            # OVERWRITE strategie: bestaande rijen overschrijven
            # Bij 'OVERWRITE' worden bestaande rijen in de target geupdate 
            # met de nieuwe waarde, IS_ACTIVE blijft TRUE en START_TS wordt 
            # bijgewerkt naar CURRENT_TIMESTAMP().
            # -------------------------------------------------
            sql = f"""
            UPDATE {config['target_table']} t
            SET ROW_HASH = s.ROW_HASH, START_TS = CURRENT_TIMESTAMP(), 
                IS_ACTIVE = TRUE, CDC_OPERATION = 'U',
                {', '.join([f'{col} = s.{col}' for col in business_cols])}
            FROM {config['source_table']} s
            WHERE t.{config['pk_column']} = s.{config['pk_column']}
              AND s.{config['pk_column']} IS NOT NULL
              AND s.{config['pk_column']} != ''
              AND t.IS_ACTIVE = TRUE
              AND t.ROW_HASH <> s.ROW_HASH
              AND (SELECT COUNT(*) FROM {config['source_table']} s2
                   WHERE s2.{config['pk_column']} = s.{config['pk_column']}) = 1
            """
        
        try:
            self.cursor.execute(sql)
            updates_count = self.cursor.rowcount
            logger.info(f"Verwerkt: {updates_count} updates")
            return updates_count
        except ProgrammingError as e:
            logger.error(f"Fout bij verwerken updates: {e}")
            return 0
    
    def _process_deletes(self, config: Dict) -> int:
        """
        Deletes uitvoeren (CDC_PROCESS stap 6)
        
        Deletes worden uitgevoerd afhankelijk van de delete strategie.
        - SOFT: rijen op non actief zetten (IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP())
        - HARD: rijen fysiek verwijderen
        """
        logger.info(f"Deletes verwerken voor {config['entity_name']} met strategie: {config['delete_strategy']}")
        
        # -------------------------------------------------
        # Deletes
        # Deletes worden uitgevoerd afhankelijk van de delete strategie.
        # -------------------------------------------------
        if config['delete_strategy'] == 'SOFT':
            # -------------------------------------------------
            # SOFT delete: rijen op non actief zetten
            # (IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP())
            # -------------------------------------------------
            sql = f"""
            UPDATE {config['target_table']} t
            SET IS_ACTIVE = FALSE, END_TS = CURRENT_TIMESTAMP(), CDC_OPERATION = 'D'
            WHERE t.IS_ACTIVE = TRUE
              AND NOT EXISTS (
                  SELECT 1
                  FROM {config['source_table']} s
                  WHERE s.{config['pk_column']} = t.{config['pk_column']}
              )
            """
        else:
            # -------------------------------------------------
            # HARD delete: rijen fysiek verwijderen
            # -------------------------------------------------
            sql = f"""
            DELETE FROM {config['target_table']} t
            WHERE t.IS_ACTIVE = TRUE
              AND NOT EXISTS (
                  SELECT 1
                  FROM {config['source_table']} s
                  WHERE s.{config['pk_column']} = t.{config['pk_column']}
              )
            """
        
        try:
            self.cursor.execute(sql)
            count = self.cursor.rowcount
            logger.info(f"Verwerkt: {count} deletes")
            return count
        except ProgrammingError as e:
            logger.error(f"Fout bij verwerken deletes: {e}")
            return 0
    
    def _count_unchanged_rows(self, config: Dict) -> int:
        """
        Tel rijen die niet zijn gewijzigd (CDC_PROCESS stap 7)
        """
        sql = f"""
        SELECT COUNT(*)
        FROM {config['source_table']} s
        WHERE EXISTS (
            SELECT 1
            FROM {config['target_table']} t
            WHERE t.{config['pk_column']} = s.{config['pk_column']}
              AND t.IS_ACTIVE = TRUE
              AND t.ROW_HASH = s.ROW_HASH
        )
        """
        
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except ProgrammingError as e:
            logger.error(f"Fout bij tellen ongewijzigde rijen: {e}")
            return 0
    
    def execute(self, config_id: int, run_id: int) -> Tuple[bool, str]:
        """
        Voer CDC_PROCESS uit voor een enkele entiteit
        
        Args:
            config_id: Configuratie ID voor de entiteit
            run_id: Huidige run ID
            
        Returns:
            Tuple van (success, message)
        """
        logger.info(f"CDC_PROCESS starten voor config_id {config_id}")
        
        config = self._get_config(config_id)
        if not config:
            error_msg = f'Fout: config met id {config_id} niet gevonden of niet actief.'
            logger.error(error_msg)
            return False, error_msg
        
        try:
            start_ts = datetime.now()
            
            # -------------------------------------------------
            # Run initialiseren
            # -------------------------------------------------
            
            # ROW_HASH kolom toevoegen
            self._add_row_hash_column(config['source_table'])
            
            # Business kolommen ophalen
            business_cols = self._get_business_columns(config['source_table'], config['pk_column'])
            
            # -------------------------------------------------
            # Errors detecteren
            # -------------------------------------------------
            dup_inserts = self._detect_duplicate_inserts(config, run_id)
            dup_updates = self._detect_duplicate_updates(config, run_id)
            key_errors = self._detect_key_errors(config, run_id)
            
            # -------------------------------------------------
            # Operaties verwerken
            # -------------------------------------------------
            inserts = self._process_inserts(config, business_cols)
            updates = self._process_updates(config, business_cols)
            deletes = self._process_deletes(config)
            unchanged = self._count_unchanged_rows(config)
            
            # -------------------------------------------------
            # Run voltooien
            # De log van de entiteit wordt bijgewerkt met het aantal 
            # inserts, updates, deletes, etc.
            # -------------------------------------------------
            self.cursor.execute("USE SCHEMA LOGGING")
            self.cursor.execute("""
            INSERT INTO RUN_ENTITY_LOG (
                RUN_ID, START_TS, END_TS, ENTITY_NAME, 
                ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNCHANGED,
                DUPLICATE_INSERTS, DUPLICATE_UPDATES, KEY_ERRORS
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (run_id, start_ts, datetime.now(), config['entity_name'],
                  inserts, updates, deletes, unchanged, 
                  dup_inserts, dup_updates, key_errors))
            
            self.conn.commit()
            
            msg = f"Entity {config['entity_name']} verwerkt."
            logger.info(msg)
            return True, msg
            
        except Exception as e:
            logger.error(f"Fout bij verwerken entiteit: {e}")
            self.conn.rollback()
            return False, str(e)
    
    def close(self):
        """Sluit database verbinding"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Verbinding gesloten")
