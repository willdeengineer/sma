# Snowflake CDC component

Dit Proof of Technology implementeert een configureerbaar Change Data Capture (CDC) proces in Snowflake.

## Installatie
```bash
# Clone de repository
git clone https://github.com/willdeengineer/sma.git
cd sma
```

## Overzicht

De oplossing bestaat uit de volgende onderdelen:

### 1. **CDC_CONFIG**
Dit is een configuratietabel die bepaalt hoe het CDC proces werkt voor verschillende entiteiten:

**Opties:**
- **ENTITY_NAME**: Naam van de entiteit
- **SOURCE_TABLE**: Naam van de bron tabel
- **TARGET_TABLE**: Naam van de doel tabel (SCD Type 2)
- **PRIMARY_KEY_COLUMN**: Primary key kolom van deze entiteit
- **DELETE_STRATEGY**: 
    - `SOFT`: Records blijven bestaan met `IS_ACTIVE = FALSE` en `END_TS` op einddatum
    - `HARD`: Records worden fysiek verwijderd en bestaat dus niet meer in de doel tabel
- **ERROR_STRATEGY**:
    - `CONTINUE`: Log fouten en ga door met het proces
    - `STOP`: Stop het proces onmiddellijk wanneer een fout wordt gedetecteerd
- **IS_ACTIVE**: Of deze configuratie actief is of niet

### 2. **RUN_LOG**
Logging van elke CDC run:

**Dit wordt getracked per run:**
- Aantal inserts, updates, deletes
- Aantal duplicaten en ongewijzigde records
- Status (RUNNING, COMPLETED, FAILED) van een run
- Duur van de run
- Errors die hebben plaatsgevonden

### 3. **ERROR_LOG**
Error logging:

**Error types:**
- `DUPLICATE_INSERT`: Meerdere gelijke records in staging
- `DUPLICATE_UPDATE`: Meerdere verschillende updates voor hetzelfde record
- `CRITICAL`: Proces blokkerende fouten (TO DO)

### 4. **CDC_PROCESS** Stored Procedure
Stored procedure die werkt op basis van de configuratie.

## Bestandsstructuur

### SQL component

```
solutions/sql/
├── setup.sql                 # Basis structuur (tables, sequences)
├── loader.sql                # Data preparator (voegt run id en hash toe)
├── cdc.sql                   # CDC_PROCESS stored procedure
└── config.sql                # Configuratie voor entiteiten
```

### Python component

```
solutions/python/
└── main.ipynb
```

## Opzetten & Configuratie

### Setup

```sql
-- 1. Maak de basis structuur aan
RUN setup.sql

-- 2. Configureer de entiteiten
RUN config.sql
```

### Configuratie toevoegen (voorbeeld)

```sql
INSERT INTO CDC_CONFIG (
        ENTITY_NAME,
        SOURCE_TABLE,
        TARGET_TABLE,
        PRIMARY_KEY_COLUMNS,
        HASH_COLUMN,
        DELETE_STRATEGY,
        ERROR_STRATEGY,
        BUSINESS_COLUMNS,
        IS_ACTIVE
) VALUES (
        'EMPLOYEE',
        'STAGE_EMPLOYEE',
        'TARGET_EMPLOYEE',
        'EMPLOYEE_ID',
        'ROW_HASH',
        'SOFT',
        'CONTINUE',
        'NAME,DEPARTMENT,SALARY',
        TRUE
);
```

## CDC proces uitvoeren

### Data laden in staging

### CDC proces starten

```sql
CALL CDC_PROCESS_SQL('EMPLOYEE');
```

### Resultaat (voorbeeld)

```
RUN_ID | RUN_TS                  | ENTITY_NAME | INSERTS | UPDATES | DELETES | DUP_INSERT | DUP_UPDATE | DUP_NO_CHANGE | ERRORS | STATUS    | DURATION_SECONDS
-------|-------------------------|-------------|---------|---------|---------|------------|------------|---------------|--------|-----------|------------------
1      | 2025-12-23 10:15:32.000 | EMPLOYEE    | 150     | 23      | 5       | 2          | 1          | 8             | 0      | COMPLETED | 45
2      | 2025-12-24 10:15:32.000 | EMPLOYEE    | 23      | 2       | 8       | 0          | 1          | 1             | 4      | FAILED    | 32
```

## Belangrijkste Features

### 1. Delete strategy

**SOFT delete**
```sql
DELETE_STRATEGY = 'SOFT'
```
- `IS_ACTIVE` wordt `FALSE`
- `CDC_OPERATION` wordt `'D'`
- `END_TS` wordt een einddatum
- Alle historische data blijft bewaard
- Voor bv. auditing

**HARD delete**
```sql
DELETE_STRATEGY = 'HARD'
```
- Records worden fysiek verwijderd
- Geen historische data beschikbaar
- Voor data minimalisatie en/of wet en regelgevingen

### 2. Error handling

**CONTINUE**
```sql
ERROR_STRATEGY = 'CONTINUE'
```
- Fouten worden gelogd in ERROR_LOG
- Proces gaat door met volgende stappen
- Geschikt voor productie met monitoring

**STOP**
```sql
ERROR_STRATEGY = 'STOP'
```
- Het proces stopt bij eerste gevonden fout
- Rollback van wijzigingen
- Geschikt voor kritieke data

