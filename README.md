# Snowflake CDC Component

Dit project is een Proof of Technology van het Change Data Capture (CDC) proces in Snowflake, met twee varianten:

- SQL variant (stored procedure)
- Python variant (jupyter notebook)

 Beide varianten voeren het CDC proces uit.

 Het proces is volledig configureerbaar zonder dat code gewijzigd hoeft te worden.

 ## Installatie
 ```
git clone https://github.com/willdeengineer/sma.git
cd sma
 ```

## Doel

Het CDC proces leest data uit `STAGING` en vergelijkt records in `TARGET` en verwerkt:

- inserts
- updates
- deletes
- datakwaliteitsfouten (voor nu alleen duplicaten en missende primaire sleutels)

Alle runs en fouten worden gelogd in `LOGGING`.

## Projectstructuur

```text
.
├── README.md
├── sql.sql
├── helper/
│   ├── clear.sql (leegmaken van een database en diens tabellen)
│   ├── generate_mock.py (script om determenistische mock data te genereren)
│   └── results.sql (script om de resultaten van runs te bekijken)
├── mock_data/
│   └── Verschillende mock data bestanden die zijn aangemaakt door generate_mock.py
├── solutions/
│   ├── python/
│   │   ├── cdc_process.ipynb (het cdc proces in Python/Jupyter notebook)
│   │   └── setup_py.sql (queries om de database, schema's en tabellen aan te maken)
│   └── sql/
│       ├── setup_sql.sql (queries om de database, schema's en tabellen aan te maken)
│       ├── cdc_process.sql (queries om het cdc_proces procedure aan te maken)
│       ├── cdc_run.sql (queries om de cdc_run procedure aan te maken)
│       └── start.sql (query om een run te starten a.d.h.v. de procedures)
├── tests/
│   ├── data_quality_test.sql (queries om te testen op data kwaliteit)
│   └── performance_test.sql (queries om te testen op performance)
└── uml/
    ├── lvl1.puml (oude architectuur)
    ├── lvl2.puml (oude architectuur)
    └── usecase.puml (oude usecase)
```

## CDC component

### Configuratie (`CDC.CDC_CONFIG`)

- `ENTITY_NAME`: naam van een entiteit
- `SOURCE_TABLE`: brontabel, bijvoorbeeld `STAGING.S_Employee`
- `TARGET_TABLE`: doeltabel, bijvoorbeeld `TARGET.T_Employee`
- `PRIMARY_KEY_COLUMN`: primary key kolom
- `DELETE_STRATEGY`: `SOFT` of `HARD`
- `ERROR_STRATEGY`: `CONTINUE` of `STOP` (niet uitgewerkt)
- `UPDATE_STRATEGY`: `HISTORY` of `OVERWRITE`
- `IS_ACTIVE`: of de configuratie actief is en mee moet worden genomen in runs

### Logging

- `LOGGING.RUN_LOG`: alle belangrijke gegevens van een run, zoals start, eind, aantal inserts/updates/deletes etc.
- `LOGGING.RUN_ENTITY_LOG`: statistieken per entiteit
- `LOGGING.RUN_ERROR_LOG`: specifieke detailregels van fouten

### Strategieen

#### Update strategie
- `HISTORY` oude actieve rij afsluiten (`IS_ACTIVE = FALSE`, `END_TS`, `CDC_OPERATION = 'U'`), nieuwe versie toevoegen (`IS_ACTIVE = TRUE`, `START_TS`, `CDC_OPERATION = 'U'`)
- `OVERWRITE` de actieve rij direct overschrijven

#### Delete strategie
- `SOFT` record blijft bestaan met `CDC_OPERATION = 'D'` voor historie
- `HARD` de actieve rij wordt "fysiek" verwijderd

## Benodigdheden

- Snowflake account
- Warehouse
- Voor Python variant:
  - Python 3.14.4
  - Jupyter
  - snowflake-connector-python
  - Voor mock data genereren: `pandas`, `numpy` (pandas installeert numpy vaak automatisch mee)

## Setup en run (SQL)

Voer scripts uit in deze volgorde:

### 1. Database, schema's en tabellen aanmaken

```sql
-- file: solutions/sql/setup_sql.sql
```

### 2. CDC procedure aanmaken

```sql
-- file: solutions/sql/cdc_process.sql
```

### 3. Run procedure aanmaken

```sql
-- file: solutions/sql/cdc_run.sql
```

### 4. Configuratie toevoegen (voorbeeld)

```sql
USE DATABASE CDC_SQL_DB

INSERT INTO CDC.CDC_CONFIG (
    CONFIG_ID, ENTITY_NAME, SOURCE_TABLE, TARGET_TABLE, PRIMARY_KEY_COLUMN,
    DELETE_STRATEGY, ERROR_STRATEGY, UPDATE_STRATEGY, IS_ACTIVE
) VALUES (
    1, 'Employee', 'STAGING.S_Employee', 'TARGET.T_Employee', 'EMPLOYEE_ID',
    'SOFT', 'CONTINUE', 'HISTORY', TRUE
);
```

### 5. Brondata inladen in `STAGING` (handmatig of via `COPY INTO` vanuit bv. een stage)
Hiervoor kan je de data uit mock_data gebruiken of zelf mock data genereren.

### 6. Run starten

```sql
-- file: solutions/sql/start.sql
```

## Setup en run (Python)

### 1. Database objecten voor Python variant aanmaken:

```sql
-- file: solutions/python/setup_py.sql
```

### 2. Open notebook:

```text
solutions/python/cdc_process.ipynb
```

### 3. Pas de connectiegegevens aan
```sql
conn = snowflake.connector.connect(
    user="username",
    password="pass",
    account="account",
    warehouse="warehouse",
    database="CDC_PYTHON_DB",
    role="SYSADMIN"
)
```

### 4. Installeer de dependencies
`pip install snowflake-connector-python`

### 5. Voer notebookcellen van boven naar beneden uit (of klik op run all)

De notebook voert dezelfde stappen uit als SQL:

- initialisatie
- foutdetectie
- inserts
- updates
- deletes
- logging in `RUN_LOG`, `RUN_ENTITY_LOG`, `RUN_ERROR_LOG`

## Test en helperbestanden

- `tests/performance_test.sql`: testen van performance van de tool
- `tests/data_quality_test.sql`: testen van data kwaliteit
- `helper/generate_mock.py`: genereert employee mockdata (10 t/m 1.000.000 rijen)
- `helper/clear.sql`: truncate scripts voor beide databases (`CDC_SQL_DB` en `CDC_PYTHON_DB`)
- `helper/results.sql`: query om snelheid SQL versus Python te vergelijken

## UML

Voor de architectuur (c4 op 2 niveaus) en use case:

- `uml/lvl1.puml`
- `uml/lvl2.puml`
- `uml/usecase.puml`

