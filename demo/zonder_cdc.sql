USE DATABASE CDC_SQL_DB;

-- ------------------------------------------------
-- Zonder CDC component
-- ------------------------------------------------
USE SCHEMA TARGET;

-- Geleegde startsituatie
TRUNCATE TABLE TARGET.T_TEST_WITH_CDC;
TRUNCATE TABLE TARGET.T_TEST_WITHOUT_CDC;
TRUNCATE TABLE STAGING.S_TEST_WITH_CDC;
TRUNCATE TABLE LOGGING.RUN_LOG;
TRUNCATE TABLE LOGGING.RUN_ENTITY_LOG;
TRUNCATE TABLE LOGGING.RUN_ERROR_LOG;

-- Laden van data
USE SCHEMA TARGET;

-- Run 1
INSERT INTO T_TEST_WITHOUT_CDC (ID, NAAM, WOONPLAATS)
VALUES
		('1', 'John Doe', 'Arnhem'),
		('2', 'Jane Doe', 'Arnhem');

-- Run 2
INSERT INTO T_TEST_WITHOUT_CDC (ID, NAAM, WOONPLAATS)
VALUES
        ('2', 'Jane de Wit', 'Maastricht'),
        ('', 'Emma Ptykee', 'Rotterdam');

SELECT *
FROM TARGET.T_TEST_WITHOUT_CDC
ORDER BY ID;