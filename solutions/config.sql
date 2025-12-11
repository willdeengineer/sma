-- tabelnaam, primary key kolommen, foreign key kolommen
CREATE OR REPLACE TABLE config (
    table_name VARCHAR(255) NOT NULL,
    primary_key_columns VARCHAR(1000),
    foreign_key_columns VARCHAR(1000),
    CONSTRAINT pk_config PRIMARY KEY (table_name)
);

TRUNCATE TABLE config;

-- Voeg hier in values alle tabellen toe die je wilt gebruiken.
INSERT INTO config (table_name, primary_key_columns, foreign_key_columns)
VALUES
    ('sales_detail', 'sale_detail_id', 'product_id,sale_id'),
    ('sales', 'sale_id', 'customer_id'),
    ('products', 'product_id', NULL),
    ('customers', 'customer_id', NULL);
