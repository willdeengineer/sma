# Snowflake Migration Accelerator (SMA)

## Overzicht
Dit project bevat de code, documentatie en tests voor de Snowflake Migration Accelerator.

## Installatie
```bash
# Clone de repository
git clone https://github.com/willdeengineer/sma.git
cd sma
```

## Gebruik
Iedere oplossing moet eerst geconfigureerd worden voordat de accelerator kan worden toegepast. Dit doe je door de stappen te volgen in de `config.sql`. Hierna kan je de setup volgen in de setup, zoals `02_hs_setup.sql`, `02_st_setup.sql`, of `02_ts_setup.sql`.

## Structuur
- `/solutions`        - De verschillende Proof of Technology oplossingen
    - `/hash`         - PoT d.m.v. Hash
    - `/streams`      - PoT d.m.v. Streams & Tasks
    - `/timestamp`    - PoT d.m.v. Timestamps
- `/tests`            - Tests, benchmarks & resultaten daarvan

