# Box Office Data Warehouse

End-to-end data pipeline that ingests daily box office revenue data and enriches it with movie metadata from the OMDB API, following the **Medallion Architecture** (Bronze / Silver / Gold).


# IMPORTANT: 
There was no AI-policy specified in the assessment. To be transparent:
AI was used to function under create common_function.py + some minor syntax, You will notice anyway as no one create docs like AI. Also AI regenerated the whole documentation below based on my notes and files in the project (because why not to use it).

The concept/architecture though is my own idea, I have not verified it against AI so you can understand my reasoning/thinking.

For dashboards, I have tiny experience, should be called episode (despite academia plots). I've used AI for content here. Not going to lie.  

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [ETL Flow Diagram](#etl-flow-diagram)
- [Data Sources](#data-sources)
- [Pipeline Details](#pipeline-details)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer (Dimensional Model)](#gold-layer-dimensional-model)
- [Field Lineage](#field-lineage)
- [ER Diagram](#er-diagram)
- [Project Structure](#project-structure)
- [Assumptions](#assumptions)
- [KPIs for Box Office Performance](#kpis-for-box-office-performance)
- [Final Comments / Known Limitations](#final-comments--known-limitations)

---

## Architecture Overview

```
+---------------------+       +---------------------+       +---------------------+
|      SOURCES        |       |     BRONZE (Raw)    |       |   SILVER (Clean)    |
|                     |       |   Append-only       |       |   Deduplicated      |
|  revenues_per_day   +------>+   + _tf_ingestion_  +------>+   Merged (upsert)   |
|  .csv               |       |     time partition  | delta |                     |
|                     |       |                     |       |                     |
|  OMDB API           +------>+   + _tf_ingestion_  +------>+                     |
|                     |       |     time partition  | delta |                     |
+---------------------+       +---------------------+       +----------+----------+
                                                                       |
                                                                       | full load
                                                                       v
                                                            +---------------------+
                                                            |    GOLD (Model)     |
                                                            |                     |
                                                            |  factRevenues       |
                                                            |  dimMovies          |
                                                            |  dimDistributor     |
                                                            +---------------------+
```

---

## ETL Flow Diagram

### Revenues Stream

```
revenues_per_day.csv
        |
        | [Revenues-Bronze] read CSV, add _tf_ingestion_time, append to parquet (partitioned)
        v
  Bronze: revenues/
        |
        | [Revenues-Silver] load delta (since last_success_unix), deduplicate on (date, title),
        |                   merge into Silver (upsert by pk)
        v
  Silver: revenues/
        |
        +---> [factRevenues-Gold]   full overwrite, hash keys on (title+date), (title), (distributor)
        |
        +---> [dimDistributor-Gold] full overwrite, distinct distributors, hash key on (distributor)
```

### OMDB Stream

```
  Bronze: revenues/ (read distinct titles)
        |
        | [OMDB-Bronze] compare with existing OMDB titles, fetch new from API,
        |               add _tf_ingestion_time, append to parquet (partitioned)
        v
  Bronze: omdb/
        |
        | [OMDB-Silver] load delta (since last_success_unix), deduplicate on (title),
        |               merge into Silver (upsert by pk)
        v
  Silver: omdb/
        |
        +---> [dimMovies-Gold] full overwrite, left join revenues titles with OMDB metadata,
                               hash key on (title), is_enriched flag
```

---

## Data Sources

### 1. `source/revenues_per_day/revenues_per_day.csv`

An external CSV file updated daily by an external system. Contains box office revenue per movie per day.

| Column       | Type    | Description                        |
| ------------ | ------- | ---------------------------------- |
| `id`         | string  | Unique row identifier              |
| `date`       | string  | Date in `yyyy-mm-dd` format        |
| `title`      | string  | Movie title                        |
| `revenue`    | integer | Daily box office revenue (USD)     |
| `theaters`   | integer | Number of theaters screening       |
| `distributor`| string  | Distribution company (may be "-")  |

### 2. OMDB API

REST API providing movie metadata. Fetched per unique title found in revenues data.

Key fields returned: `title`, `year`, `rated`, `released`, `runtime`, `genre`, `director`, `writer`, `actors`, `plot`, `language`, `country`, `awards`, `imdb_rating`, `rotten_tomatoes`, `metacritic`, `metascore`, `imdb_votes`, `imdb_id`, `box_office`, `production`.

---

## Pipeline Details

Each pipeline is a Jupyter notebook in [pipeline/](pipeline/) driven by JSON config from [metadata/config/](metadata/config/). Execution status is tracked in [metadata/status/](metadata/status/).

### Bronze Layer

**Mode:** Append-only with partitioning by `_tf_ingestion_time` (unix timestamp).

| Pipeline          | Source                  | Target                       | Technical Fields Added            |
| ----------------- | ----------------------- | ---------------------------- | --------------------------------- |
| `Revenues-Bronze` | `revenues_per_day.csv`  | `data/01_bronze/revenues/`   | `_tf_ingestion_time`, `_tf_ingestion_date` |
| `OMDB-Bronze`     | OMDB API (per title)    | `data/01_bronze/omdb/`       | `_tf_ingestion_time`, `_tf_ingestion_date` |

**How it works:**
1. Read source data (CSV or API)
2. Add `_tf_ingestion_time` (unix epoch) and `_tf_ingestion_date`
3. Append to existing parquet, partitioned by `_tf_ingestion_time`
4. Update pipeline status in `metadata/status/`

OMDB-Bronze additionally compares existing titles in bronze with revenue titles and only fetches **new** titles from the API (incremental by title set).

### Silver Layer

**Mode:** Delta load from Bronze, deduplicate, then merge (upsert) into Silver.

| Pipeline          | Source Bronze        | Primary Keys      | Target                      |
| ----------------- | -------------------- | ----------------- | --------------------------- |
| `Revenues-Silver` | `01_bronze/revenues` | `date`, `title`   | `data/02_silver/revenues/`  |
| `OMDB-Silver`     | `01_bronze/omdb`     | `title`           | `data/02_silver/omdb/`      |

**How it works:**
1. Read `last_success_timestamp_unix` from `metadata/status/{pipeline}.json`
2. Load only Bronze records where `_tf_ingestion_time > last_success_unix` (delta)
3. Deduplicate by primary keys, keeping the record with the **highest** `_tf_ingestion_time`
4. Merge into Silver: concat with existing, sort by `_tf_ingestion_time`, keep last per PK
5. Update pipeline status

### Gold Layer (Dimensional Model)

**Mode:** Full overwrite on every run. Reads from Silver layer(s) and produces star-schema tables.

| Pipeline              | Source Silver(s)                | Target                          |
| --------------------- | ------------------------------- | ------------------------------- |
| `factRevenues-Gold`   | `02_silver/revenues`            | `data/03_gold/factRevenues/`    |
| `dimMovies-Gold`      | `02_silver/revenues` + `02_silver/omdb` | `data/03_gold/dimMovies/`|
| `dimDistributor-Gold` | `02_silver/revenues`            | `data/03_gold/dimDistributor/`  |

Surrogate keys are generated as **MD5 hashes** of business key columns via `createHashKey()`.

---

## Field Lineage

### factRevenues

| Gold Column        | Source                          | Derivation                                         |
| ------------------ | ------------------------------- | -------------------------------------------------- |
| `_sk_revenue_id`   | Silver revenues: `title`, `date`| `MD5(title \| date)` - surrogate key               |
| `_sk_movie`        | Silver revenues: `title`        | `MD5(title)` - FK to dimMovies                     |
| `_sk_distributor`  | Silver revenues: `distributor`  | `MD5(distributor)` - FK to dimDistributor           |
| `date`             | Silver revenues: `date`         | Pass-through                                       |
| `revenue`          | Silver revenues: `revenue`      | Pass-through                                       |
| `theaters`         | Silver revenues: `theaters`     | Pass-through                                       |

### dimMovies

| Gold Column        | Source                                    | Derivation                                           |
| ------------------ | ----------------------------------------- | ---------------------------------------------------- |
| `_sk_movie`        | Silver revenues: `title`                  | `MD5(title)` - surrogate key                         |
| `title`            | Silver revenues: `title`                  | Master list from revenues (all titles)               |
| `year`             | Silver omdb: `year`                       | LEFT JOIN from OMDB on `_sk_movie`                   |
| `rated`            | Silver omdb: `rated`                      | LEFT JOIN from OMDB on `_sk_movie`                   |
| `released`         | Silver omdb: `released`                   | LEFT JOIN from OMDB on `_sk_movie`                   |
| `runtime`          | Silver omdb: `runtime`                    | LEFT JOIN from OMDB on `_sk_movie`                   |
| `genre`            | Silver omdb: `genre`                      | LEFT JOIN from OMDB on `_sk_movie`                   |
| `director`         | Silver omdb: `director`                   | LEFT JOIN from OMDB on `_sk_movie`                   |
| `writer`           | Silver omdb: `writer`                     | LEFT JOIN from OMDB on `_sk_movie`                   |
| `actors`           | Silver omdb: `actors`                     | LEFT JOIN from OMDB on `_sk_movie`                   |
| `plot`             | Silver omdb: `plot`                       | LEFT JOIN from OMDB on `_sk_movie`                   |
| `language`         | Silver omdb: `language`                   | LEFT JOIN from OMDB on `_sk_movie`                   |
| `country`          | Silver omdb: `country`                    | LEFT JOIN from OMDB on `_sk_movie`                   |
| `awards`           | Silver omdb: `awards`                     | LEFT JOIN from OMDB on `_sk_movie`                   |
| `poster`           | Silver omdb: `poster`                     | LEFT JOIN from OMDB on `_sk_movie`                   |
| `imdb_rating`      | Silver omdb: `imdb_rating`                | LEFT JOIN from OMDB on `_sk_movie`                   |
| `rotten_tomatoes`  | Silver omdb: `rotten_tomatoes`            | LEFT JOIN from OMDB on `_sk_movie`                   |
| `metacritic`       | Silver omdb: `metacritic`                 | LEFT JOIN from OMDB on `_sk_movie`                   |
| `metascore`        | Silver omdb: `metascore`                  | LEFT JOIN from OMDB on `_sk_movie`                   |
| `imdb_votes`       | Silver omdb: `imdb_votes`                 | LEFT JOIN from OMDB on `_sk_movie`                   |
| `imdb_id`          | Silver omdb: `imdb_id`                    | LEFT JOIN from OMDB on `_sk_movie`                   |
| `box_office`       | Silver omdb: `box_office`                 | LEFT JOIN from OMDB on `_sk_movie`                   |
| `production`       | Silver omdb: `production`                 | LEFT JOIN from OMDB on `_sk_movie`                   |
| `is_enriched`      | Derived                                   | `1` if OMDB data exists, `0` otherwise              |

### dimDistributor

| Gold Column        | Source                          | Derivation                                         |
| ------------------ | ------------------------------- | -------------------------------------------------- |
| `_sk_distributor`  | Silver revenues: `distributor`  | `MD5(distributor)` - surrogate key                 |
| `distributor`      | Silver revenues: `distributor`  | Distinct values from revenues                      |

### Silver revenues (intermediate lineage)

| Silver Column          | Source                                 | Derivation                                          |
| ---------------------- | -------------------------------------- | --------------------------------------------------- |
| `id`                   | Bronze revenues: `id`                  | Pass-through, from CSV `id`                         |
| `date`                 | Bronze revenues: `date`                | Pass-through, from CSV `date`                       |
| `title`                | Bronze revenues: `title`               | Pass-through, from CSV `title`                      |
| `revenue`              | Bronze revenues: `revenue`             | Pass-through, from CSV `revenue`                    |
| `theaters`             | Bronze revenues: `theaters`            | Pass-through, from CSV `theaters`                   |
| `distributor`          | Bronze revenues: `distributor`         | Pass-through, from CSV `distributor`                |
| `_tf_ingestion_time`   | Bronze revenues: `_tf_ingestion_time`  | Kept after dedup (latest wins)                      |
| `_tf_ingestion_date`   | Bronze revenues: `_tf_ingestion_date`  | Kept after dedup (latest wins)                      |

### Silver omdb (intermediate lineage)

| Silver Column          | Source                              | Derivation                                          |
| ---------------------- | ----------------------------------- | --------------------------------------------------- |
| `title`                | Bronze omdb: `title`                | From OMDB API `Title`                               |
| `year`                 | Bronze omdb: `year`                 | From OMDB API `Year`                                |
| `rated`                | Bronze omdb: `rated`                | From OMDB API `Rated`                               |
| `released`             | Bronze omdb: `released`             | From OMDB API `Released`                            |
| `runtime`              | Bronze omdb: `runtime`              | From OMDB API `Runtime`                             |
| `genre`                | Bronze omdb: `genre`                | From OMDB API `Genre`                               |
| `director`             | Bronze omdb: `director`             | From OMDB API `Director`                            |
| `writer`               | Bronze omdb: `writer`               | From OMDB API `Writer`                              |
| `actors`               | Bronze omdb: `actors`               | From OMDB API `Actors`                              |
| `plot`                 | Bronze omdb: `plot`                 | From OMDB API `Plot`                                |
| `language`             | Bronze omdb: `language`             | From OMDB API `Language`                            |
| `country`              | Bronze omdb: `country`              | From OMDB API `Country`                             |
| `awards`               | Bronze omdb: `awards`               | From OMDB API `Awards`                              |
| `poster`               | Bronze omdb: `poster`               | From OMDB API `Poster`                              |
| `imdb_rating`          | Bronze omdb: `imdb_rating`          | Parsed from OMDB API `Ratings[]` array or `imdbRating` |
| `rotten_tomatoes`      | Bronze omdb: `rotten_tomatoes`      | Parsed from OMDB API `Ratings[]` array              |
| `metacritic`           | Bronze omdb: `metacritic`           | Parsed from OMDB API `Ratings[]` array              |
| `metascore`            | Bronze omdb: `metascore`            | From OMDB API `Metascore`                           |
| `imdb_votes`           | Bronze omdb: `imdb_votes`           | From OMDB API `imdbVotes`                           |
| `imdb_id`              | Bronze omdb: `imdb_id`              | From OMDB API `imdbID`                              |
| `box_office`           | Bronze omdb: `box_office`           | From OMDB API `BoxOffice`                           |
| `production`           | Bronze omdb: `production`           | From OMDB API `Production`                          |
| `website`              | Bronze omdb: `website`              | From OMDB API `Website`                             |
| `_tf_ingestion_time`   | Bronze omdb: `_tf_ingestion_time`   | Kept after dedup (latest wins)                      |
| `_tf_ingestion_date`   | Bronze omdb: `_tf_ingestion_date`   | Kept after dedup (latest wins)                      |

---

## ER Diagram

```
  +---------------------+          +-------------------------------+
  |   dimDistributor    |          |          dimMovies            |
  +---------------------+          +-------------------------------+
  | _sk_distributor (PK)|          | _sk_movie (PK)               |
  | distributor         |          | title                        |
  +--------+------------+          | year                         |
           |                       | rated                        |
           |                       | released                     |
           |                       | runtime                      |
           | 1                     | genre                        |
           |                       | director                     |
           |                       | writer                       |
  +--------+------------+          | actors                       |
  |    factRevenues     |          | plot                         |
  +---------------------+          | language                     |
  | _sk_revenue_id (PK) |          | country                      |
  | _sk_movie (FK)-------+-------->| awards                       |
  | _sk_distributor (FK) |   1     | poster                       |
  | date                 |         | imdb_rating                  |
  | revenue              |         | rotten_tomatoes              |
  | theaters             |         | metacritic                   |
  +---------------------+          | metascore                    |
           |                       | imdb_votes                   |
           | *                     | imdb_id                      |
           |                       | box_office                   |
           |                       | production                   |
           |                       | is_enriched                  |
           |                       +-------------------------------+
```

**Relationships:**
- `factRevenues._sk_movie` --> `dimMovies._sk_movie` (many-to-one)
- `factRevenues._sk_distributor` --> `dimDistributor._sk_distributor` (many-to-one)

---

## Project Structure

```
futuremind-assesment/
|-- .env                              # OMDB API key
|-- README.md
|-- source/
|   `-- revenues_per_day/
|       `-- revenues_per_day.csv      # External source file
|-- pipeline/
|   |-- common_function.py            # Shared utilities (config, append, merge, dedup, hash)
|   |-- Revenues-Bronze.ipynb         # CSV -> Bronze (append)
|   |-- Revenues-Silver.ipynb         # Bronze -> Silver (delta + merge)
|   |-- OMDB-Bronze.ipynb             # API -> Bronze (append)
|   |-- OMDB-Silver.ipynb             # Bronze -> Silver (delta + merge)
|   |-- factRevenues-Gold.ipynb       # Silver -> Gold fact table
|   |-- dimMovies-Gold.ipynb          # Silver -> Gold dimension (with OMDB enrichment)
|   `-- dimDistributor-Gold.ipynb     # Silver -> Gold dimension
|-- metadata/
|   |-- config/                       # Pipeline configuration (source, target, PKs, modes)
|   |   |-- Revenues-Bronze.json
|   |   |-- Revenues-Silver.json
|   |   |-- OMDB-Bronze.json
|   |   |-- OMDB-Silver.json
|   |   |-- factRevenues-Gold.json
|   |   |-- dimMovies-Gold.json
|   |   `-- dimDistributor-Gold.json
|   `-- status/                       # Pipeline run status (last success timestamps)
|       |-- Revenues-Bronze.json
|       |-- Revenues-Silver.json
|       |-- OMDB-Bronze.json
|       |-- OMDB-Silver.json
|       |-- factRevenues-Gold.json
|       |-- dimMovies-Gold.json
|       `-- dimDistributor-Gold.json
|-- data/
|   |-- 01_bronze/                    # Raw ingested data (parquet, partitioned)
|   |   |-- revenues/
|   |   `-- omdb/
|   |-- 02_silver/                    # Cleaned & deduplicated (parquet)
|   |   |-- revenues/
|   |   `-- omdb/
|   `-- 03_gold/                      # Dimensional model (parquet)
|       |-- factRevenues/
|       |-- dimMovies/
|       `-- dimDistributor/
`-- data_exploration/                 # Ad-hoc analysis notebooks
    |-- read_any_data.ipynb
    `-- revenues_per_day.ipynb
```

---

## Assumptions

### revenues_per_day.csv
- External file updated daily by an external system into `source/revenues_per_day/`
- Contains revenue per movie per day with columns: `id`, `date`, `title`, `revenue`, `theaters`, `distributor`
- Data quality varies: some movies have only 2 days of data (e.g. *"'85: The Greatest Team in Football History"*) while others have 93 days (e.g. *"Cloudy with a Chance of Meatballs"*), making cross-movie aggregations potentially misleading
- Distributor may be missing (shown as `"-"`)

### OMDB API
- Free tier with daily request limits (1,000/day)
- Pipeline handles rate limiting gracefully - fetches what it can, remaining titles picked up on next run
- Not all revenue titles will match an OMDB entry; `is_enriched` flag tracks this
- Due to limitations - once data is ingested, the system does not check the api again for same title, nevertheless, some fields can be considered as slowly changing dimensions (for example rates)

---


## Final Comments / Known Limitations

1. **No orchestration** - Pipelines are triggered manually notebook by notebook. Proper orchestration (e.g. Airflow, Prefect) is outside the scope of this assessment.
2. **Data quality checks are minimal** - Should validate on ingestion: `id` not null, `date` not null and `yyyy-mm-dd` format, `title` not null.
3. **Monitoring** - No metrics/alerting currently implemented. 
4. **Code redundancy** - Pipeline notebooks follow the same pattern; further parameterization could reduce duplication.
5. **OMDB rate limits** - Free API key allows ~1,000 requests/day. With 6,547 unique titles, full ingestion requires multiple runs.
6. **Hardcoded paths** - `absPath()` returns a hardcoded local path; should be configurable via environment variable.
7. Revenues_per_day.csv has some gaps, some movies have more data some less, the kpi's will be not accurate, maybe some handling of this would be beneficial.

## HOW TO
dashboard run :
streamlit run .\dashboard\dashboard.py
