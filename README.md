# Citibike S3 to Snowflake Pipeline

Python pipeline to discover, download, and extract Citi Bike trip data from the public [Citi Bike system data](https://citibikenyc.com/system-data) S3 bucket (`https://s3.amazonaws.com/tripdata/`), compress it for staging, load it into Snowflake, and build an integration-layer star schema for analysis.

**End-to-end Snowflake path:** `stage_files.py` → `populate_stage_manifest.py` → `SP_INGEST_STAGED_FILES` → `SP_LOAD_TRIPS_ALL` → `INT_UDM_*` build procedures → **`RPT_*` reporting views**. See [docs/SNOWFLAKE_INGEST.md](docs/SNOWFLAKE_INGEST.md), [docs/SNOWFLAKE_INTEGRATION.md](docs/SNOWFLAKE_INTEGRATION.md), [docs/SNOWFLAKE_REPORTING.md](docs/SNOWFLAKE_REPORTING.md), and [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md).

## Prerequisites

- Python 3.13+ (tested with 3.13)
- Enough local disk for raw zips and extracted CSVs (tens of GB for a full historical run)
- Snowflake database `CITIBIKE_SYSTEM_DATA` with:
  - Schemas `STAGING_NYC`, `STAGING_JC` (tables + `RAW_INGESTION` stages)
  - Schema `LOGGING` (manifest, ingest log, stored procedure)
  - Schemas `INT_UDM_NYC`, `INT_UDM_JC` (dimensions + `FACT_RIDE`, build procedures)
  - Schemas `RPT_NYC`, `RPT_JC` (reporting views on the integration layer) — deploy from `Snowflake_Scripts/`

## Setup

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp snowflake_credentials.example.py snowflake_credentials.py
# edit snowflake_credentials.py
```

Non-secret routing (regions, tables, stage name, parallelism) lives in `snowflake_config.py`. Env vars `SF_*` override credentials when set.

## Snowflake ingest architecture

```text
  gzip_staging/*.csv.gz + gzip_manifest.jsonl
           │
           ├─► [Recommended] 3-step staged ingest
           │       1. stage_files.py          → @STAGING_*/RAW_INGESTION
           │       2. populate_stage_manifest.py → LOGGING.STAGE_MANIFEST
           │       3. SP_INGEST_STAGED_FILES     → STAGING_*.TRIPS_*
           │       4. SP_LOAD_TRIPS_ALL          → STAGING_*.TRIPS_ALL
           │       5. SP_BUILD_DIM_* / FACT    → INT_UDM_*.DIM_* + FACT_RIDE
           │       6. RPT_* views              → RPT_NYC / RPT_JC (analytics)
           │       Monitor: LOGGING.INGEST_LOG + Snowflake_Scripts/TEST/
           │
           └─► [Alternative] load_to_snowflake.py
                     PUT + COPY in one Python script
                     log: snowflake_ingest_log.jsonl
```

| Step | Tool | Log / audit |
|------|------|-------------|
| PUT to stage | `stage_files.py` | `snowflake_stage_loading_log.jsonl` |
| Register work queue | `populate_stage_manifest.py` | `LOGGING.STAGE_MANIFEST` |
| COPY into staging tables | `LOGGING.SP_INGEST_STAGED_FILES` | `LOGGING.INGEST_LOG` |
| Merge to unified table | `STAGING_*.SP_LOAD_TRIPS_ALL` | `TRIPS_ALL` (per region) |
| Integration star schema | `INT_UDM_*.SP_BUILD_*` (SQL) | `DIM_DATES`, `DIM_STATION`, `FACT_RIDE` |
| Reporting views | `RPT_NYC/*`, `RPT_JC/*` (SQL views) | `RPT_RIDERSHIP_OVER_TIME`, `RPT_TOP_STATIONS`, `RPT_TOP_ROUTES` |
| All-in-one (alt.) | `load_to_snowflake.py` | `snowflake_ingest_log.jsonl` |

Deploy SQL in order: [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md). Procedure details: [Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES_DOC.txt](Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES_DOC.txt).

## Project structure

### Python

| Script | Purpose |
|--------|---------|
| `s3_index_to_csv.py` | List S3 bucket → `citibike_s3_file_index.csv` |
| `download_citibike_data.py` | Download and extract zips |
| `analyze_extracted_schema.py` | Schema inventory under `analysis_output/` |
| `extract_nested_zips.py` | Extract nested annual bundles |
| `compress_for_snowflake.py` | Build regional `.csv.gz` parts + `gzip_manifest.jsonl` |
| `stage_files.py` | Parallel PUT to `RAW_INGESTION` |
| `populate_stage_manifest.py` | Load manifest into `LOGGING.STAGE_MANIFEST` |
| `ingest_stage_files.py` | Thin wrapper / legacy alias for manifest load |
| `load_to_snowflake.py` | Alternative: PUT + COPY from Python |
| `snowflake_config.py` | Region / `schema_key` → table routing (no secrets) |
| `snowflake_credentials.py` | Credentials (**gitignored**) |

### Snowflake (`Snowflake_Scripts/`)

| Folder | Contents |
|--------|----------|
| `STAGING_NYC/`, `STAGING_JC/` | Staging tables (`TRIPS_*`), unified `TRIPS_ALL`, `SP_LOAD_*` merge procedures |
| `LOGGING/` | `STAGE_MANIFEST`, `INGEST_LOG`, views, `SP_INGEST_STAGED_FILES.sql`, full doc `.txt` |
| `INT_UDM_NYC/`, `INT_UDM_JC/` | Integration layer: `DIM_DATES`, `DIM_STATION`, `FACT_RIDE`, `SP_BUILD_*` procedures |
| `RPT_NYC/`, `RPT_JC/` | Reporting views: ridership trends, top stations, top routes |
| `TEST/` | Operator queries: pending files, failures, row-count cross-check |

### Documentation

| Doc | Contents |
|-----|----------|
| [docs/SNOWFLAKE_INGEST.md](docs/SNOWFLAKE_INGEST.md) | Staged ingest runbook, monitoring, troubleshooting |
| [docs/SNOWFLAKE_INTEGRATION.md](docs/SNOWFLAKE_INTEGRATION.md) | Integration layer runbook (`INT_UDM_*` build procedures) |
| [docs/SNOWFLAKE_REPORTING.md](docs/SNOWFLAKE_REPORTING.md) | Reporting layer runbook (`RPT_*` views) |
| [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md) | SQL deploy order and object index (full stack) |

### Routing (`snowflake_config.py`)

| Manifest `region` | Snowflake schema | Stage |
|-------------------|------------------|-------|
| `nyc` | `STAGING_NYC` | `RAW_INGESTION` |
| `jersey_city` | `STAGING_JC` | `RAW_INGESTION` |

| `schema_key` | Target table | CSV layout |
|--------------|--------------|------------|
| `dc497b4333c4` | `TRIPS_MODERN` | schema_1 — 13 columns |
| `473144999085` | `TRIPS_LEGACY_V1` | schema_2 — lowercase legacy |
| `e24ee8457e0e` | `TRIPS_LEGACY_V2` | schema_3 — Title Case legacy |

## Pipeline (run in order)

### 1–5. Local data prep

```bash
python s3_index_to_csv.py
python download_citibike_data.py
python analyze_extracted_schema.py
python extract_nested_zips.py          # when analysis_summary lists nested bundles
python compress_for_snowflake.py
```

Outputs: `downloads/`, `extracted/`, `gzip_staging/`, `gzip_manifest.jsonl`.

### 6. Snowflake load (recommended)

**6a. One-time Snowflake deploy**

Run scripts listed in [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md): staging → `TRIPS_ALL` → LOGGING → ingest SP → `SP_LOAD_*` → `INT_UDM_*` → **`RPT_*` views** (final layer).

**6b. Upload gzip files to stage**

```bash
python stage_files.py --dry-run
python stage_files.py --region jersey_city --workers 4
python stage_files.py --workers 4
python stage_files.py --list-stage
```

Log: `snowflake_stage_loading_log.jsonl`. Flags: `--force`, `--no-overwrite`, `--reset-failed` — see [docs/SNOWFLAKE_INGEST.md](docs/SNOWFLAKE_INGEST.md).

**6c. Register files for the stored procedure**

```bash
python populate_stage_manifest.py
```

Inserts into `LOGGING.STAGE_MANIFEST` (idempotent per `OUTPUT_FILE`).

**6d. COPY staged files into per-layout tables**

```sql
SELECT * FROM CITIBIKE_SYSTEM_DATA.LOGGING.V_PENDING_FILES;

CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');
-- CALL ... ('nyc');  CALL ... ('jersey_city');
```

Monitor with `Snowflake_Scripts/TEST/*.sql` or:

```sql
SELECT * FROM CITIBIKE_SYSTEM_DATA.LOGGING.V_LATEST_INGEST_STATUS ORDER BY ENDED_AT_UTC DESC;
SELECT * FROM CITIBIKE_SYSTEM_DATA.LOGGING.V_TABLE_ROW_COUNTS;
```

**6e. Merge staging tables into TRIPS_ALL**

After ingest succeeds, normalize all layouts into one table per region:

```sql
CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_ALL();
CALL CITIBIKE_SYSTEM_DATA.STAGING_JC.SP_LOAD_TRIPS_ALL();

SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.STAGING_NYC.TRIPS_ALL;
SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.STAGING_JC.TRIPS_ALL;
```

`SP_LOAD_TRIPS_*` procedures dedupe on `_SOURCE_FILE` + `_SOURCE_ROW_NUMBER`. Safe to re-run after new data lands in `TRIPS_MODERN` / `TRIPS_LEGACY_*`.

**6f. Build integration layer (Snowflake SQL)**

Deploy `INT_UDM_*` DDL and procedures per [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md) (steps 9–16). Processing is **stored procedures only** (no Python script in this repo).

```sql
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_DATES();
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_STATION_NYC();
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_FACT_RIDE_NYC(FALSE);

CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_DIM_STATION_JC();
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_FACT_RIDE_JC(FALSE);

SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE;
SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.INT_UDM_JC.FACT_RIDE;
```

Runbook and re-run guidance: [docs/SNOWFLAKE_INTEGRATION.md](docs/SNOWFLAKE_INTEGRATION.md). `SP_BUILD_FACT_RIDE_*` is idempotent on source file + row number; pass `TRUE` to truncate and full-reload the fact table.

**6g. Deploy reporting views (final layer)**

After `FACT_RIDE` is populated, deploy views from `Snowflake_Scripts/RPT_NYC/` and `RPT_JC/` (schemas + three views per region). No procedures — views query the integration layer directly.

```sql
-- Deploy all RPT_*.sql scripts, then query:
SELECT * FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_RIDERSHIP_OVER_TIME LIMIT 100;
SELECT * FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_TOP_STATIONS LIMIT 50;
SELECT * FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_TOP_ROUTES LIMIT 50;

SELECT * FROM CITIBIKE_SYSTEM_DATA.RPT_JC.RPT_TOP_STATIONS LIMIT 20;
```

Details: [docs/SNOWFLAKE_REPORTING.md](docs/SNOWFLAKE_REPORTING.md). Re-run integration builds only when facts change; redeploy views only when SQL definitions change.

### 6 (alternative). `load_to_snowflake.py`

Single-script PUT + COPY; does not use `LOGGING` or the stored procedure:

```bash
python load_to_snowflake.py --region jersey_city
python load_to_snowflake.py
```

Requires staging table DDL only. Log: `snowflake_ingest_log.jsonl`.

## What is not in this repo

See `.gitignore`: `downloads/`, `extracted/`, `gzip_staging/nyc/*`, `gzip_staging/jersey_city/*`, `venv/`, `snowflake_credentials.py`.

Runtime logs: `snowflake_stage_loading_log.jsonl`, `snowflake_ingest_log.jsonl`. Snowflake-side audit: `LOGGING.INGEST_LOG`.

## Schema notes

Use `analysis_output/schema_signatures.csv` with `Snowflake_Scripts/` when changing DDL or `SCHEMA_KEY_TO_TABLE` in `snowflake_config.py`.

## License / data

Trip data is provided by Citi Bike via the public S3 bucket and [system data page](https://citibikenyc.com/system-data). Check their data use policy for your project.

## Author

<!-- Add your name, GitHub username, or team here -->
