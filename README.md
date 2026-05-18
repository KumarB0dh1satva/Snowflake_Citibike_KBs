# Citibike S3 to Snowflake Pipeline

Python pipeline to discover, download, and extract Citi Bike trip data from the public S3 bucket (`https://s3.amazonaws.com/tripdata/`), compress it for staging, and load it into Snowflake.

**Recommended Snowflake path:** upload with `stage_files.py` → register files in `LOGGING.STAGE_MANIFEST` with `ingest_stage_files.py` → `COPY INTO` staging tables via `LOGGING.SP_INGEST_STAGED_FILES`. See [docs/SNOWFLAKE_INGEST.md](docs/SNOWFLAKE_INGEST.md) for the full operator runbook.

## Prerequisites

- Python 3.13+ (tested with 3.13)
- Enough local disk for raw zips and extracted CSVs (tens of GB for a full historical run)
- Snowflake database `CITIBIKE_SYSTEM_DATA` with:
  - Schemas `STAGING_NYC`, `STAGING_JC` (tables + `RAW_INGESTION` stages)
  - Schema `LOGGING` (manifest, ingest log, stored procedure) — deploy from `Snowflake_Scripts/`

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
           │       2. ingest_stage_files.py   → LOGGING.STAGE_MANIFEST
           │       3. SP_INGEST_STAGED_FILES    → STAGING_*.TRIPS_*
           │       Monitor: LOGGING.INGEST_LOG + Snowflake_Scripts/TEST/
           │
           └─► [Alternative] load_to_snowflake.py
                     PUT + COPY in one Python script
                     log: snowflake_ingest_log.jsonl
```

| Step | Tool | Log / audit |
|------|------|-------------|
| PUT to stage | `stage_files.py` | `snowflake_stage_loading_log.jsonl` |
| Register work queue | `ingest_stage_files.py` | `LOGGING.STAGE_MANIFEST` |
| COPY into tables | `LOGGING.SP_INGEST_STAGED_FILES` | `LOGGING.INGEST_LOG` |
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
| `ingest_stage_files.py` | Load manifest into `LOGGING.STAGE_MANIFEST` |
| `load_to_snowflake.py` | Alternative: PUT + COPY from Python |
| `snowflake_config.py` | Region / `schema_key` → table routing (no secrets) |
| `snowflake_credentials.py` | Credentials (**gitignored**) |

### Snowflake (`Snowflake_Scripts/`)

| Folder | Contents |
|--------|----------|
| `STAGING_NYC/`, `STAGING_JC/` | `CREATE TABLE` for `TRIPS_MODERN`, `TRIPS_LEGACY_V1`, `TRIPS_LEGACY_V2` |
| `LOGGING/` | `STAGE_MANIFEST`, `INGEST_LOG`, views, `SP_INGEST_STAGED_FILES.sql`, full doc `.txt` |
| `TEST/` | Operator queries: pending files, failures, row-count cross-check |

### Documentation

| Doc | Contents |
|-----|----------|
| [docs/SNOWFLAKE_INGEST.md](docs/SNOWFLAKE_INGEST.md) | Staged ingest runbook, monitoring, troubleshooting |
| [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md) | SQL deploy order and object index |

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

Run scripts listed in [Snowflake_Scripts/README.md](Snowflake_Scripts/README.md) (staging tables → LOGGING tables/views → stored procedure).

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
python ingest_stage_files.py
```

Inserts into `LOGGING.STAGE_MANIFEST` (idempotent per `OUTPUT_FILE`).

**6d. COPY staged files into tables**

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

Trip data is provided by Citi Bike / NYC Open Data via the public S3 bucket. Check their terms of use for your project.

## Author

<!-- Add your name, GitHub username, or team here -->
