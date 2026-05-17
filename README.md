# Citibike S3 to Snowflake Pipeline

Python pipeline to discover, download, and extract Citi Bike trip data from the public S3 bucket (`https://s3.amazonaws.com/tripdata/`), compress it for staging, and load it into Snowflake.

## Prerequisites

- Python 3.13+ (tested with 3.13)
- Enough local disk for raw zips and extracted CSVs (tens of GB for a full historical run)
- A Snowflake account with:
  - Database: `CITIBIKE_SYSTEM_DATA`
  - Schemas: `STAGING_NYC`, `STAGING_JC`
  - Internal stages: `RAW_INGESTION` in each schema
  - Staging tables from `Snowflake_Scripts/` (only if using `load_to_snowflake.py`)

## Setup

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Snowflake credentials

Secrets are kept out of git. Copy the example file and fill in your account details:

```bash
cp snowflake_credentials.example.py snowflake_credentials.py
# edit snowflake_credentials.py
```

Alternatively, set environment variables (they override the credentials file):

```bash
export SF_ACCOUNT="your_account_identifier"
export SF_USER="your_username"
export SF_PASSWORD="your_password"
export SF_WAREHOUSE="LOADING_LOCAL"
export SF_DATABASE="CITIBIKE_SYSTEM_DATA"
export SF_ROLE="SYSADMIN"
```

Non-secret settings (schema names, stage name, table mapping, retries) live in `snowflake_config.py`.

## Snowflake ingest architecture

After compression (step 5), pick **one** Python loader:

```text
  gzip_staging/*.csv.gz
           │
           ├─► stage_files.py  (alternative: stage only)
           │         parallel PUT → @STAGING_NYC|JC.RAW_INGESTION
           │         log: snowflake_stage_loading_log.jsonl
           │
           └─► load_to_snowflake.py  (alternative: stage + tables)
                     PUT + COPY INTO staging tables
                     log: snowflake_ingest_log.jsonl
```

| Script | What it does | Log file | Tables required? |
|--------|----------------|----------|------------------|
| `stage_files.py` | Upload `.csv.gz` to `RAW_INGESTION` only (parallel) | `snowflake_stage_loading_log.jsonl` | No |
| `load_to_snowflake.py` | `PUT` + `COPY INTO` `TRIPS_*` tables | `snowflake_ingest_log.jsonl` | Yes (run `Snowflake_Scripts/` DDL first) |

Both read `gzip_staging/gzip_manifest.jsonl`, use `snowflake_credentials.py` / `snowflake_config.py`, and target the same internal stages. `stage_files.py` does not run `COPY INTO` or reference downstream Snowflake jobs.

## Project structure

| Script / file | Purpose |
|---------------|---------|
| `s3_index_to_csv.py` | Lists the public S3 bucket and writes `citibike_s3_file_index.csv` |
| `download_citibike_data.py` | Downloads zips, extracts them, logs to `download_log.jsonl` |
| `analyze_extracted_schema.py` | Scans `extracted/` for CSV schemas and nested zips; writes `analysis_output/` |
| `extract_nested_zips.py` | Extracts inner monthly zips for annual bundles |
| `compress_for_snowflake.py` | Builds `.csv.gz` parts (100–250 MB) under `gzip_staging/` |
| `stage_files.py` | Parallel **PUT** to `RAW_INGESTION` only; logs to `snowflake_stage_loading_log.jsonl` |
| `load_to_snowflake.py` | **PUT + COPY INTO** staging tables from Python |
| `snowflake_config.py` | Region → schema, `schema_key` → table, stage name (no passwords) |
| `snowflake_credentials.py` | Account, user, password, warehouse, database, role (**gitignored**) |
| `snowflake_credentials.example.py` | Template for `snowflake_credentials.py` |
| `Snowflake_Scripts/` | `CREATE TABLE` DDL for `STAGING_NYC` and `STAGING_JC` |

### Snowflake DDL (`Snowflake_Scripts/`)

Run these in the Snowflake UI (or `snowsql`) **once** before loading. Each file uses `CREATE TABLE IF NOT EXISTS` on `CITIBIKE_SYSTEM_DATA`.

| Path | Table | Source CSV layout |
|------|-------|-------------------|
| `STAGING_NYC/TRIPS_MODERN.sql` | `STAGING_NYC.TRIPS_MODERN` | schema_1 — 13 cols, `ride_id` (2020+) |
| `STAGING_NYC/TRIPS_LEGACY_V1.sql` | `STAGING_NYC.TRIPS_LEGACY_V1` | schema_2 — lowercase legacy |
| `STAGING_NYC/TRIPS_LEGACY_V2.sql` | `STAGING_NYC.TRIPS_LEGACY_V2` | schema_3 — Title Case legacy |
| `STAGING_JC/TRIPS_MODERN.sql` | `STAGING_JC.TRIPS_MODERN` | Same as NYC modern |
| `STAGING_JC/TRIPS_LEGACY_V1.sql` | `STAGING_JC.TRIPS_LEGACY_V1` | Same as NYC legacy v1 |
| `STAGING_JC/TRIPS_LEGACY_V2.sql` | `STAGING_JC.TRIPS_LEGACY_V2` | Same as NYC legacy v2 |

Tables include metadata columns `_SOURCE_FILE`, `_SOURCE_ROW_NUMBER`, `_LOADED_AT` (filled by the ingest procedure or loader).

```sql
SHOW TABLES IN SCHEMA CITIBIKE_SYSTEM_DATA.STAGING_NYC;
SHOW TABLES IN SCHEMA CITIBIKE_SYSTEM_DATA.STAGING_JC;
```

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

`schema_key` is an MD5 fingerprint of column headers from `compress_for_snowflake.py`, not a Snowflake schema name.

## Pipeline (run in order)

### 1. Build S3 file index

```bash
python s3_index_to_csv.py
```

**Output:** `citibike_s3_file_index.csv`

### 2. Download and extract

```bash
python download_citibike_data.py
```

**Outputs:** `downloads/`, `extracted/`, `download_log.jsonl`

### 3. Analyze extracted data

```bash
python analyze_extracted_schema.py
```

**Outputs:** `analysis_output/` (inventory, `schema_signatures.csv`, etc.)

### 4. Extract nested zips (when needed)

```bash
python extract_nested_zips.py
```

**Output:** `nested_extract_log.jsonl`

### 5. Compress for Snowflake staging

```bash
python compress_for_snowflake.py
```

**Outputs:**

- `gzip_staging/nyc/{schema_key}/*.csv.gz`
- `gzip_staging/jersey_city/{schema_key}/*.csv.gz`
- `gzip_staging/gzip_manifest.jsonl`

Options: `--region nyc|jersey_city|all`, `--dry-run`, `--min-mb 100`, `--max-mb 250`

### 6a. Load to stage only (`stage_files.py`)

Uploads local `.csv.gz` files to `RAW_INGESTION` in parallel. No `COPY INTO`, no table writes, no downstream steps in this script.

#### Pre-flight

| Check | |
|-------|---|
| `snowflake_credentials.py` configured | |
| `RAW_INGESTION` stages exist in `STAGING_NYC` and `STAGING_JC` | |
| `gzip_manifest.jsonl` and local `.csv.gz` files present | |

```bash
python stage_files.py --dry-run              # preview PUT targets
python stage_files.py --region jersey_city   # 7 files — good first test
python stage_files.py --workers 4            # parallel uploads (default from snowflake_config.py)
python stage_files.py                        # all pending (~59 parts)
```

| Flag | Description |
|------|-------------|
| `--region nyc` / `jersey_city` / `all` | Limit by region |
| `--workers N` | Concurrent file uploads (default: `PARALLEL_STAGE_WORKERS` in config) |
| `--dry-run` | Print plan only |
| `--reset-failed` | Re-queue after max retries |
| `--force` | Re-upload with `OVERWRITE=TRUE` |
| `--no-overwrite` | Use `OVERWRITE=FALSE` (PUT may return `SKIPPED` if file already on stage) |
| `--list-stage` | Print files currently on each `RAW_INGESTION` stage |

Default PUT uses `OVERWRITE=TRUE` (`STAGE_PUT_OVERWRITE` in `snowflake_config.py`). After each PUT, the script runs `LIST` on the stage to verify the file is present.

**Log:** `snowflake_stage_loading_log.jsonl` — `SUCCESS` with `put_status` `UPLOADED` (new bytes sent) or `SKIPPED` (already on stage, verified). Console shows `ON_STAGE` for the latter — not a failure. Files with verified `SUCCESS` are skipped on re-run unless `--force`.

```bash
python stage_files.py --list-stage   # confirm what Snowflake has
```

```bash
tail -f snowflake_stage_loading_log.jsonl
```

Tune parallelism in `snowflake_config.py`:

- `PARALLEL_STAGE_WORKERS` — how many files upload at once (`--workers`)
- `PARALLEL_PUT_THREADS` — Snowflake `PUT` threads per file

### 6b. Load to stage + tables (`load_to_snowflake.py`)

Uploads and `COPY INTO` staging tables in one Python run:

```bash
python load_to_snowflake.py --dry-run
python load_to_snowflake.py --region jersey_city
python load_to_snowflake.py
```

**Log:** `snowflake_ingest_log.jsonl`

Requires staging tables from `Snowflake_Scripts/` before running.

## What is not in this repo

See `.gitignore`:

- `downloads/`, `extracted/`
- `gzip_staging/nyc/*`, `gzip_staging/jersey_city/*` (manifest kept)
- `venv/`, `__pycache__/`
- `snowflake_credentials.py`

Generated logs (`snowflake_stage_loading_log.jsonl`, `snowflake_ingest_log.jsonl`) may exist locally; they are runtime artifacts.

## Schema notes

Use `analysis_output/schema_signatures.csv` with `Snowflake_Scripts/` when adjusting tables or `SCHEMA_KEY_TO_TABLE` in `snowflake_config.py`.

## License / data

Trip data is provided by Citi Bike / NYC Open Data via the public S3 bucket. Check their terms of use for your project.

## Author

<!-- Add your name, GitHub username, or team here -->
