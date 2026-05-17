# Citibike S3 to Snowflake Pipeline

Python pipeline to discover, download, and extract Citi Bike trip data from the public S3 bucket (`https://s3.amazonaws.com/tripdata/`), compress it for staging, and load it into Snowflake.

## Prerequisites

- Python 3.13+ (tested with 3.13)
- Enough local disk for raw zips and extracted CSVs (tens of GB for a full historical run)
- A Snowflake account with:
  - Database: `CITIBIKE_SYSTEM_DATA`
  - Schemas: `STAGING_NYC`, `STAGING_JC`
  - Internal stages: `RAW_INGESTION` in each schema
  - Staging tables from `Snowflake_Scripts/` (before loading)
  - Stored procedure `LOGGING.SP_INGEST_STAGED_FILES` (if using the split staging path)

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

After compression (step 5), you can load data in one of two ways:

```text
  gzip_staging/*.csv.gz
           │
           ├─► [Recommended] stage_files.py
           │         PUT → @STAGING_NYC|JC.RAW_INGESTION
           │         log: snowflake_stage_log.jsonl
           │              │
           │              ▼
           │         CALL LOGGING.SP_INGEST_STAGED_FILES(...)
           │         (COPY INTO tables in Snowflake)
           │
           └─► [Alternative] load_to_snowflake.py
                     PUT + COPY INTO from Python
                     log: snowflake_ingest_log.jsonl
```

| Approach | Python script | Snowflake step | Log file |
|----------|---------------|----------------|----------|
| **Split (recommended)** | `stage_files.py` — upload only | `CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');` | `snowflake_stage_log.jsonl` |
| **All-in-one** | `load_to_snowflake.py` — PUT + COPY | None (COPY runs in Python) | `snowflake_ingest_log.jsonl` |

Both paths read `gzip_staging/gzip_manifest.jsonl`, use the same credentials and `snowflake_config.py` routing, and target the same stages and tables.

## Project structure

| Script / file | Purpose |
|---------------|---------|
| `s3_index_to_csv.py` | Lists the public S3 bucket and writes `citibike_s3_file_index.csv` |
| `download_citibike_data.py` | Downloads zips, extracts them, logs to `download_log.jsonl` |
| `analyze_extracted_schema.py` | Scans `extracted/` for CSV schemas and nested zips; writes `analysis_output/` |
| `extract_nested_zips.py` | Extracts inner monthly zips for annual bundles |
| `compress_for_snowflake.py` | Builds `.csv.gz` parts (100–250 MB) under `gzip_staging/` |
| `stage_files.py` | **PUT** gzip parts onto `RAW_INGESTION` (no COPY); logs to `snowflake_stage_log.jsonl` |
| `load_to_snowflake.py` | **PUT + COPY INTO** from Python (alternative to stage + stored procedure) |
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

### 6a. Stage files (PUT only) — recommended

Uploads local `.csv.gz` files to the internal stage. Does **not** run `COPY INTO` or write to tables.

#### Pre-flight

| Check | |
|-------|---|
| `snowflake_credentials.py` configured | |
| `RAW_INGESTION` stages exist in `STAGING_NYC` and `STAGING_JC` | |
| `gzip_manifest.jsonl` and local `.csv.gz` files present | |
| Staging tables created (DDL scripts) | |

```bash
python stage_files.py --dry-run              # preview PUT targets
python stage_files.py --region jersey_city   # 7 files — good first test
python stage_files.py                        # all pending (~59 parts)
```

| Flag | Description |
|------|-------------|
| `--region nyc` / `jersey_city` / `all` | Limit by region |
| `--dry-run` | Print plan only |
| `--reset-failed` | Re-queue after max retries |
| `--force` | Re-upload with `OVERWRITE=TRUE` |

**Log:** `snowflake_stage_log.jsonl` — one JSON line per attempt (`STAGED`, `FAILED`, `DRY_RUN`). Files with `STAGED` are skipped on re-run unless `--force`.

```bash
tail -f snowflake_stage_log.jsonl
```

### 6b. Ingest staged files (Snowflake)

After `stage_files.py` completes, run the stored procedure in Snowflake to `COPY INTO` the correct tables:

```sql
-- All regions
CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');

-- Or per region
CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('nyc');
CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('jersey_city');
```

The procedure uses `schema_key` / region routing aligned with `snowflake_config.py` to pick the target table (`TRIPS_MODERN`, `TRIPS_LEGACY_V1`, `TRIPS_LEGACY_V2`).

### 6 (alternative). Load from Python (PUT + COPY)

Single script that uploads and copies in one pass (no stored procedure):

```bash
python load_to_snowflake.py --dry-run
python load_to_snowflake.py --region jersey_city
python load_to_snowflake.py
```

**Log:** `snowflake_ingest_log.jsonl`

Use this path if you prefer not to deploy `SP_INGEST_STAGED_FILES`, or for debugging COPY issues from Python.

## What is not in this repo

See `.gitignore`:

- `downloads/`, `extracted/`
- `gzip_staging/nyc/*`, `gzip_staging/jersey_city/*` (manifest kept)
- `venv/`, `__pycache__/`
- `snowflake_credentials.py`

Generated logs (`snowflake_stage_log.jsonl`, `snowflake_ingest_log.jsonl`) may exist locally; they are runtime artifacts.

## Schema notes

Use `analysis_output/schema_signatures.csv` with `Snowflake_Scripts/` when adjusting tables or `SCHEMA_KEY_TO_TABLE` in `snowflake_config.py`.

## License / data

Trip data is provided by Citi Bike / NYC Open Data via the public S3 bucket. Check their terms of use for your project.

## Author

<!-- Add your name, GitHub username, or team here -->
