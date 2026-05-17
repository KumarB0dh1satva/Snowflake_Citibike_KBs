# Citibike S3 to Snowflake Pipeline

Python pipeline to discover, download, and extract Citi Bike trip data from the public S3 bucket (`https://s3.amazonaws.com/tripdata/`), compress it for staging, and load it into Snowflake via the Python connector.

## Prerequisites

- Python 3.13+ (tested with 3.13)
- Enough local disk for raw zips and extracted CSVs (tens of GB for a full historical run)
- A Snowflake account with:
  - Database: `CITIBIKE_SYSTEM_DATA`
  - Schemas: `STAGING_NYC`, `STAGING_JC`
  - Internal stages: `RAW_INGESTION` in each schema (already created in your account)
  - Staging tables created from `Snowflake_Scripts/` (see below) **before** running the loader

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

Non-secret load settings (schema names, stage name, table mapping) live in `snowflake_config.py`.

## Project structure

| Script / file | Purpose |
|---------------|---------|
| `s3_index_to_csv.py` | Lists the public S3 bucket and writes `citibike_s3_file_index.csv` |
| `download_citibike_data.py` | Downloads zips, extracts them, logs to `download_log.jsonl` |
| `analyze_extracted_schema.py` | Scans `extracted/` for CSV schemas and nested zips; writes `analysis_output/` |
| `extract_nested_zips.py` | Extracts inner monthly zips for annual bundles (uses `analysis_output/analysis_summary.json`) |
| `compress_for_snowflake.py` | Builds `.csv.gz` parts (100–250 MB) under `gzip_staging/` |
| `load_to_snowflake.py` | `PUT` + `COPY INTO` gzip parts into Snowflake; logs to `snowflake_ingest_log.jsonl` |
| `snowflake_config.py` | Region → schema, `schema_key` → table, stage name (no passwords) |
| `snowflake_credentials.py` | Account, user, password, warehouse, database, role (**gitignored**) |
| `snowflake_credentials.example.py` | Template for `snowflake_credentials.py` |
| `Snowflake_Scripts/` | `CREATE TABLE` DDL for staging tables (NYC + JC) |

### Snowflake DDL (`Snowflake_Scripts/`)

Run these scripts in the Snowflake UI (or `snowsql`) **once per schema** before loading data. Each script uses `CREATE TABLE IF NOT EXISTS` against `CITIBIKE_SYSTEM_DATA`.

| Path | Table | Source CSV layout |
|------|-------|-------------------|
| `STAGING_NYC/TRIPS_MODERN.sql` | `STAGING_NYC.TRIPS_MODERN` | schema_1 — 13 cols, `ride_id` (2020+) |
| `STAGING_NYC/TRIPS_LEGACY_V1.sql` | `STAGING_NYC.TRIPS_LEGACY_V1` | schema_2 — 15 cols, lowercase (`tripduration`, …) |
| `STAGING_NYC/TRIPS_LEGACY_V2.sql` | `STAGING_NYC.TRIPS_LEGACY_V2` | schema_3 — 15 cols, Title Case (`Trip Duration`, …) |
| `STAGING_JC/TRIPS_MODERN.sql` | `STAGING_JC.TRIPS_MODERN` | Same as NYC modern |
| `STAGING_JC/TRIPS_LEGACY_V1.sql` | `STAGING_JC.TRIPS_LEGACY_V1` | Same as NYC legacy v1 |
| `STAGING_JC/TRIPS_LEGACY_V2.sql` | `STAGING_JC.TRIPS_LEGACY_V2` | Same as NYC legacy v2 |

Every table includes three ingestion metadata columns (populated by defaults or a future loader enhancement):

- `_SOURCE_FILE` — source `.csv.gz` name
- `_SOURCE_ROW_NUMBER` — row index in file
- `_LOADED_AT` — load timestamp (default `CURRENT_TIMESTAMP()`)

Example (repeat for all six files):

```sql
-- In Snowflake worksheet: run contents of Snowflake_Scripts/STAGING_NYC/TRIPS_MODERN.sql
```

Verify tables exist:

```sql
SHOW TABLES IN SCHEMA CITIBIKE_SYSTEM_DATA.STAGING_NYC;
SHOW TABLES IN SCHEMA CITIBIKE_SYSTEM_DATA.STAGING_JC;
```

You should see `TRIPS_MODERN`, `TRIPS_LEGACY_V1`, and `TRIPS_LEGACY_V2` in each schema.

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

**Outputs:**

- `downloads/` — raw `.zip` files from S3
- `extracted/` — first-level extract per file
- `download_log.jsonl` — download and unzip status per file

Re-runs skip files already logged as complete with matching size (see script for retry/cleanup behavior).

### 3. Analyze extracted data

```bash
python analyze_extracted_schema.py
```

**Outputs (under `analysis_output/`):**

- `bundle_structure_report.csv` — per-bundle layout (CSV vs nested zip)
- `extracted_file_inventory.csv` — file inventory
- `schema_signatures.csv` — unique column layouts
- `analysis_summary.json` — summary including `nested_zip_only_bundles` when applicable
- `nested_zip_inventory.csv` — contents of nested zips (when present)

### 4. Extract nested zips (annual bundles only)

Run after step 3 when `analysis_summary.json` lists bundles under `nested_zip_only_bundles` (e.g. 2020–2023 annual packs that contain monthly zips).

```bash
python extract_nested_zips.py
```

**Output:** `nested_extract_log.jsonl`  
Nested zips under `extracted/` are removed after a verified extract to save space; parent zips remain in `downloads/` for re-processing if needed.

### 5. Compress for Snowflake staging

```bash
python compress_for_snowflake.py
```

**Outputs:**

- `gzip_staging/nyc/{schema_key}/*.csv.gz` — NYC data
- `gzip_staging/jersey_city/{schema_key}/*.csv.gz` — Jersey City (`JC-*`) data
- `gzip_staging/gzip_manifest.jsonl` — audit log of source files per gzip part

Options: `--region nyc|jersey_city|all`, `--dry-run`, `--min-mb 100`, `--max-mb 250`

### 6. Load into Snowflake

#### Pre-flight checklist

| Step | Check |
|------|--------|
| 1 | `snowflake_credentials.py` exists and warehouse is running |
| 2 | All six `Snowflake_Scripts/**/*.sql` files executed in Snowflake |
| 3 | `gzip_staging/gzip_manifest.jsonl` exists and local `.csv.gz` files are present |
| 4 | `python load_to_snowflake.py --dry-run` shows expected schema → table targets |

#### Run the loader

```bash
python load_to_snowflake.py --dry-run    # preview targets (no Snowflake calls)
python load_to_snowflake.py --region jersey_city   # smaller test: 7 files
python load_to_snowflake.py                # all pending manifest files (~59 parts)
```

**Options:**

| Flag | Description |
|------|-------------|
| `--region nyc` | NYC only → `STAGING_NYC` |
| `--region jersey_city` | Jersey City only → `STAGING_JC` |
| `--region all` | Both regions (default) |
| `--dry-run` | Print load plan only |
| `--reset-failed` | Re-queue files that hit max retries |

**Per file:** reads `gzip_manifest.jsonl` → `PUT` to `@CITIBIKE_SYSTEM_DATA.<schema>.RAW_INGESTION` → `COPY INTO` the mapped table → appends one JSON line to `snowflake_ingest_log.jsonl`.

Successful files are skipped on later runs. Failed files retry up to 3 times (`MAX_RETRIES` in `snowflake_config.py`).

Monitor progress:

```bash
tail -f snowflake_ingest_log.jsonl
```

#### Snowflake routing (`snowflake_config.py`)

| Manifest `region` | Snowflake schema | Stage |
|-------------------|------------------|-------|
| `nyc` | `STAGING_NYC` | `RAW_INGESTION` |
| `jersey_city` | `STAGING_JC` | `RAW_INGESTION` |

`schema_key` is an MD5 fingerprint of the CSV column layout (from `compress_for_snowflake.py`), **not** a Snowflake schema name:

| `schema_key` | Target table | CSV layout (`analysis_output/schema_signatures.csv`) |
|--------------|--------------|------------------------------------------------------|
| `dc497b4333c4` | `TRIPS_MODERN` | schema_1 — 13 columns, `ride_id`, … |
| `473144999085` | `TRIPS_LEGACY_V1` | schema_2 — 15 columns, lowercase (`tripduration`, …) |
| `e24ee8457e0e` | `TRIPS_LEGACY_V2` | schema_3 — 15 columns, Title Case (`Trip Duration`, …) |

The same mapping applies in both `STAGING_NYC` and `STAGING_JC`; only the schema prefix changes.

#### COPY behavior note

`load_to_snowflake.py` runs a standard `COPY INTO <table> FROM @stage` with `SKIP_HEADER = 1`. Snowflake matches CSV columns to table columns **by position** unless you extend the loader. Your DDL tables have **18 / 16 / 16** columns (data + 3 metadata columns). If loads fail with column-count or header errors, either:

- load only into the data columns via an explicit column list + `MATCH_BY_COLUMN_NAME`, or  
- temporarily use tables without the `_SOURCE_*` columns for the first test.

Start with `--region jersey_city` and one manifest part to validate before the full NYC run.

## What is not in this repo

Large and generated paths are gitignored (see `.gitignore`):

- `downloads/`
- `extracted/`
- `gzip_staging/nyc/*` and `gzip_staging/jersey_city/*` (compressed data; manifest is kept)
- `venv/`
- `__pycache__/`
- `snowflake_credentials.py` (passwords)

Clone the repo, configure Snowflake credentials locally, run the DDL scripts, and run the Python pipeline to reproduce data.

## Schema notes

Citibike files use several column layouts over the years. Use `analysis_output/schema_signatures.csv` alongside `Snowflake_Scripts/` when adjusting tables or `SCHEMA_KEY_TO_TABLE` in `snowflake_config.py`.

## License / data

Trip data is provided by Citi Bike / NYC Open Data via the public S3 bucket. Check their terms of use for your project.

## Author

<!-- Add your name, GitHub username, or team here -->
