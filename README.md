# Citibike S3 to Snowflake Pipeline

Python pipeline to discover, download, and extract Citi Bike trip data from the public S3 bucket (`https://s3.amazonaws.com/tripdata/`), with analysis helpers for schema discovery and nested annual bundles.

Snowflake loading is planned as a later step.

## Prerequisites

- Python 3.13+ (tested with 3.13)
- Enough local disk for raw zips and extracted CSVs (tens of GB for a full historical run)

## Setup

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Project structure

| Script | Purpose |
|--------|---------|
| `s3_index_to_csv.py` | Lists the public S3 bucket and writes `citibike_s3_file_index.csv` |
| `download_citibike_data.py` | Downloads zips, extracts them, logs to `download_log.jsonl` |
| `analyze_extracted_schema.py` | Scans `extracted/` for CSV schemas and nested zips; writes `analysis_output/` |
| `extract_nested_zips.py` | Extracts inner monthly zips for annual bundles (uses `analysis_output/analysis_summary.json`) |
| `compress_for_snowflake.py` | Builds `.csv.gz` parts (100–250 MB) under `gzip_staging/nyc/` and `gzip_staging/jersey_city/` |

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

- `gzip_staging/nyc/{schema_key}/*.csv.gz` — NYC data (map to your NYC Snowflake schema)
- `gzip_staging/jersey_city/{schema_key}/*.csv.gz` — Jersey City (`JC-*`) data
- `gzip_staging/gzip_manifest.jsonl` — audit log of source files per gzip part

Options: `--region nyc|jersey_city|all`, `--dry-run`, `--min-mb 100`, `--max-mb 250`

## What is not in this repo

Large and generated paths are gitignored (see `.gitignore`):

- `downloads/`
- `extracted/`
- `venv/`
- `__pycache__/`

Clone the repo and run the scripts locally to reproduce data.

## Schema notes

Citibike files use several column layouts over the years (e.g. pre-2020 vs modern `ride_id` format). Use `analysis_output/schema_signatures.csv` before designing Snowflake tables or transforms.

## License / data

Trip data is provided by Citi Bike / NYC Open Data via the public S3 bucket. Check their terms of use for your project.

## Author

<!-- Add your name, GitHub username, or team here -->
