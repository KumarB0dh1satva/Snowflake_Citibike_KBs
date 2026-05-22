# Snowflake staged ingest — operator guide

This document describes the **recommended** path from compressed local files to populated `STAGING_NYC` / `STAGING_JC` tables using `stage_files.py`, `populate_stage_manifest.py`, `LOGGING.SP_INGEST_STAGED_FILES`, and optionally `SP_LOAD_TRIPS_ALL` into `TRIPS_ALL`.

For S3 download and compression steps, see the main [README](../README.md).

## Architecture

```text
┌─────────────────────┐     PUT      ┌──────────────────────────┐
│  gzip_staging/      │ ──────────►  │ @STAGING_NYC.RAW_INGESTION │
│  *.csv.gz           │              │ @STAGING_JC.RAW_INGESTION  │
└─────────────────────┘              └────────────┬─────────────┘
         │                                        │
         │ populate_stage_manifest.py             │ SP_INGEST_STAGED_FILES
         ▼                                        ▼
┌─────────────────────┐              ┌──────────────────────────┐
│ LOGGING.            │              │ STAGING_NYC.TRIPS_*      │
│ STAGE_MANIFEST      │ ──queue──►   │ STAGING_JC.TRIPS_*       │
└─────────────────────┘              └──────────────────────────┘
         │                                        │
         └──────────── INGEST_LOG ◄───────────────┘
```

| Layer | Location | Responsibility |
|-------|----------|----------------|
| Local staging | `stage_files.py` | Upload gzip parts; log to `snowflake_stage_loading_log.jsonl` |
| Work queue | `LOGGING.STAGE_MANIFEST` | Which file → which schema/table |
| Ingest | `SP_INGEST_STAGED_FILES` | `COPY INTO` with positional columns + `_SOURCE_*` metadata |
| Audit | `LOGGING.INGEST_LOG` | Per-attempt rows loaded, errors, timestamps |
| Ops | `Snowflake_Scripts/TEST/*.sql` | Pending / failed / row-count checks |

## One-time Snowflake setup

1. Create six staging tables — `Snowflake_Scripts/STAGING_NYC/*.sql` and `STAGING_JC/*.sql`.
2. Deploy LOGGING DDL and views — see [Snowflake_Scripts/README.md](../Snowflake_Scripts/README.md).
3. Deploy the procedure — `Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES.sql`.

Detailed DDL comments and embedded procedure source: `Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES_DOC.txt`.

## Runbook

### 1. Compress (if not done)

```bash
python compress_for_snowflake.py
```

Produces `gzip_staging/gzip_manifest.jsonl` and regional `.csv.gz` parts.

### 2. Upload to Snowflake stage

```bash
python stage_files.py --dry-run
python stage_files.py --region jersey_city --workers 4   # smoke test
python stage_files.py --workers 4
python stage_files.py --list-stage                       # verify LIST output
```

- Log: `snowflake_stage_loading_log.jsonl`
- `UPLOADED` = bytes sent this run; `ON_STAGE` = already present (verified with `LIST`)

### 3. Load manifest into LOGGING

```bash
python populate_stage_manifest.py
```

Populates `LOGGING.STAGE_MANIFEST` from `gzip_manifest.jsonl` using `snowflake_config.py` routing. Safe to re-run (skips existing `OUTPUT_FILE` values).

Verify:

```sql
SELECT REGION, SF_TABLE, COUNT(*) 
FROM CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST 
GROUP BY 1, 2 ORDER BY 1, 2;
```

Expect ~59 rows total (7 JC + 52 NYC) for a full compress run.

### 4. COPY into staging tables

```sql
-- Preview queue
SELECT * FROM CITIBIKE_SYSTEM_DATA.LOGGING.V_PENDING_FILES;

-- Run ingest
CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');
```

Returns a summary string, e.g. `success=52 | failed=0`.

Per-file results land in `LOGGING.INGEST_LOG` immediately (no need to wait for the whole batch).

### 5. Merge into TRIPS_ALL (optional unified table)

After data is in `TRIPS_MODERN` / `TRIPS_LEGACY_V1` / `TRIPS_LEGACY_V2`:

```sql
CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_ALL();
CALL CITIBIKE_SYSTEM_DATA.STAGING_JC.SP_LOAD_TRIPS_ALL();
```

Deploy `TRIPS_ALL.sql` and `SP_LOAD_*.sql` from `Snowflake_Scripts/STAGING_NYC/` and `STAGING_JC/` first. Procedures skip rows already present in `TRIPS_ALL` (same `_SOURCE_FILE` + `_SOURCE_ROW_NUMBER`).

### 6. Monitor and validate

```sql
-- Latest status per file
SELECT * FROM CITIBIKE_SYSTEM_DATA.LOGGING.V_LATEST_INGEST_STATUS
ORDER BY ENDED_AT_UTC DESC;

-- Aggregated run stats (see TEST/RUN_STATUS_OVERVIEW.sql)
-- Failed only (see TEST/FAILED_FILES.sql)
-- Log vs table counts (see TEST/ROW_COUNT_CROSS_CHECK.sql)
SELECT * FROM CITIBIKE_SYSTEM_DATA.LOGGING.V_TABLE_ROW_COUNTS;
```

## How the stored procedure loads data

For each pending row in `STAGE_MANIFEST`:

1. Resolves stage path: `@CITIBIKE_SYSTEM_DATA.<SF_SCHEMA>.RAW_INGESTION/<basename>`.
2. Builds `COPY INTO <table> (data_cols…, _source_file, _source_row_number)` from a `SELECT` over the staged file.
3. Uses **positional** `$1..$N` columns so CSV header casing does not matter.
4. Sets `METADATA$FILENAME` and `METADATA$FILE_ROW_NUMBER` into `_SOURCE_FILE` / `_SOURCE_ROW_NUMBER`.
5. `PURGE=TRUE` removes the file from the stage after a successful load.
6. Inserts one row into `INGEST_LOG` with `ROWS_LOADED`, `ERRORS_SEEN`, and any error text.

Table column order in the SP must match `Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES.sql` (`TABLE_COLS` in JavaScript).

## Alternative: all-in-one Python load

`load_to_snowflake.py` performs PUT + COPY from Python and writes `snowflake_ingest_log.jsonl`. Use it when you do not want the LOGGING schema / stored procedure. It does not populate `STAGE_MANIFEST` or `INGEST_LOG`.

## Troubleshooting

| Symptom | Likely cause | Action |
|---------|----------------|--------|
| PUT shows `ON_STAGE` / `SKIPPED` | File already on stage | Normal if re-running; use `stage_files.py --list-stage` or `LIST @...` |
| `V_PENDING_FILES` non-empty after SP | COPY failed or not run | Check `INGEST_LOG`; run `TEST/FAILED_FILES.sql` |
| `ROWS_LOADED = 0`, large `ERRORS_SEEN` | Column/type mismatch | Compare CSV sample to DDL; check SP `TABLE_COLS` order |
| `LOAD_SKIPPED` in `COPY_STATUS_RAW` | Snowflake load history | See SP doc — may need truncate or load history refresh |
| Manifest row missing | `populate_stage_manifest.py` skipped unmapped key | Add key to `SCHEMA_KEY_TO_TABLE` in `snowflake_config.py` |
| Stage file not found | Basename mismatch | Ensure `stage_files.py` completed; basename = last segment of `output_file` |

## Related files

| Path | Description |
|------|-------------|
| [../README.md](../README.md) | Full S3 → Snowflake pipeline |
| [../Snowflake_Scripts/README.md](../Snowflake_Scripts/README.md) | SQL deploy order and object index |
| [../Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES_DOC.txt](../Snowflake_Scripts/LOGGING/SP_INGEST_STAGED_FILES_DOC.txt) | Full procedure documentation |
| [../stage_files.py](../stage_files.py) | Parallel PUT to stage |
| [../populate_stage_manifest.py](../populate_stage_manifest.py) | Load `STAGE_MANIFEST` |
| [../snowflake_config.py](../snowflake_config.py) | Shared routing config |
