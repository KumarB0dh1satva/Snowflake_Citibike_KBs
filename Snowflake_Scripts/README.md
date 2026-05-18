# Snowflake scripts — deploy guide

All objects live in database **`CITIBIKE_SYSTEM_DATA`**.

## Schema layout

| Schema | Purpose |
|--------|---------|
| `STAGING_NYC` | NYC trip tables + internal stage `RAW_INGESTION` |
| `STAGING_JC` | Jersey City trip tables + internal stage `RAW_INGESTION` |
| `LOGGING` | Work queue (`STAGE_MANIFEST`), run history (`INGEST_LOG`), monitoring views, ingest stored procedure |
| `TEST` | Convenience SELECTs for operators (not a separate Snowflake schema — run these queries in a worksheet) |

## Recommended deploy order

Run in the Snowflake UI or `snowsql` in this order:

| Step | Files | What it creates |
|------|--------|-----------------|
| 1 | `STAGING_NYC/*.sql`, `STAGING_JC/*.sql` | Six `TRIPS_*` staging tables |
| 2 | `LOGGING/STAGE_MANIFEST.sql` | `LOGGING.STAGE_MANIFEST` |
| 3 | `LOGGING/INGEST_LOG.sql` | `LOGGING.INGEST_LOG` |
| 4 | `LOGGING/V_LATEST_INGEST_STATUS.sql` | View — latest attempt per file |
| 5 | `LOGGING/V_PENDING_FILES.sql` | View — manifest rows not yet `SUCCESS` |
| 6 | `LOGGING/V_TABLE_ROW_COUNTS.sql` | View — log vs table row cross-check |
| 7 | `LOGGING/SP_INGEST_STAGED_FILES.sql` | Stored procedure — `COPY INTO` from stage |

Stages `RAW_INGESTION` must already exist in `STAGING_NYC` and `STAGING_JC` (created outside this repo or in your account setup).

## End-to-end ingest flow (Python + Snowflake)

```text
compress_for_snowflake.py
        │
        ▼
gzip_staging/gzip_manifest.jsonl
        │
        ├─► stage_files.py
        │         PUT → @STAGING_NYC|JC.RAW_INGESTION
        │         log: snowflake_stage_loading_log.jsonl
        │
        ├─► ingest_stage_files.py
        │         INSERT → LOGGING.STAGE_MANIFEST
        │
        └─► CALL LOGGING.SP_INGEST_STAGED_FILES('all')
                  COPY INTO TRIPS_* (positional columns + metadata)
                  log: LOGGING.INGEST_LOG
```

### Python (from repo root)

```bash
python stage_files.py --workers 4
python ingest_stage_files.py
```

### Snowflake

```sql
CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');
-- or 'nyc' | 'jersey_city'
```

## LOGGING objects

| Object | Role |
|--------|------|
| `STAGE_MANIFEST` | One row per `.csv.gz` part: output path, region, `schema_key`, target `SF_SCHEMA` / `SF_TABLE` |
| `INGEST_LOG` | One row per COPY attempt (append-only history) |
| `V_LATEST_INGEST_STATUS` | Latest `INGEST_LOG` row per `OUTPUT_FILE` |
| `V_PENDING_FILES` | Manifest rows where latest status ≠ `SUCCESS` |
| `V_TABLE_ROW_COUNTS` | Sum of `ROWS_LOADED` in log vs `COUNT(*)` on each staging table |
| `SP_INGEST_STAGED_FILES` | Processes pending manifest rows; `PURGE=TRUE` on COPY |

Full procedure design, column mappings, and troubleshooting: **`LOGGING/SP_INGEST_STAGED_FILES_DOC.txt`**.

## TEST queries (`TEST/`)

Run these in a worksheet after the stored procedure (they query `LOGGING` views/tables):

| File | Use |
|------|-----|
| `RUN_STATUS_OVERVIEW.sql` | Counts by `STATUS`, region, table |
| `FILES_STILL_PENDING.sql` | `SELECT * FROM V_PENDING_FILES` |
| `FAILED_FILES.sql` | Latest failed files and error messages |
| `PARTIAL_LOADS.sql` | Files with `ERRORS_SEEN > 0` |
| `ROW_COUNT_CROSS_CHECK.sql` | `SELECT * FROM V_TABLE_ROW_COUNTS` |

## `schema_key` → table mapping

Must match `snowflake_config.py` / `ingest_stage_files.py`:

| `schema_key` | Table | CSV layout |
|--------------|-------|------------|
| `dc497b4333c4` | `TRIPS_MODERN` | 13-column modern (`ride_id`, …) |
| `473144999085` | `TRIPS_LEGACY_V1` | 15-column lowercase legacy |
| `e24ee8457e0e` | `TRIPS_LEGACY_V2` | 15-column Title Case legacy |

Same table names in both `STAGING_NYC` and `STAGING_JC`; region comes from manifest `region`.

## Stage file naming

`stage_files.py` uploads using the **basename** only (e.g. `nyc_dc497b4333c4_part0028.csv.gz`).  
`SP_INGEST_STAGED_FILES` copies from `@<schema>.RAW_INGESTION/<basename>`.  
`LIST @...RAW_INGESTION` may show names prefixed with `raw_ingestion/` — that is normal for internal stages.
