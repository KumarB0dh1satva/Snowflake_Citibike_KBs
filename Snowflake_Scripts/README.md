# Snowflake scripts — deploy guide

All objects live in database **`CITIBIKE_SYSTEM_DATA`**.

## Schema layout

| Schema | Purpose |
|--------|---------|
| `STAGING_NYC` | NYC staging tables, `TRIPS_ALL`, merge procedures, `RAW_INGESTION` |
| `STAGING_JC` | Jersey City staging tables, `TRIPS_ALL`, merge procedures, `RAW_INGESTION` |
| `LOGGING` | `STAGE_MANIFEST`, `INGEST_LOG`, views, `SP_INGEST_STAGED_FILES` |
| `TEST/` | Operator SQL files (run in a worksheet) |

## Recommended deploy order

| Step | Files | What it creates |
|------|--------|-----------------|
| 1a | `STAGING_NYC/TRIPS_*.sql`, `STAGING_JC/TRIPS_*.sql` | Six per-schema staging tables (`TRIPS_MODERN`, `TRIPS_LEGACY_V1`, `TRIPS_LEGACY_V2`) |
| 1b | `STAGING_NYC/TRIPS_ALL.sql`, `STAGING_JC/TRIPS_ALL.sql` | Unified `TRIPS_ALL` table per region |
| 2 | `LOGGING/STAGE_MANIFEST.sql` | `LOGGING.STAGE_MANIFEST` |
| 3 | `LOGGING/INGEST_LOG.sql` | `LOGGING.INGEST_LOG` |
| 4 | `LOGGING/V_LATEST_INGEST_STATUS.sql` | Latest attempt per file |
| 5 | `LOGGING/V_PENDING_FILES.sql` | Pending manifest rows |
| 6 | `LOGGING/V_TABLE_ROW_COUNTS.sql` | Log vs staging table row counts |
| 7 | `LOGGING/SP_INGEST_STAGED_FILES.sql` | `COPY INTO` from stage → `TRIPS_*` |
| 8 | `STAGING_NYC/SP_LOAD_*.sql`, `STAGING_JC/SP_LOAD_*.sql` | Merge staging → `TRIPS_ALL` |

Stages `RAW_INGESTION` must exist in `STAGING_NYC` and `STAGING_JC`.

## End-to-end flow

```text
compress_for_snowflake.py → gzip_manifest.jsonl
        │
        ├─► stage_files.py              → @STAGING_*/RAW_INGESTION
        ├─► populate_stage_manifest.py  → LOGGING.STAGE_MANIFEST
        └─► SP_INGEST_STAGED_FILES        → TRIPS_MODERN | TRIPS_LEGACY_V1 | V2
                    │
                    └─► SP_LOAD_TRIPS_ALL → TRIPS_ALL (unified)
```

### Python

```bash
python stage_files.py --workers 4
python populate_stage_manifest.py
```

### Snowflake — ingest from stage

```sql
CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');
```

### Snowflake — merge into TRIPS_ALL

```sql
-- NYC
CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_ALL();
-- or individually:
CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_MODERN();
CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_LEGACY_V1();
CALL CITIBIKE_SYSTEM_DATA.STAGING_NYC.SP_LOAD_TRIPS_LEGACY_V2();

-- Jersey City
CALL CITIBIKE_SYSTEM_DATA.STAGING_JC.SP_LOAD_TRIPS_ALL();
```

Each `SP_LOAD_*` procedure is **idempotent**: rows already in `TRIPS_ALL` (same `_SOURCE_FILE` + `_SOURCE_ROW_NUMBER`) are skipped.

## STAGING objects

### Per-layout tables (from `SP_INGEST_STAGED_FILES`)

| Table | Source layout |
|-------|----------------|
| `TRIPS_MODERN` | 13-col `ride_id` (2020+) |
| `TRIPS_LEGACY_V1` | 15-col lowercase legacy |
| `TRIPS_LEGACY_V2` | 15-col Title Case legacy |

### TRIPS_ALL (unified)

| File | Object |
|------|--------|
| `STAGING_NYC/TRIPS_ALL.sql` | `STAGING_NYC.TRIPS_ALL` |
| `STAGING_JC/TRIPS_ALL.sql` | `STAGING_JC.TRIPS_ALL` |

Normalized columns: `RIDE_ID`, `STARTED_AT`/`ENDED_AT`, `TRIP_DURATION`, stations, lat/lng, `MEMBER_CASUAL`, legacy `BIKEID`/`BIRTH_YEAR`/`GENDER`, plus `_SOURCE_*` and `_LOADED_AT_TBL`.

### Merge procedures

| Procedure | Source → target |
|-----------|-----------------|
| `SP_LOAD_TRIPS_MODERN` | `TRIPS_MODERN` → `TRIPS_ALL` |
| `SP_LOAD_TRIPS_LEGACY_V1` | `TRIPS_LEGACY_V1` → `TRIPS_ALL` |
| `SP_LOAD_TRIPS_LEGACY_V2` | `TRIPS_LEGACY_V2` → `TRIPS_ALL` |
| `SP_LOAD_TRIPS_ALL` | Calls all three above |

## LOGGING objects

See **`LOGGING/SP_INGEST_STAGED_FILES_DOC.txt`** for full ingest procedure documentation.

## TEST queries (`TEST/`)

| File | Use |
|------|-----|
| `RUN_STATUS_OVERVIEW.sql` | Ingest counts by status |
| `FILES_STILL_PENDING.sql` | `V_PENDING_FILES` |
| `FAILED_FILES.sql` | Latest failures |
| `PARTIAL_LOADS.sql` | Success with errors |
| `ROW_COUNT_CROSS_CHECK.sql` | Log vs table counts |

## `schema_key` → staging table

| `schema_key` | Table |
|--------------|-------|
| `dc497b4333c4` | `TRIPS_MODERN` |
| `473144999085` | `TRIPS_LEGACY_V1` |
| `e24ee8457e0e` | `TRIPS_LEGACY_V2` |

Must match `snowflake_config.py` / `populate_stage_manifest.py`.
