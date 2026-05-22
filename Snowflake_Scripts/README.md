# Snowflake scripts — deploy guide

All objects live in database **`CITIBIKE_SYSTEM_DATA`**.

## Schema layout

| Schema | Purpose |
|--------|---------|
| `STAGING_NYC` | NYC staging tables, `TRIPS_ALL`, merge procedures, `RAW_INGESTION` |
| `STAGING_JC` | Jersey City staging tables, `TRIPS_ALL`, merge procedures, `RAW_INGESTION` |
| `LOGGING` | `STAGE_MANIFEST`, `INGEST_LOG`, views, `SP_INGEST_STAGED_FILES` |
| `INT_UDM_NYC` | NYC integration layer — `DIM_DATES`, `DIM_STATION`, `FACT_RIDE`, build procedures |
| `INT_UDM_JC` | Jersey City integration layer — same star schema, region-specific station/fact builds |
| `RPT_NYC` | NYC reporting views on `INT_UDM_NYC` (ridership, stations, routes) |
| `RPT_JC` | Jersey City reporting views on `INT_UDM_JC` |
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
| 9 | `INT_UDM_NYC/INT_UDM_NYC.sql`, `INT_UDM_JC/INT_UDM_JC.sql` | Integration schemas |
| 10 | `INT_UDM_NYC/DIM_DATES.sql` | `INT_UDM_NYC.DIM_DATES` (DDL only; data via procedure) |
| 11 | `INT_UDM_JC/DIM_DATES.sql` | `INT_UDM_JC.DIM_DATES` (clone of NYC table) |
| 12 | `INT_UDM_NYC/DIM_STATION.sql`, `INT_UDM_JC/DIM_STATION.sql` | Station dimensions (empty until built) |
| 13 | `INT_UDM_NYC/FACT_RIDE.sql`, `INT_UDM_JC/FACT_RIDE.sql` | Fact tables (empty until built) |
| 14 | `INT_UDM_NYC/SP_BUILD_DIM_DATES.sql` | Date spine 2013–2030 + holiday flags; syncs to JC |
| 15 | `INT_UDM_NYC/SP_BUILD_DIM_STATION_NYC.sql`, `INT_UDM_JC/SP_BUILD_DIM_STATION_JC.sql` | Stations from `TRIPS_ALL` |
| 16 | `INT_UDM_NYC/SP_BUILD_FACT_RIDE_NYC.sql`, `INT_UDM_JC/SP_BUILD_FACT_RIDE_JC.sql` | Rides from `TRIPS_ALL` with dimension FKs |
| 17 | `RPT_NYC/RPT_NYC.sql`, `RPT_JC/RPT_JC.sql` | Reporting schemas |
| 18–20 | `RPT_NYC/RPT_*.sql` | NYC views: ridership, stations, routes |
| 21–23 | `RPT_JC/RPT_*.sql` | JC views: same three reports |

Stages `RAW_INGESTION` must exist in `STAGING_NYC` and `STAGING_JC`.

**Integration layer prerequisites:** `STAGING_*`.`TRIPS_ALL` populated (steps 1–8). Run `SP_BUILD_DIM_DATES` before fact builds so `START_DATE_SK` / `END_DATE_SK` resolve. Run `SP_BUILD_DIM_STATION_*` before `SP_BUILD_FACT_RIDE_*` so station SKs resolve.

**Reporting layer prerequisites:** `INT_UDM_*`.`FACT_RIDE` populated (step 16). Deploy `RPT_*` views last (steps 17–23). Views are read-only; no stored procedures.

## End-to-end flow

```text
compress_for_snowflake.py → gzip_manifest.jsonl
        │
        ├─► stage_files.py              → @STAGING_*/RAW_INGESTION
        ├─► populate_stage_manifest.py  → LOGGING.STAGE_MANIFEST
        └─► SP_INGEST_STAGED_FILES        → TRIPS_MODERN | TRIPS_LEGACY_V1 | V2
                    │
                    └─► SP_LOAD_TRIPS_ALL → TRIPS_ALL (unified)
                              │
                              └─► INT_UDM_* (dimensions + FACT_RIDE)
                                        │
                                        └─► RPT_* views (reporting)
```

### Integration layer (after `TRIPS_ALL`)

```sql
-- Shared date spine (NYC + JC)
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_DATES();

-- NYC
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_STATION_NYC();
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_FACT_RIDE_NYC(FALSE);  -- incremental
-- CALL ... SP_BUILD_FACT_RIDE_NYC(TRUE);  -- full reload (truncate first)

-- Jersey City
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_DIM_STATION_JC();
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_FACT_RIDE_JC(FALSE);
```

### Reporting layer (after `FACT_RIDE`)

Deploy `RPT_NYC/*.sql` and `RPT_JC/*.sql`, then query:

```sql
SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_RIDERSHIP_OVER_TIME;
SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.RPT_JC.RPT_TOP_ROUTES;
```

Full runbook: [../docs/SNOWFLAKE_REPORTING.md](../docs/SNOWFLAKE_REPORTING.md).

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

## INT_UDM objects (integration layer)

Star-schema layer on top of unified `TRIPS_ALL`. Trip fields align with [Citi Bike system data](https://citibikenyc.com/system-data) (ride ID, stations, lat/lng, member/casual, legacy demographics). Source: `STAGING_NYC.TRIPS_ALL` / `STAGING_JC.TRIPS_ALL`.

### Schemas

| File | Object |
|------|--------|
| `INT_UDM_NYC/INT_UDM_NYC.sql` | `INT_UDM_NYC` |
| `INT_UDM_JC/INT_UDM_JC.sql` | `INT_UDM_JC` |

### Dimension tables

| Table | Schema | Populated by | Notes |
|-------|--------|--------------|-------|
| `DIM_DATES` | NYC | `SP_BUILD_DIM_DATES` | `DATE_SK` = `YYYYMMDD`; calendar, season, US federal + NY holidays (2013–2030) |
| `DIM_DATES` | JC | `SP_BUILD_DIM_DATES` (sync) | `CLONE` of NYC DDL; rows copied from NYC in same procedure |
| `DIM_STATION` | NYC / JC | `SP_BUILD_DIM_STATION_NYC` / `_JC` | One row per canonical station name (`TRIM` + `UPPER`); median lat/lng; legacy vs modern IDs |

### Fact table

| Table | Source | Clustering |
|-------|--------|------------|
| `FACT_RIDE` | Region `TRIPS_ALL` | `START_DATE_SK` |

One row per trip. Surrogate `RIDE_SK`; links to `DIM_STATION` (start/end), `DIM_DATES` (start/end dates), and staging via `TRIP_SK_STAGING`. Derived: `TRIP_DURATION_MIN`, `START_HOUR`, `AGE_AT_RIDE`, `AGE_BUCKET` (legacy). Filters: duration 60–86400 s (matches Citi Bike published minimum), dates within dim spine.

### Build procedures

| Procedure | Arguments | Behavior |
|-----------|-----------|----------|
| `SP_BUILD_DIM_DATES` | `P_START_DATE`, `P_END_DATE` (defaults `2013-01-01` … `2030-12-31`) | `TRUNCATE` + reload NYC `DIM_DATES`; `TRUNCATE` + copy into JC |
| `SP_BUILD_DIM_STATION_NYC` | — | `TRUNCATE` + rebuild from `STAGING_NYC.TRIPS_ALL`; NYC lat/lng bounds |
| `SP_BUILD_DIM_STATION_JC` | — | `TRUNCATE` + rebuild from `STAGING_JC.TRIPS_ALL`; tighter JC bounds |
| `SP_BUILD_FACT_RIDE_NYC` | `P_TRUNCATE_FIRST` (default `FALSE`) | Insert new trips only (`_SOURCE_FILE` + `_SOURCE_ROW_NUMBER`); optional full truncate |
| `SP_BUILD_FACT_RIDE_JC` | `P_TRUNCATE_FIRST` (default `FALSE`) | Same idempotency for JC |

Station resolution uses **station name** as the master key (IDs differ between legacy and modern layouts). `AVAILABILITY` on `DIM_STATION`: `BOTH` | `LEGACY_ONLY` | `MODERN_ONLY`.

### File index (`INT_UDM_*`)

| File | Creates |
|------|---------|
| `INT_UDM_NYC/DIM_DATES.sql` | Table DDL |
| `INT_UDM_NYC/DIM_STATION.sql` | Table DDL |
| `INT_UDM_NYC/FACT_RIDE.sql` | Table DDL |
| `INT_UDM_NYC/SP_BUILD_DIM_DATES.sql` | Procedure (+ JC date sync) |
| `INT_UDM_NYC/SP_BUILD_DIM_STATION_NYC.sql` | Procedure |
| `INT_UDM_NYC/SP_BUILD_FACT_RIDE_NYC.sql` | Procedure |
| `INT_UDM_JC/DIM_DATES.sql` | Clone table DDL |
| `INT_UDM_JC/DIM_STATION.sql` | Table DDL |
| `INT_UDM_JC/FACT_RIDE.sql` | Table DDL |
| `INT_UDM_JC/SP_BUILD_DIM_STATION_JC.sql` | Procedure |
| `INT_UDM_JC/SP_BUILD_FACT_RIDE_JC.sql` | Procedure |

## RPT objects (reporting layer)

Analyst-facing views on the integration star schema. **No build procedures** — deploy views after `FACT_RIDE` has data.

### Schemas

| File | Object |
|------|--------|
| `RPT_NYC/RPT_NYC.sql` | `RPT_NYC` |
| `RPT_JC/RPT_JC.sql` | `RPT_JC` |

### Views (per region)

| View | Business question | Grain |
|------|-------------------|-------|
| `RPT_RIDERSHIP_OVER_TIME` | How has ridership grown? MoM / YoY by member vs casual | Month × `MEMBER_CASUAL` |
| `RPT_TOP_STATIONS` | Which stations are busiest? Departures, arrivals, net flow | One row per station |
| `RPT_TOP_ROUTES` | Most popular origin–destination pairs | One row per start/end station pair |

Sources: `INT_UDM_<region>.FACT_RIDE`, `DIM_STATION`, `DIM_DATES`.

### File index (`RPT_*`)

| File | Creates |
|------|---------|
| `RPT_NYC/RPT_RIDERSHIP_OVER_TIME.sql` | View |
| `RPT_NYC/RPT_TOP_STATIONS.sql` | View |
| `RPT_NYC/RPT_TOP_ROUTES.sql` | View |
| `RPT_JC/RPT_RIDERSHIP_OVER_TIME.sql` | View |
| `RPT_JC/RPT_TOP_STATIONS.sql` | View |
| `RPT_JC/RPT_TOP_ROUTES.sql` | View |
