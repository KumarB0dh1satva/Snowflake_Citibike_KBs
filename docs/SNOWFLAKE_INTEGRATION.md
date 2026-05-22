# Snowflake integration layer (INT_UDM)

After staging ingest and `TRIPS_ALL` merge, build the **intermediate unified data model** (`INT_UDM_NYC`, `INT_UDM_JC`): date and station dimensions plus a ride fact table suitable for analytics.

Trip attributes follow the [Citi Bike system data](https://citibikenyc.com/system-data) publication (modern 13-column layout and legacy 15-column layouts). SQL definitions: `Snowflake_Scripts/INT_UDM_NYC/`, `Snowflake_Scripts/INT_UDM_JC/`. Deploy order: [Snowflake_Scripts/README.md](../Snowflake_Scripts/README.md) steps 9–16.

## Architecture

```text
STAGING_NYC.TRIPS_ALL ──► INT_UDM_NYC.DIM_STATION ──┐
        │                      INT_UDM_NYC.DIM_DATES ├──► INT_UDM_NYC.FACT_RIDE
        └────────────────────────────────────────────┘

STAGING_JC.TRIPS_ALL  ──► INT_UDM_JC.DIM_STATION  ──┐
        │                      INT_UDM_JC.DIM_DATES  ├──► INT_UDM_JC.FACT_RIDE
        │                      (synced from NYC)     │
        └────────────────────────────────────────────┘
```

| Layer | Tool | Responsibility |
|-------|------|----------------|
| Staging | `SP_INGEST_STAGED_FILES`, `SP_LOAD_TRIPS_ALL` | Raw CSV layouts → unified `TRIPS_ALL` |
| Integration | `SP_BUILD_DIM_*`, `SP_BUILD_FACT_RIDE_*` | Star schema with surrogates and derived metrics |

There is **no Python wrapper** for this layer; run procedures in a Snowflake worksheet (or schedule via Task).

## One-time DDL deploy

Run scripts in [Snowflake_Scripts/README.md](../Snowflake_Scripts/README.md) steps 9–13, then deploy procedures (steps 14–16). `INT_UDM_JC.DIM_DATES` is a **clone** of the NYC table — create NYC `DIM_DATES` first.

## Runbook

Prerequisite: `TRIPS_ALL` has data for the region (`SP_LOAD_TRIPS_ALL` completed).

### 1. Date dimension (both regions)

```sql
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_DATES();
-- Optional range:
-- CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_DATES('2013-01-01', '2030-12-31');

SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_DATES;
SELECT COUNT(*) FROM CITIBIKE_SYSTEM_DATA.INT_UDM_JC.DIM_DATES;
```

Idempotent: truncates and reloads each run. JC rows are copied from NYC (shared calendar).

### 2. Station dimensions

```sql
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_STATION_NYC();
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_DIM_STATION_JC();

SELECT AVAILABILITY, COUNT(*) FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_STATION GROUP BY 1;
```

Stations keyed by normalized name; coordinates are **median** across trip observations. NYC uses broader lat/lng filters than JC.

### 3. Fact rides

**Incremental** (default — skips rows already loaded by `_SOURCE_FILE` + `_SOURCE_ROW_NUMBER`):

```sql
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_FACT_RIDE_NYC(FALSE);
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_FACT_RIDE_JC(FALSE);
```

**Full reload** (truncate fact, then reload all qualifying staging rows):

```sql
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_FACT_RIDE_NYC(TRUE);
CALL CITIBIKE_SYSTEM_DATA.INT_UDM_JC.SP_BUILD_FACT_RIDE_JC(TRUE);
```

Quality filters (aligned with published trip history exclusions):

- `TRIP_DURATION` between 60 and 86400 seconds
- `STARTED_AT` date within `DIM_DATES` spine (2013–2030)

### 4. Sanity checks

```sql
SELECT COUNT(*) AS fact_rows FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE;
SELECT COUNT(*) AS missing_start_station
FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE WHERE START_STATION_SK IS NULL;

SELECT d.YEAR_NUM, COUNT(*) AS rides
FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.FACT_RIDE f
JOIN CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_DATES d ON d.DATE_SK = f.START_DATE_SK
GROUP BY 1 ORDER BY 1;
```

## Re-run after new staging data

1. Ingest + `SP_LOAD_TRIPS_ALL` for new files (staging).
2. `SP_BUILD_DIM_STATION_*` if new stations appeared (truncate + full rebuild).
3. `SP_BUILD_FACT_RIDE_*(FALSE)` for incremental fact load.
4. `SP_BUILD_DIM_DATES` only if extending the calendar beyond 2030 or fixing holidays.

## Object reference

See [Snowflake_Scripts/README.md](../Snowflake_Scripts/README.md#int_udm-objects-integration-layer) for the full file and procedure index.
