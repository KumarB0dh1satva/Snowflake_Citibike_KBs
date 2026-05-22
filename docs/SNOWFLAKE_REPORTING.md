# Snowflake reporting layer (RPT)

The **reporting layer** exposes analyst-ready views on top of the integration star schema (`INT_UDM_NYC`, `INT_UDM_JC`). There are no build procedures — views read from `FACT_RIDE`, `DIM_STATION`, and `DIM_DATES` after integration builds complete.

SQL lives under `Snowflake_Scripts/RPT_NYC/` and `Snowflake_Scripts/RPT_JC/`.

## Architecture

```text
INT_UDM_NYC.FACT_RIDE + DIM_STATION + DIM_DATES
        │
        └──► RPT_NYC.RPT_RIDERSHIP_OVER_TIME
        └──► RPT_NYC.RPT_TOP_STATIONS
        └──► RPT_NYC.RPT_TOP_ROUTES

INT_UDM_JC.FACT_RIDE + DIM_STATION + DIM_DATES
        │
        └──► RPT_JC.RPT_RIDERSHIP_OVER_TIME
        └──► RPT_JC.RPT_TOP_STATIONS
        └──► RPT_JC.RPT_TOP_ROUTES
```

| Schema | Source | Consumer |
|--------|--------|----------|
| `RPT_NYC` | `INT_UDM_NYC.*` | Dashboards, ad hoc SQL, BI tools |
| `RPT_JC` | `INT_UDM_JC.*` | Same, Jersey City only |

## Prerequisites

1. Staging ingest complete (`SP_INGEST_STAGED_FILES`, `SP_LOAD_TRIPS_ALL`).
2. Integration layer built (`SP_BUILD_DIM_DATES`, `SP_BUILD_DIM_STATION_*`, `SP_BUILD_FACT_RIDE_*` with data in `FACT_RIDE`).

See [SNOWFLAKE_INTEGRATION.md](SNOWFLAKE_INTEGRATION.md) for the integration runbook.

## Deploy order

Run in Snowflake after integration procedures have populated facts:

| Step | File | Object |
|------|------|--------|
| 17 | `RPT_NYC/RPT_NYC.sql` | Schema `RPT_NYC` |
| 18 | `RPT_JC/RPT_JC.sql` | Schema `RPT_JC` |
| 19 | `RPT_NYC/RPT_RIDERSHIP_OVER_TIME.sql` | View |
| 20 | `RPT_NYC/RPT_TOP_STATIONS.sql` | View |
| 21 | `RPT_NYC/RPT_TOP_ROUTES.sql` | View |
| 22 | `RPT_JC/RPT_RIDERSHIP_OVER_TIME.sql` | View |
| 23 | `RPT_JC/RPT_TOP_STATIONS.sql` | View |
| 24 | `RPT_JC/RPT_TOP_ROUTES.sql` | View |

Views are `CREATE OR REPLACE` — safe to re-run after logic changes. No data movement; refresh is immediate when underlying `FACT_RIDE` changes.

## Reporting views

### `RPT_RIDERSHIP_OVER_TIME`

**Question:** How has ridership changed over time?

| Grain | Columns (high level) |
|-------|----------------------|
| One row per month per `MEMBER_CASUAL` | `YEAR_MONTH`, `TOTAL_RIDES`, avg/median duration, MoM and YoY growth % |

Sources: `FACT_RIDE` joined to `DIM_DATES` on `START_DATE_SK`.

### `RPT_TOP_STATIONS`

**Question:** Which stations are the busiest hubs?

| Grain | Columns (high level) |
|-------|----------------------|
| One row per station | Departures, arrivals, total activity, net flow, flow type (`NET EXPORTER` / `NET IMPORTER` / `BALANCED`), ranks |

Sources: `DIM_STATION` with departure/arrival aggregates from `FACT_RIDE`.

### `RPT_TOP_ROUTES`

**Question:** What are the most popular station-to-station pairs?

| Grain | Columns (high level) |
|-------|----------------------|
| One row per start/end station pair | Station names and lat/lng, trip count, avg/median duration, round-trip flag, `ROUTE_RANK` |

Sources: `FACT_RIDE` joined to `DIM_STATION` for start and end.

NYC views include fuller comments and explicit column lists; JC views mirror the same logic with `INT_UDM_JC` sources.

## Example queries

```sql
-- NYC monthly trends (members vs casual)
SELECT * FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_RIDERSHIP_OVER_TIME
WHERE YEAR_NUM >= 2020
ORDER BY YEAR_MONTH, MEMBER_CASUAL;

-- Top 20 NYC stations by activity
SELECT STATION_NAME, TOTAL_DEPARTURES, TOTAL_ARRIVALS, NET_FLOW, FLOW_TYPE
FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_TOP_STATIONS
ORDER BY ACTIVITY_RANK
LIMIT 20;

-- Top 10 NYC routes
SELECT START_STATION, END_STATION, TOTAL_TRIPS, IS_ROUND_TRIP
FROM CITIBIKE_SYSTEM_DATA.RPT_NYC.RPT_TOP_ROUTES
WHERE ROUTE_RANK <= 10;

-- Jersey City station leaderboard
SELECT * FROM CITIBIKE_SYSTEM_DATA.RPT_JC.RPT_TOP_STATIONS
ORDER BY TOTAL_ACTIVITY DESC
LIMIT 15;
```

## Re-run after new data

When new trips are loaded into staging and rebuilt into `FACT_RIDE`:

1. Re-run integration procedures (incremental `SP_BUILD_FACT_RIDE_*` with `FALSE`, or full reload with `TRUE`).
2. **No reporting redeploy required** — views reflect updated facts automatically.
3. Re-execute `CREATE OR REPLACE VIEW` only if view definitions change in git.

## Related docs

| Doc | Topic |
|-----|--------|
| [SNOWFLAKE_INGEST.md](SNOWFLAKE_INGEST.md) | Stage → staging tables |
| [SNOWFLAKE_INTEGRATION.md](SNOWFLAKE_INTEGRATION.md) | `TRIPS_ALL` → `INT_UDM_*` |
| [../Snowflake_Scripts/README.md](../Snowflake_Scripts/README.md) | Full SQL deploy index |
