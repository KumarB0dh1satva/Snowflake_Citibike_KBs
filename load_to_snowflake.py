"""
Load compressed Citibike .csv.gz files into Snowflake.

Flow per file
─────────────
  1. Read gzip_manifest.jsonl → build list of parts to load.
  2. Consult snowflake_ingest_log.jsonl → skip SUCCESS, retry FAILED (≤ MAX_RETRIES).
  3. PUT  the .csv.gz to the internal Snowflake stage.
  4. COPY INTO the target table (PURGE = FALSE so stage files can be re-used).
  5. Append a result record to snowflake_ingest_log.jsonl.

Usage
─────
  python load_to_snowflake.py                          # load everything pending
  python load_to_snowflake.py --region nyc             # NYC only
  python load_to_snowflake.py --region jersey_city     # Jersey City only
  python load_to_snowflake.py --dry-run                # print plan, no Snowflake calls
  python load_to_snowflake.py --reset-failed           # re-queue all FAILED entries
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from tqdm import tqdm

import snowflake_config as cfg

# ──────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────

GZIP_STAGING_DIR = Path("gzip_staging")
GZIP_MANIFEST    = GZIP_STAGING_DIR / "gzip_manifest.jsonl"
INGEST_LOG       = Path("snowflake_ingest_log.jsonl")

# ──────────────────────────────────────────────────────
# LOGGING HELPERS
# ──────────────────────────────────────────────────────

def _now_utc() -> str:
    return str(datetime.now(timezone.utc))


def load_ingest_log() -> dict:
    """
    Returns {output_file: latest_log_entry} — last entry wins so retries
    overwrite earlier FAILED records in memory (file keeps full history).
    """
    index: dict = {}
    if not INGEST_LOG.exists():
        return index
    with open(INGEST_LOG, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                index[entry["output_file"]] = entry
            except (json.JSONDecodeError, KeyError):
                continue
    return index


def append_log(entry: dict) -> None:
    with open(INGEST_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# ──────────────────────────────────────────────────────
# MANIFEST READER
# ──────────────────────────────────────────────────────

def load_manifest(region_filter: str | None = None) -> list[dict]:
    if not GZIP_MANIFEST.exists():
        raise FileNotFoundError(f"Gzip manifest not found: {GZIP_MANIFEST}")
    records = []
    with open(GZIP_MANIFEST, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if region_filter and rec.get("region") != region_filter:
                continue
            records.append(rec)
    return records


# ──────────────────────────────────────────────────────
# SNOWFLAKE CONNECTION
# ──────────────────────────────────────────────────────

def get_connection():
    cfg.validate_config()
    return snowflake.connector.connect(
        account=cfg.ACCOUNT,
        user=cfg.USER,
        password=cfg.PASSWORD,
        warehouse=cfg.WAREHOUSE,
        database=cfg.DATABASE,
        role=cfg.ROLE,
    )


# ──────────────────────────────────────────────────────
# SNOWFLAKE HELPERS
# ──────────────────────────────────────────────────────

def resolve_table(schema_key: str, region: str) -> tuple[str, str]:
    """
    Returns (snowflake_schema, table_name) for a given schema_key + region.
    Raises KeyError with a helpful message when the mapping is missing.
    """
    sf_schema = cfg.SCHEMA_MAP.get(region)
    if not sf_schema:
        raise KeyError(
            f"No Snowflake schema mapped for region '{region}'. "
            f"Add it to SCHEMA_MAP in snowflake_config.py."
        )
    table = cfg.SCHEMA_KEY_TO_TABLE.get(schema_key)
    if not table:
        raise KeyError(
            f"No table mapped for schema_key '{schema_key}'. "
            f"Add it to SCHEMA_KEY_TO_TABLE in snowflake_config.py."
        )
    return sf_schema, table


def put_file(cur, local_path: Path, sf_schema: str) -> dict:
    """
    Upload a local .csv.gz to the Snowflake internal stage.
    Returns the PUT result row.
    """
    stage_path = f"@{cfg.stage_fqn(sf_schema)}"
    file_uri = local_path.resolve().as_posix()
    sql = (
        f"PUT 'file://{file_uri}' {stage_path} "
        f"AUTO_COMPRESS=FALSE "
        f"PARALLEL={cfg.PARALLEL_PUT_THREADS} "
        f"OVERWRITE=TRUE"
    )
    cur.execute(sql)
    rows = cur.fetchall()
    # PUT returns one row per file; status is in column "status" (index 6)
    if not rows:
        raise RuntimeError("PUT returned no result rows.")
    status = rows[0][6] if len(rows[0]) > 6 else rows[0][-1]
    if status not in ("UPLOADED", "SKIPPED"):
        raise RuntimeError(f"PUT status: {status!r}  row={rows[0]}")
    return {"put_status": status, "put_rows": len(rows)}


def copy_into(cur, local_filename: str, sf_schema: str, table: str, force: bool = False) -> dict:
    """
    COPY the staged file into the target table.

    Uses a SELECT subquery so that METADATA$FILENAME and
    METADATA$FILE_ROW_NUMBER can be injected as _SOURCE_FILE and
    _SOURCE_ROW_NUMBER.  The data columns are addressed positionally
    ($1, $2, ...) which is robust to CSV column-name casing differences.

    Column counts per table (data columns only, excluding the 3 metadata
    columns _SOURCE_FILE, _SOURCE_ROW_NUMBER, _LOADED_AT):
        TRIPS_LEGACY_V1  → 15 data columns  ($1 .. $15)
        TRIPS_LEGACY_V2  → 15 data columns  ($1 .. $15)
        TRIPS_MODERN     → 13 data columns  ($1 .. $13)
    """
    stage_path      = f"@{cfg.stage_fqn(sf_schema)}/{local_filename}"
    qualified_table = f"{cfg.DATABASE}.{sf_schema}.{table}"

    # Build the positional column projection for each table type.
    # _LOADED_AT is omitted from the column list so its DEFAULT fires.
    TABLE_DATA_COLS = {
        "TRIPS_LEGACY_V1": (
            # 15 data columns
            "tripduration, starttime, stoptime, "
            "start_station_id, start_station_name, "
            "start_station_latitude, start_station_longitude, "
            "end_station_id, end_station_name, "
            "end_station_latitude, end_station_longitude, "
            "bikeid, usertype, birth_year, gender"
        ),
        "TRIPS_LEGACY_V2": (
            # 15 data columns (Title Case source, snake_case target)
            "trip_duration, start_time, stop_time, "
            "start_station_id, start_station_name, "
            "start_station_latitude, start_station_longitude, "
            "end_station_id, end_station_name, "
            "end_station_latitude, end_station_longitude, "
            "bike_id, user_type, birth_year, gender"
        ),
        "TRIPS_MODERN": (
            # 13 data columns
            "ride_id, rideable_type, started_at, ended_at, "
            "start_station_name, start_station_id, "
            "end_station_name, end_station_id, "
            "start_lat, start_lng, end_lat, end_lng, "
            "member_casual"
        ),
    }

    data_col_names = TABLE_DATA_COLS.get(table)
    if data_col_names is None:
        raise ValueError(
            f"No column mapping defined for table '{table}'. "
            f"Add it to TABLE_DATA_COLS in copy_into()."
        )

    # Count data columns to build $1..$N projection
    n_data_cols = len(data_col_names.split(","))
    positional   = ", ".join(f"${i}" for i in range(1, n_data_cols + 1))

    # Full target column list: data cols + two metadata cols
    # (_LOADED_AT is excluded → DEFAULT CURRENT_TIMESTAMP() fires automatically)
    target_cols = f"{data_col_names}, _source_file, _source_row_number"

    sql = f"""
        COPY INTO {qualified_table} (
            {target_cols}
        )
        FROM (
            SELECT
                {positional},
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER
            FROM {stage_path}
        )
        FILE_FORMAT = (
            TYPE                         = CSV
            COMPRESSION                  = GZIP
            SKIP_HEADER                  = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF                      = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL          = TRUE
            DATE_FORMAT                  = AUTO
            TIMESTAMP_FORMAT             = AUTO
        )
        ON_ERROR = 'CONTINUE'
        PURGE    = FALSE
        FORCE    = {str(force).upper()}
    """
    cur.execute(sql)
    rows = cur.fetchall()

    # COPY result columns: file, status, rows_loaded, errors_seen, …
    rows_loaded   = sum(int(r[3]) for r in rows if r[3] is not None)
    errors_seen   = sum(int(r[4]) for r in rows if r[4] is not None)
    copy_statuses = list({r[1] for r in rows})

    # Snowflake returns LOAD_SKIPPED when it has already seen this file
    # in its load history and FORCE = FALSE.  Treat this as an error so
    # the caller knows rows were NOT actually loaded.
    if any(s == "LOAD_SKIPPED" for s in copy_statuses):
        raise RuntimeError(
            "COPY returned LOAD_SKIPPED — Snowflake has already loaded this "
            "file. The table likely has 0 rows from the previous failed run. "
            "Re-run with --force to bypass Snowflake load history, then "
            "TRUNCATE the target table first to avoid duplicate rows."
        )

    return {
        "copy_rows_loaded": rows_loaded,
        "copy_errors_seen": errors_seen,
        "copy_statuses":    copy_statuses,
    }


# ──────────────────────────────────────────────────────
# CORE LOAD LOGIC
# ──────────────────────────────────────────────────────

def load_one(
    rec: dict,
    log_index: dict,
    dry_run: bool,
    conn: snowflake.connector.SnowflakeConnection | None = None,
    force: bool = False,
) -> dict:
    """
    Attempt to PUT + COPY a single manifest record.
    Updates log_index in-place and appends to INGEST_LOG.
    Returns the log entry dict.
    """
    output_file = rec["output_file"]
    region      = rec["region"]
    schema_key  = rec["schema_key"]
    local_path  = GZIP_STAGING_DIR / output_file

    # ── base log entry ──────────────────────────────
    entry = {
        "output_file":   output_file,
        "region":        region,
        "schema_key":    schema_key,
        "gzip_size_mb":  round(rec.get("gzip_size_bytes", 0) / (1024 * 1024), 2),
        "source_files":  rec.get("source_files", []),
        "attempt":       (log_index.get(output_file, {}).get("attempt", 0) or 0) + 1,
        "started_at_utc": _now_utc(),
        "ended_at_utc":  None,
        "status":        "FAILED",
        "sf_schema":     None,
        "sf_stage":      None,
        "sf_table":      None,
        "put_status":    None,
        "copy_rows_loaded": None,
        "copy_errors_seen": None,
        "copy_statuses": None,
        "error_message": None,
    }

    # ── pre-flight checks ───────────────────────────
    if not local_path.exists():
        entry["error_message"] = f"Local file not found: {local_path}"
        entry["ended_at_utc"] = _now_utc()
        append_log(entry)
        log_index[output_file] = entry
        print(f"  MISSING  {output_file}")
        return entry

    try:
        sf_schema, table = resolve_table(schema_key, region)
    except KeyError as exc:
        entry["error_message"] = str(exc)
        entry["ended_at_utc"] = _now_utc()
        append_log(entry)
        log_index[output_file] = entry
        print(f"  CONFIG?  {output_file}: {exc}")
        return entry

    entry["sf_schema"] = sf_schema
    entry["sf_stage"]  = cfg.stage_fqn(sf_schema)
    entry["sf_table"]  = table

    if dry_run:
        entry["status"] = "DRY_RUN"
        entry["ended_at_utc"] = _now_utc()
        print(
            f"  [dry-run] {output_file}  →  "
            f"@{entry['sf_stage']}  →  {cfg.DATABASE}.{sf_schema}.{table}"
            + ("  [FORCE]" if force else "")
        )
        return entry

    if conn is None:
        raise RuntimeError("Snowflake connection required when not in dry-run mode.")

    # ── PUT + COPY ──────────────────────────────────
    try:
        with conn.cursor() as cur:
            put_result = put_file(cur, local_path, sf_schema)
            copy_result = copy_into(cur, local_path.name, sf_schema, table, force=force)

        entry.update(put_result)
        entry.update(copy_result)
        entry["status"]        = "SUCCESS"
        entry["ended_at_utc"]  = _now_utc()

        print(
            f"  SUCCESS  {output_file}  "
            f"({entry['copy_rows_loaded']:,} rows loaded, "
            f"{entry['copy_errors_seen']} errors)"
        )

    except Exception as exc:
        entry["error_message"] = str(exc)
        entry["ended_at_utc"]  = _now_utc()
        print(f"  FAILED   {output_file}: {exc}")

    append_log(entry)
    log_index[output_file] = entry
    return entry


# ──────────────────────────────────────────────────────
# RETRY WRAPPER
# ──────────────────────────────────────────────────────

def load_with_retry(
    rec: dict,
    log_index: dict,
    dry_run: bool,
    conn: snowflake.connector.SnowflakeConnection | None = None,
    force: bool = False,
) -> dict:
    """
    Run load_one up to MAX_RETRIES times for a single record.
    Sleeps RETRY_DELAY_SECONDS between attempts on failure.
    """
    for attempt in range(1, cfg.MAX_RETRIES + 1):
        entry = load_one(rec, log_index, dry_run, conn=conn, force=force)
        if entry["status"] in ("SUCCESS", "DRY_RUN"):
            return entry
        if attempt < cfg.MAX_RETRIES:
            print(
                f"    → retry {attempt}/{cfg.MAX_RETRIES - 1} "
                f"in {cfg.RETRY_DELAY_SECONDS}s …"
            )
            time.sleep(cfg.RETRY_DELAY_SECONDS)
    return entry   # last failed entry


# ──────────────────────────────────────────────────────
# QUEUE BUILDER
# ──────────────────────────────────────────────────────

def build_queue(manifest: list[dict], log_index: dict, reset_failed: bool) -> list[dict]:
    """
    Decide which manifest records need processing:
      - No log entry         → always include (first run).
      - status == SUCCESS    → skip.
      - status == FAILED     → include if attempt count < MAX_RETRIES
                               OR if --reset-failed is set.
      - status == DRY_RUN    → include (re-run for real).
    """
    queue = []
    for rec in manifest:
        key   = rec["output_file"]
        prior = log_index.get(key)

        if prior is None:
            queue.append(rec)
        elif prior["status"] == "SUCCESS":
            pass  # already done
        elif prior["status"] == "DRY_RUN":
            queue.append(rec)
        elif prior["status"] == "FAILED":
            attempts_so_far = prior.get("attempt", 1)
            if reset_failed or attempts_so_far < cfg.MAX_RETRIES:
                queue.append(rec)
            else:
                print(
                    f"  SKIPPING (max retries reached): {key}  "
                    f"— use --reset-failed to force re-queue"
                )
    return queue


# ──────────────────────────────────────────────────────
# SUMMARY PRINTER
# ──────────────────────────────────────────────────────

def print_summary(results: list[dict]) -> None:
    success = [r for r in results if r["status"] == "SUCCESS"]
    failed  = [r for r in results if r["status"] == "FAILED"]
    dry     = [r for r in results if r["status"] == "DRY_RUN"]
    skipped = [r for r in results if r["status"] not in ("SUCCESS", "FAILED", "DRY_RUN")]

    total_rows = sum(r.get("copy_rows_loaded") or 0 for r in success)

    print("\n" + "═" * 60)
    print("INGEST SUMMARY")
    print("═" * 60)
    print(f"  Files processed : {len(results)}")
    print(f"  SUCCESS         : {len(success)}  ({total_rows:,} rows loaded)")
    print(f"  FAILED          : {len(failed)}")
    print(f"  DRY_RUN         : {len(dry)}")

    if failed:
        print("\n  Failed files:")
        for r in failed:
            print(f"    • {r['output_file']}")
            print(f"      {r.get('error_message', '(no message)')}")

    print(f"\n  Log: {INGEST_LOG.resolve()}")
    print("═" * 60)


# ──────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Citibike .csv.gz files from gzip_staging/ into Snowflake."
    )
    parser.add_argument(
        "--region",
        choices=["all", "nyc", "jersey_city"],
        default="all",
        help="Restrict loading to one region (default: all).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the load plan without connecting to Snowflake.",
    )
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        help="Re-queue files that previously exceeded MAX_RETRIES.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Pass FORCE=TRUE to Snowflake COPY, bypassing load history. "
            "Use when a previous run loaded 0 rows due to a bad COPY. "
            "Always TRUNCATE the target tables in Snowflake first to avoid duplicates."
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # ── load inputs ─────────────────────────────────
    region_filter = None if args.region == "all" else args.region
    manifest  = load_manifest(region_filter)
    log_index = load_ingest_log()

    print(f"Manifest entries : {len(manifest)}")
    print(f"Log entries      : {len(log_index)}")

    queue = build_queue(manifest, log_index, reset_failed=args.reset_failed)
    print(f"Files to process : {len(queue)}\n")

    if not queue:
        print("Nothing to do — all files already loaded successfully.")
        return

    if not args.dry_run:
        cfg.validate_config()
        force_note = "  *** FORCE=TRUE — load history bypassed ***" if args.force else ""
        print(
            f"Snowflake target: {cfg.DATABASE}  "
            f"(warehouse={cfg.WAREHOUSE}, role={cfg.ROLE})\n"
            f"{force_note}"
        )

    # ── process ─────────────────────────────────────
    results = []
    conn = None
    if not args.dry_run:
        conn = get_connection()
    try:
        for rec in tqdm(queue, desc="Loading to Snowflake", unit="file"):
            entry = load_with_retry(
                rec, log_index,
                dry_run=args.dry_run,
                conn=conn,
                force=args.force,
            )
            results.append(entry)
    finally:
        if conn is not None:
            conn.close()

    print_summary(results)


if __name__ == "__main__":
    main()