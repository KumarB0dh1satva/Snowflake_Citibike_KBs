"""
stage_files.py — PUT .csv.gz files onto the Snowflake internal stage.

Responsibility: local disk → Snowflake stage ONLY.
No COPY, no table writes, no schema logic.

After this script completes successfully, call the Snowflake stored procedure
SP_INGEST_STAGED_FILES to run the COPY INTO for each staged file.

Usage
─────
  python stage_files.py                       # stage everything pending
  python stage_files.py --region nyc          # NYC only
  python stage_files.py --region jersey_city  # Jersey City only
  python stage_files.py --dry-run             # print plan, no uploads
  python stage_files.py --reset-failed        # re-queue FAILED entries
  python stage_files.py --force               # re-upload even if already staged

Log
───
  snowflake_stage_log.jsonl — one entry per PUT attempt:
    output_file, region, schema_key, gzip_size_mb,
    attempt, started_at_utc, ended_at_utc,
    status [STAGED|FAILED|SKIPPED|DRY_RUN],
    put_status, sf_stage, error_message
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
STAGE_LOG        = Path("snowflake_stage_log.jsonl")


# ──────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────

def _now_utc() -> str:
    return str(datetime.now(timezone.utc))


def load_stage_log() -> dict:
    """Returns {output_file: latest_entry}. Last entry wins."""
    index = {}
    if not STAGE_LOG.exists():
        return index
    with open(STAGE_LOG, encoding="utf-8") as f:
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


def append_stage_log(entry: dict) -> None:
    with open(STAGE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


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


def get_connection():
    cfg.validate_config()
    return snowflake.connector.connect(
        account   = cfg.ACCOUNT,
        user      = cfg.USER,
        password  = cfg.PASSWORD,
        warehouse = cfg.WAREHOUSE,
        database  = cfg.DATABASE,
        role      = cfg.ROLE,
    )


# ──────────────────────────────────────────────────────
# PUT
# ──────────────────────────────────────────────────────

def put_file(cur, local_path: Path, sf_schema: str, overwrite: bool) -> dict:
    """
    PUT a single .csv.gz to the Snowflake internal stage.

    Returns {"put_status": "UPLOADED"|"SKIPPED", "sf_stage": "..."}

    PUT result columns (Snowflake):
        0  source          1  target         2  source_size
        3  target_size     4  source_compression  5  target_compression
        6  status          7  message
    """
    stage_fqn  = cfg.stage_fqn(sf_schema)
    file_uri   = local_path.resolve().as_posix()
    overwrite_clause = "TRUE" if overwrite else "FALSE"

    sql = (
        f"PUT 'file://{file_uri}' @{stage_fqn} "
        f"AUTO_COMPRESS=FALSE "
        f"PARALLEL={cfg.PARALLEL_PUT_THREADS} "
        f"OVERWRITE={overwrite_clause}"
    )
    cur.execute(sql)
    rows = cur.fetchall()

    if not rows:
        raise RuntimeError("PUT returned no result rows.")

    status = rows[0][6] if len(rows[0]) > 6 else rows[0][-1]
    if status not in ("UPLOADED", "SKIPPED"):
        raise RuntimeError(f"PUT unexpected status: {status!r}  row={rows[0]}")

    return {"put_status": status, "sf_stage": stage_fqn}


# ──────────────────────────────────────────────────────
# QUEUE
# ──────────────────────────────────────────────────────

def build_queue(
    manifest: list[dict],
    log_index: dict,
    reset_failed: bool,
) -> list[dict]:
    queue = []
    for rec in manifest:
        key   = rec["output_file"]
        prior = log_index.get(key)

        if prior is None:
            queue.append(rec)
        elif prior["status"] == "STAGED":
            pass   # already on stage — SP will handle COPY
        elif prior["status"] in ("DRY_RUN", "SKIPPED"):
            queue.append(rec)
        elif prior["status"] == "FAILED":
            attempts = prior.get("attempt", 1)
            if reset_failed or attempts < cfg.MAX_RETRIES:
                queue.append(rec)
            else:
                print(
                    f"  SKIPPING (max retries reached): {key} "
                    f"— use --reset-failed to force re-queue"
                )
    return queue


# ──────────────────────────────────────────────────────
# CORE
# ──────────────────────────────────────────────────────

def stage_one(
    rec: dict,
    log_index: dict,
    conn,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    output_file = rec["output_file"]
    region      = rec["region"]
    schema_key  = rec["schema_key"]
    local_path  = GZIP_STAGING_DIR / output_file

    sf_schema = cfg.SCHEMA_MAP.get(region)
    if not sf_schema:
        raise KeyError(
            f"No Snowflake schema mapped for region '{region}'. "
            f"Add it to SCHEMA_MAP in snowflake_config.py."
        )

    prior   = log_index.get(output_file, {})
    attempt = (prior.get("attempt") or 0) + 1

    entry = {
        "output_file":    output_file,
        "region":         region,
        "schema_key":     schema_key,
        "gzip_size_mb":   round(rec.get("gzip_size_bytes", 0) / (1024 * 1024), 2),
        "attempt":        attempt,
        "started_at_utc": _now_utc(),
        "ended_at_utc":   None,
        "status":         "FAILED",
        "put_status":     None,
        "sf_stage":       None,
        "error_message":  None,
    }

    if not local_path.exists():
        entry["error_message"] = f"Local file not found: {local_path}"
        entry["ended_at_utc"]  = _now_utc()
        append_stage_log(entry)
        log_index[output_file] = entry
        print(f"  MISSING  {output_file}")
        return entry

    if dry_run:
        stage_fqn = cfg.stage_fqn(sf_schema)
        entry["status"]       = "DRY_RUN"
        entry["sf_stage"]     = stage_fqn
        entry["ended_at_utc"] = _now_utc()
        print(
            f"  [dry-run] {output_file}  →  @{stage_fqn}"
            + ("  [FORCE/OVERWRITE]" if force else "")
        )
        return entry

    try:
        with conn.cursor() as cur:
            result = put_file(cur, local_path, sf_schema, overwrite=force)

        entry.update(result)
        entry["status"]       = "STAGED"
        entry["ended_at_utc"] = _now_utc()
        print(f"  {result['put_status']:<8} {output_file}  →  @{result['sf_stage']}")

    except Exception as exc:
        entry["error_message"] = str(exc)
        entry["ended_at_utc"]  = _now_utc()
        print(f"  FAILED   {output_file}: {exc}")

    append_stage_log(entry)
    log_index[output_file] = entry
    return entry


def stage_with_retry(rec, log_index, conn, dry_run, force) -> dict:
    for attempt in range(1, cfg.MAX_RETRIES + 1):
        entry = stage_one(rec, log_index, conn, dry_run=dry_run, force=force)
        if entry["status"] in ("STAGED", "DRY_RUN"):
            return entry
        if attempt < cfg.MAX_RETRIES:
            print(f"    → retry {attempt}/{cfg.MAX_RETRIES - 1} in {cfg.RETRY_DELAY_SECONDS}s …")
            time.sleep(cfg.RETRY_DELAY_SECONDS)
    return entry


# ──────────────────────────────────────────────────────
# SUMMARY
# ──────────────────────────────────────────────────────

def print_summary(results: list[dict]) -> None:
    staged  = [r for r in results if r["status"] == "STAGED"]
    failed  = [r for r in results if r["status"] == "FAILED"]
    dry     = [r for r in results if r["status"] == "DRY_RUN"]
    skipped = [r for r in results if r["status"] == "SKIPPED"]

    total_mb = sum(r.get("gzip_size_mb") or 0 for r in staged)

    print("\n" + "=" * 62)
    print("STAGE SUMMARY")
    print("=" * 62)
    print(f"  Files processed : {len(results)}")
    print(f"  STAGED          : {len(staged)}  ({total_mb:.1f} MB uploaded)")
    print(f"  FAILED          : {len(failed)}")
    print(f"  DRY_RUN         : {len(dry)}")
    print(f"  SKIPPED         : {len(skipped)}")

    if failed:
        print("\n  Failed files:")
        for r in failed:
            print(f"    * {r['output_file']}")
            print(f"      {r.get('error_message', '(no message)')}")

    if staged:
        print(
            f"\n  Next step — run the stored procedure in Snowflake:\n"
            f"    CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');\n"
            f"  or per region:\n"
            f"    CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('nyc');\n"
            f"    CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('jersey_city');"
        )

    print(f"\n  Stage log: {STAGE_LOG.resolve()}")
    print("=" * 62)


# ──────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="PUT Citibike .csv.gz files onto the Snowflake stage."
    )
    parser.add_argument(
        "--region",
        choices=["all", "nyc", "jersey_city"],
        default="all",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print plan only — no uploads.",
    )
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        help="Re-queue files that hit MAX_RETRIES.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-upload files already on stage (OVERWRITE=TRUE).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    region_filter = None if args.region == "all" else args.region
    manifest  = load_manifest(region_filter)
    log_index = load_stage_log()

    print(f"Manifest entries : {len(manifest)}")
    print(f"Stage log entries: {len(log_index)}")

    queue = build_queue(manifest, log_index, reset_failed=args.reset_failed)
    print(f"Files to stage   : {len(queue)}\n")

    if not queue:
        print("Nothing to stage — all files already uploaded.")
        print("Run SP_INGEST_STAGED_FILES in Snowflake to COPY pending files.")
        return

    conn = None
    if not args.dry_run:
        cfg.validate_config()
        print(
            f"Snowflake target : {cfg.DATABASE}  "
            f"(warehouse={cfg.WAREHOUSE}, role={cfg.ROLE})"
            + ("  [OVERWRITE=TRUE]" if args.force else "")
            + "\n"
        )
        conn = get_connection()

    results = []
    try:
        for rec in tqdm(queue, desc="Staging files", unit="file"):
            entry = stage_with_retry(
                rec, log_index, conn,
                dry_run=args.dry_run,
                force=args.force,
            )
            results.append(entry)
    finally:
        if conn is not None:
            conn.close()

    print_summary(results)


if __name__ == "__main__":
    main()