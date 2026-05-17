"""
Upload Citibike .csv.gz files to the Snowflake internal stage (PUT only).

Alternative to load_to_snowflake.py: no COPY INTO tables.

Usage
─────
  python stage_files.py
  python stage_files.py --region nyc
  python stage_files.py --workers 6
  python stage_files.py --dry-run
  python stage_files.py --force
  python stage_files.py --no-overwrite
  python stage_files.py --list-stage

Log: snowflake_stage_loading_log.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from tqdm import tqdm

import snowflake_config as cfg

GZIP_STAGING_DIR = Path("gzip_staging")
GZIP_MANIFEST = GZIP_STAGING_DIR / "gzip_manifest.jsonl"
STAGE_LOADING_LOG = Path("snowflake_stage_loading_log.jsonl")

_log_lock = threading.Lock()


def _now_utc() -> str:
    return str(datetime.now(timezone.utc))


def stage_basename(output_file: str) -> str:
    """Snowflake stage object name (PUT uses file basename only)."""
    return Path(output_file).name


def load_stage_loading_log() -> dict:
    index: dict = {}
    if not STAGE_LOADING_LOG.exists():
        return index
    with open(STAGE_LOADING_LOG, encoding="utf-8") as f:
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


def append_stage_loading_log(entry: dict) -> None:
    with _log_lock:
        with open(STAGE_LOADING_LOG, "a", encoding="utf-8") as f:
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
        account=cfg.ACCOUNT,
        user=cfg.USER,
        password=cfg.PASSWORD,
        warehouse=cfg.WAREHOUSE,
        database=cfg.DATABASE,
        role=cfg.ROLE,
    )


def list_stage_file(cur, sf_schema: str, basename: str) -> dict | None:
    """
    Return stage file metadata if basename is present on the stage, else None.
    LIST name column may be 'raw_ingestion/file.gz' or 'file.gz'.
    """
    stage_fqn = cfg.stage_fqn(sf_schema)
    escaped = re.escape(basename)
    cur.execute(f"LIST @{stage_fqn} PATTERN='.*{escaped}$'")
    rows = cur.fetchall()
    if not rows:
        return None
    row = rows[0]
    list_name = row[0]
    size = int(row[1]) if row[1] is not None else None
    return {"stage_list_name": list_name, "stage_size_bytes": size}


def put_file(cur, local_path: Path, sf_schema: str, overwrite: bool) -> dict:
    stage_fqn = cfg.stage_fqn(sf_schema)
    file_uri = local_path.resolve().as_posix()
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
    target = rows[0][1] if len(rows[0]) > 1 else None
    if status not in ("UPLOADED", "SKIPPED"):
        raise RuntimeError(f"PUT unexpected status: {status!r}  row={rows[0]}")
    return {
        "put_status": status,
        "sf_stage": stage_fqn,
        "put_target": target,
    }


def build_queue(
    manifest: list[dict],
    log_index: dict,
    reset_failed: bool,
    force: bool,
) -> list[dict]:
    queue = []
    for rec in manifest:
        key = rec["output_file"]
        prior = log_index.get(key)

        if force:
            queue.append(rec)
        elif prior is None:
            queue.append(rec)
        elif prior.get("status") == "SUCCESS" and prior.get("stage_verified"):
            pass
        elif prior.get("status") in ("DRY_RUN",):
            queue.append(rec)
        elif prior.get("status") == "FAILED":
            attempts = prior.get("attempt", 1)
            if reset_failed or attempts < cfg.MAX_RETRIES:
                queue.append(rec)
            else:
                print(
                    f"  SKIPPING (max retries reached): {key} "
                    f"— use --reset-failed to force re-queue"
                )
    return queue


def _base_entry(rec: dict, prior: dict) -> dict:
    return {
        "output_file": rec["output_file"],
        "stage_file_name": stage_basename(rec["output_file"]),
        "region": rec["region"],
        "schema_key": rec.get("schema_key"),
        "gzip_size_mb": round(rec.get("gzip_size_bytes", 0) / (1024 * 1024), 2),
        "attempt": (prior.get("attempt") or 0) + 1,
        "started_at_utc": _now_utc(),
        "ended_at_utc": None,
        "status": "FAILED",
        "put_status": None,
        "sf_stage": None,
        "stage_list_name": None,
        "stage_verified": False,
        "error_message": None,
    }


def stage_one_attempt(
    rec: dict,
    prior: dict,
    dry_run: bool,
    overwrite: bool,
) -> dict:
    output_file = rec["output_file"]
    region = rec["region"]
    local_path = GZIP_STAGING_DIR / output_file
    basename = stage_basename(output_file)
    local_size = rec.get("gzip_size_bytes") or local_path.stat().st_size if local_path.exists() else 0

    sf_schema = cfg.SCHEMA_MAP.get(region)
    if not sf_schema:
        entry = _base_entry(rec, prior)
        entry["error_message"] = f"No SCHEMA_MAP entry for region '{region}'"
        entry["ended_at_utc"] = _now_utc()
        return entry

    entry = _base_entry(rec, prior)
    stage_fqn = cfg.stage_fqn(sf_schema)

    if not local_path.exists():
        entry["status"] = "MISSING"
        entry["error_message"] = f"Local file not found: {local_path}"
        entry["ended_at_utc"] = _now_utc()
        return entry

    if dry_run:
        entry["status"] = "DRY_RUN"
        entry["sf_stage"] = stage_fqn
        entry["ended_at_utc"] = _now_utc()
        return entry

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            result = put_file(cur, local_path, sf_schema, overwrite=overwrite)
            entry.update(result)

            if result["put_status"] == "SKIPPED" and not overwrite:
                on_stage = list_stage_file(cur, sf_schema, basename)
                if on_stage is None:
                    result = put_file(cur, local_path, sf_schema, overwrite=True)
                    entry.update(result)
                    entry["put_status"] = result["put_status"]
                    if result["put_status"] == "SKIPPED":
                        entry["error_message"] = (
                            "PUT returned SKIPPED but file not found on stage; "
                            "stage may need to be cleared or use --force."
                        )
                        entry["ended_at_utc"] = _now_utc()
                        return entry

            on_stage = list_stage_file(cur, sf_schema, basename)
            if on_stage is None:
                entry["error_message"] = (
                    f"After PUT ({entry['put_status']}), "
                    f"{basename!r} not found on @{stage_fqn}"
                )
                entry["ended_at_utc"] = _now_utc()
                return entry

            entry.update(on_stage)
            entry["stage_verified"] = True
            stage_bytes = on_stage.get("stage_size_bytes")
            if stage_bytes is not None and local_size and abs(stage_bytes - local_size) > 1024:
                entry["error_message"] = (
                    f"Stage size {stage_bytes} differs from local {local_size}"
                )

        entry["status"] = "SUCCESS"
        entry["ended_at_utc"] = _now_utc()
    except Exception as exc:
        entry["error_message"] = str(exc)
        entry["ended_at_utc"] = _now_utc()
    finally:
        conn.close()

    return entry


def stage_with_retry(
    rec: dict,
    log_index: dict,
    dry_run: bool,
    overwrite: bool,
) -> dict:
    prior = log_index.get(rec["output_file"], {})
    entry = None

    for attempt_num in range(1, cfg.MAX_RETRIES + 1):
        prior = log_index.get(rec["output_file"], prior)
        entry = stage_one_attempt(rec, prior, dry_run=dry_run, overwrite=overwrite)
        entry["attempt"] = (prior.get("attempt") or 0) + 1

        if not dry_run:
            append_stage_loading_log(entry)
            with _log_lock:
                log_index[rec["output_file"]] = entry

        if entry["status"] in ("SUCCESS", "DRY_RUN", "MISSING"):
            return entry
        if attempt_num < cfg.MAX_RETRIES:
            time.sleep(cfg.RETRY_DELAY_SECONDS)

    return entry


def _stage_worker(args: tuple) -> dict:
    rec, log_index, dry_run, overwrite = args
    return stage_with_retry(rec, log_index, dry_run, overwrite)


def _format_result_line(entry: dict, output_file: str) -> str:
    stage = entry.get("sf_stage") or "?"
    put_status = entry.get("put_status") or "-"
    if entry["status"] == "DRY_RUN":
        return f"  [dry-run] {output_file}  →  @{stage}"
    if entry["status"] == "SUCCESS":
        if put_status == "UPLOADED":
            label = "UPLOADED"
        elif put_status == "SKIPPED":
            label = "ON_STAGE"  # already present; verified via LIST
        else:
            label = put_status
        listed = entry.get("stage_list_name") or entry.get("stage_file_name")
        return f"  {label:<10} {output_file}  →  @{stage}  ({listed})"
    if entry["status"] == "MISSING":
        return f"  MISSING    {output_file}"
    return f"  FAILED     {output_file}: {entry.get('error_message')}"


def print_summary(results: list[dict]) -> None:
    success = [r for r in results if r["status"] == "SUCCESS"]
    uploaded = [r for r in success if r.get("put_status") == "UPLOADED"]
    on_stage = [r for r in success if r.get("put_status") == "SKIPPED"]
    failed = [r for r in results if r["status"] == "FAILED"]
    dry = [r for r in results if r["status"] == "DRY_RUN"]
    missing = [r for r in results if r["status"] == "MISSING"]

    uploaded_mb = sum(r.get("gzip_size_mb") or 0 for r in uploaded)

    print("\n" + "=" * 62)
    print("STAGE LOADING SUMMARY")
    print("=" * 62)
    print(f"  Files processed : {len(results)}")
    print(f"  SUCCESS         : {len(success)}")
    print(f"    UPLOADED      : {len(uploaded)}  ({uploaded_mb:.1f} MB sent this run)")
    print(f"    ON_STAGE      : {len(on_stage)}  (already on stage, verified)")
    print(f"  FAILED          : {len(failed)}")
    print(f"  DRY_RUN         : {len(dry)}")
    print(f"  MISSING         : {len(missing)}")

    if failed:
        print("\n  Failed files:")
        for r in failed:
            print(f"    * {r['output_file']}")
            print(f"      {r.get('error_message', '(no message)')}")

    print(f"\n  Verify in Snowflake: LIST @{cfg.stage_fqn('STAGING_NYC')};")
    print(f"  Log: {STAGE_LOADING_LOG.resolve()}")
    print("=" * 62)


def list_stages(region_filter: str | None) -> None:
    cfg.validate_config()
    conn = get_connection()
    try:
        schemas = []
        if region_filter == "nyc":
            schemas = [cfg.SCHEMA_MAP["nyc"]]
        elif region_filter == "jersey_city":
            schemas = [cfg.SCHEMA_MAP["jersey_city"]]
        else:
            schemas = list(cfg.SCHEMA_MAP.values())

        with conn.cursor() as cur:
            for sf_schema in schemas:
                stage_fqn = cfg.stage_fqn(sf_schema)
                cur.execute(f"LIST @{stage_fqn}")
                rows = cur.fetchall()
                print(f"\n@{stage_fqn}  ({len(rows)} files)")
                for row in rows[:20]:
                    print(f"  {row[0]}  {row[1]} bytes")
                if len(rows) > 20:
                    print(f"  ... and {len(rows) - 20} more")
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "PUT Citibike .csv.gz files onto Snowflake RAW_INGESTION "
            "(stage-only alternative to load_to_snowflake.py)."
        )
    )
    parser.add_argument("--region", choices=["all", "nyc", "jersey_city"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset-failed", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-upload even if already logged SUCCESS (OVERWRITE=TRUE).",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help=f"Use OVERWRITE=FALSE on PUT (default is {cfg.STAGE_PUT_OVERWRITE}).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=cfg.PARALLEL_STAGE_WORKERS,
        help=f"Concurrent file uploads (default: {cfg.PARALLEL_STAGE_WORKERS}).",
    )
    parser.add_argument(
        "--list-stage",
        action="store_true",
        help="List files currently on each RAW_INGESTION stage and exit.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    region_filter = None if args.region == "all" else args.region

    if args.list_stage:
        list_stages(region_filter)
        return

    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    overwrite = cfg.STAGE_PUT_OVERWRITE and not args.no_overwrite
    if args.force:
        overwrite = True

    manifest = load_manifest(region_filter)
    log_index = load_stage_loading_log()

    print(f"Manifest entries : {len(manifest)}")
    print(f"Loading log      : {len(log_index)} entries")

    queue = build_queue(
        manifest, log_index, reset_failed=args.reset_failed, force=args.force
    )
    print(f"Files to load    : {len(queue)}")
    if args.workers > 1 and not args.dry_run:
        print(f"Parallel workers : {args.workers}")
    if not args.dry_run:
        print(f"PUT OVERWRITE    : {overwrite}\n")
    else:
        print()

    if not queue:
        print("Nothing to load — all files already on stage (see log or --list-stage).")
        print("Use --force to re-upload.")
        return

    if not args.dry_run:
        cfg.validate_config()
        print(
            f"Snowflake target : {cfg.DATABASE}  "
            f"(warehouse={cfg.WAREHOUSE}, role={cfg.ROLE})\n"
        )

    results: list[dict] = []

    if args.dry_run or args.workers == 1:
        for rec in tqdm(queue, desc="Loading to stage", unit="file"):
            entry = stage_with_retry(rec, log_index, args.dry_run, overwrite)
            results.append(entry)
            if entry["status"] != "FAILED" or entry.get("error_message"):
                tqdm.write(_format_result_line(entry, rec["output_file"]))
    else:
        work = [(rec, log_index, args.dry_run, overwrite) for rec in queue]
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_stage_worker, item): item[0] for item in work}
            with tqdm(total=len(futures), desc="Loading to stage", unit="file") as pbar:
                for future in as_completed(futures):
                    rec = futures[future]
                    try:
                        entry = future.result()
                        results.append(entry)
                        tqdm.write(_format_result_line(entry, rec["output_file"]))
                    except Exception as exc:
                        entry = {
                            "output_file": rec["output_file"],
                            "status": "FAILED",
                            "error_message": str(exc),
                        }
                        results.append(entry)
                        tqdm.write(f"  FAILED     {rec['output_file']}: {exc}")
                    pbar.update(1)

    print_summary(results)


if __name__ == "__main__":
    main()
