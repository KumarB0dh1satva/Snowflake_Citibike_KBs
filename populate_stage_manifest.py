"""
populate_stage_manifest.py
──────────────────────────
One-time (and safe-to-rerun) script that loads gzip_manifest.jsonl
into CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST.

Deduplication is handled in Python — existing OUTPUT_FILE keys are
fetched from Snowflake first and filtered out before any INSERT,
so executemany only ever sees simple single-table inserts (the
Snowflake connector cannot handle WHERE NOT EXISTS in executemany).

Schema resolution
─────────────────
  sf_schema  ←  rec["region"]  via cfg.SCHEMA_MAP
                e.g. "nyc" → "STAGING_NYC", "jersey_city" → "STAGING_JC"

  sf_table   ←  rec["schema_key"]  via TABLE_MAP below
                The same schema_key means the same column layout
                regardless of region — region only decides which
                Snowflake schema (NYC vs JC) the data lands in.

Usage
─────
  python populate_stage_manifest.py               # load all
  python populate_stage_manifest.py --dry-run     # print rows, no insert
  python populate_stage_manifest.py --replace     # truncate then reload
"""

import argparse
import json
from pathlib import Path

import snowflake.connector

import snowflake_config as cfg

# ──────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────

MANIFEST_FILE = Path("gzip_staging/gzip_manifest.jsonl")
TARGET_TABLE  = "CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST"

# Maps schema_key → target table name ONLY.
# Region is NOT encoded here — it comes from rec["region"] in the manifest
# and is resolved to a Snowflake schema via cfg.SCHEMA_MAP.
# This means the same key works for both NYC and JC files.
TABLE_MAP = {
    "dc497b4333c4": "TRIPS_MODERN",     # 13-col ride_id layout (NYC + JC)
    "473144999085": "TRIPS_LEGACY_V1",  # 15-col lowercase legacy
    "e24ee8457e0e": "TRIPS_LEGACY_V2",  # 15-col Title Case legacy (2016-style)
    # Add any newly discovered schema_keys here — table name only:
    # "<new_key>": "TRIPS_MODERN",
}


# ──────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────

def load_manifest(region_filter: str | None = None) -> list[dict]:
    if not MANIFEST_FILE.exists():
        raise FileNotFoundError(f"Manifest not found: {MANIFEST_FILE}")
    records = []
    with open(MANIFEST_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if region_filter and rec.get("region") != region_filter:
                continue
            records.append(rec)
    return records


def build_rows(manifest: list[dict]) -> tuple[list[tuple], list[str]]:
    """
    Convert manifest dicts into INSERT tuples.

    sf_schema is resolved from rec["region"] via cfg.SCHEMA_MAP —
    e.g. "nyc" → "STAGING_NYC", "jersey_city" → "STAGING_JC".

    sf_table is resolved from rec["schema_key"] via TABLE_MAP —
    same key maps to the same table in both schemas.

    Returns (rows_to_insert, skipped_reason_strings).
    """
    rows    = []
    skipped = []

    for rec in manifest:
        output_file = rec["output_file"]
        region      = rec.get("region", "")
        schema_key  = rec.get("schema_key", "")

        # resolve Snowflake schema from region
        sf_schema = cfg.SCHEMA_MAP.get(region)
        if not sf_schema:
            skipped.append(
                f"{output_file}  "
                f"(region={region!r} not in cfg.SCHEMA_MAP — "
                f"add it to snowflake_config.py)"
            )
            continue

        # resolve target table from schema_key
        sf_table = TABLE_MAP.get(schema_key)
        if not sf_table:
            skipped.append(
                f"{output_file}  "
                f"(schema_key={schema_key!r} not in TABLE_MAP — "
                f"add it to populate_stage_manifest.py)"
            )
            continue

        rows.append((
            output_file,
            region,
            schema_key,
            sf_schema,
            sf_table,
            int(rec.get("gzip_size_bytes") or 0),
            len(rec.get("source_files") or []),
        ))

    return rows, skipped


def fetch_existing_keys(cur) -> set[str]:
    """Return the set of OUTPUT_FILE values already in STAGE_MANIFEST."""
    cur.execute(f"SELECT OUTPUT_FILE FROM {TARGET_TABLE}")
    return {row[0] for row in cur.fetchall()}


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
# MAIN
# ──────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Populate LOGGING.STAGE_MANIFEST from gzip_manifest.jsonl"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print rows that would be inserted without writing to Snowflake.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="TRUNCATE STAGE_MANIFEST before inserting (full reload).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # ── read and validate manifest ───────────────────
    print(f"Reading {MANIFEST_FILE} …")
    manifest = load_manifest()
    print(f"  {len(manifest)} entries in manifest")

    all_rows, skipped = build_rows(manifest)
    print(f"  {len(all_rows)} rows mapped to tables")

    if skipped:
        print(f"\n  WARNING — {len(skipped)} entries skipped (schema_key not in TABLE_MAP):")
        for s in skipped:
            print(f"    * {s}")
        print("  Add the missing keys to TABLE_MAP in this script before re-running.\n")

    if not all_rows:
        print("Nothing to insert.")
        return

    # ── dry-run ──────────────────────────────────────
    if args.dry_run:
        print("\n[dry-run] Rows that would be inserted:")
        print(f"  {'OUTPUT_FILE':<60}  {'SF_SCHEMA':<15}  SF_TABLE")
        print("  " + "-" * 95)
        for r in all_rows:
            print(f"  {r[0]:<60}  {r[3]:<15}  {r[4]}")
        print(f"\n  Total: {len(all_rows)} rows  (no insert performed)")
        return

    # ── connect ──────────────────────────────────────
    conn = get_connection()
    try:
        with conn.cursor() as cur:

            # optional truncate
            if args.replace:
                print(f"\nTRUNCATING {TARGET_TABLE} …")
                cur.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
                existing_keys = set()
                print("  Done.")
            else:
                print(f"\nFetching existing keys from {TARGET_TABLE} …")
                existing_keys = fetch_existing_keys(cur)
                print(f"  {len(existing_keys)} rows already present")

            # filter to new rows only
            new_rows = [r for r in all_rows if r[0] not in existing_keys]
            already  = len(all_rows) - len(new_rows)

            print(f"  {already} rows skipped (already in table)")
            print(f"  {len(new_rows)} new rows to insert")

            if not new_rows:
                print("\nStage manifest is already up to date.")
                return

            # ── INSERT ───────────────────────────────
            # Simple positional insert — no subquery, connector handles fine
            insert_sql = f"""
                INSERT INTO {TARGET_TABLE} (
                    OUTPUT_FILE, REGION, SCHEMA_KEY,
                    SF_SCHEMA, SF_TABLE,
                    GZIP_SIZE_BYTES, SOURCE_FILE_COUNT
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            print(f"\nInserting {len(new_rows)} rows …")
            cur.executemany(insert_sql, new_rows)
            print("  Done.")

            # ── verify ───────────────────────────────
            cur.execute(
                f"SELECT COUNT(*), REGION, SF_TABLE "
                f"FROM {TARGET_TABLE} "
                f"GROUP BY REGION, SF_TABLE "
                f"ORDER BY REGION, SF_TABLE"
            )
            results = cur.fetchall()
            print(f"\nStage manifest contents:")
            print(f"  {'COUNT':<8}  {'REGION':<15}  SF_TABLE")
            print("  " + "-" * 45)
            total = 0
            for count, region, table in results:
                print(f"  {count:<8}  {region:<15}  {table}")
                total += count
            print(f"  {'─' * 45}")
            print(f"  {total:<8}  total rows")

    finally:
        conn.close()

    print(f"\nStage manifest populated. Next steps:")
    print(f"  1. python stage_files.py")
    print(f"  2. CALL CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES('all');")


if __name__ == "__main__":
    main()