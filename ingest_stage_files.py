"""
Load gzip_manifest.jsonl into LOGGING.STAGE_MANIFEST for SP_INGEST_STAGED_FILES.

Run once after compress_for_snowflake.py (and ideally after stage_files.py has
uploaded files to RAW_INGESTION).

Routing uses snowflake_config.py (region + schema_key → SF_SCHEMA / SF_TABLE).
"""

import json
from pathlib import Path

import snowflake.connector

import snowflake_config as cfg

MANIFEST = Path("gzip_staging/gzip_manifest.jsonl")
INSERT_SQL = """
    INSERT INTO CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST (
        OUTPUT_FILE, REGION, SCHEMA_KEY, SF_SCHEMA, SF_TABLE,
        GZIP_SIZE_BYTES, SOURCE_FILE_COUNT
    )
    SELECT %s, %s, %s, %s, %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM CITIBIKE_SYSTEM_DATA.LOGGING.STAGE_MANIFEST
        WHERE OUTPUT_FILE = %s
    )
"""


def resolve_target(region: str, schema_key: str) -> tuple[str, str] | None:
    sf_schema = cfg.SCHEMA_MAP.get(region)
    sf_table = cfg.SCHEMA_KEY_TO_TABLE.get(schema_key)
    if not sf_schema or not sf_table:
        return None
    return sf_schema, sf_table


def main() -> None:
    if not MANIFEST.exists():
        raise FileNotFoundError(f"Manifest not found: {MANIFEST}")

    cfg.validate_config()
    rows = []
    skipped = 0

    with open(MANIFEST, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            target = resolve_target(rec["region"], rec["schema_key"])
            if not target:
                skipped += 1
                continue
            sf_schema, sf_table = target
            rows.append((
                rec["output_file"],
                rec["region"],
                rec["schema_key"],
                sf_schema,
                sf_table,
                rec.get("gzip_size_bytes", 0),
                len(rec.get("source_files", [])),
                rec["output_file"],
            ))

    conn = snowflake.connector.connect(
        account=cfg.ACCOUNT,
        user=cfg.USER,
        password=cfg.PASSWORD,
        warehouse=cfg.WAREHOUSE,
        database=cfg.DATABASE,
        role=cfg.ROLE,
    )
    try:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)
        print(f"STAGE_MANIFEST: {len(rows)} rows inserted (skipped {skipped} unmapped)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
