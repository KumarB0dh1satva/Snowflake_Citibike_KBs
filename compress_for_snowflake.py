"""
Compress extracted Citibike CSVs to .csv.gz files sized for Snowflake staging.

- NYC and Jersey City (JC-*) are written to separate folder trees (for different
  Snowflake schemas).
- Files with the same column layout are batched together (one header per .gz).
- Each output .gz is targeted at 100–250 MB compressed; large sources are split
  by line when needed.
"""

import argparse
import csv
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

# ==========================================
# CONFIG
# ==========================================

EXTRACT_DIR = Path("extracted")
INVENTORY_FILE = Path("analysis_output/extracted_file_inventory.csv")
OUTPUT_DIR = Path("gzip_staging")
MANIFEST_FILE = OUTPUT_DIR / "gzip_manifest.jsonl"

MIN_GZIP_BYTES = 100 * 1024 * 1024   # 100 MB target (last part may be smaller)
MAX_GZIP_BYTES = 250 * 1024 * 1024   # 250 MB hard cap per .csv.gz
SIZE_CHECK_INTERVAL = 5000           # rows between compressed size checks

REGION_NYC = "nyc"
REGION_JERSEY_CITY = "jersey_city"

# Snowflake schema folder names (align with your Snowflake DATABASE.SCHEMA)
REGION_OUTPUT_DIRS = {
    REGION_NYC: "nyc",
    REGION_JERSEY_CITY: "jersey_city",
}


def detect_region(relative_path: str, file_name: str) -> str:
    """Jersey City files are prefixed with JC- in path or filename."""

    parts = Path(relative_path).parts
    if file_name.upper().startswith("JC-"):
        return REGION_JERSEY_CITY
    if parts and parts[0].upper().startswith("JC-"):
        return REGION_JERSEY_CITY
    if "/JC-" in relative_path.replace("\\", "/"):
        return REGION_JERSEY_CITY
    return REGION_NYC


def schema_key_from_columns(columns):
    normalized = "|".join(c.strip().lower() for c in columns)
    digest = hashlib.md5(normalized.encode("utf-8")).hexdigest()[:12]
    return digest, normalized


def read_header(path: Path):
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        return next(reader, None)


def discover_csv_files():
    """Build file list from inventory or by scanning extracted/."""

    records = []

    if INVENTORY_FILE.exists():
        with open(INVENTORY_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("file_type") != "csv":
                    continue
                rel = row["relative_path"]
                path = EXTRACT_DIR / rel
                if not path.is_file():
                    continue
                columns_raw = row.get("columns") or ""
                columns = columns_raw.split("|") if columns_raw else None
                records.append({
                    "path": path,
                    "relative_path": rel,
                    "size_bytes": int(row.get("size_bytes") or path.stat().st_size),
                    "columns": columns,
                })
    else:
        for path in sorted(EXTRACT_DIR.rglob("*.csv")):
            if "__MACOSX" in path.parts:
                continue
            rel = str(path.relative_to(EXTRACT_DIR))
            records.append({
                "path": path,
                "relative_path": rel,
                "size_bytes": path.stat().st_size,
                "columns": None,
            })

    # Deduplicate by resolved path
    seen = set()
    unique = []
    for rec in records:
        key = rec["path"].resolve()
        if key in seen:
            continue
        seen.add(key)
        unique.append(rec)

    for rec in unique:
        if not rec["columns"]:
            header = read_header(rec["path"])
            if not header:
                continue
            rec["columns"] = header
        key, _ = schema_key_from_columns(rec["columns"])
        rec["schema_key"] = key
        rec["region"] = detect_region(
            rec["relative_path"],
            rec["path"].name,
        )

    return unique


def load_manifest_index():
    index = {}
    if not MANIFEST_FILE.exists():
        return index
    with open(MANIFEST_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            index[entry["output_file"]] = entry
    return index


def append_manifest(entry):
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


class GzipPartWriter:
    """Write one or more .csv.gz parts capped at MAX_GZIP_BYTES compressed size."""

    def __init__(self, output_dir: Path, region: str, schema_key: str, on_part_closed=None):
        self.output_dir = output_dir
        self.region = region
        self.schema_key = schema_key
        self.on_part_closed = on_part_closed
        self.part_index = 0
        self.gz_stream = None
        self.csv_writer = None
        self.gz_path = None
        self.header = None
        self.header_written = False
        self.source_files = []
        self.completed_parts = []
        self._rows_since_check = 0

    def _part_filename(self):
        self.part_index += 1
        return f"{self.region}_{self.schema_key}_part{self.part_index:04d}.csv.gz"

    def _current_size(self):
        if self.gz_stream:
            self.gz_stream.flush()
        if self.gz_path and self.gz_path.exists():
            return self.gz_path.stat().st_size
        return 0

    def _close_part(self):
        if not self.gz_stream:
            return

        self.gz_stream.close()
        self.gz_stream = None
        self.csv_writer = None

        if self.gz_path and self.gz_path.exists():
            if self._current_size() == 0:
                self.gz_path.unlink(missing_ok=True)
            else:
                part_info = {
                    "path": self.gz_path,
                    "source_files": list(self.source_files),
                }
                self.completed_parts.append(part_info)
                if self.on_part_closed:
                    self.on_part_closed(part_info)

        self.source_files = []
        self.header_written = False
        self._rows_since_check = 0

    def _open_part(self):
        self._close_part()
        self.gz_path = self.output_dir / self._part_filename()
        self.gz_stream = gzip.open(
            self.gz_path,
            "wt",
            encoding="utf-8",
            newline="",
            compresslevel=6,
        )
        self.csv_writer = csv.writer(self.gz_stream)

    def _rotate_if_needed(self):
        if self._current_size() >= MAX_GZIP_BYTES:
            self._open_part()

    def _write_row(self, row, is_header_row=False):
        if not self.gz_stream:
            self._open_part()

        if is_header_row:
            if self.header is None:
                self.header = row
            if not self.header_written:
                self.csv_writer.writerow(self.header)
                self.header_written = True
            return

        if not self.header_written:
            if self.header is None:
                self.header = row
            self.csv_writer.writerow(self.header)
            self.header_written = True

        self.csv_writer.writerow(row)
        self._rows_since_check += 1
        if self._rows_since_check >= SIZE_CHECK_INTERVAL:
            self._rotate_if_needed()
            self._rows_since_check = 0

    def add_file(self, path: Path):
        header = read_header(path)
        if not header:
            return

        if self.header is None:
            self.header = header
        elif header != self.header:
            raise ValueError(
                f"Column mismatch in {path}; expected same schema as batch"
            )

        rel = str(path.relative_to(EXTRACT_DIR))
        if rel not in self.source_files:
            self.source_files.append(rel)

        if not self.gz_stream:
            self._open_part()

        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    self._write_row(row, is_header_row=True)
                else:
                    self._write_row(row)

        self._rotate_if_needed()

    def finalize(self):
        self._close_part()
        return self.completed_parts


def group_files(csv_records):
    groups = {}
    for rec in csv_records:
        key = (rec["region"], rec["schema_key"])
        groups.setdefault(key, []).append(rec)
    for key in groups:
        groups[key].sort(key=lambda r: r["size_bytes"])
    return groups


def build_output_parts(group_records, region, schema_key, dry_run=False):
    region_dir = OUTPUT_DIR / REGION_OUTPUT_DIRS[region] / schema_key
    region_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        for rec in group_records:
            print(f"  would add: {rec['relative_path']} ({rec['size_bytes']:,} B)")
        return []

    created = []

    def on_part_closed(part):
        entry = write_part_manifest(
            part["path"],
            region,
            schema_key,
            source_files=part["source_files"],
        )
        if entry:
            created.append(entry)

    writer = GzipPartWriter(
        region_dir, region, schema_key, on_part_closed=on_part_closed
    )

    for rec in tqdm(group_records, desc=f"{region}/{schema_key}", unit="file"):
        writer.add_file(rec["path"])

    writer.finalize()
    return created


def write_part_manifest(part_path, region, schema_key, source_files=None):
    rel_out = str(part_path.relative_to(OUTPUT_DIR))
    manifest_index = load_manifest_index()

    if rel_out in manifest_index:
        existing = manifest_index[rel_out]
        if (
            part_path.exists()
            and existing.get("gzip_size_bytes") == part_path.stat().st_size
        ):
            print(f"SKIP exists: {rel_out}")
            return None

    size = part_path.stat().st_size if part_path.exists() else 0
    entry = {
        "output_file": rel_out,
        "region": region,
        "snowflake_schema_folder": REGION_OUTPUT_DIRS[region],
        "schema_key": schema_key,
        "gzip_size_bytes": size,
        "source_files": source_files or [],
        "created_at_utc": str(datetime.now(timezone.utc)),
    }
    append_manifest(entry)
    print(
        f"WROTE {rel_out} ({size / (1024 * 1024):.1f} MB gzip, "
        f"{len(entry['source_files'])} source file(s))"
    )
    return entry


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compress extracted CSVs to Snowflake-ready .csv.gz files."
    )
    parser.add_argument(
        "--region",
        choices=["all", REGION_NYC, REGION_JERSEY_CITY],
        default="all",
        help="Process only NYC, Jersey City, or all regions.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List grouping only; do not write gzip files.",
    )
    parser.add_argument(
        "--min-mb",
        type=int,
        default=100,
        help="Target minimum gzip part size in MB (default: 100).",
    )
    parser.add_argument(
        "--max-mb",
        type=int,
        default=250,
        help="Target maximum gzip part size in MB (default: 250).",
    )
    return parser.parse_args()


def main():
    global MIN_GZIP_BYTES, MAX_GZIP_BYTES

    args = parse_args()
    MIN_GZIP_BYTES = args.min_mb * 1024 * 1024
    MAX_GZIP_BYTES = args.max_mb * 1024 * 1024

    if not EXTRACT_DIR.exists():
        raise FileNotFoundError(f"Extract directory not found: {EXTRACT_DIR}")

    print("Discovering CSV files...")
    records = discover_csv_files()
    if args.region != "all":
        records = [r for r in records if r["region"] == args.region]

    groups = group_files(records)
    print(
        f"Found {len(records)} CSV(s) in {len(groups)} "
        f"(region, schema) group(s).\n"
    )

    for (region, schema_key), group_records in sorted(groups.items()):
        total_mb = sum(r["size_bytes"] for r in group_records) / (1024 * 1024)
        print(
            f"Group {REGION_OUTPUT_DIRS[region]}/{schema_key}: "
            f"{len(group_records)} file(s), ~{total_mb:.0f} MB uncompressed"
        )
        build_output_parts(group_records, region, schema_key, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\nManifest: {MANIFEST_FILE.resolve()}")
        print(f"Output:   {OUTPUT_DIR.resolve()}/")


if __name__ == "__main__":
    main()
