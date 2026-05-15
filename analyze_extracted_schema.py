"""
Scan extracted Citibike bundles: detect nested zips, inventory files,
and compare CSV column schemas across years.
"""

import json
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path

import pandas as pd

EXTRACT_DIR = Path("extracted")
OUTPUT_DIR = Path("analysis_output")

BUNDLE_REPORT = OUTPUT_DIR / "bundle_structure_report.csv"
FILE_INVENTORY = OUTPUT_DIR / "extracted_file_inventory.csv"
SCHEMA_SIGNATURES = OUTPUT_DIR / "schema_signatures.csv"
ANALYSIS_SUMMARY = OUTPUT_DIR / "analysis_summary.json"


def normalize_columns(columns):
    return tuple(str(c).strip() for c in columns)


def read_csv_schema(csv_path):
    df = pd.read_csv(csv_path, nrows=0)
    columns = normalize_columns(df.columns)
    return {
        "column_count": len(columns),
        "columns": "|".join(columns),
        "schema_id": hash(columns),
    }


def read_csv_schema_from_zip(zip_path, member_name):
    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(member_name) as raw:
            with TextIOWrapper(raw, encoding="utf-8", errors="replace") as text:
                df = pd.read_csv(text, nrows=0)
    columns = normalize_columns(df.columns)
    return {
        "column_count": len(columns),
        "columns": "|".join(columns),
        "schema_id": hash(columns),
    }


def inspect_nested_zip(zip_path):
    rows = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.endswith("/"):
                continue
            entry = {
                "nested_zip": str(zip_path),
                "inner_member": name,
                "inner_type": Path(name).suffix.lower().lstrip(".") or "unknown",
                "inner_size_bytes": zf.getinfo(name).file_size,
            }
            if name.lower().endswith(".csv"):
                try:
                    schema = read_csv_schema_from_zip(zip_path, name)
                    entry.update(schema)
                except Exception as exc:
                    entry["schema_error"] = str(exc)
            rows.append(entry)
    return rows


def classify_bundle(csv_count, nested_zip_count):
    if nested_zip_count and not csv_count:
        return "nested_zip_only"
    if nested_zip_count and csv_count:
        return "mixed_csv_and_nested_zip"
    if csv_count:
        return "csv_present"
    return "empty_or_other"


def scan_extracted_root(extract_root=EXTRACT_DIR):
    bundle_rows = []
    inventory_rows = []
    nested_zip_details = []
    schema_groups = defaultdict(list)

    for bundle_dir in sorted(p for p in extract_root.iterdir() if p.is_dir()):
        bundle_name = bundle_dir.name
        csv_files = [
            p for p in bundle_dir.rglob("*.csv")
            if "__MACOSX" not in p.parts
        ]
        nested_zips = [
            p for p in bundle_dir.rglob("*.zip")
            if "__MACOSX" not in p.parts
        ]

        structure_type = classify_bundle(len(csv_files), len(nested_zips))

        bundle_rows.append({
            "bundle_name": bundle_name,
            "structure_type": structure_type,
            "csv_file_count": len(csv_files),
            "nested_zip_count": len(nested_zips),
            "needs_nested_extract": structure_type == "nested_zip_only",
        })

        for csv_path in csv_files:
            rel_path = csv_path.relative_to(extract_root)
            row = {
                "bundle_name": bundle_name,
                "relative_path": str(rel_path),
                "file_type": "csv",
                "size_bytes": csv_path.stat().st_size,
                "inside_nested_zip": False,
            }
            try:
                schema = read_csv_schema(csv_path)
                row.update(schema)
                schema_groups[schema["columns"]].append(str(rel_path))
            except Exception as exc:
                row["schema_error"] = str(exc)
            inventory_rows.append(row)

        for zip_path in nested_zips:
            rel_path = zip_path.relative_to(extract_root)
            inventory_rows.append({
                "bundle_name": bundle_name,
                "relative_path": str(rel_path),
                "file_type": "zip",
                "size_bytes": zip_path.stat().st_size,
                "inside_nested_zip": False,
            })
            for inner in inspect_nested_zip(zip_path):
                inner_row = {
                    "bundle_name": bundle_name,
                    "relative_path": str(rel_path),
                    "file_type": "nested_zip_member",
                    "inside_nested_zip": True,
                    **inner,
                }
                nested_zip_details.append(inner_row)
                if "columns" in inner:
                    schema_groups[inner["columns"]].append(
                        f"{rel_path}::{inner['inner_member']}"
                    )

    return bundle_rows, inventory_rows, nested_zip_details, schema_groups


def build_schema_signature_rows(schema_groups):
    rows = []
    for idx, (columns, paths) in enumerate(
        sorted(schema_groups.items(), key=lambda x: len(x[1]), reverse=True),
        start=1,
    ):
        sample_paths = paths[:5]
        rows.append({
            "schema_signature": f"schema_{idx}",
            "file_count": len(paths),
            "column_count": len(columns.split("|")),
            "columns": columns,
            "sample_paths": " ; ".join(sample_paths),
        })
    return rows


def print_summary(bundle_rows, schema_rows, nested_zip_details):
    nested_only = [
        b for b in bundle_rows if b["structure_type"] == "nested_zip_only"
    ]

    print("\n=== Citibike extracted data analysis ===\n")
    print(f"Bundles scanned: {len(bundle_rows)}")
    print(f"Unique CSV schemas: {len(schema_rows)}")
    print(f"Nested zip inner files inspected: {len(nested_zip_details)}")

    if nested_only:
        print("\nBundles with nested zips only (no CSV on disk yet):")
        for row in nested_only:
            print(
                f"  - {row['bundle_name']}: "
                f"{row['nested_zip_count']} inner zip file(s)"
            )

    print("\nSchema signatures (top 5 by file count):")
    for row in schema_rows[:5]:
        print(
            f"  - {row['schema_signature']}: "
            f"{row['file_count']} file(s), "
            f"{row['column_count']} columns"
        )

    print(f"\nReports written to: {OUTPUT_DIR.resolve()}/")


def main():
    if not EXTRACT_DIR.exists():
        raise FileNotFoundError(f"Extract directory not found: {EXTRACT_DIR}")

    OUTPUT_DIR.mkdir(exist_ok=True)

    bundle_rows, inventory_rows, nested_zip_details, schema_groups = (
        scan_extracted_root()
    )
    schema_rows = build_schema_signature_rows(schema_groups)

    pd.DataFrame(bundle_rows).to_csv(BUNDLE_REPORT, index=False)
    pd.DataFrame(inventory_rows).to_csv(FILE_INVENTORY, index=False)
    pd.DataFrame(schema_rows).to_csv(SCHEMA_SIGNATURES, index=False)

    if nested_zip_details:
        pd.DataFrame(nested_zip_details).to_csv(
            OUTPUT_DIR / "nested_zip_inventory.csv",
            index=False,
        )

    summary = {
        "analyzed_at_utc": str(datetime.now(timezone.utc)),
        "bundle_count": len(bundle_rows),
        "unique_schemas": len(schema_rows),
        "nested_zip_only_bundles": [
            b["bundle_name"]
            for b in bundle_rows
            if b["structure_type"] == "nested_zip_only"
        ],
    }
    with open(ANALYSIS_SUMMARY, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print_summary(bundle_rows, schema_rows, nested_zip_details)


if __name__ == "__main__":
    main()
