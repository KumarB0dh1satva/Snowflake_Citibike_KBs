"""
Extract inner monthly zips for annual Citibike bundles flagged in
analysis_output/analysis_summary.json (nested_zip_only_bundles).

After a verified extract, nested zip files under extracted/ are deleted to
save space. Re-run download_citibike_data.py against downloads/ if you need
to restore the parent annual zip and nested archives.
"""

import json
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ANALYSIS_SUMMARY = Path("analysis_output/analysis_summary.json")
EXTRACT_DIR = Path("extracted")
LOG_FILE = Path("nested_extract_log.jsonl")


def load_target_bundles(summary_path=ANALYSIS_SUMMARY):
    if not summary_path.exists():
        raise FileNotFoundError(
            f"Analysis summary not found: {summary_path}. "
            "Run analyze_extracted_schema.py first."
        )

    with open(summary_path, encoding="utf-8") as f:
        summary = json.load(f)

    bundles = summary.get("nested_zip_only_bundles", [])
    if not bundles:
        raise ValueError(
            f"No nested_zip_only_bundles in {summary_path}. "
            "Nothing to extract."
        )

    return bundles


def load_latest_logs(log_path=LOG_FILE):
    latest = {}
    if not log_path.exists():
        return latest

    with open(log_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                latest[entry["nested_zip_path"]] = entry
            except (json.JSONDecodeError, KeyError):
                continue

    return latest


def write_log(log_entry):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry) + "\n")


def find_nested_zips(bundle_dir):
    return sorted(
        p for p in bundle_dir.rglob("*.zip")
        if "__MACOSX" not in p.parts
    )


def expected_csv_members(zip_path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        return [
            name for name in zf.namelist()
            if name.lower().endswith(".csv") and not name.endswith("/")
        ]


def get_nested_extract_dir(zip_path):
    return zip_path.parent / zip_path.stem


def is_nested_extracted(zip_path):
    extract_dir = get_nested_extract_dir(zip_path)
    if not extract_dir.is_dir():
        return False, "extract_folder_missing"

    csv_files = [
        p for p in extract_dir.glob("*.csv")
        if p.is_file() and p.stat().st_size > 0
    ]
    if not csv_files:
        return False, "no_csv_in_extract_folder"

    if zip_path.exists():
        try:
            expected = expected_csv_members(zip_path)
        except zipfile.BadZipFile:
            return False, "invalid_zip"

        if not expected:
            return False, "zip_has_no_csv_members"

        expected_names = {Path(member).name for member in expected}
        found_names = {p.name for p in csv_files}
        missing = expected_names - found_names
        if missing:
            return False, f"missing_csv ({len(missing)} of {len(expected)})"

    return True, "complete"


def delete_nested_zip(zip_path):
    """Remove nested zip after verified extract (parent zip stays in downloads/)."""

    rel_zip = zip_path.relative_to(EXTRACT_DIR)
    if zip_path.exists():
        zip_path.unlink()
        print(f"DELETED nested zip: {rel_zip}")
        return True
    return False


def cleanup_nested_extract(zip_path):
    extract_dir = get_nested_extract_dir(zip_path)
    if extract_dir.is_dir():
        for child in extract_dir.iterdir():
            if child.is_file():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)
        extract_dir.rmdir()
        print(f"DELETED incomplete extract: {extract_dir}")


def extract_nested_zip(zip_path, latest_logs):
    rel_zip = str(zip_path.relative_to(EXTRACT_DIR))
    extract_dir = get_nested_extract_dir(zip_path)
    log_entry = latest_logs.get(rel_zip, {})

    extracted, reason = is_nested_extracted(zip_path)
    if extracted:
        print(f"NESTED SKIPPED: {rel_zip} ({reason})")
        if zip_path.exists():
            delete_nested_zip(zip_path)
        return

    if extract_dir.exists():
        cleanup_nested_extract(zip_path)

    print(f"NESTED EXTRACTING: {rel_zip} ({reason})")
    start_time = datetime.now(timezone.utc)

    record = {
        "bundle_name": zip_path.relative_to(EXTRACT_DIR).parts[0],
        "nested_zip_path": rel_zip,
        "extract_dir": str(extract_dir.relative_to(EXTRACT_DIR)),
        "extract_start_utc": str(start_time),
        "extract_end_utc": None,
        "extract_status": "FAILED",
        "csv_file_count": None,
        "nested_zip_deleted": False,
        "error_message": None,
    }

    try:
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        ok, verify_reason = is_nested_extracted(zip_path)
        if not ok:
            raise ValueError(f"Post-extract verification failed: {verify_reason}")

        csv_count = len(list(extract_dir.glob("*.csv")))
        record["extract_end_utc"] = str(datetime.now(timezone.utc))
        record["extract_status"] = "SUCCESS"
        record["csv_file_count"] = csv_count
        record["nested_zip_deleted"] = delete_nested_zip(zip_path)
        print(f"NESTED SUCCESS: {rel_zip} ({csv_count} csv file(s))")

    except Exception as exc:
        record["error_message"] = str(exc)
        cleanup_nested_extract(zip_path)
        # Keep nested zip on disk so parent bundle in downloads/ can be re-used
        print(f"NESTED FAILED: {rel_zip}")
        print(exc)

    write_log(record)


def main():
    bundles = load_target_bundles()
    latest_logs = load_latest_logs()

    print(f"Bundles from {ANALYSIS_SUMMARY}: {', '.join(bundles)}\n")

    total_zips = 0
    for bundle_name in bundles:
        bundle_dir = EXTRACT_DIR / bundle_name
        if not bundle_dir.is_dir():
            print(f"WARNING: bundle folder missing: {bundle_dir}")
            continue

        nested_zips = find_nested_zips(bundle_dir)
        if not nested_zips:
            print(f"WARNING: no nested zips found in {bundle_name}")
            continue

        print(f"--- {bundle_name} ({len(nested_zips)} nested zip(s)) ---")
        for zip_path in nested_zips:
            total_zips += 1
            extract_nested_zip(zip_path, latest_logs)

    print(f"\nDone. Processed {total_zips} nested zip file(s).")
    print(f"Log: {LOG_FILE.resolve()}")


if __name__ == "__main__":
    main()
