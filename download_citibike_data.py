import pandas as pd
import requests
import zipfile
import json
import os
import shutil
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

# ==========================================
# CONFIG
# ==========================================

CSV_INDEX_FILE = "citibike_s3_file_index.csv"

DOWNLOAD_DIR = "downloads"
EXTRACT_DIR = "extracted"
LOG_FILE = "download_log.jsonl"

MAX_WORKERS = 1
CHUNK_SIZE = 1024 * 1024  # 1 MB

Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
Path(EXTRACT_DIR).mkdir(exist_ok=True)

df = pd.read_csv(CSV_INDEX_FILE)

# ==========================================
# LOGGING
# ==========================================

def write_log(log_entry):

    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(log_entry) + "\n")


def update_log_extraction(file_name, unzip_status):
    """Append a log line updating unzip_status from the latest entry."""

    latest = LATEST_LOGS.get(file_name)
    now = str(datetime.now(timezone.utc))

    if latest:
        log_entry = {
            **latest,
            "unzip_status": unzip_status,
            "extract_updated_utc": now,
        }
    else:
        log_entry = {
            "file_name": file_name,
            "download_status": "UNKNOWN",
            "unzip_status": unzip_status,
            "extract_updated_utc": now,
        }

    write_log(log_entry)
    LATEST_LOGS[file_name] = log_entry


def load_latest_logs(log_path=LOG_FILE):
    """Return the most recent log entry per file_name."""

    latest = {}

    if not os.path.exists(log_path):
        return latest

    with open(log_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                latest[entry["file_name"]] = entry
            except (json.JSONDecodeError, KeyError):
                continue

    return latest


LATEST_LOGS = load_latest_logs()

# ==========================================
# DOWNLOAD STATE
# ==========================================

def get_extract_path(file_name):

    return os.path.join(EXTRACT_DIR, file_name.replace(".zip", ""))


def cleanup_local_artifacts(file_name):

    local_zip_path = os.path.join(DOWNLOAD_DIR, file_name)
    extract_path = get_extract_path(file_name)

    if os.path.exists(local_zip_path):
        os.remove(local_zip_path)
        print(f"DELETED incomplete zip: {file_name}")

    if os.path.isdir(extract_path):
        shutil.rmtree(extract_path)
        print(f"DELETED extract folder: {file_name}")


def is_download_complete(row, latest_logs):
    """
    Skip download when the log shows SUCCESS, on-disk size matches the
    index, and the logged size matches the index.
  """

    file_name = row["file_name"]
    expected_size = int(row["file_size_bytes"])
    local_zip_path = os.path.join(DOWNLOAD_DIR, file_name)

    log_entry = latest_logs.get(file_name)

    if not os.path.exists(local_zip_path):
        return False, "missing_local_file"

    local_size = os.path.getsize(local_zip_path)
    if local_size != expected_size:
        return False, f"size_mismatch (local={local_size}, expected={expected_size})"

    if log_entry is None:
        return False, "no_log_entry"

    if log_entry.get("download_status") != "SUCCESS":
        return False, f"log_status={log_entry.get('download_status')}"

    logged_size = log_entry.get("file_size_bytes")
    if logged_size is None:
        return False, "log_missing_size"

    if int(logged_size) != expected_size:
        return False, (
            f"log_size_mismatch (logged={logged_size}, expected={expected_size})"
        )

    return True, "complete"


def is_extracted(file_name, latest_logs):
    """Check whether the zip has already been extracted (jsonl + folder)."""

    extract_path = get_extract_path(file_name)
    log_entry = latest_logs.get(file_name)

    log_marked_extracted = (
        log_entry is not None
        and log_entry.get("unzip_status") == "SUCCESS"
    )

    if not os.path.isdir(extract_path):
        if log_marked_extracted:
            return False, "log_says_extracted_but_folder_missing"
        return False, "extract_folder_missing"

    has_content = any(
        p.is_file() and p.name != "__MACOSX"
        for p in Path(extract_path).iterdir()
    ) or any(Path(extract_path).rglob("*.csv"))

    if not has_content:
        if log_marked_extracted:
            return False, "log_says_extracted_but_folder_empty"
        return False, "extract_folder_empty"

    if log_marked_extracted:
        return True, "already_extracted (log + folder)"

    return True, "already_extracted (folder only, log not updated)"


def perform_extraction(file_name, local_zip_path, persist_log=True):
    """
    Extract zip if needed. Returns unzip_status for logging and prints status.
    When persist_log is True (skip-download path), updates download_log.jsonl.
    """

    log_entry = LATEST_LOGS.get(file_name)
    logged_unzip = log_entry.get("unzip_status") if log_entry else None

    def persist(unzip_status):
        if persist_log and logged_unzip != unzip_status:
            update_log_extraction(file_name, unzip_status)

    if not file_name.lower().endswith(".zip"):
        print(f"EXTRACT SKIP: {file_name} (not a zip file)")
        persist("NOT_A_ZIP")
        return "NOT_A_ZIP"

    if not os.path.exists(local_zip_path):
        print(f"EXTRACT SKIP: {file_name} (zip file missing)")
        return "ZIP_MISSING"

    if not zipfile.is_zipfile(local_zip_path):
        print(f"EXTRACT SKIP: {file_name} (invalid zip file)")
        persist("INVALID_ZIP")
        return "INVALID_ZIP"

    extracted, reason = is_extracted(file_name, LATEST_LOGS)
    if extracted:
        print(f"EXTRACT SKIPPED: {file_name} ({reason})")
        persist("SUCCESS")
        return "SUCCESS"

    extract_path = get_extract_path(file_name)
    print(f"EXTRACTING: {file_name} ({reason})")
    Path(extract_path).mkdir(exist_ok=True)
    with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)
    print(f"EXTRACT SUCCESS: {file_name}")
    persist("SUCCESS")
    return "SUCCESS"


def prepare_download(row, latest_logs):
    """
    Decide whether to skip or re-download. Deletes local artifacts when
    the file is incomplete or the log does not reflect a full download.
    """

    file_name = row["file_name"]
    complete, reason = is_download_complete(row, latest_logs)

    if complete:
        print(f"SKIPPED: {file_name} ({reason})")
        return False

    local_zip_path = os.path.join(DOWNLOAD_DIR, file_name)
    if os.path.exists(local_zip_path):
        cleanup_local_artifacts(file_name)
        print(f"RETRY: {file_name} ({reason})")
    else:
        print(f"DOWNLOAD: {file_name} ({reason})")

    return True

# ==========================================
# RETRYABLE DOWNLOAD
# ==========================================

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5)
)
def download_file(row):

    file_name = row["file_name"]
    local_zip_path = os.path.join(DOWNLOAD_DIR, file_name)

    if not prepare_download(row, LATEST_LOGS):
        unzip_status = perform_extraction(file_name, local_zip_path)
        print(f"EXTRACT STATUS (from log): {file_name} -> {unzip_status}")
        return
    file_url = row["file_url"]
    expected_size = int(row["file_size_bytes"])

    start_time = datetime.now(timezone.utc)

    log_entry = {
        "file_name": file_name,
        "file_url": file_url,
        "expected_size_bytes": expected_size,
        "download_start_utc": str(start_time),
        "download_end_utc": None,
        "download_status": "FAILED",
        "file_size_bytes": None,
        "unzip_status": "NOT_ATTEMPTED",
        "error_message": None
    }

    try:

        response = requests.get(
            file_url,
            stream=True,
            timeout=60
        )

        response.raise_for_status()

        total_size = int(
            response.headers.get("content-length", 0)
        )

        with open(local_zip_path, "wb") as f:

            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=file_name[:40],
                ascii=True,
                dynamic_ncols=True
            ) as progress_bar:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        progress_bar.update(len(chunk))

        actual_size = os.path.getsize(local_zip_path)

        if actual_size != expected_size:
            raise ValueError(
                f"Downloaded size {actual_size} does not match "
                f"index size {expected_size}"
            )

        end_time = datetime.now(timezone.utc)

        log_entry["download_end_utc"] = str(end_time)
        log_entry["download_status"] = "SUCCESS"
        log_entry["file_size_bytes"] = actual_size

        log_entry["unzip_status"] = perform_extraction(
            file_name, local_zip_path, persist_log=False
        )
        print(f"DOWNLOAD SUCCESS: {file_name}")

    except Exception as e:
        log_entry["error_message"] = str(e)
        if os.path.exists(local_zip_path):
            cleanup_local_artifacts(file_name)
        print(f"FAILED: {file_name}")
        print(str(e))

    write_log(log_entry)

# ==========================================
# PARALLEL EXECUTION
# ==========================================

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(download_file, [
        row for _, row in df.iterrows()
    ])
print("\nALL DOWNLOADS COMPLETED")
