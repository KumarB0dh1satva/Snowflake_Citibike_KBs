"""
Snowflake load configuration (non-secret settings).

Connection secrets live in snowflake_credentials.py (gitignored).
"""

import os

try:
    from snowflake_credentials import (
        ACCOUNT,
        DATABASE,
        PASSWORD,
        ROLE,
        USER,
        WAREHOUSE,
    )
except ImportError as exc:
    raise ImportError(
        "Missing snowflake_credentials.py — copy snowflake_credentials.example.py "
        "and fill in your Snowflake account details."
    ) from exc

# Env vars override credentials file when set (CI / production).
ACCOUNT = os.environ.get("SF_ACCOUNT", ACCOUNT)
USER = os.environ.get("SF_USER", USER)
PASSWORD = os.environ.get("SF_PASSWORD", PASSWORD)
WAREHOUSE = os.environ.get("SF_WAREHOUSE", WAREHOUSE)
DATABASE = os.environ.get("SF_DATABASE", DATABASE)
ROLE = os.environ.get("SF_ROLE", ROLE)

# gzip_manifest region → Snowflake schema
SCHEMA_MAP = {
    "nyc": "STAGING_NYC",
    "jersey_city": "STAGING_JC",
}

# schema_key (MD5 of column layout) → target table in that region's schema
SCHEMA_KEY_TO_TABLE = {
    "dc497b4333c4": "TRIPS_MODERN",   # 13-col ride_id layout (NYC + JC)
    "473144999085": "TRIPS_LEGACY_V1",   # 15-col lowercase legacy
    "e24ee8457e0e": "TRIPS_LEGACY_V2",   # 15-col Title Case legacy (2016-style)
}

STAGE_NAME = "RAW_INGESTION"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30
PARALLEL_PUT_THREADS = 4       # Snowflake PUT command threads (per file)
PARALLEL_STAGE_WORKERS = 4     # concurrent files in stage_files.py
STAGE_PUT_OVERWRITE = True     # default OVERWRITE on PUT (avoid silent SKIPPED)


def stage_fqn(schema: str) -> str:
    """Fully qualified internal stage: DATABASE.SCHEMA.STAGE."""
    return f"{DATABASE}.{schema}.{STAGE_NAME}"


def validate_config() -> None:
    placeholders = {"", "your_account_identifier", "your_username", "your_password"}
    if ACCOUNT in placeholders or USER in placeholders or PASSWORD in placeholders:
        raise ValueError(
            "Snowflake credentials are not configured. "
            "Edit snowflake_credentials.py or set SF_* environment variables."
        )
    for region, schema in SCHEMA_MAP.items():
        if not schema:
            raise ValueError(f"SCHEMA_MAP missing entry for region {region!r}")
