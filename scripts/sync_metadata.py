#!/usr/bin/env python3
"""
sync_metadata.py
Syncs Schema Registry subjects to Snowflake CONFIG.TABLE_METADATA.
Triggered by Dagster registry_sensor when new subjects are detected.

Usage:
    python sync_metadata.py [--dry-run]

Environment variables required (from .env):
    SCHEMA_REGISTRY_URL, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
    SNOWFLAKE_PRIVATE_KEY, SNOWFLAKE_DATABASE, SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE
"""

import argparse
import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import requests
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

REGISTRY_URL     = os.environ["SCHEMA_REGISTRY_URL"]
SF_ACCOUNT       = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER          = os.environ["SNOWFLAKE_USER"]
SF_PRIVATE_KEY   = os.environ["SNOWFLAKE_PRIVATE_KEY"]
SF_DATABASE      = os.environ.get("SNOWFLAKE_DATABASE", "CDC_POC")
SF_ROLE          = os.environ.get("SNOWFLAKE_ROLE", "CDC_ROLE")
SF_WAREHOUSE     = os.environ.get("SNOWFLAKE_WAREHOUSE", "CDC_WH")
CONFIG_SCHEMA    = "CONFIG"
METADATA_TABLE   = f"{SF_DATABASE}.{CONFIG_SCHEMA}.TABLE_METADATA"
HISTORY_TABLE    = f"{SF_DATABASE}.{CONFIG_SCHEMA}.METADATA_HISTORY"

# Mapping from Avro doc field to TABLE_METADATA columns.
# Format in doc: "table_type=entity,cdc_strategy=upsert,unique_key=id"
DOC_DEFAULTS = {
    "table_type":   "entity",
    "cdc_strategy": "upsert",
    "unique_key":   "id",
}


# ── Schema Registry ───────────────────────────────────────────────────────────

def get_subjects() -> list[str]:
    """Returns all value subjects from Schema Registry."""
    resp = requests.get(f"{REGISTRY_URL}/subjects", timeout=10)
    resp.raise_for_status()
    return [s for s in resp.json() if s.endswith("-value")]


def get_latest_schema(subject: str) -> dict:
    """Returns the latest schema version for a subject."""
    resp = requests.get(
        f"{REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10
    )
    resp.raise_for_status()
    return resp.json()


def parse_doc_metadata(doc: Optional[str]) -> dict:
    """
    Parses the doc field of an Avro schema for CDC metadata.
    Expected format: "table_type=entity,cdc_strategy=upsert,unique_key=id"
    Returns only fields explicitly present in doc — no defaults applied.
    Callers apply DOC_DEFAULTS only when inserting a new row.
    """
    result = {}
    if not doc:
        return result
    for part in doc.split(","):
        part = part.strip()
        if "=" in part:
            key, _, value = part.partition("=")
            key = key.strip()
            if key in DOC_DEFAULTS:
                result[key] = value.strip()
    return result


def subject_to_table_name(subject: str) -> str:
    """
    Converts a Schema Registry subject to a table name.
    e.g. 'pg.public.usuarios-value' → 'usuarios'
    """
    # Remove -value suffix, take last segment after last dot
    base = subject.removesuffix("-value")
    return base.split(".")[-1]


def subject_to_topic(subject: str) -> str:
    """
    Converts a Schema Registry subject to a Kafka topic name.
    e.g. 'pg.public.usuarios-value' → 'pg.public.usuarios'
    """
    return subject.removesuffix("-value")


# ── Snowflake ─────────────────────────────────────────────────────────────────

def get_snowflake_conn():
    """Returns a Snowflake connection using private key authentication.
    SNOWFLAKE_PRIVATE_KEY is base64-encoded PKCS8 DER — pass directly to connector."""
    private_key_der = base64.b64decode(SF_PRIVATE_KEY)
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=private_key_der,
        database=SF_DATABASE,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
    )


def get_existing_metadata(conn) -> dict:
    """Returns current TABLE_METADATA as dict keyed by table_name."""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT table_name, table_type, cdc_strategy, unique_key, active
        FROM {METADATA_TABLE}
    """)
    return {
        row[0]: {
            "table_type":   row[1],
            "cdc_strategy": row[2],
            "unique_key":   row[3],
            "active":       row[4],
        }
        for row in cursor.fetchall()
    }


def upsert_metadata(conn, table_name: str, topic: str, meta: dict,
                    existing: dict, dry_run: bool) -> str:
    """
    Inserts or updates TABLE_METADATA for a single table.
    Records changes in METADATA_HISTORY.
    Returns change_type: 'insert' | 'update' | 'no_change'
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    changed_by = "sync_metadata.py"

    if table_name not in existing:
        # New table: fill any missing doc fields with DOC_DEFAULTS
        meta_insert = {**DOC_DEFAULTS, **meta}
        change_type = "insert"
        log.info(f"  [INSERT] {table_name} → strategy={meta_insert['cdc_strategy']}")

        if not dry_run:
            conn.cursor().execute(f"""
                INSERT INTO {METADATA_TABLE}
                    (table_name, topic, table_type, cdc_strategy,
                     unique_key, source, changed_by, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'schema_registry', %s, %s)
            """, (table_name, topic, meta_insert["table_type"], meta_insert["cdc_strategy"],
                  meta_insert["unique_key"], changed_by, now))

            conn.cursor().execute(f"""
                INSERT INTO {HISTORY_TABLE}
                    (table_name, changed_by, change_type, field_changed,
                     old_value, new_value, source)
                VALUES (%s, %s, 'insert', 'all', null, %s, 'schema_registry')
            """, (table_name, changed_by, json.dumps(meta_insert)))

    else:
        if not meta:
            # doc absent or empty: preserve existing metadata, never overwrite with defaults
            log.warning(f"  [SKIP] {table_name}: doc field absent in schema, existing metadata preserved")
            return "no_change"

        current = existing[table_name]
        changes = []

        # Compare only fields explicitly declared in doc
        for field in meta:
            old_val = current.get(field)
            new_val = meta[field]
            if old_val != new_val:
                changes.append((field, old_val, new_val))

        if not changes:
            return "no_change"

        change_type = "update"
        for field, old_val, new_val in changes:
            log.info(f"  [UPDATE] {table_name}.{field}: {old_val!r} → {new_val!r}")

        # Merge: keep existing values for fields not present in doc
        meta_update = {
            "table_type":   current["table_type"],
            "cdc_strategy": current["cdc_strategy"],
            "unique_key":   current["unique_key"],
            **meta,
        }

        strategy_changed = any(f == "cdc_strategy" for f, _, _ in changes)
        previous_strategy = current["cdc_strategy"] if strategy_changed else current.get("previous_strategy")

        if not dry_run:
            conn.cursor().execute(f"""
                UPDATE {METADATA_TABLE}
                SET table_type        = %s,
                    cdc_strategy      = %s,
                    unique_key        = %s,
                    previous_strategy = %s,
                    changed_by        = %s,
                    updated_at        = %s,
                    source            = 'schema_registry'
                WHERE table_name = %s
            """, (meta_update["table_type"], meta_update["cdc_strategy"], meta_update["unique_key"],
                  previous_strategy, changed_by, now, table_name))

            for field, old_val, new_val in changes:
                conn.cursor().execute(f"""
                    INSERT INTO {HISTORY_TABLE}
                        (table_name, changed_by, change_type, field_changed,
                         old_value, new_value, source)
                    VALUES (%s, %s, 'update', %s, %s, %s, 'schema_registry')
                """, (table_name, changed_by, field,
                      str(old_val), str(new_val)))

    return change_type


# ── Main ──────────────────────────────────────────────────────────────────────

def sync(dry_run: bool = False) -> dict:
    """
    Full sync: Registry subjects → TABLE_METADATA.
    Returns summary dict with counts of inserts, updates, no_changes.
    """
    log.info(f"Starting sync {'(DRY RUN) ' if dry_run else ''}from {REGISTRY_URL}")

    subjects = get_subjects()
    log.info(f"Found {len(subjects)} value subjects in Schema Registry")

    conn = get_snowflake_conn()
    existing = get_existing_metadata(conn)
    log.info(f"Found {len(existing)} tables in TABLE_METADATA")

    summary = {"insert": 0, "update": 0, "no_change": 0, "errors": 0}

    for subject in subjects:
        table_name = subject_to_table_name(subject)
        topic      = subject_to_topic(subject)

        try:
            schema_data = get_latest_schema(subject)
            avro_schema = json.loads(schema_data["schema"])
            doc         = avro_schema.get("doc", "")
            meta        = parse_doc_metadata(doc)

            log.info(f"Processing {subject} → table={table_name} "
                     f"type={meta.get('table_type', '?')} strategy={meta.get('cdc_strategy', '?')}")

            change_type = upsert_metadata(
                conn, table_name, topic, meta, existing, dry_run
            )
            summary[change_type] += 1

        except Exception as exc:
            log.error(f"  [ERROR] {subject}: {exc}")
            summary["errors"] += 1

    if not dry_run:
        conn.commit()

    conn.close()

    log.info(
        f"Sync complete — "
        f"inserted: {summary['insert']}, "
        f"updated: {summary['update']}, "
        f"no_change: {summary['no_change']}, "
        f"errors: {summary['errors']}"
    )
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Schema Registry to TABLE_METADATA")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing to Snowflake")
    args = parser.parse_args()

    summary = sync(dry_run=args.dry_run)
    sys.exit(1 if summary["errors"] > 0 else 0)
