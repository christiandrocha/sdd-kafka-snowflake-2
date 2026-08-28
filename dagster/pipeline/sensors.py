"""
sensors.py — v5 (after the credit-consumption diagnosis)

CHANGES RELATIVE TO THE PREVIOUS VERSION:

1. bronze_new_data_sensor
   Before: every 60s it opened a Snowflake connection and ran up to 20
   `SELECT MAX(RECORD_METADATA:CreateTime)` -- one per Bronze table -- to
   decide whether to trigger the dbt run. Since AUTO_SUSPEND on the CDC_WH
   warehouse is 60s, the warehouse never had a real window of inactivity and
   stayed effectively always on (~24 credits/day at zero activity).

   Now: it queries only CONFIG.PENDING_RUNS (populated by Snowflake's native
   Tasks -- see scripts/streams_and_tasks.sql, ADR-0019). That is 1 light
   SELECT instead of 20, and Snowflake only writes rows there once
   SYSTEM$STREAM_HAS_DATA has confirmed real data in a Bronze table, not a
   guess.

   CORRECTION (2026-08-10, measured against the real account): the previous
   version of this docstring claimed the interval could stay at 60s "because
   the query itself is trivial". The weight of the query is not the relevant
   variable. ANY query on a real table resumes the warehouse, and Snowflake
   bills a 60-second minimum per resume. With CDC_WH at AUTO_SUSPEND=60 and
   the sensor querying every 60s, billing becomes continuous: ~1,440
   resumes/day x 60s = 24h of X-Small = ~24 credits/day -- exactly the number
   that motivated this feature. The native gate (Streams+Tasks) is not undone
   on the Snowflake side, but it was undone by the sensor from the outside.

   Hence the sensor now uses the SAME zero-cost gate as
   registry_new_subject_sensor: it queries Prometheus first, and only opens a
   Snowflake connection if there was a new message in Kafka since the last
   cycle (or if Prometheus is down -- fail-open). At rest the sensor does not
   touch the warehouse at all, and the cost sits where it should: on the
   Tasks, which only fire on real data.

2. registry_new_subject_sensor
   Before: it queried CONFIG.TABLE_METADATA in Snowflake every 300s,
   unconditionally -- contradicting the initial assumption (documented in this
   same analysis) that this sensor did not touch Snowflake.

   Now: it queries Prometheus first (zero cost in Snowflake credits) to learn
   whether any topic had activity since the last cycle. It only opens a
   Snowflake connection if there is a sign of activity -- fail-open if
   Prometheus is unreachable (prioritising never missing a new subject over
   never spending a credit needlessly, consistent with the decision recorded
   in ADR-0019).

STATUS (2026-08-10): validated against the real Snowflake account, the Kafka
and the Prometheus of the local stack. Validation found two defects that static
analysis would not have caught -- a metric name that does not exist in the
Prometheus gate, and the cost of the sensor itself -- both fixed and annotated
at the point in the code.
"""

import json
import os
from datetime import UTC, datetime

import requests
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from .jobs import cdc_pipeline_job, sync_metadata_job
from .resources import SnowflakeResource

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")

# CORRECTION (2026-08-10): the previous constant was
# `kafka_server_brokertopicmetrics_messagesin_total`, with a `_total` suffix.
# That metric does NOT exist in this stack -- the jmx-exporter publishes
# `kafka_server_brokertopicmetrics_messagesin` (no suffix). The query returned
# an empty list, the gate fell into the "no series at all" fail-open and
# registry_new_subject_sensor touched Snowflake on EVERY cycle, from the start.
# Verified against Prometheus's /api/v1/label/__name__/values.
#
# The topic filter is necessary too: of the 4 series published at rest, all 4
# are internal (__consumer_offsets, _schemas, connect_configs,
# connect_statuses). connect_statuses receives a heartbeat from Kafka Connect,
# so without the filter the gate would read infrastructure traffic as new CDC
# data.
CDC_TOPIC_PATTERN = os.getenv("CDC_TOPIC_PATTERN", "pg[.]public[.].*")
KAFKA_MESSAGES_METRIC = (
    f'kafka_server_brokertopicmetrics_messagesin{{topic=~"{CDC_TOPIC_PATTERN}"}}'
)

# How many cycles bronze_new_data_sensor keeps checking Snowflake after the
# last activity seen in Kafka. It exists because the two ends are asynchronous:
# the message arrives in Kafka on one cycle, but the native Task only writes
# PENDING_RUNS up to a minute later. Without that margin, the last row of a
# burst would go unconsumed until the next message -- which in a quiet period
# may never come.
HOT_CYCLES_AFTER_ACTIVITY = 3


# ── Zero-cost gate: Prometheus before any Snowflake connection ───────────

def _get_kafka_message_totals() -> dict:
    """Zero cost in Snowflake credits -- only local HTTP to Prometheus."""
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": KAFKA_MESSAGES_METRIC},
        timeout=5,
    )
    resp.raise_for_status()
    results = resp.json()["data"]["result"]
    return {r["metric"].get("topic", "unknown"): float(r["value"][1]) for r in results}


def _kafka_had_activity(context, last_totals: dict, initialized: bool) -> tuple:
    """
    (had_activity, current_totals) -- without touching Snowflake.

    Fail-open in two situations, both where missing a trigger is worse than
    spending one warehouse resume:

    1. Prometheus unreachable.
    2. First tick with this cursor (`initialized=False`). There may be a row in
       PENDING_RUNS written before the sensor existed, or before a broker
       restart that zeroed the counters.

    Note that the condition in item 2 is "the cursor is empty", NOT "the query
    came back empty". No `pg.public.*` series exists until the first CDC
    message from the current broker, and treating that as fail-open would keep
    the sensor hitting Snowflake forever -- which was exactly the defect
    measured on 2026-08-10.

    A counter reset (broker restart) counts as activity: the series disappears
    or comes back smaller, and the comparison is `!=`, not `>`.
    """
    try:
        current = _get_kafka_message_totals()
    except Exception as e:
        context.log.warning(f"Prometheus unavailable ({e}) -- fail-open, checking Snowflake.")
        # Keep the old totals: when Prometheus comes back, the delta is
        # measured against the last value actually observed.
        return True, last_totals

    if not initialized:
        return True, current

    changed = any(
        current.get(t, 0) != last_totals.get(t, 0)
        for t in set(current) | set(last_totals)
    )
    return changed, current


# ── Bronze: the native gate (Streams + Tasks) already did the heavy work ──

@sensor(
    job=cdc_pipeline_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def bronze_new_data_sensor(context, snowflake: SnowflakeResource) -> SensorResult:
    """
    Queries CONFIG.PENDING_RUNS -- populated by Snowflake's native Tasks
    (scripts/streams_and_tasks.sql), which only run when
    SYSTEM$STREAM_HAS_DATA confirms real data in a Bronze table.

    CORRECTION (compared against an external second opinion, 2026-08-04):
    the previous version filtered by `detected_at > cursor` ON TOP OF
    `consumed = FALSE`. That reintroduced the same global-watermark bug that
    motivated replacing the original sensor: if a high-volume domain writes a
    more recent `detected_at` and advances the cursor, a low-volume domain
    whose Task only finishes later (writing a `detected_at` older than the
    already-advanced cursor) becomes invisible forever -- `consumed` stays
    FALSE, but `detected_at > cursor` never matches. `consumed = FALSE` alone
    is already sufficient as an idempotency guard; the cursor comparison was
    redundant AND dangerous. Removed -- no row of PENDING_RUNS is filtered by
    time any more.

    `context.cursor` came back into use (2026-08-10), but for something else:
    it holds the Kafka counters of the cost gate and the hot-cycle counter. It
    does NOT filter any row of PENDING_RUNS -- when the sensor decides to
    query, it reads every row with `consumed = FALSE`, with no time cut. The
    watermark bug does not come back through here.
    """
    state = json.loads(context.cursor or "{}")
    last_totals = state.get("totals", {})
    hot = state.get("hot", 0)
    initialized = state.get("initialized", False)

    # Phase 1 -- zero cost: no Snowflake connection, no CDC_WH resume.
    activity, current_totals = _kafka_had_activity(context, last_totals, initialized)

    if activity:
        hot = HOT_CYCLES_AFTER_ACTIVITY
    elif hot > 0:
        hot -= 1
    else:
        return SensorResult(
            skip_reason=SkipReason(
                "No Kafka activity (via Prometheus) -- Snowflake not queried."
            ),
            cursor=json.dumps({"totals": current_totals, "hot": 0, "initialized": True}),
        )

    new_cursor = json.dumps({"totals": current_totals, "hot": hot, "initialized": True})

    # Phase 2 -- only now does it touch Snowflake.
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT domain, detected_at
            FROM CONFIG.PENDING_RUNS
            WHERE consumed = FALSE
            ORDER BY detected_at DESC
            """
        )
        rows = cur.fetchall()

        if not rows:
            return SensorResult(
                skip_reason=SkipReason("CONFIG.PENDING_RUNS has no new entries."),
                cursor=new_cursor,
            )

        domains = sorted({r[0] for r in rows})
        newest_ts = max(r[1] for r in rows)

        # Marks as consumed by the row's implicit PRIMARY KEY, not by a time
        # cut -- avoids marking as consumed a row that had not yet been read
        # (the same class of bug as the cursor above).
        cur.execute(
            """
            UPDATE CONFIG.PENDING_RUNS
            SET consumed = TRUE
            WHERE consumed = FALSE
            """
        )
        conn.commit()

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"bronze-{newest_ts.isoformat()}",
                tags={"triggered_by": "native_streams_tasks", "domains": ",".join(domains)},
            )
        ],
        cursor=new_cursor,
    )


# ── Registry: same gate, applied before touching Snowflake ───────────────
# (_get_kafka_message_totals now lives in the shared block above, because both
#  sensors use the same gate.)

def _get_registered_subjects() -> list:
    resp = requests.get(
        f"{os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')}/subjects",
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


def _get_synced_tables(conn) -> set:
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM CONFIG.TABLE_METADATA")
    return {r[0] for r in cur.fetchall()}


@sensor(
    job=sync_metadata_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def registry_new_subject_sensor(context, snowflake: SnowflakeResource) -> SensorResult:
    # Cursor migrated on 2026-08-10 from the old format (raw dict of totals) to
    # {"totals": ..., "initialized": ...}. A cursor in the old format falls into
    # `initialized=False` and checks once -- self-healing, no manual step.
    state = json.loads(context.cursor or "{}")
    last_totals = state.get("totals", {})
    initialized = state.get("initialized", False)

    # Phase 1 -- zero cost: was there activity on any CDC topic since the last
    # cycle? Same gate as bronze_new_data_sensor, same function.
    has_activity, current_totals = _kafka_had_activity(context, last_totals, initialized)
    new_cursor = json.dumps({"totals": current_totals, "initialized": True})

    if not has_activity:
        return SensorResult(
            skip_reason=SkipReason("No Kafka activity (via Prometheus)."),
            cursor=new_cursor,
        )

    # Phase 2 -- only now does it touch Snowflake.
    try:
        subjects = _get_registered_subjects()
    except Exception as e:
        return SensorResult(skip_reason=SkipReason(f"Schema Registry unavailable: {e}"))

    if not subjects:
        return SensorResult(
            skip_reason=SkipReason("No subjects registered."),
            cursor=new_cursor,
        )

    subject_tables = {s.split("-")[0].upper() for s in subjects}

    with snowflake.get_connection() as conn:
        synced_tables = _get_synced_tables(conn)

    new_tables = subject_tables - synced_tables

    if not new_tables:
        return SensorResult(
            skip_reason=SkipReason("All subjects already synced into TABLE_METADATA."),
            cursor=new_cursor,
        )

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"registry-sync-{datetime.now(UTC).isoformat()}",
                tags={"new_tables": ",".join(sorted(new_tables))},
            )
        ],
        cursor=new_cursor,
    )
