import json
import os
from datetime import UTC, datetime
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_snowflake import SnowflakeResource

from .resources import DBT_PROJECT_DIR

SF_DATABASE   = os.getenv("SNOWFLAKE_DATABASE", "CDC_POC")
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


def _manifest_has_models(path: Path) -> bool:
    """
    The @dbt_assets decorator resolves the 'fqn:*' selector at import time and
    raises EventCompilationError ("The selection criterion 'fqn:*' does not
    match any nodes") if the manifest has no models -- which brings down the
    entire code location, not just the dbt asset.

    While the dbt project is only a skeleton (the 10 Bronze models arrive with
    the MIGRACAO_INGESTAO_V4 build), this check keeps the code location
    loadable: sensors and jobs come up normally and the dbt assets appear on
    their own as soon as the first model exists. Nothing to configure later.
    """
    try:
        with open(path) as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    return any(k.startswith("model.") for k in manifest.get("nodes", {}))


# ── dbt assets (Bronze -> Silver -> Gold) ─────────────────────────────────────

# Empty list while there are no models; becomes [cdc_dbt_assets] once there are.
CDC_DBT_ASSETS: list = []

if _manifest_has_models(MANIFEST_PATH):

    @dbt_assets(manifest=MANIFEST_PATH)
    def cdc_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        """
        Runs all dbt models in dependency order: bronze → silver → gold.
        After completion, writes execution results to PROCESSING_LOG via
        the log_processing_results asset (downstream dependency).
        """
        dbt_invocation = dbt.cli(
            ["run", "--select", "bronze silver gold"],
            context=context,
        )
        yield from dbt_invocation.stream()

        # Store run results in context metadata for log_processing_results
        try:
            run_results_path = DBT_PROJECT_DIR / "target" / "run_results.json"
            if run_results_path.exists():
                context.add_output_metadata({
                    "run_results_path": str(run_results_path)
                })
        except Exception:
            pass

        yield from dbt.cli(
            ["test", "--select", "bronze silver gold"],
            context=context,
        ).stream()

    CDC_DBT_ASSETS.append(cdc_dbt_assets)


# ── Processing log asset ───────────────────────────────────────────────────────

@asset(
    deps=CDC_DBT_ASSETS,
    description=(
        "Reads dbt run_results.json after each pipeline execution and "
        "inserts one row per model into CONFIG.PROCESSING_LOG. "
        "Captures status, row counts, duration and errors."
    ),
    group_name="observability",
)
def log_processing_results(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> None:
    run_results_path = DBT_PROJECT_DIR / "target" / "run_results.json"

    if not run_results_path.exists():
        context.log.warning("run_results.json not found — skipping processing log.")
        return

    with open(run_results_path) as f:
        run_results = json.load(f)

    invocation_id = run_results.get("metadata", {}).get("invocation_id", "unknown")
    results       = run_results.get("results", [])

    rows_to_insert = []
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Build table row counts from Snowflake for rows_processed
    # (adapter_response is empty for the custom MERGE)
    row_counts: dict[str, int] = {}
    with snowflake.get_connection() as _conn:
        _cur = _conn.cursor()
        for result in results:
            uid = result.get("unique_id", "")
            if not uid.startswith("model."):
                continue
            mname = uid.split(".")[-1]
            if mname.startswith("bronze_"):
                schema = "BRONZE"
            elif mname.startswith("silver_"):
                schema = "SILVER"
            elif mname.startswith("gold_"):
                schema = "GOLD"
            else:
                continue
            try:
                _cur.execute(f"SELECT COUNT(*) FROM {SF_DATABASE}.{schema}.{mname.upper()}")
                row_counts[mname] = _cur.fetchone()[0]
            except Exception:
                row_counts[mname] = 0

    for result in results:
        node_name = result.get("unique_id", "")

        if not node_name.startswith("model."):
            continue

        model_name   = node_name.split(".")[-1]        # bronze_payment_events
        status       = result.get("status", "unknown")
        timing       = result.get("timing", [])

        # Determine layer from model name prefix
        if model_name.startswith("bronze_"):
            layer = "bronze"
        elif model_name.startswith("silver_"):
            layer = "silver"
        elif model_name.startswith("gold_"):
            layer = "gold"
        else:
            layer = "other"

        table_name = (
            model_name
            .replace("bronze_", "")
            .replace("silver_", "")
            .replace("gold_", "")
        )

        rows_processed = row_counts.get(model_name, 0)

        started_at  = None
        finished_at = None
        duration    = result.get("execution_time", 0)
        for t in timing:
            if t.get("name") == "execute":
                started_at  = t.get("started_at")
                finished_at = t.get("completed_at")

        error_message = None
        if status in ("error", "fail"):
            error_message = str(result.get("message", ""))[:2000]

        rows_to_insert.append((
            table_name, layer, model_name, invocation_id,
            context.run_id,
            "success" if status == "success" else "error",
            rows_processed,
            started_at, finished_at, round(duration, 3),
            error_message, "sensor",
            now,
        ))

    if not rows_to_insert:
        context.log.info("No model results to log.")
        return

    insert_sql = f"""
        INSERT INTO {SF_DATABASE}.CONFIG.PROCESSING_LOG (
            table_name, layer, dbt_model, dbt_invocation_id, run_id,
            status, rows_processed,
            started_at, finished_at, duration_seconds,
            error_message, triggered_by, logged_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s)
    """

    with snowflake.get_connection() as conn:
        conn.cursor().executemany(insert_sql, rows_to_insert)
        conn.commit()

    context.log.info(
        f"Logged {len(rows_to_insert)} model results to PROCESSING_LOG "
        f"(invocation: {invocation_id})."
    )
