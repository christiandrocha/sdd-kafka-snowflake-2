"""
jobs.py -- the two jobs that the sensors in sensors.py trigger.

  cdc_pipeline_job   <- bronze_new_data_sensor      (new data in Bronze)
  sync_metadata_job  <- registry_new_subject_sensor (new subject in Registry)

Ported from the previous project with one path fix: there the script was
referenced as "/opt/dagster/app/../scripts/sync_metadata.py", which resolves to
/opt/dagster/scripts/sync_metadata.py by accidental relative path. Here it is
written directly, which is where docker-compose mounts ./scripts
(volumes: ./scripts:/opt/dagster/scripts).
"""

import os
import subprocess

from dagster import AssetSelection, define_asset_job, job, op

SYNC_METADATA_SCRIPT = "/opt/dagster/scripts/sync_metadata.py"


# ── Job 1: dbt pipeline (Bronze -> Silver -> Gold) ───────────────────────────

cdc_pipeline_job = define_asset_job(
    name="cdc_pipeline_job",
    selection=AssetSelection.all(),
    description="Roda todos os models dbt: bronze → silver → gold.",
)


# ── Job 2: sync_metadata (Schema Registry -> CONFIG.TABLE_METADATA) ──────────

@op(description="Roda sync_metadata.py para sincronizar Schema Registry → TABLE_METADATA")
def run_sync_metadata(context):
    result = subprocess.run(
        ["python", SYNC_METADATA_SCRIPT],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"sync_metadata.py falhou com código {result.returncode}")
    context.log.info("sync_metadata.py concluído com sucesso.")


@job(description="Disparado pelo registry_new_subject_sensor quando há subject novo.")
def sync_metadata_job():
    run_sync_metadata()
