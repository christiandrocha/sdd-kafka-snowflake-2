"""
Dagster code location -- loaded by workspace.yaml
(load_from: python_package: pipeline, working_directory: /opt/dagster/app).
"""

from dagster import Definitions

from .assets import CDC_DBT_ASSETS, log_processing_results
from .jobs import cdc_pipeline_job, sync_metadata_job
from .resources import dbt_resource, snowflake_resource
from .sensors import bronze_new_data_sensor, registry_new_subject_sensor

# CDC_DBT_ASSETS is empty while the dbt project has no models (see
# assets.py). As soon as the first model exists, the dbt asset enters here on
# its own -- there is nothing to change in this file.
defs = Definitions(
    assets=[*CDC_DBT_ASSETS, log_processing_results],
    resources={
        "dbt":       dbt_resource,
        "snowflake": snowflake_resource,
    },
    sensors=[
        bronze_new_data_sensor,
        registry_new_subject_sensor,
    ],
    jobs=[
        cdc_pipeline_job,
        sync_metadata_job,
    ],
)
