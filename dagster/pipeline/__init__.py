"""
Code location do Dagster — carregado por workspace.yaml
(load_from: python_package: pipeline, working_directory: /opt/dagster/app).
"""

from dagster import Definitions

from .assets import CDC_DBT_ASSETS, log_processing_results
from .jobs import cdc_pipeline_job, sync_metadata_job
from .resources import dbt_resource, snowflake_resource
from .sensors import bronze_new_data_sensor, registry_new_subject_sensor

# CDC_DBT_ASSETS está vazio enquanto o projeto dbt não tiver models
# (ver assets.py). Assim que o primeiro model existir, o asset do dbt entra
# aqui sozinho — não há nada a mudar neste arquivo.
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
