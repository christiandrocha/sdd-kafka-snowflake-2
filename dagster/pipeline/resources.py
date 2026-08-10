"""
resources.py — recursos compartilhados do code location.

Portado do projeto anterior sem mudança de comportamento. O reexport de
`SnowflakeResource` aqui é load-bearing: `sensors.py` faz
`from .resources import SnowflakeResource` para anotar o parâmetro dos dois
sensores, e é este import que amarra o recurso `snowflake` definido em
`__init__.py` à assinatura da função.

Identidade: as credenciais vêm do `.env` (env_file no docker-compose), que é
a identidade do STACK — o `DAGSTER_SERVICE_USER`, dono de `keys/dagster_key.p8`.
As identidades do Cursor e do data-agents NÃO passam por aqui; elas vivem em
conexões nomeadas no `~/.snowflake/config.toml` do host.
"""

import os
from pathlib import Path

from dagster import RetryPolicy, Backoff
from dagster_dbt import DbtCliResource
from dagster_snowflake import SnowflakeResource

DBT_PROJECT_DIR = Path("/opt/dagster/dbt")
DBT_TARGET      = os.getenv("DBT_TARGET", "dev")

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    target=DBT_TARGET,
)

snowflake_resource = SnowflakeResource(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_path=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    role=os.environ.get("SNOWFLAKE_ROLE", "CDC_ROLE"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CDC_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "CDC_POC"),
)

# Política de retry para falhas transitórias de rede/Snowflake.
SNOWFLAKE_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
)

__all__ = [
    "dbt_resource",
    "snowflake_resource",
    "SnowflakeResource",
    "DBT_PROJECT_DIR",
    "DBT_TARGET",
    "SNOWFLAKE_RETRY_POLICY",
]
