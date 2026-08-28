"""
resources.py -- shared resources of the code location.

Ported from the previous project with no behaviour change. The re-export of
`SnowflakeResource` here is load-bearing: `sensors.py` does
`from .resources import SnowflakeResource` to annotate the parameter of both
sensors, and it is this import that binds the `snowflake` resource defined in
`__init__.py` to the function signature.

Identity: credentials come from `.env` (env_file in docker-compose), which is
the STACK's identity -- `DAGSTER_SERVICE_USER`, owner of `keys/dagster_key.p8`.
The Cursor and data-agents identities do NOT pass through here; they live in
named connections in the host's `~/.snowflake/config.toml`.
"""

import os
from pathlib import Path

from dagster import Backoff, RetryPolicy
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

# Retry policy for transient network/Snowflake failures.
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
