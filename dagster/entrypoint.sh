#!/bin/bash
# Gera o manifest do dbt antes do Dagster subir — exigido pelo decorator
# @dbt_assets em pipeline/assets.py, que lê target/manifest.json em tempo de
# import do code location.
#
# O `|| echo` no compile é deliberado: se o Snowflake estiver inacessível no
# boot, o container ainda sobe e o manifest anterior (se houver) basta para
# montar o grafo de assets.
set -e

cd /opt/dagster/dbt
echo "[entrypoint] dbt deps..."
dbt deps --quiet

echo "[entrypoint] dbt parse (target: ${DBT_TARGET:-dev})..."
# `parse` gera o manifest.json SEM conectar no Snowflake — diferente de
# `compile`, que abre conexão. Com o projeto ainda sem models (os 10 Bronze
# chegam no build do MIGRACAO_INGESTAO_V4), `compile` falharia por conexão e
# deixaria o code location sem manifest nenhum para carregar.
dbt parse --target "${DBT_TARGET:-dev}" --quiet \
    || echo "[entrypoint] dbt parse falhou — usando manifest existente, se houver"

exec "$@"
