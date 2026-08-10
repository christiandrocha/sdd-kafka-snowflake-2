FROM python:3.11-slim

WORKDIR /opt/dagster

# Pins herdados do projeto anterior (combinação já testada em execução real).
# dagster-dbt/dagster-snowflake seguem o versionamento 0.22.* que corresponde
# ao core 1.6.* — mudar um sem o outro quebra a resolução de dependência.
# `dagster-postgres` é obrigatório aqui e não estava na lista do projeto
# anterior: lá o storage era SQLite. O dagster.yaml deste repo aponta para
# `dagster_postgres.DagsterPostgresStorage` (TD-08 / ADR-0018) e, sem o
# pacote, o webserver morre no boot com
# "Couldn't import module dagster_postgres".
RUN pip install --no-cache-dir \
    dagster==1.6.* \
    dagster-webserver==1.6.* \
    dagster-postgres==0.22.* \
    dagster-dbt==0.22.* \
    dagster-snowflake==0.22.* \
    dbt-snowflake==1.7.* \
    dbt-core==1.7.* \
    snowflake-connector-python==3.6.* \
    requests==2.31.* \
    cryptography==41.*

COPY dagster/dagster.yaml     /opt/dagster/dagster_home/dagster.yaml
COPY dagster/workspace.yaml   /opt/dagster/app/workspace.yaml
COPY dagster/pipeline         /opt/dagster/app/pipeline
COPY dagster/entrypoint.sh    /opt/dagster/entrypoint.sh
COPY dbt                      /opt/dagster/dbt

RUN chmod +x /opt/dagster/entrypoint.sh

ENTRYPOINT ["/opt/dagster/entrypoint.sh"]
