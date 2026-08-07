"""
sensors.py — v5 (pós-diagnóstico de consumo de créditos)

MUDANÇAS EM RELAÇÃO À VERSÃO ANTERIOR:

1. bronze_new_data_sensor
   Antes: a cada 60s, abria conexão Snowflake e rodava até 20
   `SELECT MAX(RECORD_METADATA:CreateTime)` — um por tabela Bronze — para
   decidir se disparava o dbt run. Como AUTO_SUSPEND do warehouse CDC_WH é
   60s, o warehouse nunca tinha uma janela real de inatividade e ficava
   praticamente sempre ligado (~24 créditos/dia em atividade zero).

   Agora: consulta só CONFIG.PENDING_RUNS (populada pelas Tasks nativas do
   Snowflake — ver scripts/streams_and_tasks.sql, ADR-0019). É 1 SELECT
   leve em vez de 20, e mais importante: o Snowflake só grava linhas ali
   quando SYSTEM$STREAM_HAS_DATA já confirmou dado real na tabela Bronze,
   não uma suposição. O intervalo do sensor pode continuar em 60s sem
   reintroduzir o problema, porque a query em si é trivial (SELECT em uma
   tabela de controle pequena, não MAX() em 20 tabelas de fato).

   Isso NÃO elimina o warehouse sendo tocado a cada 60s pelo sensor — ainda
   toca. O que elimina o desperdício é que as TASKS nativas (que rodam
   antes do sensor, na própria Snowflake) só ligam o warehouse quando há
   dado de verdade. O sensor Dagster, ao consultar CONFIG.PENDING_RUNS,
   ainda vai encontrar o warehouse já ligado (a task acabou de rodar) ou
   suspenso (nada aconteceu) — o custo real migrou quase todo para as
   Tasks, que só cobram quando disparam de fato.

2. registry_new_subject_sensor
   Antes: consultava CONFIG.TABLE_METADATA no Snowflake a cada 300s,
   incondicionalmente — contrariando a suposição inicial (documentada nesta
   mesma análise) de que esse sensor não tocava o Snowflake.

   Agora: primeiro consulta o Prometheus (custo zero em créditos Snowflake)
   para saber se algum tópico teve atividade desde o último ciclo. Só abre
   conexão Snowflake se houver sinal de atividade — fail-open se o
   Prometheus estiver inacessível (prioriza nunca perder um subject novo
   sobre nunca gastar crédito à toa, coerente com a decisão registrada em
   ADR-0019).

STATUS: não testado contra Snowflake/Kafka/Prometheus reais — não há
ambiente ativo disponível durante esta análise. Validar em ambiente de
teste antes de qualquer demo de cliente.
"""

import json
import os
from datetime import datetime, timezone

import requests
from dagster import sensor, SensorResult, SkipReason, RunRequest

from .jobs import cdc_pipeline_job, sync_metadata_job
from .resources import SnowflakeResource

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
KAFKA_MESSAGES_METRIC = "kafka_server_brokertopicmetrics_messagesin_total"


# ── Bronze: gate nativo (Streams + Tasks) já fez o trabalho pesado ────────

@sensor(job=cdc_pipeline_job, minimum_interval_seconds=60)
def bronze_new_data_sensor(context, snowflake: SnowflakeResource) -> SensorResult:
    """
    Consulta CONFIG.PENDING_RUNS — populada pelas Tasks nativas do
    Snowflake (scripts/streams_and_tasks.sql), que só rodam quando
    SYSTEM$STREAM_HAS_DATA confirma dado real numa tabela Bronze.

    CORREÇÃO (comparação com segunda opinião externa, 2026-08-04):
    a versão anterior filtrava por `detected_at > cursor` ALÉM de
    `consumed = FALSE`. Isso reintroduzia o mesmo bug de watermark global
    que motivou trocar o sensor original: se um domínio de alto volume
    grava um `detected_at` mais recente e avança o cursor, um domínio de
    baixo volume cuja Task só termina de rodar depois (gravando um
    `detected_at` mais antigo que o cursor já avançado) fica invisível
    pra sempre — `consumed` continua FALSE, mas `detected_at > cursor`
    nunca bate. `consumed = FALSE` sozinho já é suficiente como guarda de
    idempotência; a comparação de cursor era redundante E perigosa.
    Removida — não há mais cursor nenhum neste sensor.
    """
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
                skip_reason=SkipReason("CONFIG.PENDING_RUNS sem entradas novas.")
            )

        domains = sorted({r[0] for r in rows})
        newest_ts = max(r[1] for r in rows)

        # Marca como consumido pelo PRIMARY KEY implícito da linha, não por
        # um corte de tempo — evita marcar como consumida uma linha que
        # ainda não tinha sido lida (mesma classe de bug do cursor acima).
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
    )


# ── Registry: gate via Prometheus antes de tocar Snowflake ────────────────

def _get_kafka_message_totals() -> dict:
    """Custo zero em créditos Snowflake — só HTTP local ao Prometheus."""
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": KAFKA_MESSAGES_METRIC},
        timeout=5,
    )
    resp.raise_for_status()
    results = resp.json()["data"]["result"]
    return {r["metric"].get("topic", "unknown"): float(r["value"][1]) for r in results}


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


@sensor(job=sync_metadata_job, minimum_interval_seconds=60)
def registry_new_subject_sensor(context, snowflake: SnowflakeResource) -> SensorResult:
    last_totals = json.loads(context.cursor or "{}")

    # Fase 1 — custo zero: houve atividade em algum tópico desde o último ciclo?
    try:
        current_totals = _get_kafka_message_totals()
        prometheus_ok = True
    except Exception as e:
        context.log.warning(f"Prometheus indisponível ({e}) — fail-open, cai para checagem direta.")
        current_totals = {}
        prometheus_ok = False

    has_activity = (not prometheus_ok) or any(
        # fail-open também em reset de contador (restart do broker Kafka):
        # se o valor atual for MENOR que o último visto, trata como atividade
        # em vez de gerar um delta negativo silencioso.
        current_totals.get(t, 0) < last_totals.get(t, 0)
        or current_totals.get(t, 0) > last_totals.get(t, 0)
        for t in set(current_totals) | set(last_totals)
    ) if (current_totals or last_totals) else True  # primeira execução: checa

    if not has_activity:
        return SensorResult(
            skip_reason=SkipReason("Sem atividade no Kafka (via Prometheus)."),
            cursor=json.dumps(current_totals),
        )

    # Fase 2 — só agora toca o Snowflake.
    try:
        subjects = _get_registered_subjects()
    except Exception as e:
        return SensorResult(skip_reason=SkipReason(f"Schema Registry indisponível: {e}"))

    if not subjects:
        return SensorResult(
            skip_reason=SkipReason("Nenhum subject registrado."),
            cursor=json.dumps(current_totals),
        )

    subject_tables = {s.split("-")[0].upper() for s in subjects}

    with snowflake.get_connection() as conn:
        synced_tables = _get_synced_tables(conn)

    new_tables = subject_tables - synced_tables

    if not new_tables:
        return SensorResult(
            skip_reason=SkipReason("Todos os subjects já sincronizados em TABLE_METADATA."),
            cursor=json.dumps(current_totals),
        )

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"registry-sync-{datetime.now(timezone.utc).isoformat()}",
                tags={"new_tables": ",".join(sorted(new_tables))},
            )
        ],
        cursor=json.dumps(current_totals),
    )
