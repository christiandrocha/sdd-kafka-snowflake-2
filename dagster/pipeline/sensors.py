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
   leve em vez de 20, e o Snowflake só grava linhas ali quando
   SYSTEM$STREAM_HAS_DATA já confirmou dado real na tabela Bronze, não uma
   suposição.

   CORREÇÃO (2026-08-10, medida contra a conta real): a versão anterior
   deste docstring afirmava que o intervalo podia continuar em 60s "porque
   a query em si é trivial". O peso da query não é a variável relevante.
   QUALQUER query numa tabela real resume o warehouse, e o Snowflake cobra
   um mínimo de 60 segundos por resume. Com CDC_WH em AUTO_SUSPEND=60 e o
   sensor consultando a cada 60s, o faturamento vira contínuo: ~1.440
   resumes/dia x 60s = 24h de X-Small = ~24 créditos/dia — exatamente o
   número que motivou esta feature. O gate nativo (Streams+Tasks) não é
   desfeito no lado do Snowflake, mas era desfeito pelo sensor do lado de
   fora.

   Por isso o sensor agora usa o MESMO gate de custo zero do
   registry_new_subject_sensor: consulta o Prometheus antes, e só abre
   conexão Snowflake se houve mensagem nova no Kafka desde o último ciclo
   (ou se o Prometheus estiver fora — fail-open). Em repouso o sensor não
   toca o warehouse nenhuma vez, e o custo fica onde deveria: nas Tasks,
   que só disparam com dado de verdade.

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

STATUS (2026-08-10): validado contra a conta Snowflake real, o Kafka e o
Prometheus do stack local. A validação encontrou dois defeitos que a análise
estática não pegaria — nome de métrica inexistente no gate do Prometheus e
o custo do próprio sensor — ambos corrigidos e anotados no ponto do código.
"""

import json
import os
from datetime import datetime, timezone

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

# CORREÇÃO (2026-08-10): a constante anterior era
# `kafka_server_brokertopicmetrics_messagesin_total`, com sufixo `_total`.
# Essa métrica NÃO existe neste stack — o jmx-exporter publica
# `kafka_server_brokertopicmetrics_messagesin` (sem sufixo). A query retornava
# lista vazia, o gate caía no fail-open de "sem série nenhuma" e o
# registry_new_subject_sensor tocava o Snowflake em TODOS os ciclos desde
# sempre. Verificado contra /api/v1/label/__name__/values do Prometheus.
#
# O filtro por tópico também é necessário: das 4 séries publicadas em repouso,
# as 4 são internas (__consumer_offsets, _schemas, connect_configs,
# connect_statuses). connect_statuses recebe heartbeat do Kafka Connect, então
# sem filtro o gate leria trânsito de infraestrutura como dado CDC novo.
CDC_TOPIC_PATTERN = os.getenv("CDC_TOPIC_PATTERN", "pg[.]public[.].*")
KAFKA_MESSAGES_METRIC = (
    f'kafka_server_brokertopicmetrics_messagesin{{topic=~"{CDC_TOPIC_PATTERN}"}}'
)

# Quantos ciclos o bronze_new_data_sensor segue checando o Snowflake depois da
# última atividade vista no Kafka. Existe porque as duas pontas são assíncronas:
# a mensagem chega no Kafka num ciclo, mas a Task nativa só grava PENDING_RUNS
# até 1 minuto depois. Sem essa margem, a última linha de uma rajada ficaria
# sem consumo até a próxima mensagem — que num período parado pode não vir.
HOT_CYCLES_AFTER_ACTIVITY = 3


# ── Gate de custo zero: Prometheus antes de qualquer conexão Snowflake ────

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


def _kafka_had_activity(context, last_totals: dict, initialized: bool) -> tuple:
    """
    (houve_atividade, totais_atuais) — sem tocar o Snowflake.

    Fail-open em duas situações, ambas onde perder um disparo é pior que
    gastar um resume do warehouse:

    1. Prometheus inacessível.
    2. Primeiro tick com este cursor (`initialized=False`). Pode haver linha
       em PENDING_RUNS gravada antes de o sensor existir, ou antes de um
       restart do broker que zerou os contadores.

    Note que a condição do item 2 é "o cursor está vazio", NÃO "a query veio
    vazia". Nenhuma série `pg.public.*` existe até a primeira mensagem CDC
    do broker atual, e tratar isso como fail-open manteria o sensor batendo
    no Snowflake para sempre — que foi exatamente o defeito medido em
    2026-08-10.

    Reset de contador (restart do broker) conta como atividade: a série some
    ou volta menor, e a comparação é `!=`, não `>`.
    """
    try:
        current = _get_kafka_message_totals()
    except Exception as e:
        context.log.warning(f"Prometheus indisponível ({e}) — fail-open, checa o Snowflake.")
        # Preserva os totais antigos: quando o Prometheus voltar, o delta é
        # medido contra o último valor realmente observado.
        return True, last_totals

    if not initialized:
        return True, current

    changed = any(
        current.get(t, 0) != last_totals.get(t, 0)
        for t in set(current) | set(last_totals)
    )
    return changed, current


# ── Bronze: gate nativo (Streams + Tasks) já fez o trabalho pesado ────────

@sensor(
    job=cdc_pipeline_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
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
    Removida — nenhuma linha de PENDING_RUNS é mais filtrada por tempo.

    O `context.cursor` voltou a ser usado (2026-08-10), mas para outra
    coisa: guarda os contadores do Kafka do gate de custo e o contador de
    ciclos quentes. Ele NÃO filtra linha nenhuma de PENDING_RUNS — quando o
    sensor decide consultar, lê todas as linhas com `consumed = FALSE`, sem
    corte de tempo. O bug de watermark não volta por aqui.
    """
    state = json.loads(context.cursor or "{}")
    last_totals = state.get("totals", {})
    hot = state.get("hot", 0)
    initialized = state.get("initialized", False)

    # Fase 1 — custo zero: nenhuma conexão Snowflake, nenhum resume do CDC_WH.
    activity, current_totals = _kafka_had_activity(context, last_totals, initialized)

    if activity:
        hot = HOT_CYCLES_AFTER_ACTIVITY
    elif hot > 0:
        hot -= 1
    else:
        return SensorResult(
            skip_reason=SkipReason("Sem atividade no Kafka (via Prometheus) — Snowflake não consultado."),
            cursor=json.dumps({"totals": current_totals, "hot": 0, "initialized": True}),
        )

    new_cursor = json.dumps({"totals": current_totals, "hot": hot, "initialized": True})

    # Fase 2 — só agora toca o Snowflake.
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
                skip_reason=SkipReason("CONFIG.PENDING_RUNS sem entradas novas."),
                cursor=new_cursor,
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
        cursor=new_cursor,
    )


# ── Registry: mesmo gate, aplicado antes de tocar Snowflake ──────────────
# (_get_kafka_message_totals vive agora no bloco compartilhado lá em cima,
#  porque os dois sensores usam o mesmo gate.)

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
    # Cursor migrado em 2026-08-10 do formato antigo (dict cru de totais) para
    # {"totals": ..., "initialized": ...}. Cursor no formato velho cai em
    # `initialized=False` e checa uma vez — self-healing, sem passo manual.
    state = json.loads(context.cursor or "{}")
    last_totals = state.get("totals", {})
    initialized = state.get("initialized", False)

    # Fase 1 — custo zero: houve atividade em algum tópico CDC desde o último
    # ciclo? Mesmo gate do bronze_new_data_sensor, mesma função.
    has_activity, current_totals = _kafka_had_activity(context, last_totals, initialized)
    new_cursor = json.dumps({"totals": current_totals, "initialized": True})

    if not has_activity:
        return SensorResult(
            skip_reason=SkipReason("Sem atividade no Kafka (via Prometheus)."),
            cursor=new_cursor,
        )

    # Fase 2 — só agora toca o Snowflake.
    try:
        subjects = _get_registered_subjects()
    except Exception as e:
        return SensorResult(skip_reason=SkipReason(f"Schema Registry indisponível: {e}"))

    if not subjects:
        return SensorResult(
            skip_reason=SkipReason("Nenhum subject registrado."),
            cursor=new_cursor,
        )

    subject_tables = {s.split("-")[0].upper() for s in subjects}

    with snowflake.get_connection() as conn:
        synced_tables = _get_synced_tables(conn)

    new_tables = subject_tables - synced_tables

    if not new_tables:
        return SensorResult(
            skip_reason=SkipReason("Todos os subjects já sincronizados em TABLE_METADATA."),
            cursor=new_cursor,
        )

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"registry-sync-{datetime.now(timezone.utc).isoformat()}",
                tags={"new_tables": ",".join(sorted(new_tables))},
            )
        ],
        cursor=new_cursor,
    )
