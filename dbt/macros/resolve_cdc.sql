{% macro resolve_cdc(source_ref, model_name=none) %}
{#
    Resolve uma tabela Bronze CDC no estado atual da entidade, para a Silver.
    A estrategia vem de CONFIG.TABLE_METADATA via get_config_for().

    Estrategias:
        upsert -> deduplica por unique_key, mantem a versao mais recente e
                  descarta linha apagada na origem
        append -> sem deduplicacao, descarta so a linha apagada
        log    -> mantem tudo, inclusive delete, como registro historico

    Contrato de entrada: `source_ref` precisa apontar para um modelo Bronze
    deste projeto, que expoe as colunas de controle `op`, `source_ts_ms`,
    `kafka_offset` e `kafka_partition`. Ver dbt/models/bronze/bronze_*.sql.

    Uso num modelo Silver:
        {{ resolve_cdc(ref('bronze_orders')) }}
        {{ resolve_cdc(ref('bronze_orders'), model_name='silver_orders_enriched') }}

    PORTE DO PROJETO ANTERIOR (v6, 2026-08-10). Tres mudancas:

    1. Desempate deterministico. A versao anterior ordenava so por
       `source_ts_ms DESC`. Esse campo vem do Debezium em MILISSEGUNDOS: duas
       mudancas na mesma linha dentro do mesmo milissegundo -- comum em
       carga em lote e em UPDATE em cascata -- empatam, e o ROW_NUMBER
       escolhe uma delas de forma nao deterministica. O mesmo `dbt run`
       rodado duas vezes podia produzir Silver diferente. Agora o desempate
       segue por `kafka_offset DESC`, que e monotonico por particao, igual ao
       que os proprios modelos Bronze ja fazem na deduplicacao deles.

    2. Filtro de delete a prova de NULL, MAIS descarte de tombstone. Era
       `op != 'd'`. Em SQL, `NULL != 'd'` e NULL, nao TRUE -- entao qualquer
       linha com `op` nulo era descartada em silencio, apesar de nao ser um
       delete. Trocado por `IS DISTINCT FROM`, que trata NULL como valor.

       Mas so essa troca abre um buraco, medido em 2026-08-10: o conector
       roda com `drop.tombstones=false`, entao todo DELETE produz DUAS
       mensagens -- a linha reescrita com `__OP='d'` e, logo depois, um
       tombstone (valor nulo) que o sink materializa como uma linha de
       colunas todas nulas, unique_key inclusive. Com `op != 'd'` esse lixo
       sumia por acidente (NULL != 'd' e NULL); com `IS DISTINCT FROM` ele
       passava a vazar para a Silver. Dai o `{{ id_col }} IS NOT NULL`: uma
       linha sem chave de negocio nao e um registro CDC, e um artefato do
       protocolo de compactacao do Kafka. O descarte agora e explicito e
       intencional, nao um efeito colateral de semantica de NULL.

    3. A coluna auxiliar de ranking sai do resultado via EXCLUDE, como antes,
       mas as colunas de controle CDC (`op`, `source_ts_ms`, `kafka_*`)
       PERMANECEM na Silver de proposito: sao a linhagem que liga a linha ao
       evento Kafka que a produziu, e o `gold_payment_lifecycle` do projeto
       anterior dependia delas.

    NAO CONFUNDIR com a deduplicacao dos modelos Bronze. La ela e por lote de
    ingestao (idempotencia sobre reentrega do Snowpipe Streaming); aqui e
    sobre o historico inteiro da entidade (colapsar N versoes em 1 estado).
#}

{% set calling_model = model_name or this.name %}
{% set cfg = get_config_for(calling_model) %}
{% set strategy = cfg.get('cdc_strategy', 'upsert') %}
{% set id_col   = cfg.get('unique_key', 'id') %}

{% if strategy == 'upsert' %}

    {# Entidade: uma linha por chave, o estado mais recente nao apagado. #}
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ id_col }}
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _cdc_row_num
        FROM {{ source_ref }}
    )
    SELECT * EXCLUDE (_cdc_row_num)
    FROM ranked
    WHERE _cdc_row_num = 1
      AND op IS DISTINCT FROM 'd'
      AND {{ id_col }} IS NOT NULL

{% elif strategy == 'append' %}

    {# Fato: toda linha vale, sem deduplicar; so delete e tombstone saem. #}
    SELECT *
    FROM {{ source_ref }}
    WHERE op IS DISTINCT FROM 'd'
      AND {{ id_col }} IS NOT NULL

{% elif strategy == 'log' %}

    {# Log/auditoria: nada e descartado, delete inclusive -- so o tombstone,
       que nao carrega informacao nenhuma (nem sequer qual chave morreu). #}
    SELECT *
    FROM {{ source_ref }}
    WHERE {{ id_col }} IS NOT NULL

{% else %}

    {{ exceptions.raise_compiler_error(
        "cdc_strategy '" ~ strategy ~ "' desconhecida para o modelo "
        ~ calling_model ~ ". Valores validos: upsert | append | log. "
        ~ "Confira CONFIG.TABLE_METADATA ou rode scripts/sync_metadata.py."
    ) }}

{% endif %}

{% endmacro %}
