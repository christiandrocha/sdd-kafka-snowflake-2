{% macro get_table_config() %}
{#
    Le CONFIG.TABLE_METADATA e devolve um dict indexado por table_name.

    Formato de retorno:
    {
        'orders': {
            'table_type':   'entity',
            'cdc_strategy': 'upsert',
            'unique_key':   'order_id',
            'active':       true
        },
        ...
    }

    PORTE DO PROJETO ANTERIOR (v6, 2026-08-10). Duas mudancas:

    1. O fallback estatico caiu de 20 para 10 dominios. Os 10 Tier 2
       (payments, gps_events, order_status, routes, receipts,
       support_tickets, products, menu_sections, ratings, inventory) sairam
       do pipeline inteiro por DEFINE_MIGRACAO_INGESTAO_V4 -- nao ha tabela
       no Postgres fonte, topico no Kafka, Stream no Snowflake nem modelo
       Bronze para nenhum deles. Manter os 20 aqui faria a macro devolver
       config para dominio inexistente, e o modelo Silver que a consultasse
       compilaria contra uma fonte que nao existe.

    2. Os 10 abaixo batem 1:1 com o seed de scripts/bootstrap_config.sql.
       Se os dois divergirem, TABLE_METADATA vence em tempo de execucao --
       o fallback so age quando nao ha conexao (parse) ou quando o dominio
       ainda nao foi registrado.

    POR QUE O FALLBACK EXISTE: o entrypoint do Dagster roda `dbt parse`, que
    e offline. Sem conexao, `execute` e False e `run_query` nao pode rodar.
    Sem fallback, o `unique_key` do config() do modelo sairia vazio no
    manifest e a estrategia merge do incremental perderia a chave.

    Uso num modelo:
        {% set cfg = get_config_for(this.name) %}
        {% set unique_key = cfg.get('unique_key') %}
#}

{# Cache por execucao. `context` nao e uma API publica do dbt e pode nao    #}
{# existir dependendo da versao, entao o acesso e sempre guardado por       #}
{# `is defined`: se nao existir, a macro apenas reconsulta -- sao 10 linhas #}
{# numa tabela de controle, com o warehouse ja ligado pelo proprio dbt run. #}
{% if context is defined and '_table_config_cache' in context %}
    {{ return(context._table_config_cache) }}
{% endif %}

{% set static_fallback = {
    'payment_events':  {'table_type': 'fact',   'cdc_strategy': 'upsert', 'unique_key': 'event_id',      'active': true},
    'search_events':   {'table_type': 'fact',   'cdc_strategy': 'upsert', 'unique_key': 'search_id',     'active': true},
    'recommendations': {'table_type': 'fact',   'cdc_strategy': 'upsert', 'unique_key': 'event_id',      'active': true},
    'orders':          {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'order_id',      'active': true},
    'driver_shifts':   {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'shift_id',      'active': true},
    'users_mongo':     {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',          'active': true},
    'users_mssql':     {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',          'active': true},
    'restaurants':     {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',          'active': true},
    'drivers':         {'table_type': 'entity', 'cdc_strategy': 'upsert', 'unique_key': 'uuid',          'active': true},
    'order_items':     {'table_type': 'fact',   'cdc_strategy': 'upsert', 'unique_key': 'order_item_id', 'active': true}
} %}

{% if not execute %}
    {{ return(static_fallback) }}
{% endif %}

{% set query %}
    SELECT
        table_name,
        table_type,
        cdc_strategy,
        unique_key,
        active
    FROM {{ target.database }}.CONFIG.TABLE_METADATA
    WHERE active = TRUE
{% endset %}

{% set config_dict = {} %}
{% set results = run_query(query) %}
{% for row in results %}
    {% set _ = config_dict.update({
        row[0]: {
            'table_type':   row[1],
            'cdc_strategy': row[2],
            'unique_key':   row[3],
            'active':       row[4]
        }
    }) %}
{% endfor %}

{# Dominio ainda nao registrado em TABLE_METADATA cai no fallback. #}
{% for k, v in static_fallback.items() %}
    {% if k not in config_dict %}
        {% set _ = config_dict.update({k: v}) %}
    {% endif %}
{% endfor %}

{% if context is defined %}
    {% set _ = context.update({'_table_config_cache': config_dict}) %}
{% endif %}

{{ return(config_dict) }}

{% endmacro %}


{% macro get_config_for(model_name) %}
{#
    Config de um dominio so. Tira o prefixo de camada para achar o nome da
    tabela: silver_orders -> orders.

    Uso:
        {% set cfg = get_config_for('silver_orders') %}
        {% set strategy = cfg.get('cdc_strategy') %}
#}

{% set all_config = get_table_config() %}
{% set clean_name = model_name
    | replace('bronze_', '')
    | replace('silver_', '')
    | replace('gold_', '') %}

{% set default_config = {
    'table_type':   'entity',
    'cdc_strategy': 'upsert',
    'unique_key':   'id',
    'active':       true
} %}

{% if clean_name not in all_config %}
    {{ log(
        "AVISO: '" ~ clean_name ~ "' nao esta em CONFIG.TABLE_METADATA nem no "
        ~ "fallback estatico de get_table_config. Usando defaults: "
        ~ default_config | tojson ~ ". Se o dominio e novo, rode "
        ~ "scripts/sync_metadata.py; se e um modelo derivado (ex: "
        ~ "silver_orders_enriched), passe model_name explicitamente para "
        ~ "resolve_cdc apontando o dominio de origem.",
        info=true
    ) }}
{% endif %}

{{ return(all_config.get(clean_name, default_config)) }}

{% endmacro %}
