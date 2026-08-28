{% macro get_table_config() %}
{#
    Reads CONFIG.TABLE_METADATA and returns a dict keyed by table_name.

    Return shape:
    {
        'orders': {
            'table_type':   'entity',
            'cdc_strategy': 'upsert',
            'unique_key':   'order_id',
            'active':       true
        },
        ...
    }

    PORTED FROM THE PREVIOUS PROJECT (v6, 2026-08-10). Two changes:

    1. The static fallback dropped from 20 domains to 10. The 10 Tier 2 ones
       (payments, gps_events, order_status, routes, receipts, support_tickets,
       products, menu_sections, ratings, inventory) left the pipeline entirely
       under DEFINE_MIGRACAO_INGESTAO_V4 -- there is no source table in
       Postgres, no Kafka topic, no Snowflake Stream and no Bronze model for
       any of them. Keeping all 20 here would make the macro return config for
       a domain that does not exist, and the Silver model consulting it would
       compile against a source that is not there.

    2. The 10 below match the seed in scripts/bootstrap_config.sql one to one.
       If the two diverge, TABLE_METADATA wins at runtime -- the fallback only
       acts when there is no connection (parse) or when the domain has not been
       registered yet.

    WHY THE FALLBACK EXISTS: the Dagster entrypoint runs `dbt parse`, which is
    offline. With no connection, `execute` is False and `run_query` cannot run.
    Without the fallback, the model's `unique_key` would come out empty in the
    manifest and the incremental merge strategy would lose its key.

    Use in a model:
        {% set cfg = get_config_for(this.name) %}
        {% set unique_key = cfg.get('unique_key') %}
#}

{# Per-run cache. `context` is not a public dbt API and may not exist        #}
{# depending on the version, so access is always guarded by `is defined`:    #}
{# if it does not exist the macro simply re-queries -- 10 rows on a control  #}
{# table, with the warehouse already up from the `dbt run` itself.           #}
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

{# A domain not yet registered in TABLE_METADATA falls back to the static map. #}
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
    Config for a single domain. Strips the layer prefix to find the table name:
    silver_orders -> orders.

    Use:
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
        "WARNING: '" ~ clean_name ~ "' is in neither CONFIG.TABLE_METADATA nor the "
        ~ "static fallback of get_table_config. Using defaults: "
        ~ default_config | tojson ~ ". If the domain is new, run "
        ~ "scripts/sync_metadata.py; if it is a derived model (e.g. "
        ~ "silver_orders_enriched), pass model_name explicitly to "
        ~ "resolve_cdc pointing at the source domain.",
        info=true
    ) }}
{% endif %}

{{ return(all_config.get(clean_name, default_config)) }}

{% endmacro %}
