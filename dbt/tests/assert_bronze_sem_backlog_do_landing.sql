-- Did Bronze absorb everything the sink delivered?
--
-- WHY THIS IS NOT A FRESHNESS TEST
--
-- `dbt/models/config/sources.yml` already declares freshness (warn 5min, error
-- 15min) and NOTHING in this project runs `dbt source freshness` -- orphan
-- configuration since the day it was written. The temptation would be to wire
-- it up. I did not, for a concrete reason: a clock test cannot tell "the stack
-- is deliberately switched off" from "ingestion broke". In this project the
-- stack sits idle most of the time, so such a test would live permanently red,
-- and an alarm that lives red is one nobody reads -- which is precisely the
-- problem it was supposed to solve.
--
-- This test asks the question that HAS an objective answer with the stack
-- stopped: does every row that reached the landing table exist in the Bronze
-- model? It catches the real failure mode -- the sink delivered and dbt did
-- not process, or the incremental watermark (`MAX(kafka_created_at)`) got
-- stuck -- without depending on there being traffic right now.
--
-- COMPARISON: DISTINCT non-null keys in the landing table against rows in the
-- model. Distinct because Snowpipe Streaming can redeliver; non-null because
-- the Kafka tombstone row arrives with everything null and the model discards
-- it on purpose (see the header of models/bronze/schema.yml).

{% set dominios = [
    ('PAYMENT_EVENTS',  'bronze_payment_events',  'EVENT_ID'),
    ('ORDERS',          'bronze_orders',          'ORDER_ID'),
    ('ORDER_ITEMS',     'bronze_order_items',     'ORDER_ITEM_ID'),
    ('DRIVER_SHIFTS',   'bronze_driver_shifts',   'SHIFT_ID'),
    ('SEARCH_EVENTS',   'bronze_search_events',   'SEARCH_ID'),
    ('RECOMMENDATIONS', 'bronze_recommendations', 'EVENT_ID'),
    ('USERS_MONGO',     'bronze_users_mongo',     'UUID'),
    ('USERS_MSSQL',     'bronze_users_mssql',     'UUID'),
    ('RESTAURANTS',     'bronze_restaurants',     'UUID'),
    ('DRIVERS',         'bronze_drivers',         'UUID')
] %}

WITH comparacao AS (

{% for tabela, modelo, chave in dominios %}
    SELECT
        '{{ modelo }}' AS dominio,
        (
            SELECT COUNT(DISTINCT {{ chave }})
            FROM {{ source('bronze_raw', tabela) }}
            WHERE {{ chave }} IS NOT NULL
        ) AS chaves_no_landing,
        (
            SELECT COUNT(*) FROM {{ ref(modelo) }}
        ) AS linhas_no_modelo
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

)

SELECT
    dominio,
    chaves_no_landing,
    linhas_no_modelo,
    chaves_no_landing - linhas_no_modelo AS backlog
FROM comparacao
WHERE chaves_no_landing <> linhas_no_modelo
