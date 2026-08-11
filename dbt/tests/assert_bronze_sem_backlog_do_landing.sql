-- A Bronze absorveu tudo que o sink entregou?
--
-- POR QUE NAO E UM TESTE DE FRESCOR
--
-- `dbt/models/config/sources.yml` ja declara freshness (warn 5min, error
-- 15min) e NADA neste projeto executa `dbt source freshness` -- configuracao
-- orfa desde que foi escrita. A tentacao seria ligar aquilo. Nao liguei, por
-- um motivo concreto: um teste de relogio nao distingue "a stack esta
-- desligada de proposito" de "a ingestao quebrou". Neste projeto a stack fica
-- parada a maior parte do tempo, entao um teste desses passaria a vida
-- vermelho, e alarme que vive vermelho ninguem le -- que e justamente o
-- problema que ele deveria resolver.
--
-- Este teste faz a pergunta que TEM resposta objetiva com a stack parada:
-- toda linha que chegou na tabela de landing existe no modelo Bronze? Ele
-- pega o modo de falha real -- sink entregou e o dbt nao processou, ou o
-- watermark incremental (`MAX(kafka_created_at)`) travou -- sem depender de
-- haver trafego agora.
--
-- COMPARACAO: chaves DISTINTAS nao nulas no landing contra linhas no modelo.
-- Distintas porque o Snowpipe Streaming pode reentregar; nao nulas porque a
-- linha de tombstone do Kafka chega com tudo nulo e o modelo a descarta de
-- proposito (ver o cabecalho de models/bronze/schema.yml).

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
