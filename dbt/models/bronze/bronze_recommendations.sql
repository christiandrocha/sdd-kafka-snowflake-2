{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: eventos de recomendacao de ML. Tipos observados nos dados em
-- 2026-08-10: recommendation_served, add_to_cart, click, view. (Ate essa
-- data este comentario dizia "view, click, purchase, dismiss" -- palpite que
-- nunca bateu com a origem e que contaminou o teste da Silver.)
-- A coluna de origem chama-se TIMESTAMP; aqui vem entre aspas e sai como
-- event_timestamp, para nao colidir com o tipo TIMESTAMP no SQL gerado.
-- Merge por event_id -- idempotente sobre reentrega do Snowpipe Streaming.
--
-- v4: as colunas chegam TIPADAS e em MAIUSCULO (schematizacao do
-- SnowflakeStreamingSinkConnector), entao nao ha mais extracao
-- RECORD_CONTENT:campo::TIPO. O RECORD_METADATA continua sendo escrito pelo
-- conector (chaves snowflake.metadata.* seguem no JAR 4.1.0) e continua
-- servindo de watermark incremental e de desempate na deduplicacao.

WITH source AS (
    SELECT
        EVENT_ID                           AS event_id,
        USER_ID                            AS user_id,
        PRODUCT_ID                         AS product_id,
        EVENT_TYPE                         AS event_type,
        "TIMESTAMP"                        AS event_timestamp,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'RECOMMENDATIONS') }}

    -- Descarta o tombstone do Kafka: `drop.tombstones=false` no Debezium faz
    -- todo DELETE emitir, depois da linha `__OP='d'`, uma mensagem de valor
    -- nulo que o sink materializa como linha inteiramente nula. Sem este
    -- filtro ela entra aqui, e como o MERGE por EVENT_ID nunca casa com
    -- chave nula, cada DELETE deixa uma linha-lixo permanente na Bronze.
    WHERE EVENT_ID IS NOT NULL

    {% if is_incremental() %}
      AND RECORD_METADATA:CreateTime::BIGINT > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
