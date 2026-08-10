{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'search_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: buscas do usuario. Sem dt_current_timestamp na origem.
-- Mesma questao do TIMESTAMP citada em bronze_recommendations.
-- Merge por search_id -- idempotente sobre reentrega do Snowpipe Streaming.
--
-- v4: as colunas chegam TIPADAS e em MAIUSCULO (schematizacao do
-- SnowflakeStreamingSinkConnector), entao nao ha mais extracao
-- RECORD_CONTENT:campo::TIPO. O RECORD_METADATA continua sendo escrito pelo
-- conector (chaves snowflake.metadata.* seguem no JAR 4.1.0) e continua
-- servindo de watermark incremental e de desempate na deduplicacao.

WITH source AS (
    SELECT
        SEARCH_ID                          AS search_id,
        USER_ID                            AS user_id,
        QUERY_TEXT                         AS query_text,
        FILTERS                            AS filters,
        RESULT_COUNT                       AS result_count,
        CLICKED_PRODUCT_ID                 AS clicked_product_id,
        "TIMESTAMP"                        AS search_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'SEARCH_EVENTS') }}

    {% if is_incremental() %}
    WHERE RECORD_METADATA:CreateTime::BIGINT > (
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
                PARTITION BY search_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
