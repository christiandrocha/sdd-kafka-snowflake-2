{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'order_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: tabela hub. Liga os dominios via chaves de negocio heterogeneas:
-- user_key (CPF), restaurant_key (CNPJ), driver_key (string), payment_key e
-- rating_key (UUID). rating_key aponta para dominio Tier 2 removido.
-- Merge por order_id -- idempotente sobre reentrega do Snowpipe Streaming.
--
-- v4: as colunas chegam TIPADAS e em MAIUSCULO (schematizacao do
-- SnowflakeStreamingSinkConnector), entao nao ha mais extracao
-- RECORD_CONTENT:campo::TIPO. O RECORD_METADATA continua sendo escrito pelo
-- conector (chaves snowflake.metadata.* seguem no JAR 4.1.0) e continua
-- servindo de watermark incremental e de desempate na deduplicacao.

WITH source AS (
    SELECT
        ORDER_ID                           AS order_id,
        ORDER_DATE                         AS order_date,
        TOTAL_AMOUNT                       AS total_amount,
        USER_KEY                           AS user_key,
        RESTAURANT_KEY                     AS restaurant_key,
        DRIVER_KEY                         AS driver_key,
        PAYMENT_KEY                        AS payment_key,
        RATING_KEY                         AS rating_key,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'ORDERS') }}

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
                PARTITION BY order_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
