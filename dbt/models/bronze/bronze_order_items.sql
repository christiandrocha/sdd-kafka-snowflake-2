{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'order_item_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: itens de linha do pedido. Maior volume do projeto.
-- No v2.1.2 tinha conector proprio por causa do buffer client-side; no v4
-- esse buffer nao existe mais (buffer.count.records ausente do JAR) e o
-- dominio voltou para o conector unico.
-- Merge por order_item_id -- idempotente sobre reentrega do Snowpipe Streaming.
--
-- v4: as colunas chegam TIPADAS e em MAIUSCULO (schematizacao do
-- SnowflakeStreamingSinkConnector), entao nao ha mais extracao
-- RECORD_CONTENT:campo::TIPO. O RECORD_METADATA continua sendo escrito pelo
-- conector (chaves snowflake.metadata.* seguem no JAR 4.1.0) e continua
-- servindo de watermark incremental e de desempate na deduplicacao.

WITH source AS (
    SELECT
        ORDER_ITEM_ID                      AS order_item_id,
        ORDER_ID                           AS order_id,
        PRODUCT_ID                         AS product_id,
        RESTAURANT_ID                      AS restaurant_id,
        PRODUCT_NAME                       AS product_name,
        PRODUCT_TYPE                       AS product_type,
        CUISINE_TYPE                       AS cuisine_type,
        UNIT_PRICE                         AS unit_price,
        QUANTITY                           AS quantity,
        SUBTOTAL                           AS subtotal,
        DISCOUNT_APPLIED                   AS discount_applied,
        MODIFIERS                          AS modifiers,
        IS_COMBO                           AS is_combo,
        IS_VEGETARIAN                      AS is_vegetarian,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'ORDER_ITEMS') }}

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
                PARTITION BY order_item_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
