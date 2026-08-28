{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'order_item_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: order line items. Largest volume in the project.
-- Under v2.1.2 it had its own connector because of the client-side buffer; in
-- v4 that buffer no longer exists (buffer.count.records absent from the JAR)
-- and the domain went back to the single connector.
-- Merge on order_item_id -- idempotent over Snowpipe Streaming redelivery.
--
-- v4: columns arrive TYPED and UPPERCASE (schematization by the
-- SnowflakeStreamingSinkConnector), so there is no more
-- RECORD_CONTENT:field::TYPE extraction. RECORD_METADATA is still written by
-- the connector (the snowflake.metadata.* keys remain in JAR 4.1.0) and still
-- serves as the incremental watermark and the deduplication tie-break.

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

    -- Discards the Kafka tombstone: `drop.tombstones=false` in Debezium makes
    -- every DELETE emit, right after the `__OP='d'` row, a null-valued message
    -- that the sink materializes as an entirely null row. Without this filter
    -- it lands here, and since the MERGE on ORDER_ITEM_ID never matches a null key,
    -- every DELETE leaves a permanent junk row in Bronze.
    WHERE ORDER_ITEM_ID IS NOT NULL

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
                PARTITION BY order_item_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
