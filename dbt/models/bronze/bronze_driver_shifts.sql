{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'shift_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: desempenho por turno do entregador (ganhos, distancia, pedidos).
-- Merge por shift_id -- idempotente sobre reentrega do Snowpipe Streaming.
--
-- v4: as colunas chegam TIPADAS e em MAIUSCULO (schematizacao do
-- SnowflakeStreamingSinkConnector), entao nao ha mais extracao
-- RECORD_CONTENT:campo::TIPO. O RECORD_METADATA continua sendo escrito pelo
-- conector (chaves snowflake.metadata.* seguem no JAR 4.1.0) e continua
-- servindo de watermark incremental e de desempate na deduplicacao.

WITH source AS (
    SELECT
        SHIFT_ID                           AS shift_id,
        DRIVER_ID                          AS driver_id,
        CITY                               AS city,
        REGION                             AS region,
        SHIFT_TYPE                         AS shift_type,
        LOGIN_METHOD                       AS login_method,
        DEVICE_OS                          AS device_os,
        START_TIME                         AS start_time,
        END_TIME                           AS end_time,
        SHIFT_DURATION_MIN                 AS shift_duration_min,
        NUM_ORDERS                         AS num_orders,
        DISTANCE_COVERED_KM                AS distance_covered_km,
        EARNINGS_BRL                       AS earnings_brl,
        SHIFT_RATING                       AS shift_rating,
        ISSUES_REPORTED                    AS issues_reported,
        AVAILABLE                          AS available,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'DRIVER_SHIFTS') }}

    -- Descarta o tombstone do Kafka: `drop.tombstones=false` no Debezium faz
    -- todo DELETE emitir, depois da linha `__OP='d'`, uma mensagem de valor
    -- nulo que o sink materializa como linha inteiramente nula. Sem este
    -- filtro ela entra aqui, e como o MERGE por SHIFT_ID nunca casa com
    -- chave nula, cada DELETE deixa uma linha-lixo permanente na Bronze.
    WHERE SHIFT_ID IS NOT NULL

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
                PARTITION BY shift_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
