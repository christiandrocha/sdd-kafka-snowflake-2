{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'uuid',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: entregadores. driver_id e o driver_key usado em orders e driver_shifts.
-- Merge por uuid -- idempotente sobre reentrega do Snowpipe Streaming.
--
-- v4: as colunas chegam TIPADAS e em MAIUSCULO (schematizacao do
-- SnowflakeStreamingSinkConnector), entao nao ha mais extracao
-- RECORD_CONTENT:campo::TIPO. O RECORD_METADATA continua sendo escrito pelo
-- conector (chaves snowflake.metadata.* seguem no JAR 4.1.0) e continua
-- servindo de watermark incremental e de desempate na deduplicacao.

WITH source AS (
    SELECT
        UUID                               AS uuid,
        DRIVER_ID                          AS driver_id,
        FIRST_NAME                         AS first_name,
        LAST_NAME                          AS last_name,
        PHONE_NUMBER                       AS phone_number,
        CITY                               AS city,
        COUNTRY                            AS country,
        DATE_BIRTH                         AS date_birth,
        LICENSE_NUMBER                     AS license_number,
        VEHICLE_TYPE                       AS vehicle_type,
        VEHICLE_MAKE                       AS vehicle_make,
        VEHICLE_MODEL                      AS vehicle_model,
        VEHICLE_YEAR                       AS vehicle_year,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'DRIVERS') }}

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
                PARTITION BY uuid
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
