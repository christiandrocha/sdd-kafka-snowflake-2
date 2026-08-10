{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'uuid',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: usuarios (origem MongoDB). CPF e o user_key usado em orders.
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
        USER_ID                            AS user_id,
        CPF                                AS cpf,
        EMAIL                              AS email,
        PHONE_NUMBER                       AS phone_number,
        CITY                               AS city,
        COUNTRY                            AS country,
        DELIVERY_ADDRESS                   AS delivery_address,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'USERS_MONGO') }}

    -- Descarta o tombstone do Kafka: `drop.tombstones=false` no Debezium faz
    -- todo DELETE emitir, depois da linha `__OP='d'`, uma mensagem de valor
    -- nulo que o sink materializa como linha inteiramente nula. Sem este
    -- filtro ela entra aqui, e como o MERGE por UUID nunca casa com
    -- chave nula, cada DELETE deixa uma linha-lixo permanente na Bronze.
    WHERE UUID IS NOT NULL

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
                PARTITION BY uuid
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
