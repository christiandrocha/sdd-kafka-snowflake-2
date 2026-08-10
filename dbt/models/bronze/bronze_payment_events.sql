{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: eventos do ciclo de vida do pagamento.
-- Merge por event_id -- idempotente sobre reentrega do Snowpipe Streaming.
-- Ciclo real: created -> authorized -> captured -> succeeded -> settled -> closed
--                                              -> refunded -> closed
--
-- v4: as colunas escalares chegam tipadas. O campo `event` NAO -- ele e JSONB
-- no Postgres e o Debezium o serializa como string (io.debezium.data.Json),
-- entao a schematizacao cria VARCHAR e o PARSE_JSON continua necessario.
--
-- O timestamp interno do evento chega como int OU float em notacao
-- cientifica, ambos epoch ms. O CAST via FLOAT cobre as duas formas.

WITH source AS (
    SELECT
        EVENT_ID                           AS event_id,
        PAYMENT_ID                         AS payment_id,
        PARSE_JSON(EVENT):event_name::VARCHAR AS event_name,

        CAST(
            PARSE_JSON(EVENT):timestamp::FLOAT AS BIGINT
        )                                  AS event_timestamp_ms,

        TO_TIMESTAMP_NTZ(
            CAST(PARSE_JSON(EVENT):timestamp::FLOAT AS BIGINT) / 1000
        )                                  AS event_timestamp,

        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'PAYMENT_EVENTS') }}

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
