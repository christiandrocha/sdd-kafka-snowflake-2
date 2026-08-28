{{
    config(
        materialized         = 'incremental',
        schema               = 'GOLD',
        unique_key           = 'payment_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Gold: one row per payment, with the instant of each lifecycle stage pivoted
-- into a column. This is the project's reference model for ADDITIVE-
-- PARTITIONABLE aggregation: a payment's state depends only on the events of
-- that payment_id, so a new event forces recomputing ONE payment, not the
-- whole table.
--
-- HOW THE INCREMENTAL WORKS HERE. The `alvo` CTE finds WHICH payments changed;
-- it does not filter the rows that feed the aggregation. Once the affected
-- payment_ids are known, the `eventos` CTE brings the COMPLETE history of each
-- one -- otherwise a `closed` event arriving alone would produce a row with no
-- `criado_em`, and the MERGE would overwrite the good version with a mutilated
-- one. Verified live on 2026-08-11: `criado_em` and `autorizado_em` survived
-- the isolated arrival of a `closed`.
--
-- WHY THIS IS NOT A GLOBAL WATERMARK (fixed on 2026-08-11)
--
-- The previous version compared each event against `MAX(ultimo_evento_ms)` of
-- the ENTIRE table. That assumes a new event always arrives with a timestamp
-- greater than any event of any other payment -- an assumption CDC does not
-- guarantee. A single event whose timestamp predated the most recent event of
-- ANOTHER payment was enough for it to be silently ignored.
--
-- Reproduced before fixing, with real data: a `captured` for payment 55555555
-- with a timestamp between its own `created` and `closed` travelled through
-- Debezium, Kafka and the sink, landed in Bronze (`SUCCESS 1`) and in Silver
-- (`SUCCESS 1`), and Gold returned `SUCCESS 0`. Silver held 4 events and Gold
-- kept saying 3, with a null `capturado_em` -- and all 26 tests in the chain
-- passed. A silent failure, with the pipeline reporting success.
--
-- The comparison is now PER PAYMENT, and by count before timestamp:
-- `COUNT(*) <> total_eventos` catches any new event regardless of the order it
-- arrived in, which is the only formulation robust to out-of-order delivery.
-- The `MAX(...) <> ultimo_evento_ms` remains as a second guard.
--
-- COST. `alvo` now aggregates all of Silver on every run, instead of filtering
-- by a scalar. In this database that is thousands of rows and the cost is
-- irrelevant; Silver is rebuilt in full on every run anyway
-- (`materialized='table'`). If Silver grows until that GROUP BY hurts, the
-- answer is an INGESTION watermark (`dt_current_timestamp`) rather than event
-- time -- never a return to the global maximum.
--
-- MIND THE CARDINALITY OF THE CURRENT DATA (measured 2026-08-10): the 2,210
-- events are spread across only 8 distinct payment_ids, seven of them with
-- more than 300 events each. The model's structure is right, but on this data
-- it returns 8 rows and the durations carry no business meaning -- the
-- synthetic generator reused the identifiers. Worth checking before using any
-- number from here in a decision.
--
-- There is no join with `orders`: `orders.payment_key` has 410 distinct values
-- and ZERO intersection with `payment_events.payment_id`. The two do not
-- reference the same identifier space in this database.

WITH alvo AS (

{% if is_incremental() %}

    -- Compares the state of EACH payment in Silver against what Gold already
    -- recorded for it. A payment enters if it is new, if it gained an event,
    -- or if its most recent event changed. There is no global watermark here
    -- -- see the "WHY THIS IS NOT A GLOBAL WATERMARK" note in the header.
    SELECT e.payment_id
    FROM {{ ref('silver_payment_events') }} e
    LEFT JOIN {{ this }} t ON t.payment_id = e.payment_id
    WHERE e.payment_id IS NOT NULL
    GROUP BY e.payment_id, t.payment_id, t.total_eventos, t.ultimo_evento_ms
    HAVING t.payment_id IS NULL
        OR COUNT(*)                  <> t.total_eventos
        OR MAX(e.event_timestamp_ms) <> t.ultimo_evento_ms

{% else %}

    SELECT DISTINCT payment_id
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL

{% endif %}

),

eventos AS (

    SELECT e.*
    FROM {{ ref('silver_payment_events') }} e
    INNER JOIN alvo a ON a.payment_id = e.payment_id

)

SELECT
    payment_id,

    COUNT(*)                                                          AS total_eventos,
    COUNT(DISTINCT event_name)                                        AS etapas_distintas,
    MIN(event_timestamp)                                              AS primeiro_evento,
    MAX(event_timestamp)                                              AS ultimo_evento,
    MAX(event_timestamp_ms)                                           AS ultimo_evento_ms,

    MIN(CASE WHEN event_name = 'created'    THEN event_timestamp END) AS criado_em,
    MIN(CASE WHEN event_name = 'authorized' THEN event_timestamp END) AS autorizado_em,
    MIN(CASE WHEN event_name = 'captured'   THEN event_timestamp END) AS capturado_em,
    MIN(CASE WHEN event_name = 'succeeded'  THEN event_timestamp END) AS aprovado_em,
    MIN(CASE WHEN event_name = 'settled'    THEN event_timestamp END) AS liquidado_em,
    MIN(CASE WHEN event_name = 'closed'     THEN event_timestamp END) AS fechado_em,
    MIN(CASE WHEN event_name = 'refunded'   THEN event_timestamp END) AS reembolsado_em,

    COUNT_IF(event_name = 'refunded') > 0                             AS teve_reembolso,
    COUNT_IF(event_name = 'closed')   > 0                             AS foi_fechado,

    DATEDIFF(
        'second',
        MIN(CASE WHEN event_name = 'created' THEN event_timestamp END),
        MIN(CASE WHEN event_name = 'closed'  THEN event_timestamp END)
    )                                                                 AS segundos_ate_fechamento

FROM eventos
GROUP BY payment_id
