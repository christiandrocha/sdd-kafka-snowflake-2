{{
    config(
        materialized         = 'incremental',
        schema               = 'GOLD',
        unique_key           = 'event_name',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Gold: volume per payment lifecycle stage. One row per event_name.
--
-- WHY INCREMENTAL WITHOUT A WATERMARK. This is a GLOBAL RATIO aggregation: the
-- percentage of each stage depends on the total event count, so there is no
-- correct incremental slice -- filtering by new events would change the
-- denominator and produce a wrong percentage. What the incremental solves here
-- is something else: the table has 7 rows of fixed cardinality, and the MERGE
-- on event_name updates them in place, preserving each row's identity across
-- runs instead of dropping and recreating the table.
--
-- In short: a full scan on every run, by arithmetic necessity; MERGE for key
-- stability. Over 2,210 events that is irrelevant in cost. If the cardinality
-- of event_name ever explodes, the decision changes.

WITH base AS (

    SELECT *
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL

),

total AS (

    SELECT COUNT(*) AS eventos_total FROM base

)

SELECT
    b.event_name,

    COUNT(*)                                                    AS eventos,
    COUNT(DISTINCT b.payment_id)                                AS pagamentos,
    ROUND(100.0 * COUNT(*) / NULLIF(MAX(t.eventos_total), 0), 2) AS pct_dos_eventos,

    MIN(b.event_timestamp)                                      AS primeiro_evento,
    MAX(b.event_timestamp)                                      AS ultimo_evento

FROM base b
CROSS JOIN total t
GROUP BY b.event_name
