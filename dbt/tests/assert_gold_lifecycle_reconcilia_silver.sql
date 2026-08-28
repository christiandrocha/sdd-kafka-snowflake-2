-- Reconciliation of Silver -> gold_payment_lifecycle.
--
-- WHY THIS TEST EXISTS
--
-- The 185 schema tests in this project (unique, not_null, accepted_values,
-- relationships) verify the SHAPE of the data. None of them verifies that Gold
-- reflects Silver -- and that is exactly the question an incremental model
-- answers wrongly when it breaks.
--
-- On 2026-08-11 the `alvo` CTE of gold_payment_lifecycle ignored an event that
-- arrived out of order. Silver held 4 events for payment 55555555 and Gold
-- kept saying 3, with a null `capturado_em`. All 26 tests in the chain passed
-- and dbt reported success. This test would have failed.
--
-- WHAT IT ASSERTS: for every payment_id, the event count and the timestamp of
-- the most recent event in Gold equal those in Silver -- and neither side has
-- a payment the other does not.
--
-- KNOWN FALSE POSITIVE: a partial build (`dbt build --select` including Silver
-- but not Gold, or the reverse) legitimately leaves the two out of sync. If
-- this test fails right after a selective run, run the whole project before
-- investigating.

WITH silver AS (

    SELECT
        payment_id,
        COUNT(*)                AS eventos,
        MAX(event_timestamp_ms) AS ultimo_ms
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL
    GROUP BY payment_id

)

SELECT
    COALESCE(s.payment_id, g.payment_id) AS payment_id,
    s.eventos                            AS eventos_silver,
    g.total_eventos                      AS eventos_gold,
    s.ultimo_ms                          AS ultimo_ms_silver,
    g.ultimo_evento_ms                   AS ultimo_ms_gold,
    CASE
        WHEN g.payment_id IS NULL              THEN 'pagamento ausente na Gold'
        WHEN s.payment_id IS NULL              THEN 'pagamento fantasma na Gold'
        WHEN s.eventos <> g.total_eventos      THEN 'contagem divergente'
        ELSE                                        'watermark divergente'
    END                                  AS motivo

FROM silver s
FULL OUTER JOIN {{ ref('gold_payment_lifecycle') }} g
    ON g.payment_id = s.payment_id

WHERE s.payment_id IS NULL
   OR g.payment_id IS NULL
   OR s.eventos   <> g.total_eventos
   OR s.ultimo_ms <> g.ultimo_evento_ms
