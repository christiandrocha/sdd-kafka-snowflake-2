{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: payment lifecycle funnel -- volume per stage, conversion against the
-- top of the funnel and against the previous stage.
--
-- WHY 'table' AND NOT INCREMENTAL. A pure global ratio: every percentage
-- column depends on the totals of the other stages. There is no subset of new
-- rows that can be aggregated in isolation and still produce the right number.
-- Full refresh is the honest materialization here, not laziness.
--
-- The stage order is declared, not inferred: alphabetical would put
-- `authorized` before `created` and the funnel would come out inverted.
-- `refunded` is NOT part of the sequence -- it is a detour from the happy
-- path, not a step of it; it enters as a separate column, repeated on every
-- row, to give context without contaminating the conversion rates.
--
-- A stage with no events appears as zero, it does not vanish: the LEFT JOIN
-- starts from the declared list of stages. An empty step is precisely what the
-- funnel needs to show.
--
-- On the current data, see the cardinality note in gold_payment_lifecycle.sql:
-- 8 distinct payment_ids for 2,210 events. The per-stage counts are real; the
-- reading of "conversion" is not.

WITH etapas AS (

    SELECT *
    FROM VALUES
        ('created',    1),
        ('authorized', 2),
        ('captured',   3),
        ('succeeded',  4),
        ('settled',    5),
        ('closed',     6)
    AS t (event_name, ordem)

),

agregado AS (

    SELECT
        event_name,
        COUNT(*)                     AS eventos,
        COUNT(DISTINCT payment_id)   AS pagamentos
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL
    GROUP BY event_name

),

reembolso AS (

    SELECT COALESCE(SUM(eventos), 0) AS eventos_reembolso
    FROM agregado
    WHERE event_name = 'refunded'

),

funil AS (

    SELECT
        e.ordem,
        e.event_name,
        COALESCE(a.eventos, 0)    AS eventos,
        COALESCE(a.pagamentos, 0) AS pagamentos
    FROM etapas e
    LEFT JOIN agregado a ON a.event_name = e.event_name

)

SELECT
    f.ordem,
    f.event_name,
    f.eventos,
    f.pagamentos,

    FIRST_VALUE(f.eventos) OVER (ORDER BY f.ordem)  AS eventos_no_topo,
    LAG(f.eventos)         OVER (ORDER BY f.ordem)  AS eventos_etapa_anterior,

    ROUND(100.0 * f.eventos / NULLIF(
        FIRST_VALUE(f.eventos) OVER (ORDER BY f.ordem), 0), 2)      AS pct_do_topo,

    ROUND(100.0 * f.eventos / NULLIF(
        LAG(f.eventos) OVER (ORDER BY f.ordem), 0), 2)              AS pct_da_etapa_anterior,

    r.eventos_reembolso

FROM funil f
CROSS JOIN reembolso r
ORDER BY f.ordem
