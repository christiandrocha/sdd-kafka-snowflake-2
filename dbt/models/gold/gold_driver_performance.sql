{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: cumulative performance per driver -- shifts, orders, distance,
-- earnings and derived measures (earnings per km, per hour, orders per shift).
--
-- Category: additive-partitionable per driver_id. It is deliberately not
-- incrementalized: 469 shifts for 355 drivers, and the full refresh costs less
-- than the complexity of a watermark. If shift volume grows by an order of
-- magnitude, the path is the same pattern as gold_payment_lifecycle -- find
-- which driver_ids got a new shift and recompute only those, in full.
--
-- TWO TRAPS HANDLED HERE:
--
--   1. Registry fan-out. `silver_drivers` guarantees uniqueness by `uuid`,
--      which is the CDC technical key -- NOT by `driver_id`, which is the
--      business key used in the join. If the same driver exists under two
--      uuids, the join would multiply their shifts. Hence the QUALIFY reduces
--      the registry to one row per driver_id before the join, using the same
--      deterministic criterion as resolve_cdc (source_ts_ms, then
--      kafka_offset).
--
--   2. Shift with no registry entry. 84 driver_shifts rows point at a
--      driver_id absent from drivers (verified 2026-08-10; a property of the
--      source database, not of the pipeline). The LEFT JOIN starts from the
--      SHIFTS, so those drivers are still measured, with null attributes and
--      the `sem_cadastro` flag on. An INNER JOIN here would silently erase 84
--      shifts from the report.
--
-- `issues_reported` is categorical text ('Late Start', 'App Crash', 'Lost
-- GPS', 'Low Battery', 'Accident') and uses the string 'None' for no problem
-- -- not NULL. Comparing against NULL would not work.

WITH entregadores AS (

    SELECT *
    FROM {{ ref('silver_drivers') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY driver_id
        ORDER BY source_ts_ms DESC, kafka_offset DESC
    ) = 1

),

turnos AS (

    SELECT
        driver_id,
        COUNT(*)                          AS turnos,
        SUM(num_orders)                   AS pedidos,
        SUM(distance_covered_km)          AS km_percorridos,
        SUM(earnings_brl)                 AS ganhos_brl,
        SUM(shift_duration_min)           AS minutos_trabalhados,
        ROUND(AVG(shift_rating), 2)       AS rating_medio,
        COUNT_IF(issues_reported IS NOT NULL
                 AND issues_reported <> 'None') AS turnos_com_problema,
        COUNT_IF(available)               AS turnos_disponivel,
        MIN(start_time)                   AS primeiro_turno,
        MAX(end_time)                     AS ultimo_turno
    FROM {{ ref('silver_driver_shifts') }}
    WHERE driver_id IS NOT NULL
    GROUP BY driver_id

)

SELECT
    t.driver_id,
    d.uuid                AS driver_uuid,
    d.first_name,
    d.last_name,
    d.city,
    d.country,
    d.vehicle_type,

    t.turnos,
    t.pedidos,
    t.km_percorridos,
    t.ganhos_brl,
    t.minutos_trabalhados,
    t.rating_medio,
    t.turnos_com_problema,
    t.turnos_disponivel,

    ROUND(t.ganhos_brl / NULLIF(t.km_percorridos, 0), 2)          AS ganho_por_km,
    ROUND(t.ganhos_brl / NULLIF(t.minutos_trabalhados / 60.0, 0), 2) AS ganho_por_hora,
    ROUND(t.pedidos    / NULLIF(t.turnos, 0), 2)                  AS pedidos_por_turno,
    ROUND(100.0 * t.turnos_com_problema / NULLIF(t.turnos, 0), 2) AS pct_turnos_com_problema,

    t.primeiro_turno,
    t.ultimo_turno,

    d.driver_id IS NULL   AS sem_cadastro

FROM turnos t
LEFT JOIN entregadores d ON d.driver_id = t.driver_id
