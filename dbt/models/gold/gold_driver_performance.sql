{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: desempenho acumulado por entregador -- turnos, pedidos, distancia,
-- ganhos e derivados (ganho por km, ganho por hora, pedidos por turno).
--
-- Categoria: aditivo-particionavel por driver_id. Nao esta incrementalizado
-- de proposito: sao 469 turnos para 355 entregadores, e o full refresh custa
-- menos que a complexidade do watermark. Se o volume de turnos crescer em
-- ordem de grandeza, o caminho e o mesmo padrao de gold_payment_lifecycle --
-- descobrir quais driver_id tiveram turno novo e recalcular so eles por
-- inteiro.
--
-- DUAS ARMADILHAS TRATADAS AQUI:
--
--   1. Fan-out do cadastro. `silver_drivers` garante unicidade por `uuid`,
--      que e a chave tecnica do CDC -- NAO por `driver_id`, que e a chave de
--      negocio usada na juncao. Se o mesmo entregador existir com dois uuid,
--      o join multiplicaria os turnos dele. Por isso o QUALIFY reduz o
--      cadastro a uma linha por driver_id antes da juncao, usando o mesmo
--      criterio deterministico do resolve_cdc (source_ts_ms, depois
--      kafka_offset).
--
--   2. Turno sem cadastro. 84 linhas de driver_shifts apontam para driver_id
--      inexistente em drivers (verificado em 2026-08-10, e propriedade da
--      base de origem, nao do pipeline). O LEFT JOIN parte dos TURNOS, entao
--      esses entregadores continuam sendo medidos, com os atributos nulos e
--      a flag `sem_cadastro` ligada. Um INNER JOIN aqui apagaria 84 turnos do
--      relatorio em silencio.
--
-- `issues_reported` e categorico em texto ('Late Start', 'App Crash', 'Lost
-- GPS', 'Low Battery', 'Accident') e usa a string 'None' para ausencia de
-- problema -- nao NULL. Comparar com NULL nao funcionaria.

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
