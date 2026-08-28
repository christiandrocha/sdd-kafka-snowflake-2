{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: cumulative behaviour per user -- orders, spend, searches and
-- recommendation interactions, plus the profile coming from the two
-- registration sources (MongoDB and MSSQL).
--
-- Category: additive-partitionable per user, CUMULATIVE -- not a sliding
-- window. There is no time slice: "first order" and "last order" bound the
-- user's entire life in the database.
--
-- THE KEY IS THE CPF, AND THAT NEEDS CARE. The model has to stitch three
-- different identifier spaces together:
--
--     orders.user_key ......... CPF (texto)
--     users_mongo.cpf ......... CPF (texto)  + users_mongo.user_id (inteiro)
--     search_events.user_id ... inteiro
--     recommendations.user_id . inteiro
--
-- `users_mongo` is the only bridge between the CPF and the numeric user_id.
-- That is why it appears twice below, in distinct roles:
--
--   `ponte`  -- ALL (cpf, user_id) pairs. Used to sum searches and
--              recommendations. It cannot be deduplicated: 95 CPFs appear with
--              more than one registration in this database (412 users for 216
--              distinct CPFs, verified 2026-08-10), and each registration has
--              its own user_id. Deduplicating here would lose the events of
--              the discarded user_ids.
--
--   `perfil` -- ONE row per CPF, chosen deterministically by the same
--              criterion as resolve_cdc (source_ts_ms, then kafka_offset).
--              Used only for descriptive attributes. Without that QUALIFY, the
--              join with orders would multiply each order by the number of
--              registrations for the CPF -- and total spend would come out
--              inflated by up to 2x.
--
-- The same holds for users_mssql, which brings the extended profile on the
-- same CPF.
--
-- A user with no orders at all stays in the table (the FROM starts from the
-- profile, and the fact joins are LEFT): someone who only searches and never
-- buys is exactly the segment this model should be able to show.

WITH ponte AS (

    SELECT DISTINCT cpf, user_id
    FROM {{ ref('silver_users_mongo') }}
    WHERE cpf IS NOT NULL
      AND user_id IS NOT NULL

),

perfil AS (

    SELECT *
    FROM {{ ref('silver_users_mongo') }}
    WHERE cpf IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cpf
        ORDER BY source_ts_ms DESC, kafka_offset DESC
    ) = 1

),

perfil_estendido AS (

    SELECT *
    FROM {{ ref('silver_users_mssql') }}
    WHERE cpf IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cpf
        ORDER BY source_ts_ms DESC, kafka_offset DESC
    ) = 1

),

pedidos AS (

    SELECT
        user_key                        AS cpf,
        COUNT(*)                        AS pedidos,
        SUM(total_amount)               AS gasto_total,
        ROUND(AVG(total_amount), 2)     AS ticket_medio,
        MIN(order_date)                 AS primeiro_pedido,
        MAX(order_date)                 AS ultimo_pedido,
        COUNT(DISTINCT restaurant_key)  AS restaurantes_distintos
    FROM {{ ref('silver_orders') }}
    WHERE user_key IS NOT NULL
    GROUP BY user_key

),

buscas AS (

    SELECT
        p.cpf,
        COUNT(*)                                AS buscas,
        COUNT(DISTINCT s.query_text)            AS termos_distintos,
        ROUND(AVG(s.result_count), 2)           AS resultados_medios,
        COUNT_IF(s.clicked_product_id IS NOT NULL) AS buscas_com_clique
    FROM {{ ref('silver_search_events') }} s
    INNER JOIN ponte p ON p.user_id = s.user_id
    GROUP BY p.cpf

),

recomendacoes AS (

    SELECT
        p.cpf,
        COUNT(*)                                          AS recomendacoes,
        COUNT_IF(r.event_type = 'recommendation_served')  AS rec_exibidas,
        COUNT_IF(r.event_type = 'view')                   AS rec_vistas,
        COUNT_IF(r.event_type = 'click')                  AS rec_clicadas,
        COUNT_IF(r.event_type = 'add_to_cart')            AS rec_no_carrinho
    FROM {{ ref('silver_recommendations') }} r
    INNER JOIN ponte p ON p.user_id = r.user_id
    GROUP BY p.cpf

)

SELECT
    pf.cpf,
    pf.user_id,
    pf.uuid                     AS user_uuid,
    pe.first_name,
    pe.last_name,
    pf.email,
    pf.city,
    pf.country,
    pe.job,
    pe.company_name,

    COALESCE(pd.pedidos, 0)     AS pedidos,
    COALESCE(pd.gasto_total, 0) AS gasto_total,
    pd.ticket_medio,
    pd.primeiro_pedido,
    pd.ultimo_pedido,
    COALESCE(pd.restaurantes_distintos, 0) AS restaurantes_distintos,

    COALESCE(b.buscas, 0)            AS buscas,
    COALESCE(b.termos_distintos, 0)  AS termos_distintos,
    b.resultados_medios,
    COALESCE(b.buscas_com_clique, 0) AS buscas_com_clique,

    COALESCE(rc.recomendacoes, 0)    AS recomendacoes,
    COALESCE(rc.rec_exibidas, 0)     AS rec_exibidas,
    COALESCE(rc.rec_vistas, 0)       AS rec_vistas,
    COALESCE(rc.rec_clicadas, 0)     AS rec_clicadas,
    COALESCE(rc.rec_no_carrinho, 0)  AS rec_no_carrinho,

    ROUND(100.0 * COALESCE(rc.rec_clicadas, 0)
          / NULLIF(rc.recomendacoes, 0), 2)   AS pct_clique_recomendacao,
    ROUND(100.0 * COALESCE(b.buscas_com_clique, 0)
          / NULLIF(b.buscas, 0), 2)           AS pct_busca_com_clique,

    COALESCE(pd.pedidos, 0) = 0               AS nunca_comprou,
    pe.cpf IS NULL                            AS sem_perfil_estendido

FROM perfil pf
LEFT JOIN perfil_estendido pe ON pe.cpf = pf.cpf
LEFT JOIN pedidos          pd ON pd.cpf = pf.cpf
LEFT JOIN buscas           b  ON b.cpf  = pf.cpf
LEFT JOIN recomendacoes    rc ON rc.cpf = pf.cpf
