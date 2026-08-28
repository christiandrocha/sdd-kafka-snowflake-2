{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: revenue and product mix per restaurant.
--
-- WHERE REVENUE COMES FROM, AND WHY NOT FROM `orders`. There were two paths:
-- sum `orders.total_amount` grouped by `restaurant_key` (CNPJ), or sum
-- `order_items.subtotal` grouped by `restaurant_id` (integer). The second was
-- chosen, for three reasons:
--
--   1. Granularity. The item carries quantity, discount, unit price, combo and
--      category -- and product mix is half of what this model exists to
--      answer. `orders.total_amount` is an opaque number.
--   2. Population. 210,002 items against 414 orders in this database.
--      Aggregating by orders would give a picture of a tiny sample.
--   3. Key consistency. An item links to a restaurant by `restaurant_id`,
--      which is the same key on both sides; an order links by CNPJ, and then
--      the join depends on a text column.
--
-- The consequence: `receita_bruta` here is the sum of items, and does NOT
-- reconcile with the sum of `orders.total_amount`. They are two different
-- measures, not an error in one of them -- 7,246 items do not even have a
-- matching order in the source database (a property of the seeded data,
-- verified 2026-08-10).
--
-- Fan-out and orphans handled as in gold_driver_performance: QUALIFY reduces
-- the registry to one row per restaurant_id before the join (the uniqueness
-- guaranteed in silver_restaurants is by `uuid`, not by `restaurant_id`), and
-- the LEFT JOIN starts from the ITEMS, so the 370 items from an unregistered
-- restaurant stay counted with the `sem_cadastro` flag.

WITH restaurantes AS (

    SELECT *
    FROM {{ ref('silver_restaurants') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY restaurant_id
        ORDER BY source_ts_ms DESC, kafka_offset DESC
    ) = 1

),

itens AS (

    SELECT
        restaurant_id,
        COUNT(*)                                  AS itens_vendidos,
        COUNT(DISTINCT order_id)                  AS pedidos,
        COUNT(DISTINCT product_id)                AS produtos_distintos,
        SUM(quantity)                             AS unidades,
        SUM(subtotal)                             AS receita_bruta,
        SUM(discount_applied)                     AS descontos,
        SUM(subtotal) - SUM(discount_applied)     AS receita_liquida,
        ROUND(AVG(unit_price), 2)                 AS preco_medio_unitario,
        COUNT_IF(is_combo)                        AS itens_combo,
        COUNT_IF(is_vegetarian)                   AS itens_vegetarianos
    FROM {{ ref('silver_order_items') }}
    WHERE restaurant_id IS NOT NULL
    GROUP BY restaurant_id

)

SELECT
    i.restaurant_id,
    r.uuid            AS restaurant_uuid,
    r.cnpj,
    r.name            AS restaurante,
    r.city,
    r.country,
    r.cuisine_type,
    r.average_rating,
    r.num_reviews,

    i.itens_vendidos,
    i.pedidos,
    i.produtos_distintos,
    i.unidades,
    i.receita_bruta,
    i.descontos,
    i.receita_liquida,
    i.preco_medio_unitario,
    i.itens_combo,
    i.itens_vegetarianos,

    ROUND(i.receita_liquida / NULLIF(i.pedidos, 0), 2)          AS receita_por_pedido,
    ROUND(i.itens_vendidos  / NULLIF(i.pedidos, 0), 2)          AS itens_por_pedido,
    ROUND(100.0 * i.descontos / NULLIF(i.receita_bruta, 0), 2)  AS pct_desconto,
    ROUND(100.0 * i.itens_combo / NULLIF(i.itens_vendidos, 0), 2) AS pct_combo,

    r.restaurant_id IS NULL AS sem_cadastro

FROM itens i
LEFT JOIN restaurantes r ON r.restaurant_id = i.restaurant_id
