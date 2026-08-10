{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: receita e mix de produto por restaurante.
--
-- DE ONDE VEM A RECEITA, E POR QUE NAO DE `orders`. Havia dois caminhos:
-- somar `orders.total_amount` agrupando por `restaurant_key` (CNPJ), ou somar
-- `order_items.subtotal` agrupando por `restaurant_id` (inteiro). Escolhido o
-- segundo, por tres motivos:
--
--   1. Granularidade. O item carrega quantidade, desconto, preco unitario,
--      combo e categoria -- e o mix de produto e metade do que este modelo
--      existe para responder. `orders.total_amount` e um numero opaco.
--   2. Populacao. Sao 210.002 itens contra 414 pedidos nesta base. Agregar
--      pelos pedidos daria um retrato de amostra minuscula.
--   3. Consistencia de chave. Item liga a restaurante por `restaurant_id`,
--      que e a mesma chave dos dois lados; pedido liga por CNPJ, e ai a
--      juncao depende de uma coluna de texto.
--
-- A consequencia: `receita_bruta` aqui e a soma dos itens, e NAO reconcilia
-- com a soma de `orders.total_amount`. Sao duas medidas diferentes, nao um
-- erro de uma delas -- 7.246 itens sequer tem pedido correspondente na base
-- de origem (propriedade do dado semeado, verificada em 2026-08-10).
--
-- Fan-out e orfaos tratados como em gold_driver_performance: QUALIFY reduz o
-- cadastro a uma linha por restaurant_id antes da juncao (a unicidade
-- garantida em silver_restaurants e por `uuid`, nao por `restaurant_id`), e o
-- LEFT JOIN parte dos ITENS, para que os 370 itens de restaurante nao
-- cadastrado continuem contabilizados com a flag `sem_cadastro`.

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
