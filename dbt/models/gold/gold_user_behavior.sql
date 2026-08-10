{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: comportamento acumulado por usuario -- pedidos, gasto, buscas e
-- interacoes com recomendacao, mais o perfil vindo das duas origens de
-- cadastro (MongoDB e MSSQL).
--
-- Categoria: aditivo-particionavel por usuario, CUMULATIVO -- nao e janela
-- deslizante. Nao ha recorte de tempo: "primeiro pedido" e "ultimo pedido"
-- delimitam a vida inteira do usuario na base.
--
-- A CHAVE E O CPF, E ISSO EXIGE CUIDADO. O modelo precisa costurar tres
-- espacos de identificador diferentes:
--
--     orders.user_key ......... CPF (texto)
--     users_mongo.cpf ......... CPF (texto)  + users_mongo.user_id (inteiro)
--     search_events.user_id ... inteiro
--     recommendations.user_id . inteiro
--
-- `users_mongo` e a unica ponte entre o CPF e o user_id numerico. Por isso
-- ele aparece duas vezes abaixo, com papeis distintos:
--
--   `ponte`  -- TODOS os pares (cpf, user_id). Usado para somar buscas e
--              recomendacoes. Nao pode ser deduplicado: 95 CPFs aparecem com
--              mais de um cadastro nesta base (412 usuarios para 216 CPFs
--              distintos, verificado em 2026-08-10), e cada cadastro tem seu
--              user_id. Deduplicar aqui perderia os eventos dos user_id
--              descartados.
--
--   `perfil` -- UMA linha por CPF, escolhida deterministicamente pelo mesmo
--              criterio do resolve_cdc (source_ts_ms, depois kafka_offset).
--              Usado so para atributos descritivos. Sem esse QUALIFY, a
--              juncao com pedidos multiplicaria cada pedido pelo numero de
--              cadastros do CPF -- e o gasto total sairia inflado ate 2x.
--
-- O mesmo vale para users_mssql, que traz o perfil estendido pelo mesmo CPF.
--
-- Usuario sem pedido nenhum continua na tabela (o FROM parte do perfil, e as
-- juncoes de fato sao LEFT): quem so busca e nunca compra e exatamente o
-- segmento que este modelo deveria conseguir mostrar.

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
