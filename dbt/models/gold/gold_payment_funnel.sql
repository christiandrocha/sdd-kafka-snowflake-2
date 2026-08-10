{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: funil do ciclo de pagamento -- volume por etapa, conversao em relacao
-- ao topo e em relacao a etapa anterior.
--
-- POR QUE 'table' E NAO INCREMENTAL. Razao global pura: toda coluna de
-- percentual depende do total das outras etapas. Nao ha subconjunto de linhas
-- novas que possa ser agregado isoladamente e ainda produzir o numero certo.
-- Full refresh e a materializacao honesta aqui, nao uma preguica.
--
-- A ordem das etapas e declarada, nao inferida: alfabetica poria `authorized`
-- antes de `created` e o funil sairia invertido. `refunded` NAO entra na
-- sequencia -- e um desvio do fluxo feliz, nao um degrau dele; entra como
-- coluna a parte, repetida em todas as linhas, para dar contexto sem
-- contaminar as taxas de conversao.
--
-- Etapa sem nenhum evento aparece com zero, nao some: o LEFT JOIN parte da
-- lista declarada de etapas. Um degrau vazio e justamente o que o funil
-- precisa mostrar.
--
-- Sobre os dados atuais, ver a nota de cardinalidade em
-- gold_payment_lifecycle.sql: 8 payment_id distintos para 2.210 eventos. As
-- contagens por etapa sao reais; a leitura de "conversao" nao e.

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
