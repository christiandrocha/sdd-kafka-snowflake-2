{{
    config(
        materialized         = 'incremental',
        schema               = 'GOLD',
        unique_key           = 'event_name',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Gold: volume por etapa do ciclo de pagamento. Uma linha por event_name.
--
-- POR QUE INCREMENTAL SEM WATERMARK. Esta e uma agregacao de RAZAO GLOBAL: o
-- percentual de cada etapa depende do total de eventos, entao nao existe
-- recorte incremental correto -- filtrar por evento novo mudaria o
-- denominador e produziria percentual errado. O que o incremental resolve
-- aqui e outra coisa: a tabela tem 7 linhas de cardinalidade fixa, e o MERGE
-- por event_name as atualiza no lugar, preservando a identidade de cada
-- linha entre execucoes em vez de derrubar e recriar a tabela.
--
-- Ou seja: varredura completa a cada run, por necessidade aritmetica; MERGE
-- por estabilidade da chave. Sobre 2.210 eventos isso e irrelevante em custo.
-- Se um dia a cardinalidade de event_name explodir, a decisao muda.

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
