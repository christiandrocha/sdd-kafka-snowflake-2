{{
    config(
        materialized         = 'incremental',
        schema               = 'GOLD',
        unique_key           = 'payment_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Gold: uma linha por pagamento, com o instante de cada etapa do ciclo
-- pivotado em coluna. E o modelo de referencia de agregacao ADITIVA-
-- PARTICIONAVEL do projeto: o estado de um pagamento so depende dos eventos
-- daquele payment_id, entao evento novo obriga a recalcular UM pagamento, nao
-- a tabela inteira.
--
-- COMO O INCREMENTAL FUNCIONA AQUI. O watermark (`ultimo_evento_ms`) serve
-- para descobrir QUAIS pagamentos mudaram, nao para filtrar as linhas que
-- entram na agregacao. Depois de identificar os payment_id afetados, o CTE
-- `eventos` traz o historico COMPLETO de cada um -- caso contrario um evento
-- `closed` que chegasse sozinho produziria uma linha sem `criado_em`, e o
-- MERGE sobrescreveria a versao boa por uma versao mutilada.
--
-- ATENCAO A CARDINALIDADE DOS DADOS ATUAIS (medida em 2026-08-10): os 2.210
-- eventos se distribuem em apenas 8 payment_id distintos, sete deles com mais
-- de 300 eventos cada. A estrutura do modelo esta certa, mas com esse dado ele
-- devolve 8 linhas e as duracoes nao tem significado de negocio -- o gerador
-- sintetico reaproveitou os identificadores. Vale conferir antes de usar
-- qualquer numero daqui em decisao.
--
-- Nao ha juncao com `orders`: `orders.payment_key` tem 410 valores distintos e
-- interseccao ZERO com `payment_events.payment_id`. Os dois nao referenciam o
-- mesmo espaco de identificadores nesta base.

WITH alvo AS (

    SELECT DISTINCT payment_id
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL

    {% if is_incremental() %}
      AND event_timestamp_ms > (
          SELECT COALESCE(MAX(ultimo_evento_ms), 0) FROM {{ this }}
      )
    {% endif %}

),

eventos AS (

    SELECT e.*
    FROM {{ ref('silver_payment_events') }} e
    INNER JOIN alvo a ON a.payment_id = e.payment_id

)

SELECT
    payment_id,

    COUNT(*)                                                          AS total_eventos,
    COUNT(DISTINCT event_name)                                        AS etapas_distintas,
    MIN(event_timestamp)                                              AS primeiro_evento,
    MAX(event_timestamp)                                              AS ultimo_evento,
    MAX(event_timestamp_ms)                                           AS ultimo_evento_ms,

    MIN(CASE WHEN event_name = 'created'    THEN event_timestamp END) AS criado_em,
    MIN(CASE WHEN event_name = 'authorized' THEN event_timestamp END) AS autorizado_em,
    MIN(CASE WHEN event_name = 'captured'   THEN event_timestamp END) AS capturado_em,
    MIN(CASE WHEN event_name = 'succeeded'  THEN event_timestamp END) AS aprovado_em,
    MIN(CASE WHEN event_name = 'settled'    THEN event_timestamp END) AS liquidado_em,
    MIN(CASE WHEN event_name = 'closed'     THEN event_timestamp END) AS fechado_em,
    MIN(CASE WHEN event_name = 'refunded'   THEN event_timestamp END) AS reembolsado_em,

    COUNT_IF(event_name = 'refunded') > 0                             AS teve_reembolso,
    COUNT_IF(event_name = 'closed')   > 0                             AS foi_fechado,

    DATEDIFF(
        'second',
        MIN(CASE WHEN event_name = 'created' THEN event_timestamp END),
        MIN(CASE WHEN event_name = 'closed'  THEN event_timestamp END)
    )                                                                 AS segundos_ate_fechamento

FROM eventos
GROUP BY payment_id
