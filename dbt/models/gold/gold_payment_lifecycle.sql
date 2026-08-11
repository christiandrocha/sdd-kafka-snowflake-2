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
-- COMO O INCREMENTAL FUNCIONA AQUI. O CTE `alvo` descobre QUAIS pagamentos
-- mudaram; ele nao filtra as linhas que entram na agregacao. Depois de
-- identificar os payment_id afetados, o CTE `eventos` traz o historico
-- COMPLETO de cada um -- caso contrario um evento `closed` que chegasse
-- sozinho produziria uma linha sem `criado_em`, e o MERGE sobrescreveria a
-- versao boa por uma versao mutilada. Verificado ao vivo em 2026-08-11:
-- `criado_em` e `autorizado_em` sobreviveram a chegada isolada de um `closed`.
--
-- POR QUE NAO E UM WATERMARK GLOBAL (corrigido em 2026-08-11)
--
-- A versao anterior comparava cada evento contra `MAX(ultimo_evento_ms)` da
-- tabela INTEIRA. Isso pressupoe que evento novo sempre chega com timestamp
-- maior que o de qualquer evento de qualquer outro pagamento -- premissa que
-- CDC nao garante. Bastava um evento cujo timestamp fosse anterior ao evento
-- mais recente de OUTRO pagamento para ele ser ignorado em silencio.
--
-- Reproduzido antes de corrigir, com dado real: um `captured` do pagamento
-- 55555555 com timestamp entre o `created` e o `closed` dele mesmo percorreu
-- Debezium, Kafka e sink, entrou na Bronze (`SUCCESS 1`) e na Silver
-- (`SUCCESS 1`), e a Gold devolveu `SUCCESS 0`. A Silver ficou com 4 eventos
-- e a Gold seguiu dizendo 3, com `capturado_em` nulo -- e os 26 testes da
-- cadeia passaram. Falha silenciosa, com o pipeline reportando sucesso.
--
-- A comparacao agora e POR PAGAMENTO, e por contagem antes de timestamp:
-- `COUNT(*) <> total_eventos` pega qualquer evento novo independente da ordem
-- em que chegou, que e a unica formulacao robusta a entrega fora de ordem.
-- O `MAX(...) <> ultimo_evento_ms` fica como segunda guarda.
--
-- CUSTO. O `alvo` agora agrega a Silver inteira a cada execucao, em vez de
-- filtrar por um escalar. Nesta base sao milhares de linhas e o custo e
-- irrelevante; a Silver e reconstruida por inteiro a cada run de qualquer
-- forma (`materialized='table'`). Se a Silver crescer a ponto de esse
-- GROUP BY pesar, a saida e um watermark de INGESTAO (`dt_current_timestamp`)
-- em vez de tempo de evento -- nunca voltar ao maximo global.
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

{% if is_incremental() %}

    -- Compara o estado de CADA pagamento na Silver contra o que a Gold ja
    -- registrou dele. Um pagamento entra se e novo, se ganhou evento, ou se o
    -- evento mais recente dele mudou. Nao ha watermark global aqui -- ver a
    -- nota "POR QUE NAO E UM WATERMARK GLOBAL" no cabecalho.
    SELECT e.payment_id
    FROM {{ ref('silver_payment_events') }} e
    LEFT JOIN {{ this }} t ON t.payment_id = e.payment_id
    WHERE e.payment_id IS NOT NULL
    GROUP BY e.payment_id, t.payment_id, t.total_eventos, t.ultimo_evento_ms
    HAVING t.payment_id IS NULL
        OR COUNT(*)                  <> t.total_eventos
        OR MAX(e.event_timestamp_ms) <> t.ultimo_evento_ms

{% else %}

    SELECT DISTINCT payment_id
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL

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
