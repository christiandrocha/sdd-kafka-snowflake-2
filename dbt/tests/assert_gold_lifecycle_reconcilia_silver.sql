-- Reconciliacao Silver -> gold_payment_lifecycle.
--
-- POR QUE ESTE TESTE EXISTE
--
-- Os 185 testes de esquema deste projeto (unique, not_null, accepted_values,
-- relationships) verificam a FORMA do dado. Nenhum deles verifica se a Gold
-- reflete a Silver -- e essa e exatamente a pergunta que um modelo incremental
-- responde errado quando quebra.
--
-- Em 2026-08-11 o CTE `alvo` do gold_payment_lifecycle ignorou um evento que
-- chegou fora de ordem. A Silver ficou com 4 eventos para o pagamento
-- 55555555 e a Gold seguiu dizendo 3, com `capturado_em` nulo. Os 26 testes
-- da cadeia passaram e o dbt reportou sucesso. Este teste teria falhado.
--
-- O QUE ELE AFIRMA: para todo payment_id, a contagem de eventos e o timestamp
-- do evento mais recente na Gold sao iguais aos da Silver -- e nenhum lado tem
-- pagamento que o outro nao tenha.
--
-- FALSO POSITIVO CONHECIDO: uma construcao parcial (`dbt build --select`
-- incluindo a Silver mas nao a Gold, ou o contrario) deixa as duas fora de
-- sincronia legitimamente. Se este teste falhar logo apos um run seletivo,
-- rode o projeto inteiro antes de investigar.

WITH silver AS (

    SELECT
        payment_id,
        COUNT(*)                AS eventos,
        MAX(event_timestamp_ms) AS ultimo_ms
    FROM {{ ref('silver_payment_events') }}
    WHERE payment_id IS NOT NULL
    GROUP BY payment_id

)

SELECT
    COALESCE(s.payment_id, g.payment_id) AS payment_id,
    s.eventos                            AS eventos_silver,
    g.total_eventos                      AS eventos_gold,
    s.ultimo_ms                          AS ultimo_ms_silver,
    g.ultimo_evento_ms                   AS ultimo_ms_gold,
    CASE
        WHEN g.payment_id IS NULL              THEN 'pagamento ausente na Gold'
        WHEN s.payment_id IS NULL              THEN 'pagamento fantasma na Gold'
        WHEN s.eventos <> g.total_eventos      THEN 'contagem divergente'
        ELSE                                        'watermark divergente'
    END                                  AS motivo

FROM silver s
FULL OUTER JOIN {{ ref('gold_payment_lifecycle') }} g
    ON g.payment_id = s.payment_id

WHERE s.payment_id IS NULL
   OR g.payment_id IS NULL
   OR s.eventos   <> g.total_eventos
   OR s.ultimo_ms <> g.ultimo_evento_ms
