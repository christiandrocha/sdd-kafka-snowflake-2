-- ──────────────────────────────────────────────────────────────────────────
-- verify_governance.sql
-- Roda ANTES de qualquer demo/execução ao vivo para o cliente.
--
-- Resolve a discrepância encontrada durante esta análise: CLAUDE.md descreve
-- um Resource Monitor "cdc_trial_monitor" (348 créditos, FREQUENCY=NEVER,
-- aplicado via ALTER ACCOUNT), enquanto scripts/snowflake_setup.sql cria um
-- Resource Monitor diferente "cdc_poc_monitor" (20 créditos, MONTHLY,
-- aplicado via ALTER WAREHOUSE CDC_WH).
--
-- Hipótese mais provável (não confirmada — rode este script para confirmar):
-- são dois monitores diferentes, coexistindo, com propósitos diferentes:
--   - cdc_trial_monitor: teto único da conta trial inteira (ALTER ACCOUNT),
--     provavelmente ligado ao período de trial de 30 dias mencionado em
--     docs/TRIAL_PLAN.md (arquivo ausente do repositório atual).
--   - cdc_poc_monitor: guarda-corpo específico do warehouse CDC_WH,
--     recriável via snowflake_setup.sql, é o que está sob controle deste
--     projeto.
-- Se SÓ o cdc_poc_monitor existir, a hipótese acima está errada e o
-- CLAUDE.md está descrevendo algo que nunca existiu ou já foi removido.
-- ──────────────────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

-- 1. Lista TODOS os resource monitors da conta — não assuma que só existe um.
SHOW RESOURCE MONITORS;

-- 2. Configuração default da conta (se ALTER ACCOUNT SET RESOURCE_MONITOR
--    foi realmente executado em algum momento, aparece aqui).
SHOW PARAMETERS LIKE 'RESOURCE_MONITOR' IN ACCOUNT;

-- 3. Qual monitor está de fato vinculado ao warehouse usado pelo pipeline.
SHOW WAREHOUSES LIKE 'CDC_WH';

-- 4. Consumo real dos últimos 30 dias — a fonte de verdade sobre se o
--    projeto está queimando crédito mesmo parado (não deveria, se está
--    desligado, mas confirme).
SELECT
    warehouse_name,
    DATE_TRUNC('day', start_time) AS day,
    SUM(credits_used)             AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'CDC_WH'
  AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2 DESC;

-- 5. Estado atual do warehouse — confirma se está suspenso agora mesmo.
SELECT
    warehouse_name,
    state,
    auto_suspend,
    auto_resume
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSES())
WHERE warehouse_name = 'CDC_WH';

-- ── Ação recomendada após rodar este script ─────────────────────────────
-- Se encontrar QUALQUER monitor com FREQUENCY = NEVER ainda ativo e
-- vinculado (via conta ou via warehouse) ao CDC_WH: ele não reseta sozinho
-- e não teria protegido o projeto de um vazamento contínuo. Substitua ou
-- remova antes de qualquer demo ao vivo:
--   ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = cdc_poc_monitor;
--   ALTER ACCOUNT UNSET RESOURCE_MONITOR;  -- se aplicável, e após revisão
