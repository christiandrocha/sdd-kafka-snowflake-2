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
--
-- ── PRIVILÉGIO NECESSÁRIO — LEIA ANTES DE RODAR ───────────────────────────
--
-- Este script EXIGE ACCOUNTADMIN. Não é preferência: os passos 1, 2 e 4
-- consultam objetos de conta, e nenhuma das identidades de serviço do
-- projeto alcança isso. Verificado em 2026-08-10 com DAGSTER_SERVICE_USER:
--
--     USE ROLE ACCOUNTADMIN
--       -> "Requested role 'ACCOUNTADMIN' is not assigned to the executing
--           user."
--
-- As três identidades de keys/ (DAGSTER_SERVICE_USER e DATA_AGENTS_MCP_USER
-- em CDC_ROLE, CURSOR_MCP_USER em CDC_ROLE_RO) têm um papel só cada uma.
-- Rode este arquivo numa worksheet do Snowflake com ACCOUNTADMIN.
--
-- ── A ARMADILHA DOS PASSOS 1 E 2 ──────────────────────────────────────────
--
-- `SHOW RESOURCE MONITORS` e `SHOW PARAMETERS ... IN ACCOUNT` filtram pelo
-- que o papel ATIVO enxerga. Sob um papel sem MONITOR USAGE na conta, os dois
-- retornam ZERO LINHAS SEM ERRO -- resultado indistinguível de "não existe
-- monitor nenhum".
--
-- Foi exatamente o que aconteceu na execução parcial de 2026-08-10 sob
-- CDC_ROLE: passos 1 e 2 vazios, passo 4 negado ("Schema
-- 'SNOWFLAKE.ACCOUNT_USAGE' does not exist or not authorized"). Zero linhas
-- ali NÃO responde a pergunta que este script existe para responder. Só
-- confie no resultado dos passos 1 e 2 se `SELECT CURRENT_ROLE()` devolver
-- ACCOUNTADMIN.
--
-- O que a execução parcial ESTABELECEU: o CDC_WH não tem monitor vinculado no
-- nível do warehouse -- o campo `resource_monitor` vem nulo, e esse campo o
-- CDC_ROLE enxerga. Logo, se existe proteção, ela está no nível da conta.
-- ──────────────────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

-- 0. Confirma o papel ativo. Se isto não devolver ACCOUNTADMIN, pare: os
--    passos 1, 2 e 4 vão mentir por omissão em vez de dar erro.
SELECT CURRENT_ROLE() AS papel_ativo, CURRENT_ACCOUNT() AS conta;

-- 1. Lista TODOS os resource monitors da conta — não assuma que só existe um.
SHOW RESOURCE MONITORS;

-- 2. Configuração default da conta (se ALTER ACCOUNT SET RESOURCE_MONITOR
--    foi realmente executado em algum momento, aparece aqui).
SHOW PARAMETERS LIKE 'RESOURCE_MONITOR' IN ACCOUNT;

-- 3. Qual monitor está de fato vinculado ao warehouse usado pelo pipeline,
--    e em que estado ele está agora. O SHOW devolve ~35 colunas; o
--    RESULT_SCAN logo abaixo projeta só as que interessam, já tipadas.
--
--    O RESULT_SCAN(LAST_QUERY_ID()) precisa vir IMEDIATAMENTE depois do SHOW,
--    na mesma sessão -- qualquer query entre os dois quebra a referência.
SHOW WAREHOUSES LIKE 'CDC_WH';

SELECT
    "name"              AS warehouse,
    "size"              AS tamanho,
    "state"             AS estado,
    "auto_suspend"      AS auto_suspend_s,
    "auto_resume"       AS auto_resume,
    "resource_monitor"  AS monitor_do_warehouse
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

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

-- 5. Consumo por hora do dia corrente, para o caso de o passo 4 estar
--    defasado. ACCOUNT_USAGE tem latência de até 3 horas; esta função de
--    INFORMATION_SCHEMA é quase em tempo real e roda com privilégio menor,
--    então serve de conferência cruzada do passo 4.
SELECT
    DATE_TRUNC('hour', start_time) AS hora,
    ROUND(SUM(credits_used), 4)    AS creditos
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -1, CURRENT_DATE()),
    WAREHOUSE_NAME   => 'CDC_WH'))
GROUP BY 1
ORDER BY 1 DESC;

-- NOTA sobre o passo 5 anterior, removido em 2026-08-10:
-- ele chamava `TABLE(INFORMATION_SCHEMA.WAREHOUSES())`, que NÃO EXISTE --
-- "Unknown table function INFORMATION_SCHEMA.WAREHOUSES". O passo nunca teria
-- funcionado, nem com ACCOUNTADMIN. O estado do warehouse, que era o objetivo
-- dele, agora vem do RESULT_SCAN do passo 3.

-- ── Ação recomendada após rodar este script ─────────────────────────────
-- Se encontrar QUALQUER monitor com FREQUENCY = NEVER ainda ativo e
-- vinculado (via conta ou via warehouse) ao CDC_WH: ele não reseta sozinho
-- e não teria protegido o projeto de um vazamento contínuo. Substitua ou
-- remova antes de qualquer demo ao vivo:
--   ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = cdc_poc_monitor;
--   ALTER ACCOUNT UNSET RESOURCE_MONITOR;  -- se aplicável, e após revisão
