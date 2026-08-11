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


-- 6. TUDO que a conta consome, por tipo de serviço.
--
--    POR QUE ESTE PASSO EXISTE. Os passos 4 e 5 leem
--    WAREHOUSE_METERING_HISTORY, que só enxerga compute de warehouse. A
--    execução de 2026-08-11 devolveu 1,72 crédito para o CDC_WH e esse número
--    foi tratado como "o consumo do projeto" -- o que é falso, e falso de um
--    jeito estrutural neste pipeline: o caminho de ingestão inteiro é
--    serverless. O Snowpipe Streaming escreve cada linha na Bronze e fatura
--    fora daquela view, assim como Tasks sem warehouse e a camada de cloud
--    services.
--
--    METERING_HISTORY é a fonte que cobre todos os tipos. A coluna
--    SERVICE_TYPE é a resposta: se aparecer alguma linha com crédito
--    relevante que não seja WAREHOUSE_METERING, o custo real do projeto é
--    maior que o documentado no README, e a diferença é justamente a parte
--    que ninguém tinha olhado.
SELECT
    service_type,
    ROUND(SUM(credits_used), 4)                  AS creditos,
    ROUND(SUM(credits_used_compute), 4)          AS creditos_compute,
    ROUND(SUM(credits_used_cloud_services), 4)   AS creditos_cloud_services,
    MIN(start_time)                              AS primeiro,
    MAX(end_time)                                AS ultimo
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;


-- 7. O Query Acceleration Service chegou a engatar alguma vez?
--
--    CONTEXTO. O CDC_WH está com ENABLE_QUERY_ACCELERATION = true e
--    SCALE_FACTOR = 2 (lido em 2026-08-11). QAS fatura crédito ALÉM da
--    compute do warehouse, e ninguém decidiu ligá-lo conscientemente -- veio
--    assim e nunca foi medido.
--
--    COMO LER O RESULTADO:
--      - zero linhas, ou crédito zerado -> nunca engatou. É custo potencial
--        sem benefício demonstrado neste workload, e desligar é de graça:
--          ALTER WAREHOUSE CDC_WH SET ENABLE_QUERY_ACCELERATION = FALSE;
--      - crédito > 0 -> engatou, e aí a conta do README muda: parte do gasto
--        não está no número do passo 4. Antes de desligar, veja quais queries
--        se beneficiaram.
SELECT
    warehouse_name,
    ROUND(SUM(credits_used), 4) AS creditos_qas,
    SUM(num_files_scanned)      AS arquivos_varridos,
    MIN(start_time)             AS primeiro,
    MAX(end_time)               AS ultimo
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;

-- AVISO SOBRE OS PASSOS 6 E 7: escritos em 2026-08-11 e NUNCA EXECUTADOS.
-- Diferente dos passos 1 a 5, que já rodaram, estes dois dependem de nomes de
-- view e de coluna que não foram confirmados contra esta conta. Se algum
-- deles falhar com "invalid identifier" ou "does not exist", o problema é a
-- query, não a conta -- e a correção vale ser commitada, porque o objetivo
-- deste arquivo é justamente não deixar pergunta sem resposta verificável.

-- ── RESULTADO DA PRIMEIRA EXECUÇÃO (2026-08-11, como ACCOUNTADMIN) ──────
--
-- Passos 1 e 2: VAZIOS. Como ACCOUNTADMIN isso deixa de ser ambíguo e vira
-- resposta: não existe Resource Monitor nenhum nesta conta, nem no nível de
-- conta nem em lugar algum. O passo 3 confirmou resource_monitor = null no
-- CDC_WH. As três camadas onde uma trava poderia existir estão vazias.
--
-- A hipótese dos dois monitores, que motivou este arquivo, estava mal
-- formulada. Não eram dois coexistindo com propósitos diferentes -- era
-- nenhum. O cdc_poc_monitor seria criado por scripts/snowflake_setup.sql,
-- que também não existia; foi escrito na mesma data e ainda não rodou.
--
-- Passos 4 e 5: 1,72 crédito no CDC_WH desde 2026-08-06, com dois dias
-- consecutivos em zero absoluto. Os dois bateram entre si na quarta casa
-- decimal para o dia corrente, então ACCOUNT_USAGE não estava defasado.
--
-- ── Ação recomendada após rodar este script ─────────────────────────────
--
-- A recomendação original supunha encontrar um monitor mal configurado. Como
-- não há monitor algum, ela virou outra coisa: CRIAR o que nunca existiu,
-- rodando scripts/snowflake_setup.sql antes de qualquer demo ao vivo.
--
-- Se numa execução futura aparecer QUALQUER monitor com FREQUENCY = NEVER
-- ativo e vinculado ao CDC_WH, a orientação antiga volta a valer: ele não
-- reseta sozinho e não protegeria de vazamento contínuo. Substitua ou remova:
--   ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = cdc_poc_monitor;
--   ALTER ACCOUNT UNSET RESOURCE_MONITOR;  -- se aplicável, e após revisão
