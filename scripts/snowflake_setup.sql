-- ------------------------------------------------------------------------
-- snowflake_setup.sql
-- Governança de conta: freio de crédito e retenção explícita.
--
-- PROBLEMA QUE ISTO RESOLVE
--
-- Este arquivo era referenciado pelo README, pelo .github/workflows/deploy.yml
-- e pela Decision "qual Resource Monitor é o canônico" do
-- DESIGN_GOVERNANCA_CUSTO_DISPARO -- e não existia. A documentação do projeto
-- afirmava, em três lugares, que a conta tinha um freio de emergência de
-- crédito. Em 2026-08-11 o verify_governance.sql foi rodado como ACCOUNTADMIN
-- pela primeira vez e mostrou o seguinte:
--
--   SHOW RESOURCE MONITORS                          -> zero linhas
--   SHOW PARAMETERS LIKE 'RESOURCE_MONITOR' IN ACCOUNT -> vazio
--   SHOW WAREHOUSES LIKE 'CDC_WH' -> resource_monitor = null
--
-- As três camadas onde uma trava de custo poderia existir estavam vazias.
-- Não havia dois monitores, como a hipótese supunha. Não havia nenhum.
-- Este script cria o que a documentação já descrevia.
--
-- COMO RODAR: Snowsight, com ACCOUNTADMIN. Nenhuma das identidades de serviço
-- de keys/ alcança isso -- as três têm um papel só (CDC_ROLE ou CDC_ROLE_RO),
-- e o erro que elas recebem ao tentar é "Insufficient privileges to operate
-- on account".
--
-- EXECUTADO PELA PRIMEIRA VEZ em 2026-08-11 09:54, e o resultado confere:
--
--   SHOW RESOURCE MONITORS -> CDC_POC_MONITOR, cota 20, usados 0,00,
--                             level WAREHOUSE, MONTHLY, notify 50%/75%,
--                             suspend 90%, suspend_immediate 100%
--   CDC_WH.resource_monitor -> CDC_POC_MONITOR (era null)
--   SHOW PARAMETERS ... IN DATABASE CDC_POC -> level = DATABASE (era herdado)
--
-- `used_credits = 0,00` está certo e não é sintoma: o monitor conta a partir
-- do START_TIMESTAMP dele, então os 1,72 crédito anteriores não entram na cota.
--
-- ARMADILHA DE VERIFICAÇÃO, custou uma leitura errada no dia: sob um papel
-- sem privilégio sobre o monitor, `SHOW WAREHOUSES` devolve
-- `resource_monitor = null` mesmo com o vínculo existindo. É o mesmo problema
-- que o verify_governance.sql descreve para "zero linhas", invertido -- null
-- sob papel fraco não distingue "não há vínculo" de "não enxergo". Confirme
-- como ACCOUNTADMIN.
--
-- Rode passo a passo, conferindo cada retorno -- não cole o arquivo inteiro
-- de uma vez. No Snowsight, executar tudo de uma vez mostra só o resultado do
-- último comando, e os RESULT_SCAN intermediários passam despercebidos.
-- ------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

-- 0. Confirma o papel. Se isto não devolver ACCOUNTADMIN, pare: os comandos
--    abaixo falham com erro de privilégio, e o CREATE RESOURCE MONITOR falha
--    DEPOIS de você já ter achado que rodou.
SELECT CURRENT_ROLE() AS papel_ativo, CURRENT_ACCOUNT() AS conta;


-- ── 1. Resource Monitor ──────────────────────────────────────────────────
--
-- Parâmetros vindos do DESIGN_GOVERNANCA_CUSTO_DISPARO: 20 créditos,
-- FREQUENCY MONTHLY, vinculado ao warehouse (não à conta).
--
-- Dimensionamento contra o consumo real medido em 2026-08-11: a conta gastou
-- ≈ 1,72 crédito em cinco dias de vida, sendo 1,12 no dia do build das três
-- camadas. Vinte créditos por mês é folgado para esse padrão de uso e ainda
-- assim limita o estrago de um vazamento contínuo -- que é o cenário real de
-- risco aqui, não o build.
--
-- Os TRIGGERS aplicam a convenção de severidade ratificada em 2026-08-11
-- (Decision 3 do DESIGN_GOVERNANCA_QUALIDADE_DADOS). Aquela decisão nomeia
-- três contextos e o TRIGGER_ACTION era o único nunca exercitado; é aqui que
-- ele passa a existir. Pelo critério dela:
--   NOTIFY           -> dado/serviço degradado mas previsível (avisar)
--   SUSPEND          -> deixa terminar o que está rodando, recusa novo
--   SUSPEND_IMMEDIATE-> mata o que está em voo; estado inconsistente é
--                       preferível à conta continuar queimando
CREATE RESOURCE MONITOR IF NOT EXISTS cdc_poc_monitor
  WITH
    CREDIT_QUOTA    = 20
    FREQUENCY       = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 50  PERCENT DO NOTIFY
      ON 75  PERCENT DO NOTIFY
      ON 90  PERCENT DO SUSPEND
      ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- CREATE ... IF NOT EXISTS não altera um monitor que já exista. Para mudar a
-- cota depois, use ALTER -- CREATE OR REPLACE zeraria o consumo acumulado do
-- período e o freio recomeçaria do zero:
--   ALTER RESOURCE MONITOR cdc_poc_monitor SET CREDIT_QUOTA = 40;

-- Vincula ao warehouse do pipeline. Sem esta linha o monitor existe e não
-- protege nada -- foi exatamente essa a confusão que a verificação desfez.
ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = cdc_poc_monitor;


-- ── 2. Notificação ───────────────────────────────────────────────────────
--
-- ARMADILHA: TRIGGERS com DO NOTIFY não entregam nada sem NOTIFY_USERS, e
-- NOTIFY_USERS só funciona para usuários com e-mail VERIFICADO. Os três
-- usuários de serviço têm EMAIL = null e IS_EMAIL_VERIFIED = false
-- (verificado no DESC USER de 2026-08-11), então não adianta listá-los: o
-- monitor suspende no gatilho de 90% sem nunca ter avisado nos de 50 e 75.
--
-- O modo de falha aqui é pior que não configurar nada: o comando é aceito sem
-- erro com e-mail não verificado, o monitor fica com cara de configurado, e
-- você só descobre que não há aviso quando o warehouse suspende em 90%.
--
-- Numa conta nova, verifique o e-mail ANTES desta linha -- Snowsight, canto
-- inferior esquerdo, My profile, e clique no link que chega por e-mail. Só o
-- Snowsight dispara a verificação; `ALTER USER ... SET EMAIL` preenche o
-- campo e deixa IS_EMAIL_VERIFIED em false.
ALTER RESOURCE MONITOR cdc_poc_monitor SET NOTIFY_USERS = ('CHRISTIANDROCHA');

-- Confirma antes de confiar no aviso. IS_EMAIL_VERIFIED = true no usuário
-- listado acima é a única evidência de que o NOTIFY não é decorativo.
DESC USER CHRISTIANDROCHA;


-- ── 3. Time Travel explícito ─────────────────────────────────────────────
--
-- A Decision 1 do DESIGN_GOVERNANCA_QUALIDADE_DADOS lista o Time Travel de
-- 1 dia na Bronze como uma das quatro coisas que dependem da invariante
-- append-only. A verificação de 2026-08-11 confirmou retention_time = 1 nos
-- 20 objetos do schema -- mas por DEFAULT, não por configuração: o dbt
-- materializa como TRANSIENT (cujo teto é 1 dia) e as tabelas de landing
-- ficam no default da conta. Nenhum retention é declarado no projeto.
--
-- A garantia é real hoje e mudaria em silêncio se algum default mudasse.
-- Esta linha a torna explícita, no nível do database, herdada por schemas e
-- tabelas que não sobrescrevam.
--
-- NOTA DE EDIÇÃO: em Standard Edition o máximo é 1 dia; em Enterprise vai a
-- 90. Se a conta for Enterprise e você quiser mais que 1 dia na Bronze,
-- lembre que tabelas TRANSIENT ignoram qualquer valor acima de 1 -- os
-- BRONZE_* do dbt continuariam em 1 de qualquer forma.
ALTER DATABASE CDC_POC SET DATA_RETENTION_TIME_IN_DAYS = 1;


-- ── 4. Verificação ───────────────────────────────────────────────────────
--
-- O resultado esperado é o oposto do que a execução de 2026-08-11 devolveu:
-- uma linha aqui, e o nome do monitor no campo do warehouse.
SHOW RESOURCE MONITORS;

SHOW WAREHOUSES LIKE 'CDC_WH';
SELECT
    "name"             AS warehouse,
    "resource_monitor" AS monitor_do_warehouse,
    "auto_suspend"     AS auto_suspend_s
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SHOW DATABASES LIKE 'CDC_POC';
SELECT
    "name"            AS banco,
    "retention_time"  AS retention_dias
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Depois disto, `scripts/verify_governance.sql` deve devolver monitor no
-- passo 1, e o passo 3 deve mostrar `monitor_do_warehouse = cdc_poc_monitor`
-- em vez de null.


-- ── O que este script deliberadamente NÃO faz ────────────────────────────
--
-- 1. Não define monitor no nível da CONTA. O DESIGN escolheu o nível de
--    warehouse, e a diferença importa: um monitor de warehouse só enxerga o
--    CDC_WH. Consumo serverless (Snowpipe Streaming, Tasks sem warehouse,
--    cloud services) NÃO passa por ele e continua sem teto. Se quiser cobrir
--    a conta inteira, é outro monitor, e a linha é:
--      ALTER ACCOUNT SET RESOURCE_MONITOR = <nome>;
--    Pense antes: um monitor de conta mal dimensionado suspende TUDO,
--    inclusive a sua própria sessão de investigação.
--
-- 2. Não mexe em ENABLE_QUERY_ACCELERATION. O CDC_WH está com QAS ligado e
--    scale factor 2 (lido em 2026-08-11), um caminho que fatura além da
--    compute do warehouse. Neste workload é quase certamente dormente -- as
--    varreduras são pequenas demais para engatar -- e desligar sem medir
--    seria mexer no que não se mediu. Fica registrado como candidato:
--      ALTER WAREHOUSE CDC_WH SET ENABLE_QUERY_ACCELERATION = FALSE;
--
-- 3. Não cria database, schemas, warehouse nem papéis. Tudo isso já existe
--    nesta conta desde 2026-08-06, e recriar teria efeito destrutivo. Papéis
--    de leitura ficam em create_readonly_role.sql; Streams e Tasks, em
--    streams_and_tasks.sql.
