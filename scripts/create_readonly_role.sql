-- create_readonly_role.sql
--
-- PROBLEMA QUE ISTO RESOLVE
--
-- CURSOR_MCP_USER, DAGSTER_SERVICE_USER e DATA_AGENTS_MCP_USER usam todos
-- a mesma CDC_ROLE, que tem CREATE TABLE / CREATE STREAM / CREATE TASK /
-- CREATE STAGE / MODIFY em CDC_POC.BRONZE, SILVER e GOLD. As três
-- identidades são equivalentes no Snowflake.
--
-- O .cursor/snowflake_tools_config.yaml declara a conexão do Cursor como
-- somente leitura (Create: false, Insert: false, Merge: false), mas esse
-- controle é do servidor MCP, não do banco. Qualquer coisa que use
-- keys/cursor_key.p8 por outro caminho — snow CLI, script Python — ignora
-- o yaml e escreve nos três schemas.
--
-- Este script cria CDC_ROLE_RO (leitura de verdade) e move o
-- CURSOR_MCP_USER para ela, tirando a CDC_ROLE dele.
--
-- COMO RODAR: Snowsight, com ACCOUNTADMIN. Nenhum dos usuários de serviço
-- tem USERADMIN/SECURITYADMIN — verificado, todos recebem
-- "Requested role ... is not assigned to the executing user".
--
-- NÃO TESTADO AO VIVO: escrito a partir dos grants lidos em 2026-08-07,
-- mas não executado por falta de privilégio.

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS CDC_ROLE_RO
  COMMENT = 'Leitura de CDC_POC. Superficie de exploracao (Cursor MCP). Sem DDL/DML.';

USE ROLE ACCOUNTADMIN;

-- ── Compute e navegação ───────────────────────────────────────────────────────
GRANT USAGE ON WAREHOUSE CDC_WH   TO ROLE CDC_ROLE_RO;
GRANT USAGE ON DATABASE  CDC_POC  TO ROLE CDC_ROLE_RO;

GRANT USAGE ON ALL SCHEMAS    IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;

-- ── Leitura ───────────────────────────────────────────────────────────────────
GRANT SELECT ON ALL TABLES            IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT SELECT ON FUTURE TABLES         IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT SELECT ON ALL VIEWS             IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT SELECT ON FUTURE VIEWS          IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT SELECT ON ALL DYNAMIC TABLES    IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;

-- SHOW TASKS / SHOW STREAMS: o yaml libera Command e Describe para
-- exploracao. Sem MONITOR o SHOW volta vazio em vez de dar erro, o que
-- confunde na hora de investigar pipeline parado.
GRANT MONITOR ON ALL TASKS    IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;
GRANT MONITOR ON FUTURE TASKS IN DATABASE CDC_POC TO ROLE CDC_ROLE_RO;

-- ── Troca de identidade do Cursor ─────────────────────────────────────────────
GRANT ROLE CDC_ROLE_RO TO USER CURSOR_MCP_USER;
ALTER USER CURSOR_MCP_USER SET DEFAULT_ROLE = CDC_ROLE_RO;
ALTER USER CURSOR_MCP_USER SET DEFAULT_WAREHOUSE = CDC_WH;

-- ESTA é a linha que fecha a brecha. Sem ela o CURSOR_MCP_USER continua
-- podendo escolher CDC_ROLE e escrever.
REVOKE ROLE CDC_ROLE FROM USER CURSOR_MCP_USER;

-- ── CDC_GOVERNANCE_RO — leitura de auditoria ─────────────────────────────────
--
-- Responde "foi o pipeline (DAGSTER_SERVICE_USER) ou foi um agente de IA
-- (DATA_AGENTS_MCP_USER)?". Essa pergunta e cross-user e por isso NAO tem
-- resposta no INFORMATION_SCHEMA, que so enxerga o proprio usuario e guarda
-- 7 dias. So o ACCOUNT_USAGE responde, com 365 dias e ~45 min de latencia.
--
-- REGRA DE DESENHO: esta role vai para o usuario PESSOAL e para mais
-- ninguem. Nenhuma das tres identidades de servico recebe. Quem e auditado
-- nao le o log de auditoria — e ACCOUNT_USAGE.QUERY_HISTORY guarda o TEXTO
-- INTEGRAL de toda query da conta, entao concede-la a um agente de IA
-- transformaria o log em superficie de vazamento.
--
-- Tambem NAO tem SELECT em CDC_POC: investigar e ver quem tocou o que, nao
-- o conteudo. Para o dado, use a conexao `cursor`.

CREATE ROLE IF NOT EXISTS CDC_GOVERNANCE_RO
  COMMENT = 'Leitura de ACCOUNT_USAGE para auditoria de atribuicao. Somente usuario pessoal.';

GRANT USAGE ON WAREHOUSE CDC_WH TO ROLE CDC_GOVERNANCE_RO;

-- Database roles recortadas, NAO "GRANT IMPORTED PRIVILEGES ON DATABASE
-- SNOWFLAKE", que abriria o schema inteiro hoje e tudo que a Snowflake
-- adicionar amanha. As quatro existem nesta conta (SHOW DATABASE ROLES,
-- 2026-08-07) e nenhuma esta concedida a ninguem ainda.
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER      TO ROLE CDC_GOVERNANCE_RO;  -- QUERY_HISTORY, WAREHOUSE_METERING_HISTORY
GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE CDC_GOVERNANCE_RO;  -- ACCESS_HISTORY (objeto tocado por query)
GRANT DATABASE ROLE SNOWFLAKE.SECURITY_VIEWER   TO ROLE CDC_GOVERNANCE_RO;  -- LOGIN_HISTORY, GRANTS_TO_USERS/ROLES
GRANT DATABASE ROLE SNOWFLAKE.MONITORING_VIEWER TO ROLE CDC_GOVERNANCE_RO;  -- TASK_HISTORY (ver ponto cego abaixo)

-- CONFIRMAR ANTES DE CONFIAR: o mapeamento view -> database role acima e
-- estimado, nao verificado. Rode e ajuste os quatro GRANTs conforme a saida:
--   SHOW GRANTS TO DATABASE ROLE SNOWFLAKE.USAGE_VIEWER;
--   SHOW GRANTS TO DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER;
--   SHOW GRANTS TO DATABASE ROLE SNOWFLAKE.SECURITY_VIEWER;
--   SHOW GRANTS TO DATABASE ROLE SNOWFLAKE.MONITORING_VIEWER;
-- Se alguma nao entregar QUERY_HISTORY / ACCESS_HISTORY / TASK_HISTORY,
-- remova o GRANT correspondente em vez de deixar acesso sem proposito.
--
-- PONTO CEGO QUE ISTO MITIGA: objetos criados pelo data-agents nascem com
-- OWNERSHIP da CDC_ROLE, nao do usuario. SHOW TASKS vai mostrar
-- owner = CDC_ROLE para tudo. A autoria so existe na query CREATE, no
-- QUERY_HISTORY, e a execucao posterior da Task roda no contexto do owner.
-- ACCESS_HISTORY + TASK_HISTORY sao o que reconstroi essa cadeia.
--
-- EDICAO: ACCESS_HISTORY exige Enterprise Edition ou superior. Em Standard
-- o GRANT passa silencioso e a view nao aparece.
--
-- CUSTO: consultar ACCOUNT_USAGE liga o CDC_WH e queima credito. E consulta
-- pontual de investigacao — nao deixe agente rodando isso em loop, ainda
-- mais com a conta trial e o resource monitor de verify_governance.sql.

GRANT ROLE CDC_GOVERNANCE_RO TO USER CHRISTIANDROCHA;

-- ── TYPE = SERVICE nas três identidades ───────────────────────────────────────
--
-- Os três estão como TYPE = 'PERSON' (confirmado via DESC USER). PERSON é
-- sujeito a policy de MFA; no dia que uma for aplicada na conta, os três
-- param de autenticar de uma vez e sem aviso — inclusive o pipeline de
-- produção. SERVICE é isento de MFA e não aceita senha, só key-pair, que
-- é exatamente como os três já funcionam.
--
-- ATENÇÃO: SET TYPE = SERVICE falha se o usuário tiver senha definida.
-- Se der "cannot be changed to SERVICE", rode o UNSET PASSWORD da linha
-- de cima antes. Isso remove o login por senha no Snowsight para esse
-- usuário — o que é o objetivo, mas confirme que você não depende dele
-- para debugar.

ALTER USER DAGSTER_SERVICE_USER  UNSET PASSWORD;
ALTER USER DATA_AGENTS_MCP_USER  UNSET PASSWORD;
ALTER USER CURSOR_MCP_USER       UNSET PASSWORD;

ALTER USER DAGSTER_SERVICE_USER  SET TYPE = SERVICE;
ALTER USER DATA_AGENTS_MCP_USER  SET TYPE = SERVICE;
ALTER USER CURSOR_MCP_USER       SET TYPE = SERVICE;

-- ── Verificação ───────────────────────────────────────────────────────────────
SHOW GRANTS TO USER CURSOR_MCP_USER;   -- deve listar CDC_ROLE_RO e NAO CDC_ROLE
SHOW GRANTS TO ROLE CDC_ROLE_RO;       -- so USAGE / SELECT / MONITOR, nenhum CREATE

DESC USER DAGSTER_SERVICE_USER;        -- TYPE deve ser SERVICE
DESC USER DATA_AGENTS_MCP_USER;        -- idem
DESC USER CURSOR_MCP_USER;             -- idem, e DEFAULT_ROLE = CDC_ROLE_RO

-- Nenhuma identidade de servico pode ter CDC_GOVERNANCE_RO. Se qualquer uma
-- das tres aparecer aqui, a separacao auditor/auditado foi quebrada:
SHOW GRANTS OF ROLE CDC_GOVERNANCE_RO;  -- esperado: so CHRISTIANDROCHA

-- Depois de rodar: trocar role = "CDC_ROLE" para "CDC_ROLE_RO" em
-- [connections.cursor] no ~/.snowflake/config.toml e reiniciar o Cursor.
--
-- Teste negativo esperado, conectando como cursor:
--   CREATE TABLE CDC_POC.BRONZE._probe (x int);
--   -> 003001 (42501): Insufficient privileges to operate on schema 'BRONZE'
