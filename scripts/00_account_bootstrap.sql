-- ------------------------------------------------------------------------
-- 00_account_bootstrap.sql
-- O grafo de objetos que todo o resto pressupõe.
--
-- PROBLEMA QUE ISTO RESOLVE
--
-- Até 2026-08-11 este projeto não podia ser reconstruído a partir do Git.
-- Um inventário do que cada script cria mostrou o buraco:
--
--   bootstrap_config.sql    -> schemas CONFIG, tabelas, procedure
--   streams_and_tasks.sql   -> 10 streams, 10 tasks, procedure
--   snowflake_setup.sql     -> resource monitor, retenção
--   create_readonly_role.sql-> CDC_ROLE_RO
--
-- Nenhum deles cria o database `CDC_POC`, o warehouse `CDC_WH`, o papel
-- `CDC_ROLE` ou os três usuários de serviço com suas chaves públicas. Tudo
-- isso existia apenas na conta e na cabeça de quem a montou. Se a conta
-- sumisse, o projeto não voltava — e é também por isso que o `environment:
-- production` do deploy.yml nunca poderia funcionar: não havia como criar o
-- ambiente que ele pressupõe.
--
-- ORDEM DE EXECUÇÃO NUMA CONTA NOVA
--
--   1. scripts/00_account_bootstrap.sql   <- este arquivo, como ACCOUNTADMIN
--   2. scripts/bootstrap_config.sql       (schema CONFIG + TABLE_METADATA)
--   3. scripts/streams_and_tasks.sql      (gate de disparo)
--   4. scripts/snowflake_setup.sql        (resource monitor + Time Travel)
--   5. scripts/create_readonly_role.sql   (CDC_ROLE_RO para ferramenta externa)
--   6. scripts/verify_governance.sql      (confere o que os 5 anteriores fizeram)
--
-- COMO RODAR: Snowsight, com ACCOUNTADMIN, passo a passo.
--
-- SEGURANÇA: todo comando aqui é `IF NOT EXISTS` ou `ALTER`. Nada usa
-- `CREATE OR REPLACE`, de propósito — rodar este arquivo por engano numa
-- conta que já existe não deve destruir dado. Se precisar mudar algo que já
-- existe, use `ALTER` explicitamente e saiba o que está fazendo.
--
-- ESTE SCRIPT NUNCA FOI EXECUTADO. Foi escrito em 2026-08-11 a partir do
-- estado lido da conta existente (privilégios, parâmetros do warehouse,
-- papéis e grants), não de uma execução em conta limpa. Trate a primeira
-- execução como um teste: rode passo a passo e confira cada retorno.
-- ------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

SELECT CURRENT_ROLE() AS papel_ativo, CURRENT_ACCOUNT() AS conta;


-- ── 1. Warehouse ─────────────────────────────────────────────────────────
--
-- Parâmetros medidos e ajustados em 2026-08-11, não defaults:
--   AUTO_SUSPEND = 60      -- o warehouse fica ligado 6h/dia de trabalho por
--                             ~67 min de fatura; ver "What it actually costs"
--   QUERY_ACCELERATION off -- nunca engatou em 30 dias de histórico
CREATE WAREHOUSE IF NOT EXISTS CDC_WH
    WAREHOUSE_SIZE            = 'X-SMALL'
    AUTO_SUSPEND              = 60
    AUTO_RESUME               = TRUE
    INITIALLY_SUSPENDED       = TRUE
    ENABLE_QUERY_ACCELERATION = FALSE
    COMMENT = 'Warehouse do pipeline CDC. AUTO_SUSPEND=60 e deliberado.';


-- ── 2. Database e schemas ────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS CDC_POC
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Pipeline CDC PostgreSQL -> Kafka -> Snowflake.';

CREATE SCHEMA IF NOT EXISTS CDC_POC.BRONZE COMMENT = 'Landing do sink + models Bronze. Append-only.';
CREATE SCHEMA IF NOT EXISTS CDC_POC.SILVER COMMENT = 'Resolucao CDC. Uma linha por entidade.';
CREATE SCHEMA IF NOT EXISTS CDC_POC.GOLD   COMMENT = 'Agregacoes analiticas.';
CREATE SCHEMA IF NOT EXISTS CDC_POC.CONFIG COMMENT = 'TABLE_METADATA, PENDING_RUNS, procedures de gate.';

-- Drop do schema PUBLIC, criado por default e não usado por nada aqui.
DROP SCHEMA IF EXISTS CDC_POC.PUBLIC;


-- ── 3. Papel de serviço ──────────────────────────────────────────────────
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS CDC_ROLE
    COMMENT = 'Papel de escrita do pipeline. Dagster, dbt e o sink do Kafka.';

USE ROLE ACCOUNTADMIN;

GRANT USAGE     ON WAREHOUSE CDC_WH  TO ROLE CDC_ROLE;
GRANT OPERATE   ON WAREHOUSE CDC_WH  TO ROLE CDC_ROLE;
GRANT USAGE     ON DATABASE  CDC_POC TO ROLE CDC_ROLE;

GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK,
      CREATE STAGE, CREATE PROCEDURE, CREATE FILE FORMAT
    ON SCHEMA CDC_POC.BRONZE TO ROLE CDC_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK,
      CREATE STAGE, CREATE PROCEDURE, CREATE FILE FORMAT
    ON SCHEMA CDC_POC.SILVER TO ROLE CDC_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK,
      CREATE STAGE, CREATE PROCEDURE, CREATE FILE FORMAT
    ON SCHEMA CDC_POC.GOLD   TO ROLE CDC_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE
    ON SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;

-- Objetos que já existirem, e os que vierem a existir.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL    TABLES IN DATABASE CDC_POC TO ROLE CDC_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE CDC_POC TO ROLE CDC_ROLE;
GRANT SELECT ON ALL    VIEWS IN DATABASE CDC_POC TO ROLE CDC_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE CDC_POC TO ROLE CDC_ROLE;

-- As gate tasks rodam sob este papel; sem isto elas nem sequer iniciam.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CDC_ROLE;


-- ── 4. Identidades de serviço ────────────────────────────────────────────
--
-- TYPE = SERVICE: sem senha, isento de política de MFA, só key-pair. Os três
-- existem separados de propósito, para que o log de auditoria distinga quem
-- fez o quê — não é cerimônia, é a diferença entre "alguém escreveu na
-- Bronze" e "o Dagster escreveu na Bronze".
--
-- SUBSTITUA os corpos abaixo pelas suas chaves públicas (sem cabeçalho PEM,
-- sem quebras de linha). Geração do par:
--
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -outform PEM \
--       -nocrypt -out keys/dagster_key.p8
--   openssl rsa -in keys/dagster_key.p8 -pubout -out keys/dagster_key.pub
--   grep -v '^-----' keys/dagster_key.pub | tr -d '\n'
--
-- ROTAÇÃO: use os dois slots (RSA_PUBLIC_KEY_2 primeiro, troque os
-- consumidores, depois sobrescreva o slot 1). Procedimento completo e a
-- lista dos consumidores estão no README.

USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS DAGSTER_SERVICE_USER
    TYPE              = SERVICE
    DEFAULT_ROLE      = CDC_ROLE
    DEFAULT_WAREHOUSE = CDC_WH
    RSA_PUBLIC_KEY    = '<COLE_AQUI_A_PUBLICA_DO_DAGSTER>'
    COMMENT           = 'Dagster, dbt e o Snowflake Sink do Kafka Connect.';

CREATE USER IF NOT EXISTS DATA_AGENTS_MCP_USER
    TYPE              = SERVICE
    DEFAULT_ROLE      = CDC_ROLE
    DEFAULT_WAREHOUSE = CDC_WH
    RSA_PUBLIC_KEY    = '<COLE_AQUI_A_PUBLICA_DO_DATA_AGENTS>'
    COMMENT           = 'Agentes de dados com escrita.';

CREATE USER IF NOT EXISTS CURSOR_MCP_USER
    TYPE              = SERVICE
    DEFAULT_ROLE      = CDC_ROLE_RO
    DEFAULT_WAREHOUSE = CDC_WH
    RSA_PUBLIC_KEY    = '<COLE_AQUI_A_PUBLICA_DO_CURSOR>'
    COMMENT           = 'Exploracao somente leitura. CDC_ROLE_RO vem do passo 5.';

USE ROLE SECURITYADMIN;

GRANT ROLE CDC_ROLE TO USER DAGSTER_SERVICE_USER;
GRANT ROLE CDC_ROLE TO USER DATA_AGENTS_MCP_USER;
-- CURSOR_MCP_USER recebe CDC_ROLE_RO no create_readonly_role.sql (passo 5).
-- NÃO conceda CDC_ROLE a ele: o ponto daquele script é justamente que o
-- controle de leitura seja do banco, não de um yaml de configuração do MCP.


-- ── 5. Verificação ───────────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

SHOW WAREHOUSES LIKE 'CDC_WH';
SHOW DATABASES  LIKE 'CDC_POC';
SHOW SCHEMAS IN DATABASE CDC_POC;
SHOW USERS LIKE '%_USER';
SHOW GRANTS TO ROLE CDC_ROLE;

-- Confere que as chaves entraram: RSA_PUBLIC_KEY_FP deve bater com o
-- fingerprint da chave local correspondente.
DESC USER DAGSTER_SERVICE_USER;

-- Próximo passo: scripts/bootstrap_config.sql
