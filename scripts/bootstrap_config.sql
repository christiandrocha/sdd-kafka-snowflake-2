-- ──────────────────────────────────────────────────────────────────────────
-- bootstrap_config.sql
-- Cria o schema CDC_POC.CONFIG e as tabelas de controle que o código do
-- Dagster e do dbt leem em runtime.
--
-- MOTIVO: `dagster/pipeline/sensors.py`, `scripts/sync_metadata.py` e o asset
-- `log_processing_results` em `dagster/pipeline/assets.py` consultam
-- CONFIG.TABLE_METADATA e CONFIG.PROCESSING_LOG, e nenhum SQL deste repo as
-- criava — só `scripts/streams_and_tasks.sql`, que cria PENDING_RUNS e
-- STREAM_CONSUMPTION_LOG mas pressupõe o schema CONFIG já existente.
--
-- ORDEM DE EXECUÇÃO:
--   1. bootstrap_config.sql   (este arquivo — cria o schema CONFIG)
--   2. streams_and_tasks.sql  (cria PENDING_RUNS/STREAM_CONSUMPTION_LOG nele)
--
-- COMO RODAR: com ACCOUNTADMIN, como foi feito em `create_readonly_role.sql`.
-- Os GRANTs no fim exigem MANAGE GRANTS ou ownership, e o CREATE SCHEMA exige
-- CREATE SCHEMA em CDC_POC — a CDC_ROLE pode não ter esse privilégio.
-- As identidades de serviço (DAGSTER_SERVICE_USER, CURSOR_MCP_USER,
-- DATA_AGENTS_MCP_USER) não devem rodar este script.
--
-- SOBRE O CDC_ROLE_RO: não há GRANT para ela aqui de propósito.
-- `create_readonly_role.sql` já concedeu SELECT ON FUTURE TABLES e USAGE ON
-- FUTURE SCHEMAS em toda a CDC_POC, então as tabelas criadas abaixo ficam
-- legíveis para ela automaticamente. Um GRANT explícito aqui seria redundante
-- e criaria dois lugares para manter a mesma decisão.
--
-- NÃO TESTADO CONTRA A CONTA REAL — escrito a partir do bootstrap do projeto
-- anterior, com o seed reduzido de 20 para 10 domínios.
-- ──────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS CDC_POC.CONFIG;

-- ── TABLE_METADATA ────────────────────────────────────────────────────────
-- Fonte da verdade sobre estratégia de CDC por domínio. Lida pelo
-- registry_new_subject_sensor (para saber o que já está sincronizado) e pela
-- macro dbt get_table_config().
CREATE TABLE IF NOT EXISTS CDC_POC.CONFIG.TABLE_METADATA (
    table_name          VARCHAR(100)  NOT NULL,
    topic               VARCHAR(200)  NOT NULL,
    table_type          VARCHAR(20)   NOT NULL,
    cdc_strategy        VARCHAR(20)   NOT NULL,
    unique_key          VARCHAR(100),
    active              BOOLEAN       NOT NULL DEFAULT true,
    registered_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source              VARCHAR(50)   NOT NULL,
    previous_strategy   VARCHAR(20),
    changed_by          VARCHAR(100),
    notes               VARCHAR(500),
    CONSTRAINT pk_table_metadata PRIMARY KEY (table_name)
);

-- ── METADATA_HISTORY ──────────────────────────────────────────────────────
-- Trilha de auditoria das mudanças em TABLE_METADATA (append-only).
CREATE TABLE IF NOT EXISTS CDC_POC.CONFIG.METADATA_HISTORY (
    history_id    NUMBER        AUTOINCREMENT PRIMARY KEY,
    table_name    VARCHAR(100)  NOT NULL,
    changed_at    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    changed_by    VARCHAR(100)  NOT NULL,
    change_type   VARCHAR(20)   NOT NULL,
    field_changed VARCHAR(100),
    old_value     VARCHAR(500),
    new_value     VARCHAR(500),
    source        VARCHAR(50)   NOT NULL
);

-- ── PROCESSING_LOG ────────────────────────────────────────────────────────
-- Uma linha por model dbt por execução. Escrita pelo asset
-- log_processing_results, que lê target/run_results.json depois do dbt run.
CREATE TABLE IF NOT EXISTS CDC_POC.CONFIG.PROCESSING_LOG (
    log_id            NUMBER        AUTOINCREMENT PRIMARY KEY,
    table_name        VARCHAR(100)  NOT NULL,
    layer             VARCHAR(20)   NOT NULL,
    dbt_model         VARCHAR(200)  NOT NULL,
    dbt_invocation_id VARCHAR(100),
    run_id            VARCHAR(100),
    status            VARCHAR(20)   NOT NULL,
    rows_processed    NUMBER        DEFAULT 0,
    started_at        TIMESTAMP_NTZ,
    finished_at       TIMESTAMP_NTZ,
    duration_seconds  NUMBER(10,3),
    error_message     VARCHAR(2000),
    triggered_by      VARCHAR(100),
    logged_at         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ── Grants para a role de escrita do pipeline ─────────────────────────────
GRANT USAGE  ON SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;
-- CREATE TABLE/PROCEDURE: streams_and_tasks.sql roda como CDC_ROLE e cria
-- PENDING_RUNS, STREAM_CONSUMPTION_LOG e SP_GATE_DOMAIN dentro deste schema.
-- Sem estes dois, o passo 2 da ordem de execução acima falha no primeiro DDL
-- com `003001 (42501) ... must have CREATE TABLE granted on SCHEMA`.
GRANT CREATE TABLE, CREATE PROCEDURE ON SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES    IN SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA CDC_POC.CONFIG TO ROLE CDC_ROLE;

-- ── Seed do TABLE_METADATA — só os 10 domínios Tier 1 ─────────────────────
-- O projeto anterior semeava 20. Os 10 Tier 2 (payments, gps_events,
-- order_status, routes, receipts, support_tickets, products, menu_sections,
-- ratings, inventory) saíram por DEFINE_MIGRACAO_INGESTAO_V4 — não são mais
-- ingeridos, não têm tabela no Postgres fonte (scripts/init.sql) nem Stream
-- no Snowflake (scripts/streams_and_tasks.sql).
--
-- Os 10 abaixo batem 1:1 com esses dois arquivos. `table_type` e
-- `cdc_strategy` são os mesmos valores do bootstrap anterior, para não
-- introduzir mudança de comportamento junto com a redução de escopo.
MERGE INTO CDC_POC.CONFIG.TABLE_METADATA AS tgt
USING (
    SELECT table_name, topic, table_type, cdc_strategy, unique_key, source, changed_by, notes
    FROM VALUES
        -- Event sourcing (append por natureza; upsert por PK para idempotência)
        ('payment_events',  'pg.public.payment_events',  'fact',   'upsert', 'event_id',      'manual', 'bootstrap', 'Eventos do ciclo de pagamento. Campo `event` JSONB aninhado.'),
        ('search_events',   'pg.public.search_events',   'log',    'upsert', 'search_id',     'manual', 'bootstrap', 'Buscas do usuário.'),
        ('recommendations', 'pg.public.recommendations', 'log',    'upsert', 'event_id',      'manual', 'bootstrap', 'Eventos de recomendação de ML.'),

        -- Entidades (upsert por PK — snapshot de estado)
        ('orders',          'pg.public.orders',          'entity', 'upsert', 'order_id',      'manual', 'bootstrap', 'Tabela hub. Liga os domínios via *_key (CPF, CNPJ, driver_id).'),
        ('driver_shifts',   'pg.public.driver_shifts',   'entity', 'upsert', 'shift_id',      'manual', 'bootstrap', 'Desempenho por turno do entregador.'),
        ('users_mongo',     'pg.public.users_mongo',     'entity', 'upsert', 'uuid',          'manual', 'bootstrap', 'Usuários (origem MongoDB). CPF = user_key em orders.'),
        ('users_mssql',     'pg.public.users_mssql',     'entity', 'upsert', 'uuid',          'manual', 'bootstrap', 'Perfil estendido (origem MSSQL). Mesmo CPF.'),
        ('restaurants',     'pg.public.restaurants',     'entity', 'upsert', 'uuid',          'manual', 'bootstrap', 'Restaurantes (origem MySQL). CNPJ = restaurant_key em orders.'),
        ('drivers',         'pg.public.drivers',         'entity', 'upsert', 'uuid',          'manual', 'bootstrap', 'Entregadores. driver_id = driver_key em orders.'),

        -- Fato de alto volume
        ('order_items',     'pg.public.order_items',     'fact',   'upsert', 'order_item_id', 'manual', 'bootstrap', 'Itens de linha do pedido. Maior tabela do projeto.')
    AS src (table_name, topic, table_type, cdc_strategy, unique_key, source, changed_by, notes)
) AS src
ON tgt.table_name = src.table_name
WHEN NOT MATCHED THEN INSERT (table_name, topic, table_type, cdc_strategy, unique_key, source, changed_by, notes)
    VALUES (src.table_name, src.topic, src.table_type, src.cdc_strategy, src.unique_key, src.source, src.changed_by, src.notes);

-- ── Desativação dos domínios Tier 2 herdados ──────────────────────────────
-- LEIA ANTES DE RODAR. A conta CDC_POC é a mesma usada pelo projeto anterior.
-- Se o bootstrap de 20 domínios já rodou nela algum dia, TABLE_METADATA tem
-- 10 linhas Tier 2 que o MERGE acima não toca (ele só faz INSERT do que falta).
-- Deixá-las ativas faz o registry_new_subject_sensor considerar sincronizados
-- domínios que não existem mais em lugar nenhum do pipeline.
--
-- `active = FALSE` em vez de DELETE: é reversível, preserva a linha para
-- auditoria, e a macro get_table_config() já filtra por `active`.
-- Para reverter: UPDATE ... SET active = TRUE WHERE table_name IN (...).
UPDATE CDC_POC.CONFIG.TABLE_METADATA
SET active     = FALSE,
    updated_at = CURRENT_TIMESTAMP(),
    changed_by = 'bootstrap_config_v2',
    notes      = 'Tier 2 — fora de escopo desde MIGRACAO_INGESTAO_V4. Sem tabela no Postgres fonte, sem Stream no Snowflake.'
WHERE table_name IN (
    'payments', 'gps_events', 'order_status', 'routes', 'receipts',
    'support_tickets', 'products', 'menu_sections', 'ratings', 'inventory'
);

-- ── Verificação ───────────────────────────────────────────────────────────
-- Esperado: 10 linhas ativas, e 0 ou 10 inativas (10 se o bootstrap antigo
-- já tinha rodado nesta conta; 0 se é conta limpa).
SELECT active, COUNT(*) AS dominios
FROM CDC_POC.CONFIG.TABLE_METADATA
GROUP BY active
ORDER BY active DESC;

SELECT table_name, table_type, cdc_strategy, unique_key, active
FROM CDC_POC.CONFIG.TABLE_METADATA
ORDER BY active DESC, table_type, table_name;

-- Confere que os 3 objetos existem antes de rodar streams_and_tasks.sql:
SHOW TABLES IN SCHEMA CDC_POC.CONFIG;
