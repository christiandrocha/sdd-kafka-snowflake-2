{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: buscas do usuario, uma linha por search_id.
--
-- ATENCAO A UMA TENSAO DE CONFIG: este dominio esta registrado com
-- table_type='log' mas cdc_strategy='upsert' -- tanto no seed de
-- scripts/bootstrap_config.sql quanto no fallback de get_table_config().
-- Consequencia pratica: resolve_cdc trata a busca como entidade, nao como
-- log. Um DELETE na origem some com a linha aqui, em vez de preserva-la
-- como registro historico (que e o que a estrategia 'log' faria).
--
-- Isso e intencional para o dominio? Se a intencao era log-de-auditoria,
-- muda-se cdc_strategy para 'log' em CONFIG.TABLE_METADATA e este modelo
-- passa a manter tudo, sem tocar em SQL nenhum. Deixado como esta porque
-- estrategia de dominio e decisao de negocio, nao de refactor.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_search_events')) }}
