{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: buscas do usuario, uma linha por search_id.
--
-- Ate 2026-08-10 este dominio estava registrado como table_type='log' com
-- cdc_strategy='upsert', o que era contraditorio: `log` descreve dominio que
-- preserva DELETE como registro historico, e `upsert` descarta DELETE.
-- Resolvido corrigindo a ETIQUETA, nao a estrategia -- a medicao na origem
-- mostrou 203 linhas para 203 chaves distintas e zero deletes, ou seja,
-- append-only, o mesmo padrao de payment_events, que ja era 'fact'.
--
-- A estrategia 'upsert' era e continua sendo a certa aqui, e ela e o que da
-- de graca a garantia de unicidade testada em schema.yml. Se um dia a
-- intencao virar auditoria ("quero saber que uma busca foi apagada"),
-- a mudanca e trocar cdc_strategy para 'log' em CONFIG.TABLE_METADATA -- mas
-- ai o `unique` de search_id e o accepted_values de `op` precisam mudar
-- junto, e o gold_user_behavior passa a precisar de deduplicacao propria.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_search_events')) }}
