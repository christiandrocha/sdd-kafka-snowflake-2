{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: eventos de recomendacao de ML, um por event_id.
-- Tipos observados nos dados em 2026-08-10: recommendation_served,
-- add_to_cart, click, view.
--
-- Era table_type='log' com cdc_strategy='upsert' ate 2026-08-10; a etiqueta
-- foi corrigida para 'fact' pelo mesmo motivo descrito em
-- silver_search_events.sql -- 255 linhas para 255 chaves distintas e zero
-- deletes na origem, append-only. A estrategia 'upsert' nao mudou.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_recommendations')) }}
