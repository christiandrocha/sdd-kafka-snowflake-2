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
-- Mesma tensao de config descrita em silver_search_events.sql:
-- table_type='log' com cdc_strategy='upsert'. Aqui o efeito e mais brando --
-- event_id de evento de ML e imutavel, entao a deduplicacao raramente
-- colapsa versoes de verdade -- mas o descarte de DELETE vale igual.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_recommendations')) }}
