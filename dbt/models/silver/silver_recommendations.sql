{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: eventos de recomendacao de ML (view, click, purchase, dismiss),
-- um por event_id.
--
-- Mesma tensao de config descrita em silver_search_events.sql:
-- table_type='log' com cdc_strategy='upsert'. Aqui o efeito e mais brando --
-- event_id de evento de ML e imutavel, entao a deduplicacao raramente
-- colapsa versoes de verdade -- mas o descarte de DELETE vale igual.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_recommendations')) }}
