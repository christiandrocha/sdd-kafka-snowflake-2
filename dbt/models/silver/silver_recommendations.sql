{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: ML recommendation events, one per event_id.
-- Types observed in the data on 2026-08-10: recommendation_served,
-- add_to_cart, click, view.
--
-- It was table_type='log' with cdc_strategy='upsert' until 2026-08-10; the
-- label was corrected to 'fact' for the same reason described in
-- silver_search_events.sql -- 255 rows for 255 distinct keys and zero deletes
-- at the source, append-only. The 'upsert' strategy did not change.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_recommendations')) }}
