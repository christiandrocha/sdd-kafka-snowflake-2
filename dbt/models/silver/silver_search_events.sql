{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: user searches, one row per search_id.
--
-- Until 2026-08-10 this domain was registered as table_type='log' with
-- cdc_strategy='upsert', which was contradictory: `log` describes a domain
-- that preserves DELETE as a historical record, and `upsert` discards DELETE.
-- Resolved by fixing the LABEL, not the strategy -- measuring at the source
-- showed 203 rows for 203 distinct keys and zero deletes, i.e. append-only,
-- the same pattern as payment_events, which was already 'fact'.
--
-- The 'upsert' strategy was and remains the right one here, and it is what
-- gives the uniqueness guarantee tested in schema.yml for free. If the intent
-- ever becomes auditing ("I want to know a search was deleted"), the change is
-- switching cdc_strategy to 'log' in CONFIG.TABLE_METADATA -- but then the
-- `unique` on search_id and the accepted_values on `op` have to change with
-- it, and gold_user_behavior starts needing its own deduplication.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_search_events')) }}
