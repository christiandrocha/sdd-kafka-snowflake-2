{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: current state of each line item, one row per order_item_id.
-- Largest volume in the project -- and the number one candidate to leave
-- 'table' if the rebuild cost starts to hurt.
--
-- Strategy 'upsert' (fact) comes from CONFIG.TABLE_METADATA; all the logic is
-- in dbt/macros/resolve_cdc.sql. The 'table' instead of silver's 'incremental'
-- default follows the reason detailed in silver_orders.sql: MERGE does not
-- delete rows, so a key deleted at the source would survive forever in an
-- incremental.

{{ resolve_cdc(ref('bronze_order_items')) }}
