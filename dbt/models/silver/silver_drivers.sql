{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: delivery drivers, one per uuid.
-- driver_id (not the uuid) is the driver_key used by orders and driver_shifts
-- -- the uuid is the CDC technical key, the business join is on driver_id.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_drivers')) }}
