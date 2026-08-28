{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: restaurants, one per uuid.
-- CNPJ (not the uuid) is the restaurant_key used by orders -- the uuid is the
-- CDC technical key, the business join is on cnpj.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_restaurants')) }}
