{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: extended user profile (MSSQL origin), one per uuid.
-- Joins silver_users_mongo on CPF, not on uuid -- see that model's comment.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_users_mssql')) }}
