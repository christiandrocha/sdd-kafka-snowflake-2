{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: users (MongoDB origin), one per uuid.
-- CPF (not the uuid) is the user_key used by orders, and it is also what links
-- this model to silver_users_mssql -- the two source systems have their own
-- independent uuids for the same person.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_users_mongo')) }}
