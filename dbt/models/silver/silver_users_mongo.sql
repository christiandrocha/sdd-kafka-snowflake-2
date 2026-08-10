{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: usuarios (origem MongoDB), um por uuid.
-- CPF (nao o uuid) e o user_key usado por orders, e e tambem o que liga este
-- modelo ao silver_users_mssql -- os dois sistemas de origem tem uuids
-- proprios e independentes para a mesma pessoa.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_users_mongo')) }}
