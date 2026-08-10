{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: entregadores, um por uuid.
-- driver_id (nao o uuid) e o driver_key usado por orders e driver_shifts --
-- o uuid e a chave tecnica do CDC, a juncao de negocio e por driver_id.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_drivers')) }}
