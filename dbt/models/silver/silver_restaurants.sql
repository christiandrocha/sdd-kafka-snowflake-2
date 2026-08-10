{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: restaurantes, um por uuid.
-- CNPJ (nao o uuid) e o restaurant_key usado por orders -- o uuid e a chave
-- tecnica do CDC, a juncao de negocio e por cnpj.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_restaurants')) }}
