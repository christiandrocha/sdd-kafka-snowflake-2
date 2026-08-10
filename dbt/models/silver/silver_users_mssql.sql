{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: perfil estendido do usuario (origem MSSQL), um por uuid.
-- Junta com silver_users_mongo por CPF, nao por uuid -- ver o comentario
-- daquele modelo.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_users_mssql')) }}
