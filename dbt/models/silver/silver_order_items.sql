{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: estado atual de cada item de linha, uma linha por order_item_id.
-- Maior volume do projeto -- e o candidato numero 1 a sair de 'table' se o
-- custo do rebuild passar a doer.
--
-- Estrategia 'upsert' (fact) vinda de CONFIG.TABLE_METADATA; toda a logica
-- esta em dbt/macros/resolve_cdc.sql. O 'table' em vez do default
-- 'incremental' de silver segue o motivo detalhado em silver_orders.sql:
-- MERGE nao apaga linha, entao chave deletada na origem sobreviveria para
-- sempre num incremental.

{{ resolve_cdc(ref('bronze_order_items')) }}
