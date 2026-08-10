{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: eventos do ciclo de pagamento, um por event_id.
-- Ciclo: created -> authorized -> captured -> succeeded -> settled -> closed
--                                          -> refunded -> closed
--
-- Event sourcing: cada event_id e imutavel por natureza, entao o 'upsert'
-- aqui nao colapsa historico de negocio -- ele so garante idempotencia
-- contra reentrega. Os multiplos eventos de um mesmo payment_id continuam
-- todos presentes, que e o que o gold_payment_lifecycle consome.
--
-- Campos ja desaninhados na Bronze (event_name, event_timestamp) vem junto:
-- resolve_cdc faz SELECT *, nao reprojeta colunas.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_payment_events')) }}
