{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: payment lifecycle events, one per event_id.
-- Cycle: created -> authorized -> captured -> succeeded -> settled -> closed
--                                         -> refunded -> closed
--
-- Event sourcing: each event_id is immutable by nature, so the 'upsert' here
-- does not collapse business history -- it only guarantees idempotency against
-- redelivery. The many events of a single payment_id all remain present, which
-- is what gold_payment_lifecycle consumes.
--
-- Fields already unnested in Bronze (event_name, event_timestamp) come along:
-- resolve_cdc does SELECT *, it does not reproject columns.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_payment_events')) }}
