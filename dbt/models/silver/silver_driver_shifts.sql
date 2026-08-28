{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: current state of each driver shift, one per shift_id
-- (earnings, distance, order count, rating).
--
-- A shift in progress is updated several times at the source before it closes
-- -- exactly the case where deduplication by source_ts_ms + kafka_offset
-- matters: without the offset tie-break, two UPDATEs in the same millisecond
-- would leave the choice of final version non-deterministic.
--
-- 'table' instead of silver's 'incremental' default: reason in
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_driver_shifts')) }}
