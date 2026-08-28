{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: current state of each order, one row per order_id.
--
-- All the logic that collapses the CDC history lives in resolve_cdc(); this
-- file only points at the source. The strategy ('upsert' for orders) comes
-- from CONFIG.TABLE_METADATA, with a static fallback in get_table_config()
-- for when there is no connection -- see dbt/macros/resolve_cdc.sql.
--
-- WHY 'table' AND NOT 'incremental' (silver's default in dbt_project.yml):
-- the two things resolve_cdc does under the upsert strategy only hold over
-- the entity's ENTIRE history.
--
--   1. The ROW_NUMBER partitions by order_id across everything in Bronze. An
--      incremental filtered by a watermark would rank only the new batch --
--      which would still give the right version via MERGE, but it stops being
--      the same operation the macro describes.
--   2. DELETE. The `op='d'` row is dropped by the filter, so it never reaches
--      the MERGE, and MERGE deletes nothing: in an incremental, a key deleted
--      at the source would stay in Silver forever. With a rebuild it simply
--      stops appearing on the next run.
--
-- The cost is scanning all of bronze_orders on every run. At the POC's current
-- volume that is cheap; if Bronze grows enough to hurt, the answer is NOT to
-- switch to incremental merge -- it is incremental with delete+insert by date
-- partition, which preserves the semantics of point 2.
--
-- The CDC control columns (op, source_ts_ms, kafka_offset, kafka_partition,
-- kafka_created_at) travel on to Silver on purpose: they are the lineage
-- tying each row to the Kafka event that produced it.

{{ resolve_cdc(ref('bronze_orders')) }}
