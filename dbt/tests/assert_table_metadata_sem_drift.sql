-- CONFIG.TABLE_METADATA has not drifted from what this commit expects.
--
-- WHY THIS TEST EXISTS
--
-- The `get_config_for` macro reads CONFIG.TABLE_METADATA at COMPILE time, and
-- from it comes the `cdc_strategy` of every Silver model -- upsert, append or
-- log. That means the same commit compiles different SQL depending on the
-- state of a table in Snowflake. Someone editing a row there changes a model's
-- semantics without producing a single diff in Git, and a `log` turning into
-- an `upsert` collapses history silently.
--
-- The table remains the source of truth at runtime -- that design is
-- deliberate and has value, because it allows reacting to a new domain without
-- a deploy. What was missing was the counterpart: a versioned copy of what we
-- expect to find there, so that divergence becomes a build failure instead of
-- a surprise.
--
-- WHEN THIS TEST FAILS, decide which side is right:
--   - intentional change to the table -> update the list below in the same
--     commit that documents why;
--   - unintentional change            -> fix the table.
-- What is not acceptable is silencing the test.
--
-- SCOPE: only the columns that change behaviour. `notes`, `registered_at`,
-- `changed_by` and the like are deliberately left out -- they are audit
-- metadata and locking on them would produce failures over harmless edits.

{% set esperado = [
    ('driver_shifts',   'entity', 'upsert', 'shift_id'),
    ('drivers',         'entity', 'upsert', 'uuid'),
    ('order_items',     'fact',   'upsert', 'order_item_id'),
    ('orders',          'entity', 'upsert', 'order_id'),
    ('payment_events',  'fact',   'upsert', 'event_id'),
    ('recommendations', 'fact',   'upsert', 'event_id'),
    ('restaurants',     'entity', 'upsert', 'uuid'),
    ('search_events',   'fact',   'upsert', 'search_id'),
    ('users_mongo',     'entity', 'upsert', 'uuid'),
    ('users_mssql',     'entity', 'upsert', 'uuid')
] %}

WITH esperado AS (

{% for tabela, tipo, estrategia, chave in esperado %}
    SELECT '{{ tabela }}' AS table_name,
           '{{ tipo }}'   AS table_type,
           '{{ estrategia }}' AS cdc_strategy,
           '{{ chave }}'  AS unique_key
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

),

atual AS (

    SELECT
        LOWER(table_name)   AS table_name,
        LOWER(table_type)   AS table_type,
        LOWER(cdc_strategy) AS cdc_strategy,
        LOWER(unique_key)   AS unique_key
    FROM {{ target.database }}.CONFIG.TABLE_METADATA
    WHERE active = TRUE

)

SELECT
    COALESCE(e.table_name, a.table_name) AS table_name,
    e.table_type   AS tipo_esperado,   a.table_type   AS tipo_atual,
    e.cdc_strategy AS estrategia_esperada, a.cdc_strategy AS estrategia_atual,
    e.unique_key   AS chave_esperada,  a.unique_key   AS chave_atual,
    CASE
        WHEN a.table_name IS NULL THEN 'missing from CONFIG.TABLE_METADATA'
        WHEN e.table_name IS NULL THEN 'present in the table and not expected by this commit'
        ELSE                           'divergent configuration'
    END AS motivo

FROM esperado e
FULL OUTER JOIN atual a ON a.table_name = e.table_name

WHERE e.table_name IS NULL
   OR a.table_name IS NULL
   OR e.table_type   <> a.table_type
   OR e.cdc_strategy <> a.cdc_strategy
   OR e.unique_key   <> a.unique_key
