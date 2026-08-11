-- CONFIG.TABLE_METADATA nao divergiu do que este commit espera.
--
-- POR QUE ESTE TESTE EXISTE
--
-- O macro `get_config_for` le CONFIG.TABLE_METADATA em tempo de COMPILACAO e
-- dela sai a `cdc_strategy` de cada modelo Silver -- upsert, append ou log.
-- Isso significa que o mesmo commit compila SQL diferente conforme o estado
-- de uma tabela no Snowflake. Alguem editar uma linha ali muda a semantica de
-- um modelo sem produzir um unico diff no Git, e um `log` virando `upsert`
-- colapsa historico em silencio.
--
-- A tabela continua sendo a fonte em runtime -- esse desenho e deliberado e
-- tem valor, porque permite reagir a um dominio novo sem deploy. O que faltava
-- era a contraparte: uma copia versionada do que se espera encontrar la, para
-- que divergencia vire falha de build em vez de surpresa.
--
-- QUANDO ESTE TESTE FALHAR, decida qual lado esta certo:
--   - mudanca intencional na tabela -> atualize a lista abaixo no mesmo commit
--     que documenta o porque;
--   - mudanca nao intencional       -> corrija a tabela.
-- O que nao vale e silenciar o teste.
--
-- ESCOPO: so as colunas que mudam comportamento. `notes`, `registered_at`,
-- `changed_by` e afins ficam de fora de proposito -- sao metadados de
-- auditoria e travar neles produziria falha por edicao inofensiva.

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
        WHEN a.table_name IS NULL THEN 'ausente na CONFIG.TABLE_METADATA'
        WHEN e.table_name IS NULL THEN 'presente na tabela e nao esperado por este commit'
        ELSE                           'configuracao divergente'
    END AS motivo

FROM esperado e
FULL OUTER JOIN atual a ON a.table_name = e.table_name

WHERE e.table_name IS NULL
   OR a.table_name IS NULL
   OR e.table_type   <> a.table_type
   OR e.cdc_strategy <> a.cdc_strategy
   OR e.unique_key   <> a.unique_key
