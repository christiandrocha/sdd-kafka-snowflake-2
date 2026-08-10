{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: estado atual de cada turno do entregador, um por shift_id
-- (ganhos, distancia, numero de pedidos, avaliacao).
--
-- Turno em andamento e atualizado varias vezes na origem ate fechar --
-- exatamente o caso em que a deduplicacao por source_ts_ms + kafka_offset
-- importa: sem o desempate por offset, dois UPDATEs no mesmo milissegundo
-- deixariam a escolha da versao final nao deterministica.
--
-- 'table' em vez do default 'incremental' de silver: motivo em
-- silver_orders.sql.

{{ resolve_cdc(ref('bronze_driver_shifts')) }}
