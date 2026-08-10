{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: estado atual de cada pedido, uma linha por order_id.
--
-- Toda a logica de colapso do historico CDC esta em resolve_cdc(); este
-- arquivo so aponta a fonte. A estrategia ('upsert' para orders) vem de
-- CONFIG.TABLE_METADATA, com fallback estatico em get_table_config() para
-- quando nao ha conexao -- ver dbt/macros/resolve_cdc.sql.
--
-- POR QUE 'table' E NAO 'incremental' (o default de silver no
-- dbt_project.yml): as duas coisas que resolve_cdc faz na estrategia upsert
-- so valem sobre o historico INTEIRO da entidade.
--
--   1. O ROW_NUMBER particiona por order_id sobre tudo que existe na Bronze.
--      Um incremental filtrado por watermark rankearia so o lote novo -- o
--      que ainda daria a versao certa via MERGE, mas deixa de ser a mesma
--      operacao descrita na macro.
--   2. DELETE. A linha `op='d'` e descartada pelo filtro, entao ela nunca
--      chega ao MERGE, e MERGE nao apaga nada: num incremental, uma chave
--      apagada na origem ficaria na Silver para sempre. Com rebuild, ela
--      simplesmente deixa de aparecer no proximo run.
--
-- O custo e varrer bronze_orders inteira a cada execucao. No volume atual da
-- POC isso e barato; se a Bronze crescer a ponto de doer, a saida NAO e
-- trocar para incremental merge -- e incremental com delete+insert por
-- particao de data, que preserva a semantica do item 2.
--
-- As colunas de controle CDC (op, source_ts_ms, kafka_offset,
-- kafka_partition, kafka_created_at) seguem para a Silver de proposito: sao
-- a linhagem que liga cada linha ao evento Kafka que a produziu.

{{ resolve_cdc(ref('bronze_orders')) }}
