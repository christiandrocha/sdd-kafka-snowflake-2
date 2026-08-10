# DESIGN: Migração de Ingestão Kafka Connector V4 (MIGRACAO_INGESTAO_V4)

> Design técnico da migração de Snowpipe clássico para Kafka Connector V4, com redução de escopo de ingestão.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | MIGRACAO_INGESTAO_V4 |
| **Date** | 2026-08-04 |
| **Author** | design-agent (via Claude) |
| **DEFINE** | [DEFINE_MIGRACAO_INGESTAO_V4.md](./DEFINE_MIGRACAO_INGESTAO_V4.md) |
| **BUILD_REPORT** | [BUILD_REPORT_MIGRACAO_INGESTAO_V4.md](../reports/BUILD_REPORT_MIGRACAO_INGESTAO_V4.md) |
| **Status** | Shipped (2026-08-07) |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                  MIGRAÇÃO DE INGESTÃO — V4                               │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  PostgreSQL ──WAL──▶ Debezium ──▶ Kafka (10 tópicos, Tier 1 só)          │
│                                        │                                  │
│                                        ▼                                  │
│                    Kafka Connect — SnowflakeStreamingSinkConnector       │
│                    (conector único, consolidado — sink+sinkitems)        │
│                    enable.schematization=true                            │
│                    validation=client_side (usa Schema Registry)          │
│                    identifier.normalization=true (MAIÚSCULO)             │
│                                        │                                  │
│                                        ▼                                  │
│                    BRONZE.<domínio> — colunas tipadas nativas            │
│                    (sem RECORD_CONTENT; RECORD_METADATA permanece)       │
│                                        │                                  │
│                                        ▼                                  │
│                    10 modelos dbt Bronze reescritos (passagem/renomeação)│
│                    lendo colunas tipadas + RECORD_METADATA p/ watermark  │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────┘
```

> **Correção v1.1 (2026-08-07).** A versão 1.0 deste diagrama dizia "sem
> RECORD_CONTENT/RECORD_METADATA VARIANT". Só o `RECORD_CONTENT` desaparece.
> O `RECORD_METADATA` continua sendo escrito pelo conector sob schematização e é
> usado deliberadamente pelos modelos. Ver Decision 3.

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Debezium | Captura CDC do Postgres, 10 tabelas (Tier 1 só) | Debezium 2.4 (Kafka Connect Source) |
| Kafka Connect Sink (v4) | Ingestão nativa Snowpipe Streaming, schematizada | `snowflake-kafka-connector` v4 |
| Bronze (dbt) | 10 modelos reescritos sobre colunas tipadas | dbt Core |

---

## Key Decisions

### Decision 1: Migração para Kafka Connector V4 com schematização nativa (substitui ADR-0021)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** Conector v2.1.2 em Snowpipe clássico, geração descontinuada pela Snowflake.

**Choice:** `SnowflakeStreamingSinkConnector` (v4), com `snowflake.enable.schematization=true`, `snowflake.validation=client_side`, `snowflake.compatibility.enable.column.identifier.normalization=true`.

**Rationale:** `client_side` usa o Schema Registry (já mantido com disciplina BACKWARD pelo projeto) como fonte de tipo, em vez de `server_side` inferir pelo primeiro registro observado (risco documentado de rebaixar tipo, ex: FLOAT64→NUMBER(38,0)). Normalização para MAIÚSCULO evita exigir aspas duplas em todo SQL dbt — decisão revertida uma vez nesta conversa depois de entender esse custo prático.

**Alternatives Rejected:**
1. **Modo compatível v3** (`schematization=false`) — não aproveitaria o ganho de simplicidade de colunas tipadas nativas.
2. **`validation=server_side`** (default v4) — rejeitado pelo risco de inferência de tipo sem usar o Schema Registry.
3. **Preservar case original do Avro** (`identifier.normalization=false`) — rejeitado depois de entender que exigiria aspas duplas em toda referência de coluna no SQL dbt.

**Consequences:**
- Os 10 modelos Bronze precisam ser reescritos — maior item de trabalho desta feature.
- ~~Combinação `schematization=true`+`client_side` não tem exemplo documentado — precisa validação empírica.~~
  **Validada em 2026-08-07** (v1.1): a combinação funciona. `DESC TABLE CDC_POC.BRONZE.RESTAURANTS`
  na conta real retorna `AVERAGE_RATING FLOAT`, `NUM_REVIEWS NUMBER(38,0)`,
  `OPENING_TIME TIME(6)`, sem coluna `RECORD_CONTENT`. Detalhe em
  [BUILD_REPORT_MIGRACAO_INGESTAO_V4.md](../reports/BUILD_REPORT_MIGRACAO_INGESTAO_V4.md).
- Duas chaves do v2.1.2 não existem no JAR 4.1.0 e foram omitidas da configuração:
  `snowflake.schema.registry.url` (a validação `client_side` usa
  `value.converter.schema.registry.url`) e `buffer.count.records` / `buffer.flush.time` /
  `buffer.size.bytes` (o buffer client-side deixou de existir).

---

### Decision 2: Tiering e redução de escopo de ingestão (substitui ADR-0022)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** 10 dos 20 domínios ingeridos nunca alimentam Silver/Gold (análise de `ref()`/`source()` real nos modelos dbt).

**Choice:** Remover do escopo de ingestão: PAYMENTS, GPS_EVENTS, ORDER_STATUS, ROUTES, RECEIPTS, SUPPORT_TICKETS, PRODUCTS, MENU_SECTIONS, RATINGS, INVENTORY.

**Rationale:** Sob o modelo de cobrança por volume do v4, ingerir dado nunca lido é desperdício direto. PAYMENTS confirmado como órfão pelo responsável do projeto — coberto por PAYMENT_EVENTS.

**Alternatives Rejected:**
1. **Manter ingestão de todos os 20, só não processar Tier 2 no dbt** — rejeitado porque não reduz custo de ingestão (que é cobrado por volume, não por transformação).

**Consequences:**
- Redução de complexidade (menos tópicos, menos Streams/Tasks).
- Perda de capacidade de demo desses 10 domínios se um cliente pedir para vê-los.

---

### Decision 3: `RECORD_METADATA` é mantido e usado como watermark (v1.1, 2026-08-07)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted — decidido durante o Build, a partir de evidência |
| **Date** | 2026-08-07 |

**Context:** A v1.0 deste DESIGN presumia que a schematização eliminaria tanto o
`RECORD_CONTENT` quanto o `RECORD_METADATA`. Isso deixava os modelos Bronze sem base para
duas coisas que o projeto anterior resolvia com esse VARIANT: o filtro incremental
(`RECORD_METADATA:CreateTime`) e o desempate da deduplicação (`RECORD_METADATA:offset`).

A introspecção do JAR 4.1.0 durante o Build mostrou que a premissa estava errada: o
literal `RECORD_METADATA` aparece em `SnowflakeSinkServiceV2` e `ConnectorConfigDefinition`,
e as chaves `snowflake.metadata.{all,createtime,offset.and.partition,topic}` continuam
existindo. Confirmado depois em tabela real — `RECORD_METADATA VARIANT` é a única coluna
VARIANT que sobra.

**Choice:** Manter o uso de `RECORD_METADATA` nos 10 modelos Bronze, exatamente com a
mesma semântica do projeto anterior: `CreateTime` como watermark do modo incremental e
`offset` como desempate no `ROW_NUMBER()` da deduplicação.

**Rationale:** Preserva um comportamento já exercitado em produção em vez de inventar
critério novo. As alternativas exigiriam degradar a garantia: `__SOURCE_TS_MS` sozinho
não desempata eventos do mesmo commit, e sem watermark o modelo incremental viraria full
refresh.

**Alternatives Rejected:**
1. **Usar só `__SOURCE_TS_MS`** — não desempata eventos com o mesmo timestamp de commit.
2. **Abandonar o modo incremental** — full refresh a cada run, custo de warehouse
   incompatível com a feature `GOVERNANCA_CUSTO_DISPARO`.

**Consequences:**
- Os modelos Bronze continuam com uma dependência de VARIANT, contra a expectativa
  original de passagem 100% tipada. É uma dependência restrita a 3 colunas de metadado —
  nenhum campo de negócio é extraído de VARIANT.
- Verificado no volume: `KAFKA_OFFSET` cobrindo 0 a 110.001 sem buraco, e
  `BRONZE_ORDER_ITEMS` com 110.002 linhas para 110.002 `order_item_id` distintos.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `connectors/debezium.json` | Create | `table.include.list` só com os 10 domínios Tier 1 | (general) | None |
| 2 | `connectors/snowflake_sink.json` | Create | Conector único v4, consolidado (substitui `sink`+`sinkitems`) | (general) | None |
| 3 | `Dockerfile.connect` | Create | Atualiza versão do JAR do conector para linha 4.x | (general) | None |
| 4-13 | `dbt/models/bronze/bronze_{orders,drivers,driver_shifts,restaurants,users_mongo,users_mssql,order_items,payment_events,recommendations,search_events}.sql` | Modify | 10 modelos Bronze reescritos sobre colunas tipadas | (general) | 2 |
| 14 | `dbt/models/bronze/schema.yml` | Modify | Novos nomes/tipos de coluna | (general) | 4-13 |
| 15 | `scripts/streams_and_tasks.sql` | Modify | Já gerado só para os 10 domínios Tier 1 (feature `GOVERNANCA_CUSTO_DISPARO`) — confirmar compatibilidade de nomes de coluna | (general) | 4-13 |
| 16 | `tests/load_to_postgres.py` | Modify | Não carregar os 10 domínios removidos | (general) | 1 |
| 17 | `scripts/init.sql` | Modify | Não criar tabelas Postgres dos domínios removidos (opcional — manter fonte intacta é aceitável) | (general) | None |
| 18 | `dbt/models/config/sources.yml` | Create | **Acrescentado em v1.1.** Define `source('bronze_raw', …)`; sem ele os 10 modelos dos itens 4-13 não parseiam. Faltava no manifesto v1.0 | (general) | None |
| 19 | `scripts/register_connectors.sh` | Modify | **Acrescentado em v1.1.** Registrava 3 conectores, incluindo `snowflake_sink_items.json`, que o item 2 elimina. Sem esta mudança o registro falha | (general) | 1, 2 |

**Total Files:** 19 (17 na v1.0 + 2 identificados no Build)

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|-----------------|-----------------|
| (general) | 1-17 | Mesma lacuna da feature anterior — nenhum agente deste template cobre Snowflake Kafka Connect ou dbt sobre Snowflake especificamente |

---

## Code Patterns

### Pattern 1: Configuração do conector v4

```properties
connector.class=com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector
snowflake.enable.schematization=true
snowflake.validation=client_side
snowflake.compatibility.enable.column.identifier.normalization=true
snowflake.compatibility.enable.autogenerated.table.name.sanitization=true
snowflake.streaming.classic.offset.migration=skip
```

### Pattern 2: Modelo Bronze pós-schematização (esqueleto)

Atualizado em v1.1 para refletir o que o Build produziu — a v1.0 omitia os metadados.

```sql
-- ANTES: RECORD_CONTENT:order_id::VARCHAR AS ORDER_ID
-- DEPOIS: coluna já chega tipada e em MAIÚSCULO
SELECT
    ORDER_ID,
    ORDER_DATE,
    TOTAL_AMOUNT,
    -- ...demais colunas já tipadas pelo conector v4

    -- Metadados: __OP e __SOURCE_TS_MS vêm tipados (SMT ExtractNewRecordState
    -- do Debezium, add.fields=op,source.ts_ms com prefixo __).
    __OP                               AS op,
    __SOURCE_TS_MS                     AS source_ts_ms,

    -- RECORD_METADATA permanece VARIANT e é a base do incremental (Decision 3).
    RECORD_METADATA:offset::BIGINT     AS kafka_offset,
    RECORD_METADATA:partition::INT     AS kafka_partition,
    RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

FROM {{ source('bronze_raw', 'ORDERS') }}

{% if is_incremental() %}
WHERE RECORD_METADATA:CreateTime::BIGINT > (
    SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
)
{% endif %}
```

Deduplicação dentro do lote, antes do merge:

```sql
ROW_NUMBER() OVER (
    PARTITION BY {unique_key}
    ORDER BY source_ts_ms DESC, kafka_offset DESC
) AS _row_num
```

---

## Data Flow

```text
1. Debezium captura mudança em uma das 10 tabelas Tier 1 do Postgres
   │
   ▼
2. Evento publicado no tópico Kafka correspondente (Avro, Schema Registry)
   │
   ▼
3. Conector v4 valida contra o schema Avro (client_side) e escreve via
   Snowpipe Streaming nativo — colunas tipadas, MAIÚSCULO
   │
   ▼
4. Modelo Bronze (dbt) faz passagem/renomeação sobre as colunas já tipadas, e lê
   RECORD_METADATA para o watermark incremental e o desempate da deduplicação
   (Decision 3)
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-------------------|------------------|
| Schema Registry | REST (validação client_side) | Nenhuma (rede interna) |
| Snowflake | Snowpipe Streaming nativo | Par de chaves RSA |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Integração | Ingestão v4 ponta a ponta | `tests/load_to_postgres.py` + inspeção manual do Bronze | Snowsight | Confirmar colunas tipadas corretas, sem truncamento de tipo |
| Regressão dbt | 10 modelos Bronze reescritos | `dbt test --select bronze` | dbt Core | 100% dos testes existentes passando |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|---------------------|--------|
| `schematization=true`+`client_side` não suportado na prática | Fallback para `server_side` (aceitando o risco de inferência de tipo) | N/A — decisão manual |
| Conector v4 falha ao processar arquivo já staged pelo v2.1.2 | Aguardar drenagem completa do buffer clássico antes de desligar o conector antigo | N/A |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `snowflake.enable.schematization` | bool | `true` | Colunas tipadas nativas |
| `snowflake.validation` | string | `client_side` | Usa Schema Registry como fonte de tipo |

---

## Security Considerations

- Nenhuma mudança de superfície de segurança em relação ao v1 — mesma autenticação por par de chaves.

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | Logs do Kafka Connect via `docker logs kafka-connect` |
| Metrics | JMX Exporter + Prometheus já existentes cobrem o novo conector sem mudança |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-04 | design-agent (via Claude) | Versão inicial, traduzida das ADRs 0021 e 0022 do v5-delivery |
| 1.1 | 2026-08-07 | iterate-agent | Correção pós-Build, a partir de evidência da execução ao vivo: (a) diagrama dizia "sem RECORD_CONTENT/RECORD_METADATA VARIANT" — só o `RECORD_CONTENT` desaparece; (b) Decision 3 nova, registrando o uso deliberado de `RECORD_METADATA` como watermark e desempate; (c) Decision 1 com a validação empírica de `schematization=true`+`client_side` e as duas chaves do v2.1.2 ausentes no JAR 4.1.0; (d) Pattern 2 reescrito com os metadados que faltavam; (e) manifesto 17 → 19 arquivos. Nenhuma mudança de código decorre desta revisão — o Build já implementou o comportamento correto |
| 1.1 | 2026-08-07 | ship-agent | Arquivado. Cópia de trabalho removida de `features/`; esta é a versão canônica |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_MIGRACAO_INGESTAO_V4.md`
