# DESIGN: Governança de Custo e Disparo (GOVERNANCA_CUSTO_DISPARO)

> Design técnico para eliminar o consumo ocioso de crédito Snowflake via gate de disparo nativo (Streams + Triggered Tasks) e storage dedicado do Dagster.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOVERNANCA_CUSTO_DISPARO |
| **Date** | 2026-08-04 |
| **Author** | design-agent (via Claude) |
| **DEFINE** | [DEFINE_GOVERNANCA_CUSTO_DISPARO.md](./DEFINE_GOVERNANCA_CUSTO_DISPARO.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                    GOVERNANÇA DE CUSTO E DISPARO                         │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  Snowpipe (Kafka Connect) ──INSERT──▶ BRONZE.<domínio> (10 tabelas)      │
│                                              │                            │
│                                              ▼                            │
│                                    STREAM <domínio>_STREAM                │
│                                    (APPEND_ONLY=TRUE)                     │
│                                              │                            │
│                    TASK <domínio>_GATE_TASK (agendada 1min)               │
│                    WHEN SYSTEM$STREAM_HAS_DATA(...) ──▶ custo zero se     │
│                                              │           falso            │
│                                    (só dispara se houver dado real)      │
│                                              ▼                            │
│                          CALL CONFIG.SP_GATE_DOMAIN(...)                  │
│                                              │                            │
│                                              ▼                            │
│                            CONFIG.PENDING_RUNS (consumed=FALSE)           │
│                                              │                            │
│                              ┌───────────────┘                           │
│                              ▼                                            │
│              bronze_new_data_sensor (Dagster, poll 60s)                  │
│              SELECT ... WHERE consumed=FALSE  (leve, sem watermark global)│
│                              │                                            │
│                              ▼                                            │
│                    RunRequest → dbt run (Silver → Gold)                  │
│                                                                            │
├──────────────────────────────────────────────────────────────────────────┤
│  dagster-postgres (storage dedicado, separado do Postgres fonte do CDC)  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Streams Bronze | Rastreiam mudanças append-only nas 10 tabelas Bronze Tier 1 | Snowflake `STREAM ... APPEND_ONLY=TRUE` |
| Triggered Tasks | Gate de disparo nativo, custo zero quando ocioso | Snowflake `TASK ... WHEN SYSTEM$STREAM_HAS_DATA` |
| `CONFIG.SP_GATE_DOMAIN` | Stored procedure compartilhada — grava sinal + consome o stream | Snowflake Scripting (SQL) |
| `CONFIG.PENDING_RUNS` | Tabela de controle lida pelo sensor Dagster | Tabela Snowflake |
| `bronze_new_data_sensor` | Consulta `PENDING_RUNS`, dispara `dbt run` | Dagster (Python) |
| `dagster-postgres` | Storage de runs do Dagster, separado da fonte CDC | PostgreSQL 15 (container dedicado) |
| Resource Monitor `cdc_poc_monitor` | Freio de emergência de crédito | Snowflake Resource Monitor |

---

## Key Decisions

### Decision 1: Streams + Triggered Tasks nativos como gate de disparo (substitui ADR-0019)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** O sensor Dagster fazia `SELECT MAX(create_time)` em até 20 tabelas Bronze a cada 60s, colidindo com `AUTO_SUSPEND=60s` do warehouse e mantendo-o praticamente sempre ligado.

**Choice:** Um `STREAM` por tabela Bronze (10 domínios Tier 1) + uma `TASK` agendada a cada 1 minuto com `WHEN SYSTEM$STREAM_HAS_DATA(...)`. A avaliação do `WHEN` é feita pelo control plane do Snowflake sem consumir warehouse quando falsa. Só quando há dado real a Task liga o warehouse, grava um sinal em `CONFIG.PENDING_RUNS` e consome o stream via `INSERT ... SELECT FROM stream`.

**Rationale:** O Stream é definido sobre a tabela Bronze real — só pode indicar "tem dado" depois que o Snowpipe já commitou a linha. Isso elimina por construção o gap de confirmação que uma alternativa baseada em Kafka (ver alternativa 1) exigiria resolver heuristicamente.

**Alternatives Rejected:**
1. **Watcher Kafka customizado → Dagster GraphQL** — rejeitado por exigir consumer group dedicado, tratamento de rebalance, semântica at-least-once, e um gap estrutural de confirmação (Kafka publicado ≠ Snowflake materializado) que exigiria debounce heurístico, não garantia.
2. **Aumentar `minimum_interval_seconds` do sensor original** — rejeitado como solução definitiva (só reduz o sintoma, não elimina — warehouse ainda liga/desliga em ciclo mesmo sem dado).

**Consequences:**
- Elimina a fonte principal do consumo ocioso.
- Introduz 10 Streams + 10 Tasks + 1 stored procedure como objetos Snowflake novos a gerenciar.
- Dagster continua necessário para orquestrar o `dbt run` real (Tasks não executam CLI externo).

---

### Decision 2: Sensor Dagster consulta `CONFIG.PENDING_RUNS` sem watermark global (substitui ADR-0024)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** Uma primeira versão do sensor reescrito usava `WHERE detected_at > cursor AND consumed = FALSE` — um cursor de watermark global. Comparação com uma segunda opinião independente identificou que esse padrão reintroduz um bug conhecido: um domínio de alto volume avança o cursor, e um domínio de baixo volume cujo `detected_at` fica mais antigo que o cursor já avançado nunca é processado, mesmo com `consumed=FALSE`.

**Choice:** Remover a comparação de cursor inteiramente. `SELECT ... WHERE consumed = FALSE`, sem filtro de timestamp. Marcar como consumido por `consumed = FALSE` no UPDATE, não por corte de tempo.

**Rationale:** `consumed = FALSE` já é suficiente como guarda de idempotência — cada linha é processada exatamente uma vez, independente de quando seu `detected_at` foi gravado em relação a outras linhas.

**Alternatives Rejected:**
1. **Manter o cursor, mas particionado por domínio** — resolveria o bug, mas adiciona complexidade (N cursores em vez de 1) sem benefício sobre simplesmente remover o cursor.

**Consequences:**
- Corrige uma classe de bug que já tinha acontecido uma vez no sensor original.
- Falta teste de regressão automatizado (AT-003 no DEFINE) — não incluído neste ciclo de Build, registrado como débito.

---

### Decision 3: Postgres dedicado para storage do Dagster (substitui ADR-0018)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** Dagster usava SQLite, inadequado para escrita concorrente e frágil a corrupção em crash. A correção óbvia (Postgres) não deveria reusar a mesma instância que é fonte do CDC — criaria dependência circular (fonte cai → orquestrador cai junto).

**Choice:** Serviço `dagster-postgres` separado no `docker-compose.yml`, com volume próprio (`dagster_postgres_data`), sem `wal_level=logical` (não é fonte de CDC).

**Rationale:** Fonte de dados e orquestrador falham independentemente.

**Alternatives Rejected:**
1. **Reusar Postgres fonte do CDC** — rejeitado pela dependência circular.
2. **SQLite com WAL mode** — aguenta baixa concorrência, mas frágil demais para um projeto que vira modelo de referência de cliente.

**Consequences:**
- Um serviço a mais para operar.
- Falta definir `run_retention` no `dagster.yaml` (não incluído neste ciclo — débito registrado).

---

### Decision 4: Canonicalização do Resource Monitor (substitui ADR-0020)

| Attribute | Value |
|-----------|-------|
| **Status** | **Resolvida em 2026-08-11 — a premissa era falsa** |
| **Date** | Proposta 2026-08-04, verificada 2026-08-11 |

**Context:** Duas fontes descrevem Resource Monitors diferentes: `CLAUDE.md` (`cdc_trial_monitor`, 348 créditos, `FREQUENCY=NEVER`, nível de conta) vs `scripts/snowflake_setup.sql` (`cdc_poc_monitor`, 20 créditos, `FREQUENCY=MONTHLY`, nível de warehouse).

**Choice:** Tratar `scripts/snowflake_setup.sql` como canônico (está versionado e é recriável). Rodar `scripts/verify_governance.sql` no Snowflake novo antes de qualquer demo para confirmar o estado real.

**Rationale:** Sem essa verificação, não há garantia de que o freio de emergência da conta está configurado como o projeto documenta.

**Resultado da verificação (2026-08-11, como `ACCOUNTADMIN`):** nenhum dos dois monitores existe. `SHOW RESOURCE MONITORS` devolveu zero linhas, o parâmetro `RESOURCE_MONITOR` da conta está vazio e `CDC_WH.resource_monitor` é nulo. A pergunta desta decisão — *qual* dos dois monitores é o real — estava mal formulada: a resposta é nenhum. O `snowflake_setup.sql` tratado aqui como canônico nem sequer está no repositório, o que é consistente com nunca ter sido executado.

**Consequences:**
- ~~Bloqueante para demo ao vivo até confirmado.~~ Confirmado, e o resultado é o pior dos possíveis: **não existe freio de emergência**. Antes de qualquer demo ao vivo, criar um monitor de verdade — não verificar um que se supunha existir.
- As três camadas onde uma trava poderia estar (monitor da conta, parâmetro da conta, vínculo do warehouse) estão vazias. O que segura custo hoje é comportamental: `AUTO_SUSPEND = 60s` e os sensores consultando o Prometheus antes do Snowflake.
- `CDC_WH` tem `ENABLE_QUERY_ACCELERATION = true` com `SCALE_FACTOR = 2`, ou seja, um caminho de consumo que fatura além da própria compute do warehouse. Provavelmente dormente neste workload — as varreduras são pequenas demais para o QAS engatar — mas é exatamente o tipo de gasto que um monitor pegaria e que aqui não tem quem pegue.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `scripts/verify_governance.sql` | Create | Verificação de Resource Monitor real antes de qualquer demo | (general) | None |
| 2 | `scripts/streams_and_tasks.sql` | Create | 10 Streams + 10 Tasks + `SP_GATE_DOMAIN` + `CONFIG.PENDING_RUNS`/`STREAM_CONSUMPTION_LOG` | (general) | None |
| 3 | `dagster/pipeline/sensors.py` | Create | `bronze_new_data_sensor` (sem watermark) + `registry_new_subject_sensor` (gated via Prometheus) | (general) | 2 |
| 4 | `dagster/dagster.yaml` | Create | Storage Postgres + `QueuedRunCoordinator` | (general) | None |
| 5 | `docker-compose.yml` | Create | Serviço `dagster-postgres` + env vars atualizadas em `dagster`/`dagster-daemon` | (general) | 4 |
| 6 | `scripts/register_connectors.sh` | Create | Registro de conectores, endurecido contra `set -e` implícito | (general) | None |
| 7 | `.github/workflows/deploy.yml` | Create | CI/CD real — restart Dagster verificado, não placeholder | (general) | 5 |
| 8 | `observability/prometheus/alert_rules.yml` | Create | Alertas de sensor caído + Kafka ativo sem disparo | (general) | 3 |

**Total Files:** 8

---

## Agent Assignment Rationale

> Agentes descobertos em `.claude/agents/` deste template.

| Agent | Files Assigned | Why This Agent |
|-------|-----------------|-----------------|
| (general) | 1-8 | Nenhum agente em `.claude/agents/` deste template cobre Snowflake, Dagster ou Kafka especificamente (o mais próximo, `data-engineering/lakeflow-*` e `medallion-architect`, são focados em Databricks/Lakeflow). Build executa direto, sem delegação a especialista. |

**Nota para o Design desta feature:** vale considerar criar um agente `snowflake-dagster-specialist` em `.claude/agents/data-engineering/` como item de infraestrutura — não incluído neste ciclo.

---

## Code Patterns

### Pattern 1: Triggered Task com gate nativo

```sql
CREATE OR REPLACE TASK BRONZE.{DOMINIO}_GATE_TASK
    WAREHOUSE = CDC_WH
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.{DOMINIO}_STREAM')
AS
    CALL CONFIG.SP_GATE_DOMAIN('BRONZE.{DOMINIO}_STREAM', '{DOMINIO}');

ALTER TASK BRONZE.{DOMINIO}_GATE_TASK RESUME;  -- tasks nascem SUSPENDED
```

### Pattern 2: Sensor sem watermark

```python
cur.execute(
    "SELECT domain, detected_at FROM CONFIG.PENDING_RUNS WHERE consumed = FALSE"
)
rows = cur.fetchall()
if not rows:
    return SensorResult(skip_reason=SkipReason("Sem entradas novas."))
# marca consumido por flag, nunca por corte de tempo
cur.execute("UPDATE CONFIG.PENDING_RUNS SET consumed = TRUE WHERE consumed = FALSE")
```

### Pattern 3: Configuration Structure

```yaml
# dagster.yaml
storage:
  postgres:
    postgres_db:
      hostname: {env: DAGSTER_PG_HOST}
      username: {env: DAGSTER_PG_USER}
      password: {env: DAGSTER_PG_PASSWORD}
      db_name: {env: DAGSTER_PG_DB}
      port: 5432
```

---

## Data Flow

```text
1. Snowpipe (Kafka Connect) grava linha nova em BRONZE.<domínio>
   │
   ▼
2. STREAM <domínio>_STREAM detecta a mudança (append-only)
   │
   ▼
3. TASK agendada (1min) avalia WHEN SYSTEM$STREAM_HAS_DATA — custo zero se falso
   │
   ▼
4. Se verdadeiro: CALL SP_GATE_DOMAIN grava CONFIG.PENDING_RUNS + consome o stream
   │
   ▼
5. bronze_new_data_sensor (Dagster, poll 60s) lê PENDING_RUNS WHERE consumed=FALSE
   │
   ▼
6. RunRequest → dbt run (Silver → Gold)
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-------------------|------------------|
| Snowflake | SQL via `SnowflakeResource` (Dagster) | Par de chaves RSA (`CDC_ROLE`) |
| Prometheus | HTTP REST (`registry_new_subject_sensor`) | Nenhuma (rede interna Docker) |
| PostgreSQL (dagster-postgres) | Driver Postgres nativo do Dagster | Usuário/senha via `.env` |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Integração manual | Streams+Tasks disparando corretamente | `scripts/streams_and_tasks.sql` + `INFORMATION_SCHEMA.TASK_HISTORY` | SnowSQL/Snowsight | Confirmar 10 tasks `started`, disparo só com dado real |
| Regressão | Bug de watermark (AT-003) | novo teste pytest do sensor (não criado neste ciclo) | pytest + mock Snowflake connection | Cenário alto-volume/baixo-volume |
| Custo | Consumo ocioso | `verify_governance.sql` | SnowSQL | <1 crédito/dia em janela de 2h sem tráfego |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|---------------------|--------|
| Prometheus indisponível (`registry_new_subject_sensor`) | Fail-open — cai para checagem Snowflake direta | Não, tenta de novo no próximo tick |
| Task falha no meio da execução | `TRIGGER_ACTION` do Resource Monitor não definido ainda (pendente — feature futura) | N/A |
| `dagster-postgres` indisponível no boot | `depends_on: condition: service_healthy` no compose bloqueia subida do Dagster | Docker Compose healthcheck retry |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `DAGSTER_PG_HOST` | string | `dagster-postgres` | Host do Postgres dedicado do Dagster |
| `DAGSTER_PG_DB` | string | `dagster` | Nome do banco |
| `PROMETHEUS_URL` | string | `http://prometheus:9090` | Usado pelo gate do `registry_new_subject_sensor` |

---

## Security Considerations

- `GRANT EXECUTE TASK ON ACCOUNT TO ROLE CDC_ROLE` é privilégio de conta — revisar se `CDC_ROLE` deveria ter escopo mais restrito.
- `dagster-postgres` não deve expor porta pro host em produção (só `docker-compose.override.yml` de dev deveria expor).

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | `context.log` do Dagster nos sensores (info/warning conforme ADR de severidade da feature `GOVERNANCA_QUALIDADE_DADOS`) |
| Metrics | Prometheus + Grafana existentes; alertas novos em `alert_rules.yml` (sensor caído, Kafka ativo sem disparo) |
| Tracing | Não implementado — fora de escopo |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-04 | design-agent (via Claude) | Versão inicial, traduzida das ADRs 0018/0019/0020/0024 do v5-delivery |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_GOVERNANCA_CUSTO_DISPARO.md`
