# sdd-kafka-snowflake

Pipeline de **Change Data Capture** de ponta a ponta: PostgreSQL → Debezium → Kafka → Snowflake, com modelagem em camadas via dbt, orquestração por Dagster e observabilidade em Prometheus + Grafana.

O diferencial do projeto não é a stack — é o **controle**: a estratégia de CDC de cada domínio vive numa tabela de metadados no Snowflake, não no SQL; o disparo do pipeline passa por um gate que consulta o Kafka antes de acordar o warehouse; e as invariantes de cada camada são testadas, com severidade escolhida caso a caso.

---

## Status

| Componente | Estado | Última verificação |
|---|---|---|
| Ingestão CDC (Debezium → Kafka → Snowflake Sink) | Operando | 2026-08-10 |
| Camada Bronze (10 modelos dbt) | Materializada | 2026-08-10 |
| Camada Silver (10 modelos dbt) | Materializada | 2026-08-10 |
| Camada Gold | Não implementada | — |
| Testes dbt (155) | 0 erros, 8 avisos | 2026-08-10 |
| CI/CD (`.github/workflows/deploy.yml`) | Nunca executado | — |

Os 8 avisos são de integridade referencial e foram rastreados até a base de origem — ver [Qualidade de dados](#qualidade-de-dados).

---

## Arquitetura

```
┌──────────────┐    WAL     ┌──────────────┐          ┌──────────────────┐
│  PostgreSQL  │──────────► │   Debezium   │────────► │      Kafka       │
│   (origem)   │            │    source    │          │  10 tópicos CDC  │
└──────────────┘            └──────────────┘          └────────┬─────────┘
                                                               │
                                          ┌────────────────────┴────────┐
                                          │      Schema Registry        │
                                          │  (contratos + schematização)│
                                          └────────────────────┬────────┘
                                                               │
                                              ┌────────────────▼─────────────┐
                                              │  Snowflake Sink Connector    │
                                              │  (Snowpipe Streaming)        │
                                              └────────────────┬─────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────▼─────────────┐
│  Snowflake · CDC_POC                                                       │
│                                                                            │
│   BRONZE ──────────────► SILVER ──────────────► GOLD                       │
│   append CDC bruto       estado atual          agregações                  │
│   (incremental/merge)    (resolve_cdc)         (não implementada)          │
│                                                                            │
│   CONFIG.TABLE_METADATA ── estratégia de CDC por domínio ───────┘          │
└────────────────────────────────────────────────────────────────────────────┘
              ▲                                            ▲
              │ orquestração                               │ métricas
      ┌───────┴────────┐                          ┌────────┴─────────┐
      │    Dagster     │◄─── gate de custo ───────│    Prometheus    │◄── JMX +
      │ sensors + jobs │     (checa Kafka)        │     Grafana      │  Kafka Exporter
      └────────────────┘                          └──────────────────┘
```

### Domínios

Dez tabelas atravessam o pipeline inteiro, do Postgres ao Silver:

| Domínio | Tipo | Chave | Papel |
|---|---|---|---|
| `orders` | entity | `order_id` | Hub — liga os demais por CPF, CNPJ e `driver_id` |
| `order_items` | fact | `order_item_id` | Maior volume (210 mil linhas) |
| `payment_events` | fact | `event_id` | Ciclo de vida do pagamento (event sourcing) |
| `users_mongo` | entity | `uuid` | Usuários (origem MongoDB); junta por CPF |
| `users_mssql` | entity | `uuid` | Perfil estendido (origem MSSQL); mesmo CPF |
| `restaurants` | entity | `uuid` | Junta com `orders` por CNPJ |
| `drivers` | entity | `uuid` | Junta por `driver_id` |
| `driver_shifts` | entity | `shift_id` | Turnos do entregador |
| `search_events` | log | `search_id` | Buscas do usuário |
| `recommendations` | log | `event_id` | Eventos de recomendação de ML |

---

## Modelagem em camadas

### Bronze — o CDC bruto, idempotente

Um modelo por domínio, `incremental` com `merge`. As colunas chegam **tipadas e em maiúsculo** (o sink roda com `snowflake.enable.schematization=true`), então não há extração de VARIANT. Cada modelo:

- deduplica por chave dentro do lote, ordenando por `source_ts_ms DESC, kafka_offset DESC`;
- usa `RECORD_METADATA:CreateTime` como watermark incremental;
- **descarta o tombstone do Kafka.** O conector roda com `drop.tombstones=false`, então todo DELETE emite duas mensagens: a linha com `__OP='d'` e, em seguida, uma de valor nulo que o sink materializa como linha inteiramente nula. Sem o filtro `WHERE <chave> IS NOT NULL`, o MERGE nunca casa com chave nula e cada DELETE deixa lixo permanente.

Colunas de controle preservadas em todas as camadas: `op`, `source_ts_ms`, `kafka_offset`, `kafka_partition`, `kafka_created_at` — são a linhagem que liga cada linha ao evento Kafka que a produziu.

### Silver — o estado atual, dirigido por metadados

Nenhum modelo Silver contém lógica de CDC. Todos têm a mesma forma:

```sql
{{ resolve_cdc(ref('bronze_orders')) }}
```

A macro [`resolve_cdc`](dbt/macros/resolve_cdc.sql) lê a estratégia do domínio em `CONFIG.TABLE_METADATA` e resolve o histórico:

| Estratégia | Comportamento |
|---|---|
| `upsert` | Uma linha por chave, a versão mais recente; DELETE é descartado |
| `append` | Sem deduplicação; só DELETE sai |
| `log` | Nada é descartado, DELETE inclusive — registro histórico |

Trocar a estratégia de um domínio é um `UPDATE` na tabela de metadados, não um deploy de SQL.

Três decisões que valem a leitura antes de mexer:

- **Desempate por `kafka_offset`.** Ordenar só por `source_ts_ms` (milissegundos) empata em UPDATE em cascata e carga em lote, e o `ROW_NUMBER` passa a escolher de forma não determinística — o mesmo `dbt run` produzindo Silver diferente.
- **`op IS DISTINCT FROM 'd'`, não `op != 'd'`.** Em SQL, `NULL != 'd'` é NULL, não TRUE: o filtro ingênuo descartava em silêncio toda linha com `op` nulo.
- **`materialized='table'`, não `incremental`.** MERGE não apaga linha. Num incremental, uma chave deletada na origem sobreviveria para sempre na Silver; com rebuild, ela simplesmente não reaparece. O custo é varrer a Bronze a cada execução — barato no volume atual, e a saída, se crescer, é `delete+insert` particionado, não `merge`.

A macro [`get_table_config`](dbt/macros/get_table_config.sql) carrega os metadados com um fallback estático, porque o entrypoint do Dagster roda `dbt parse` offline: sem conexão, `execute` é falso e a chave do `config()` sairia vazia no manifest.

### Gold

Não implementada. A pasta existe e o `dbt_project.yml` já a configura.

---

## Governança de custo

O warehouse é o item caro da conta, e o pipeline foi desenhado para não acordá-lo à toa.

Os dois sensores do Dagster (`bronze_new_data_sensor` e `registry_new_subject_sensor`, intervalo de 60s) **consultam o Prometheus antes do Snowflake**. Sem tráfego novo no Kafka, o sensor pula sem abrir conexão:

```
Sensor bronze_new_data_sensor skipped: Sem atividade no Kafka (via Prometheus) — Snowflake não consultado.
```

Em repouso, o custo do pipeline ligado é zero crédito. Complementam o desenho um Resource Monitor na conta e Time Travel de 1 dia nas tabelas Bronze — auditáveis por `scripts/verify_governance.sql`.

---

## Qualidade de dados

155 testes dbt: 83 na Bronze, 72 na Silver.

A Silver testa o que a Bronze não tem como garantir — as invariantes que a resolução de CDC adiciona:

1. **`unique` + `not_null` na chave.** Na Bronze o `unique` passa pela deduplicação por lote; na Silver ele vale sobre o histórico inteiro.
2. **`accepted_values` em `op` sem o `'d'`.** É o teste do filtro de delete. Se a estratégia de um domínio virar `log`, este teste quebra de propósito.
3. **`not_null` nas colunas de ordenação** (`source_ts_ms`, `kafka_offset`) — nulo ali significa desempate não determinístico de volta.

### Convenção de severidade

| Severidade | Quando | Exemplo |
|---|---|---|
| `error` | Invariante garantida pelo código deste repositório | `unique` na chave da entidade |
| `warn` | Integridade referencial entre domínios | `order_items.order_id → orders` |

O motivo do `warn` é concreto: os dez fluxos CDC são independentes e têm tempos de snapshot próprios. Um pedido chegar antes do entregador dele é latência normal, não defeito — derrubar o pipeline por isso seria falso positivo.

### Achados em aberto (2026-08-10)

Os avisos foram rastreados até a origem, e **nenhum é defeito do pipeline**:

| Achado | Medida | Verificação na origem |
|---|---|---|
| 7.246 linhas de `order_items` sem pedido correspondente (85 `order_id` distintos) | Nenhum desses IDs existe em `bronze_orders` — não é efeito do filtro de delete | O Postgres fonte tem exatamente os mesmos 7.246: `order_items` referencia 491 pedidos, e a tabela `orders` só tem 414. A base semeada não tem FK |
| 95 CPFs duplicados em `users_mongo` | `uuid` é único, mas a mesma pessoa aparece com vários | A origem tem 412 usuários para 216 CPFs distintos. Junção por CPF sofre fan-out — considerar ao modelar a Gold |

A Silver reproduz a origem linha a linha: 414 pedidos, 210.002 itens, os mesmos 7.246 órfãos.

---

## Estrutura

```
connectors/           Debezium source + Snowflake sink (JSON de configuração)
dagster/pipeline/     assets (dbt), jobs, sensors com gate de custo, resources
dbt/
  macros/             resolve_cdc, get_table_config, generate_schema_name
  models/bronze/      10 modelos incrementais + schema.yml (83 testes)
  models/silver/      10 modelos via resolve_cdc + schema.yml (72 testes)
  models/config/      sources.yml
observability/        Prometheus (scrape + alertas), JMX exporter
scripts/              bootstrap do schema CONFIG, streams/tasks, roles, governança
tests/                gerador de carga para o Postgres fonte
.claude/sdd/          registro do fluxo de especificação (define → design → build → ship)
```

---

## Como rodar

### Pré-requisitos

- Docker e Docker Compose
- Conta Snowflake com autenticação por par de chaves RSA
- `keys/` com a chave privada `.p8` (fora do versionamento)

### Configuração

Crie um `.env` na raiz — ignorado pelo git, nunca commitado:

```bash
# PostgreSQL fonte
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
DATABASE_URL=

# Snowflake (par de chaves)
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PRIVATE_KEY_PATH=
SNOWFLAKE_DATABASE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_ROLE=
SNOWFLAKE_URL=

# Kafka
SCHEMA_REGISTRY_URL=

# dbt
DBT_TARGET=dev

# Storage do Dagster
DAGSTER_PG_DB=
DAGSTER_PG_USER=
DAGSTER_PG_PASSWORD=
```

### Subida

```bash
docker compose up -d --build

# aguarde o Kafka Connect responder
curl -sf http://localhost:8083/connectors

# registre os conectores
./scripts/register_connectors.sh
```

Antes da primeira execução do dbt, rode os scripts Snowflake na ordem: `bootstrap_config.sql` (cria o schema `CONFIG`) e depois `streams_and_tasks.sql`.

### dbt

O dbt vive dentro do container do Dagster, com o projeto montado por bind:

```bash
# validação offline — não abre conexão com o warehouse
docker compose exec dagster-daemon \
  bash -c "cd /opt/dagster/dbt && dbt parse --target dev"

# materialização e testes
docker compose exec dagster-daemon \
  bash -c "cd /opt/dagster/dbt && dbt run  --select silver --target dev"
docker compose exec dagster-daemon \
  bash -c "cd /opt/dagster/dbt && dbt test --select silver --target dev"
```

Se o `dagster-daemon` estiver em loop de restart (por exemplo, com o `dagster-postgres` fora do ar), cada `exec` morre junto com o container. Use um container descartável, imune ao loop:

```bash
docker compose run --rm --no-deps --entrypoint bash dagster-daemon \
  -c "cd /opt/dagster/dbt && dbt test --select silver --target dev"
```

### Serviços

| Serviço | Porta | Papel |
|---|---|---|
| `zookeeper` | 2181 | Coordenação do Kafka |
| `kafka` | 9092 | Broker |
| `schema-registry` | 8081 | Contratos e schematização |
| `postgres` | 5432 | Banco fonte do CDC |
| `dagster-postgres` | — | Storage do Dagster, separado da fonte |
| `kafka-connect` | 8083 | Debezium source + Snowflake sink |
| `dagster` | 3000 | Webserver |
| `dagster-daemon` | — | Schedules e sensores |
| `kafka-ui` | 8080 | Inspeção de tópicos |
| `jmx-exporter` / `kafka-exporter` | — | Métricas do broker |
| `prometheus` | 9090 | Coleta e alertas |
| `grafana` | 3001 | Dashboards |

---

## Scripts Snowflake

Rodados manualmente via SnowSQL ou worksheet — **não** automatizados, porque mudanças de governança de conta exigem `ACCOUNTADMIN` e revisão humana:

| Script | Função |
|---|---|
| `scripts/bootstrap_config.sql` | Cria o schema `CONFIG` e semeia `TABLE_METADATA` |
| `scripts/streams_and_tasks.sql` | Streams e Tasks que alimentam os sensores |
| `scripts/create_readonly_role.sql` | Role de leitura para ferramentas externas |
| `scripts/verify_governance.sql` | Auditoria de Resource Monitor, Time Travel e consumo |
| `scripts/sync_metadata.py` | Sincroniza Schema Registry → `TABLE_METADATA` |
| `scripts/init.sql` | Inicialização do Postgres fonte |

---

## Segurança

- `.env`, `keys/`, `*.p8`, `*.key` e `*.pem` são ignorados pelo git.
- A autenticação com o Snowflake é por par de chaves RSA; a chave privada é montada por volume, nunca embutida na imagem.
- Identidades de serviço separadas por função (o Dagster usa a sua, com `CDC_ROLE`), e uma role somente-leitura para ferramentas de consulta.
- O Grafana sobe com credenciais padrão (`admin`/`admin`) — troque antes de qualquer exposição fora de `localhost`.

---

## CI/CD

`.github/workflows/deploy.yml` dispara em push para `main` quando `connectors/`, `dbt/` ou os arquivos de compose mudam, e monta o `.env` a partir dos GitHub Secrets.

**Nunca foi executado contra um ambiente real.** Antes do primeiro uso, note que ele ainda referencia dois arquivos ausentes do repositório: `docker-compose.prod.yml` e `scripts/snowflake_setup.sql`.

---

## Fluxo de trabalho

O repositório usa um fluxo de especificação em cinco fases — brainstorm, define, design, build, ship — com os artefatos versionados em `.claude/sdd/`. Cada feature entregue deixa o `DEFINE`, o `DESIGN`, o relatório de build e o registro de encerramento, o que torna as decisões de arquitetura rastreáveis muito depois do merge.
