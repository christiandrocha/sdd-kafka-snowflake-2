# sdd-kafka-snowflake

Pipeline CDC de ponta a ponta: **PostgreSQL → Debezium → Kafka → Schema Registry → Snowflake Sink → Snowflake**, com transformações dbt (Bronze/Silver/Gold) orquestradas por Dagster e observabilidade via Prometheus + Grafana.

> **Status:** a stack sobe via Docker Compose, mas os componentes que tocam Snowflake/Kafka reais **não foram testados ao vivo**. Os comentários nos arquivos marcam explicitamente o que é não-testado.

---

## Arquitetura

```
PostgreSQL (fonte)
      │  CDC
      ▼
Debezium ──► Kafka ──► Schema Registry
                │
                ▼
        Snowflake Sink Connector
                │
                ▼
           Snowflake ──► dbt (Bronze/Silver/Gold)
                              ▲
                              │ orquestração
                          Dagster
```

Observabilidade em paralelo: JMX Exporter + Kafka Exporter → Prometheus → Grafana.

## Serviços

| Serviço | Porta | Papel |
|---|---|---|
| `zookeeper` | 2181 | Coordenação do Kafka |
| `kafka` | 9092 | Broker |
| `schema-registry` | 8081 | Contratos Avro/JSON |
| `postgres` | 5432 | Banco fonte (CDC) |
| `dagster-postgres` | — | Storage do Dagster, separado da fonte (ADR-0018) |
| `kafka-connect` | 8083 | Debezium source + Snowflake sink |
| `dagster` | 3000 | Webserver |
| `dagster-daemon` | — | Schedules e sensors |
| `kafka-ui` | 8080 | Inspeção de tópicos |
| `jmx-exporter` / `kafka-exporter` | — | Métricas do broker |
| `prometheus` | 9090 | Coleta e alertas |
| `grafana` | 3001 | Dashboards |

## Pré-requisitos

- Docker + Docker Compose
- Conta Snowflake com autenticação por par de chaves (RSA)
- `keys/` local com a chave privada `.p8` (fora do versionamento)

## Configuração

Crie um `.env` na raiz — ele é ignorado pelo git e nunca deve ser commitado:

```bash
# PostgreSQL fonte
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
DATABASE_URL=

# Snowflake (autenticação por par de chaves)
SNOWFLAKE_URL=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PRIVATE_KEY_PATH=
SNOWFLAKE_DATABASE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_ROLE=

# Kafka
SCHEMA_REGISTRY_URL=

# dbt
DBT_TARGET=

# Storage do Dagster
DAGSTER_PG_DB=
DAGSTER_PG_USER=
DAGSTER_PG_PASSWORD=
```

## Subindo a stack

```bash
docker compose up -d --build

# aguarde o Kafka Connect responder
curl -sf http://localhost:8083/connectors

# registre os conectores
./scripts/register_connectors.sh
```

## Scripts Snowflake

Rodados manualmente via SnowSQL/worksheet — **não** automatizados no CI, porque mudanças de governança de conta exigem `ACCOUNTADMIN` e revisão humana:

| Script | Função |
|---|---|
| `scripts/init.sql` | Inicialização do PostgreSQL fonte |
| `scripts/create_readonly_role.sql` | Role de leitura para ferramentas (ex.: Cursor) |
| `scripts/streams_and_tasks.sql` | Streams + Tasks que alimentam os sensors do Dagster |
| `scripts/verify_governance.sql` | Auditoria de Resource Monitor, Time Travel e consumo |

## Segurança

- `.env`, `keys/`, `*.p8`, `*.key` e `*.pem` são ignorados pelo git.
- A autenticação com Snowflake usa par de chaves RSA; a chave privada é montada nos containers por volume, nunca embutida na imagem.
- O Grafana sobe com credenciais default (`admin`/`admin`) — troque antes de qualquer exposição para fora de `localhost`.

## CI/CD

`.github/workflows/deploy.yml` roda em push para `main` e monta o `.env` a partir dos GitHub Secrets. **Nunca foi executado contra um ambiente real** — antes do primeiro merge, confira os nomes dos secrets e note que o workflow referencia arquivos ainda ausentes do repositório: `docker-compose.prod.yml`, `connectors/`, `dbt/` e `scripts/snowflake_setup.sql`.
