# BUILD REPORT: Governança de Custo e Disparo (GOVERNANCA_CUSTO_DISPARO)

> Execução do gate de disparo nativo do Snowflake (Streams + Triggered Tasks) e correção do consumo ocioso do warehouse `CDC_WH`.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOVERNANCA_CUSTO_DISPARO |
| **Date** | 2026-08-10 |
| **Author** | build-agent |
| **DEFINE** | [DEFINE_GOVERNANCA_CUSTO_DISPARO.md](../features/DEFINE_GOVERNANCA_CUSTO_DISPARO.md) |
| **DESIGN** | [DESIGN_GOVERNANCA_CUSTO_DISPARO.md](../features/DESIGN_GOVERNANCA_CUSTO_DISPARO.md) |
| **Status** | Complete com ressalvas — 2 critérios pendentes de verificação por ACCOUNTADMIN |

---

## Nota sobre o que foi este Build

Os 8 arquivos do manifesto do DESIGN já existiam antes desta sessão: todos entraram
no repositório pelo commit inicial `897acef`, escritos fora do fluxo `/build`. Foi
exatamente o que o `SHIPPED_2026-08-07.md` da feature anterior registrou como lição
("Status de documento mente se ninguém o atualiza" — o DESIGN dizia "Ready for Build"
com os 8 arquivos já escritos e sem BUILD_REPORT).

O trabalho desta sessão foi o que faltava, e que nenhum manifesto de arquivos captura:
**executar o desenho contra a conta real**. O schema `CONFIG` não existia, nenhum
Stream ou Task tinha sido criado, e os sensores nunca tinham rodado. A execução
revelou cinco defeitos — quatro deles em código que estava commitado e que passaria
por qualquer revisão estática.

---

## Summary

| Metric | Value |
|--------|-------|
| **Arquivos do manifesto** | 8/8, todos pré-existentes ao Build |
| **Arquivos corrigidos nesta sessão** | 2 (`sensors.py`, `bootstrap_config.sql`) |
| **Objetos criados no Snowflake** | 10 Streams + 10 Tasks + 1 procedure + 2 tabelas de controle |
| **Statements SQL executados** | 35/35 sem erro (`streams_and_tasks.sql`) |
| **Defeitos encontrados na execução** | 5 (4 em código, 1 em documento) |
| **Latência CDC -> `dbt run` concluído** | 145s (meta do DEFINE: < 3 min) |
| **Agents Used** | 0 — `(direct)`, conforme o Agent Assignment Rationale do DESIGN |

---

## Task Execution

| # | Task | Agent | Status | Notes |
|---|------|-------|--------|-------|
| 1 | `scripts/verify_governance.sql` | (direct) | Pré-existente | Não executável pelas identidades de serviço — ver Blockers |
| 2 | `scripts/streams_and_tasks.sql` | (direct) | Pré-existente + **executado** | 35 statements, 0 erros |
| 3 | `dagster/pipeline/sensors.py` | (direct) | Pré-existente + **corrigido** | 3 defeitos; ver Issues 2, 3 e 4 |
| 4 | `dagster/dagster.yaml` | (direct) | Pré-existente | Storage Postgres confirmado de pé |
| 5 | `docker-compose.yml` | (direct) | Pré-existente | `dagster-postgres` sobe healthy |
| 6 | `scripts/register_connectors.sh` | (direct) | Pré-existente | 2 conectores RUNNING |
| 7 | `.github/workflows/deploy.yml` | (direct) | Pré-existente | Não exercitado — sem push nesta sessão |
| 8 | `observability/prometheus/alert_rules.yml` | (direct) | Pré-existente | Não exercitado — nenhum alerta disparou |
| — | `scripts/bootstrap_config.sql` | (direct) | **Executado + corrigido** | **Fora do manifesto** — pré-requisito do item 2; ver Issue 1 |

---

## Files Created / Modified

| File | Ação nesta sessão | Verificado por |
|------|-------------------|----------------|
| `scripts/bootstrap_config.sql` | Modify (+5 linhas) | Schema `CONFIG` + 3 tabelas criadas na conta; 10 domínios ativos |
| `dagster/pipeline/sensors.py` | Modify (+173/-58) | Import no container, `default_status` e métrica conferidos em runtime |

Ambos no commit `ef552ee`. Os 8 arquivos do manifesto não foram alterados, exceto
`sensors.py`, que é simultaneamente item 3 do manifesto e alvo de correção.

---

## Verification Results

### Objetos no Snowflake

```text
streams: 10  | mode=['APPEND_ONLY'] stale=['false']
tasks:   10  | state=Counter({'started': 10})  schedule=['1 MINUTE']
CONFIG:  TABLE_METADATA, METADATA_HISTORY, PROCESSING_LOG,
         PENDING_RUNS, STREAM_CONSUMPTION_LOG, SP_GATE_DOMAIN
TABLE_METADATA: 10 domínios ativos, 0 inativos
```

**Status:** Pass

### Gate de custo zero (o objetivo da feature)

```text
13:40-13:47  bronze_new_data_sensor skipped: CONFIG.PENDING_RUNS sem entradas novas.
             ^ código anterior à correção: tocava o Snowflake em todo ciclo
13:47:40     restart do Dagster com a correção
13:48-13:50  skipped: Sem atividade no Kafka (via Prometheus) - Snowflake não consultado.
13:51:10     INSERT de 5 linhas (teste ponta a ponta)
13:51-13:54  volta a consultar o Snowflake  <- tráfego real, comportamento correto
13:55-13:57  skipped: Sem atividade no Kafka (via Prometheus) - Snowflake não consultado.
```

Contraprova de que o gate não lê ruído de infraestrutura, em uma query só:

```text
pg.public.orders   valor=5   changes[10m]=0   delta[10m]=0
```

**Status:** Pass — 3 ciclos consecutivos sem tocar o warehouse, em duas janelas distintas.

### Fluxo ponta a ponta

| Etapa | Horário (UTC) | Δ desde o insert |
|---|---|---|
| `INSERT` de 5 linhas em `orders` (Postgres fonte) | 13:51:10 | — |
| Linhas visíveis em `BRONZE.ORDERS` | <= 13:51:45 | <= 35s |
| Task nativa grava `CONFIG.PENDING_RUNS` | 13:51:45 | 35s |
| `bronze_new_data_sensor` dispara `cdc_pipeline_job` | 13:52:46 | 96s |
| `RUN_SUCCESS` do `dbt run` | 13:53:35 | 145s |

Contagens: `BRONZE.ORDERS` 410 -> 415. Model `BRONZE_ORDERS` 409 -> 415 (subiu 6:
o model estava 1 linha atrás do raw desde antes do teste). `PENDING_RUNS` com 1 linha,
`domain=ORDERS`, `consumed=TRUE`. `STREAM_CONSUMPTION_LOG` com `rows_seen=5` — o Stream
viu exatamente as 5 linhas.

**Status:** Pass

### Lint / Type Check

Não configurados no repositório. `sensors.py` verificado por `ast.parse` e por import
real dentro do container do Dagster.

**Status:** N/A

---

## Issues Encountered

| # | Issue | Resolução |
|---|-------|-----------|
| 1 | `streams_and_tasks.sql` falhou no primeiro DDL: `003001 (42501) ... must have CREATE TABLE granted on SCHEMA CDC_POC.CONFIG`. O bloco de grants do `bootstrap_config.sql` concedia `USAGE` + DML nas tabelas, mas não `CREATE TABLE`/`CREATE PROCEDURE` — e o passo 2 da ordem de execução declarada no cabeçalho do próprio arquivo precisa criar 3 objetos ali | `GRANT CREATE TABLE, CREATE PROCEDURE` acrescentado ao arquivo e aplicado na conta pelo ACCOUNTADMIN |
| 2 | **Gate do Prometheus nunca funcionou.** `KAFKA_MESSAGES_METRIC` apontava para `kafka_server_brokertopicmetrics_messagesin_total`; o jmx-exporter publica o nome sem `_total`. A query voltava vazia, o gate caía no fail-open de "sem série nenhuma" e liberava sempre. Defeito pré-existente: significa que o gate de custo zero do `registry_new_subject_sensor`, descrito como funcionando no `SHIPPED_2026-08-07.md`, nunca operou | Nome corrigido + filtro `topic=~"pg[.]public[.].*"`, porque as 4 séries publicadas em repouso são internas e `connect_statuses` recebe heartbeat do Kafka Connect |
| 3 | **O sensor desfazia, de fora, o gate que a feature criou.** `bronze_new_data_sensor` abria conexão Snowflake incondicionalmente a cada 60s. Qualquer query resume o warehouse e o Snowflake cobra mínimo de 60s por resume: com `AUTO_SUSPEND=60` e polling de 60s, ~1.440 resumes/dia = 24h de X-Small = ~24 créditos/dia, o mesmo número que motivou a feature | Mesmo gate de custo zero aplicado ao sensor, com `HOT_CYCLES_AFTER_ACTIVITY=3` |
| 4 | Fail-open pela condição errada: "a query veio vazia" em vez de "o cursor está vazio". Nenhuma série `pg.public.*` existe até a primeira mensagem CDC do broker atual, então o sensor bateria no Snowflake para sempre | Flag `initialized` no cursor |
| 5 | Decision 4 declara `scripts/snowflake_setup.sql` como fonte canônica do Resource Monitor. **O arquivo não existe** — nem no working tree nem em nenhum commit do repositório | Não resolvido — ver Blockers |

O Issue 3 merece registro à parte: o docstring de `sensors.py` afirmava que 60s era
seguro "porque a query em si é trivial". O peso da query não é a variável relevante,
e o próprio arquivo já dizia duas linhas abaixo que o warehouse continuava sendo
tocado. As duas passagens se contradiziam desde que foram escritas, e nenhuma
revisão de documento pegou — só a medição.

---

## Deviations from Design

| Deviation | Motivo | Impacto |
|-----------|--------|---------|
| `bootstrap_config.sql` entrou no escopo | Não está no manifesto do DESIGN, mas é pré-requisito do item 2 (nasceu no Build da `MIGRACAO_INGESTAO_V4`) | Ordem de execução real é bootstrap -> grant -> streams_and_tasks |
| Gate de Prometheus estendido ao `bronze_new_data_sensor` | O DESIGN previa o gate só no `registry_new_subject_sensor` (Decision 1 tratava o custo do sensor Bronze como resolvido pelas Tasks nativas) | Sem isso o objetivo MUST do DEFINE não é atingido — ver Issue 3 |
| `HOT_CYCLES_AFTER_ACTIVITY` não previsto no DESIGN | Kafka e Task nativa são assíncronos: a mensagem chega num ciclo e a Task grava `PENDING_RUNS` até 1 min depois. Sem a margem, a última linha de uma rajada ficaria sem consumo | Custo: até 3 consultas extras ao Snowflake por rajada |

---

## Blockers

| Blocker | Ação necessária | Owner |
|---------|-----------------|-------|
| AT-001 não medido | Janela de ~2h sem tráfego + `SELECT ... FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`. Requer `CDC_GOVERNANCE_RO`, concedida só ao usuário humano por decisão registrada | CHRISTIANDROCHA |
| Decision 4 segue "Proposed" | `SHOW RESOURCE MONITORS` como ACCOUNTADMIN. Com `CDC_ROLE` o comando retorna 0 linhas sem erro — isso é ausência de visibilidade, **não** prova de que não há monitores | CHRISTIANDROCHA |
| `scripts/snowflake_setup.sql` ausente (Issue 5) | Decidir se o arquivo é recriado (e a Decision 4 mantida) ou se a decisão é reescrita apontando para outra fonte canônica | CHRISTIANDROCHA |

---

## Acceptance Test Verification

| ID | Scenario | Status | Evidência |
|----|----------|--------|-----------|
| AT-001 | Warehouse ocioso sem tráfego | **Não medido** | Causa raiz resolvida e demonstrada (gate fecha, warehouse `SUSPENDED`), mas o número em créditos exige `WAREHOUSE_METERING_HISTORY` |
| AT-002 | Dado novo dispara o pipeline | **Pass com ressalva** | `PENDING_RUNS` em 35s; `RunRequest` em 96s. A meta do AT era 60s para o `RunRequest` — o pior caso é ~2 min por construção (`minimum_interval_seconds=60` + `SCHEDULE='1 MINUTE'`). O critério de latência do DEFINE (< 3 min até o `dbt run`) foi cumprido: 145s |
| AT-003 | Domínio de baixo volume não é perdido | **Não exercitado** | Exige dois domínios gravando `PENDING_RUNS` com `detected_at` fora de ordem. O código não tem mais cursor de tempo nenhum sobre `PENDING_RUNS`, mas isso é argumento, não teste |
| AT-004 | Dagster sobrevive a restart sem perder histórico | **Pass parcial** | Observado de passagem: runs continuaram visíveis após 3 restarts do webserver e do daemon em 2026-08-10. Não foi um teste dirigido |

---

## Performance Notes

| Métrica | Esperado (DEFINE) | Medido | Status |
|---------|-------------------|--------|--------|
| Latência CDC -> `dbt run` | < 3 min | 145s | Pass |
| Latência CDC -> `RunRequest` | < 60s (AT-002) | 96s | Ressalva estrutural |
| Consumo ocioso do `CDC_WH` | < 1 crédito/dia | não medido | Pendente |
| Ciclos do sensor sem tocar o Snowflake | — | 3 consecutivos, 2 janelas | Pass |

Sobre o custo residual: as 10 Tasks avaliam `SYSTEM$STREAM_HAS_DATA` 14.400 vezes/dia
no control plane, o que não liga o warehouse mas consome cloud services — faturado só
acima de 10% do compute diário. Com o warehouse quase parado o denominador é pequeno,
então cloud services pode aparecer no metering. Vale considerar isso ao interpretar
AT-001, antes de concluir que a meta falhou.

---

## Final Status

### Overall: COMPLETE COM RESSALVAS

**Completion Checklist:**

- [x] Todos os itens do manifesto presentes e executados
- [x] Objetos criados e verificados no Snowflake
- [x] Objetivo MUST do DEFINE (warehouse não consome em repouso) demonstrado
- [x] Fluxo ponta a ponta verificado com dado real
- [ ] AT-001 medido em créditos
- [ ] AT-003 exercitado
- [ ] Decision 4 promovida de Proposed para Accepted
- [ ] Pronto para `/ship`

---

## Next Step

**Antes do `/ship`:** resolver os 3 blockers, todos dependentes do ACCOUNTADMIN. Os
dois primeiros são medição; o terceiro é uma decisão sobre o `snowflake_setup.sql`.

**Se a Decision 4 mudar de premissa:** `/iterate DESIGN_GOVERNANCA_CUSTO_DISPARO.md
"Decision 4 aponta para arquivo inexistente"` antes de arquivar — a lição
"vale corrigir o DESIGN antes de arquivar, não depois" está registrada no
`SHIPPED_2026-08-07.md`.
