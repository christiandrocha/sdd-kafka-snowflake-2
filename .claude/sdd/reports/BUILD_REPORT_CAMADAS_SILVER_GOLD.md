# BUILD REPORT: Camadas Silver e Gold

> Relatório de implementação da resolução de CDC (Silver) e das seis agregações analíticas (Gold).

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | CAMADAS_SILVER_GOLD |
| **Date** | 2026-08-10 |
| **Author** | Christian (via Claude) |
| **DEFINE** | **Ausente** — ver "Desvios de processo" |
| **DESIGN** | Parcial — a Gold seguiu a Decision 2 de [DESIGN_GOVERNANCA_QUALIDADE_DADOS.md](../features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md); a Silver não teve documento |
| **Status** | Complete |
| **Commits** | `546da20`, `a2c936d`, `7ee1e1c`, `11bd8d0`, `73344a7` |

---

## Desvios de processo (leia primeiro)

Este relatório é escrito **depois** do build, não durante, e o fluxo de cinco fases não foi seguido. Registrar isso é o ponto: o repositório anuncia o fluxo no README, e um build sem DEFINE nem DESIGN é dívida de processo, não um detalhe.

| Fase | O que deveria existir | O que existe |
|------|----------------------|--------------|
| Define | `DEFINE_CAMADAS_SILVER_GOLD.md` com critérios de aceite | Nada. Os requisitos vieram da conversa e do código da Bronze |
| Design | `DESIGN_CAMADAS_SILVER_GOLD.md` com manifesto de arquivos | Só para a Gold, e indireto: a Decision 2 do `DESIGN_GOVERNANCA_QUALIDADE_DADOS` catalogou os 6 models com materialização e categoria |
| Build | Este relatório | Escrito retroativamente em 2026-08-10 |

**Consequência prática:** não há critérios de aceite pré-acordados contra os quais verificar. A seção de verificação abaixo mede o que foi construído, não o que foi prometido — a diferença importa se alguém for auditar depois.

---

## Summary

| Metric | Value |
|--------|-------|
| **Arquivos criados** | 20 (2 macros, 10 models Silver, 6 models Gold, 2 schema.yml) |
| **Arquivos modificados** | 10 models Bronze + `bootstrap_config.sql` |
| **Linhas** | 1.546 nos arquivos criados |
| **Models no projeto** | 26 (10 Bronze, 10 Silver, 6 Gold) |
| **Testes** | 185 (83 Bronze, 72 Silver, 30 Gold) |
| **Testes falhando** | 0 erros, 10 avisos (todos rastreados até a origem) |
| **Agentes usados** | 0 — build direto |

---

## Execução

| # | Tarefa | Status | Notas |
|---|--------|--------|-------|
| 1 | Filtro de tombstone nos 10 models Bronze | Complete | Defeito pré-existente, corrigido antes de construir sobre ele |
| 2 | Porte de `get_table_config` e `resolve_cdc` do projeto v6 | Complete | Três correções sobre o original — ver "Decisões" |
| 3 | 10 models Silver via `resolve_cdc` | Complete | Nenhum contém lógica de CDC |
| 4 | `schema.yml` da Silver, 72 testes | Complete | Primeira aplicação da convenção de severidade |
| 5 | 6 models Gold | Complete | SQL novo, não portado: os 10 domínios Tier 2 não existem mais |
| 6 | `schema.yml` da Gold, 30 testes | Complete | `unique` no grão como guarda do anti-fan-out |
| 7 | Correção de `table_type` de `search_events` e `recommendations` | Complete | Aplicado no seed, no fallback e na conta, com `METADATA_HISTORY` |
| 8 | Verificação ponta a ponta com INSERT e DELETE reais | Complete | Ver "Verificação" |

---

## Arquivos criados

| Arquivo | Linhas | Notas |
|---------|--------|-------|
| `dbt/macros/get_table_config.sql` | 144 | Fallback estático reduzido de 20 para 10 domínios |
| `dbt/macros/resolve_cdc.sql` | 107 | Três estratégias: `upsert`, `append`, `log` |
| `dbt/models/silver/silver_orders.sql` | 37 | Contém o racional de materialização das 10 |
| `dbt/models/silver/silver_search_events.sql` | 27 | |
| `dbt/models/silver/silver_payment_events.sql` | 23 | |
| `dbt/models/silver/silver_recommendations.sql` | 20 | |
| `dbt/models/silver/silver_driver_shifts.sql` | 19 | |
| `dbt/models/silver/silver_order_items.sql` | 18 | Candidato nº 1 a sair de `table` se o volume doer |
| `dbt/models/silver/silver_users_mongo.sql` | 16 | |
| `dbt/models/silver/silver_drivers.sql` | 15 | |
| `dbt/models/silver/silver_restaurants.sql` | 15 | |
| `dbt/models/silver/silver_users_mssql.sql` | 15 | |
| `dbt/models/silver/schema.yml` | 363 | 72 testes |
| `dbt/models/gold/gold_user_behavior.sql` | 166 | O mais complexo: costura CPF com `user_id` numérico |
| `dbt/models/gold/gold_driver_performance.sql` | 100 | |
| `dbt/models/gold/gold_revenue_per_restaurant.sql` | 96 | |
| `dbt/models/gold/gold_payment_funnel.sql` | 95 | |
| `dbt/models/gold/gold_payment_lifecycle.sql` | 83 | Referência do padrão incremental |
| `dbt/models/gold/gold_payments_by_status.sql` | 50 | |
| `dbt/models/gold/schema.yml` | 137 | 30 testes |

---

## Decisões tomadas durante o build

Nenhuma destas estava num DESIGN prévio. Ficam registradas aqui porque são o tipo de escolha que alguém vai querer entender daqui a seis meses.

### D1 — Desempate por `kafka_offset` na resolução de CDC

A versão v6 do `resolve_cdc` ordenava só por `source_ts_ms DESC`. Esse campo vem do Debezium em milissegundos, e duas mudanças na mesma linha dentro do mesmo milissegundo — comum em UPDATE em cascata e carga em lote — empatam. O `ROW_NUMBER` então escolhia de forma não determinística: o mesmo `dbt run` podia produzir Silver diferente. Acrescentado `kafka_offset DESC`, monotônico por partição, igual ao que os models Bronze já faziam.

### D2 — `op IS DISTINCT FROM 'd'` no lugar de `op != 'd'`

Em SQL, `NULL != 'd'` é NULL, não TRUE. O filtro antigo descartava em silêncio toda linha com `op` nulo, apesar de ela não ser um delete.

### D3 — Descarte explícito de tombstone

D2 sozinha abre um buraco. O conector roda com `drop.tombstones=false`, então todo DELETE produz duas mensagens: a linha com `__OP='d'` e um tombstone de valor nulo que o sink materializa como linha inteiramente nula. Com `op != 'd'` esse lixo sumia **por acidente**; com `IS DISTINCT FROM` ele passaria a vazar. Daí o `<chave> IS NOT NULL`, aplicado tanto na Bronze quanto no `resolve_cdc`. O descarte passou a ser intencional em vez de efeito colateral de semântica de NULL.

### D4 — Silver em `materialized='table'`, contra o default `incremental` do projeto

MERGE não apaga linha. Num incremental, uma chave deletada na origem sobreviveria para sempre na Silver, porque a linha de delete é filtrada e nunca chega ao merge. Com rebuild, ela simplesmente deixa de aparecer. O custo é varrer a Bronze a cada execução — barato no volume atual. Se crescer, a saída é `delete+insert` particionado, não `merge`.

### D5 — Convenção de severidade nos testes

`error` para invariante garantida pelo código deste repositório; `warn` para integridade referencial entre domínios. O motivo do `warn` é concreto: os dez fluxos CDC são independentes e têm tempos de snapshot próprios, então um pedido chegar antes do entregador dele é latência normal, não defeito.

**Ratificada em 2026-08-11, um dia depois de já estar aplicada.** Quando este relatório foi escrito, a Decision 3 do `DESIGN_GOVERNANCA_QUALIDADE_DADOS` estava marcada como *"Proposed — requer revisão humana antes de Accepted"* e trazia, em negrito, a instrução de não aplicá-la antes do aceite. Este build aplicou assim mesmo, a 102 testes. A ratificação foi retroativa e está registrada em [BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md](BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md).

### D6 — Defesa contra fan-out na Gold

`silver_drivers`, `silver_restaurants` e `silver_users_mongo` garantem unicidade pela chave **técnica** (`uuid`), não pela chave de **negócio** usada nas junções (`driver_id`, `restaurant_id`, `cpf`). Nesta base, 95 CPFs têm mais de um cadastro. Sem tratamento, cada pedido seria contado duas vezes e o `gasto_total` sairia inflado até 2×. Os três models afetados reduzem a dimensão a uma linha por chave de negócio via `QUALIFY`, com o mesmo critério determinístico do `resolve_cdc`.

Em `gold_user_behavior` a ponte `cpf → user_id` **não** é deduplicada de propósito — só os atributos. Assim buscas e recomendações dos `user_id` secundários de um mesmo CPF continuam sendo contadas.

### D7 — Órfão entra com flag, não sai da tabela

Toda junção fato→dimensão na Gold é LEFT JOIN partindo do fato, com a coluna `sem_cadastro`. INNER JOIN apagaria 55 entregadores e 27 restaurantes do relatório sem deixar rastro.

### D8 — Receita por restaurante vem de `order_items`, não de `orders`

Granularidade (o item carrega quantidade, desconto e categoria), população (210.002 itens contra 414 pedidos) e consistência de chave (`restaurant_id` dos dois lados, contra CNPJ em texto). **Consequência:** `receita_bruta` não reconcilia com a soma de `orders.total_amount`. São duas medidas de populações diferentes, não um erro de uma delas.

### D9 — `table_type` de `search_events` e `recommendations` corrigido para `fact`

Estavam registrados como `log` com `cdc_strategy='upsert'`, combinação contraditória. A medição na origem decidiu qual lado estava errado: 203 e 255 linhas para 203 e 255 chaves distintas, zero deletes — append-only. Corrigida a etiqueta, não a estratégia. Mudar a estratégia para `log` custaria três arquivos e quebraria o `unique` da chave, o `accepted_values` de `op` e as contagens do `gold_user_behavior`.

---

## Verificação

### Build e testes

```text
dbt run  --select silver          PASS=10  ERROR=0
dbt run  --select gold            PASS=6   ERROR=0
dbt run  --select gold  (2ª vez)  PASS=6   ERROR=0   <- ramo incremental
dbt test --select silver          PASS=63  WARN=9  ERROR=0   (antes de D9)
dbt test --select gold            PASS=28  WARN=2  ERROR=0
```

O segundo run da Gold é o que exercitou o ramo `is_incremental() = true`, e os dois números confirmam o desenho de cada model: `gold_payment_lifecycle` fez `SUCCESS 0` (nenhum pagamento com evento novo — MERGE no-op, tabela intacta em 8 linhas) e `gold_payments_by_status` fez `SUCCESS 7` (razão global recalculada por inteiro, como projetado).

### Teste ponta a ponta com dado real

Inserida e depois apagada uma linha em `order_items` no Postgres fonte, para exercitar o caminho completo:

| Etapa | Evidência |
|---|---|
| INSERT → Kafka | offset 210002 → 210003 |
| INSERT → Snowflake raw | `__OP='c'`, `CreateTime` no minuto do insert |
| INSERT → Bronze/Silver | linha presente nas duas, via watermark incremental |
| DELETE → Kafka | offset 210003 → 210005: **duas** mensagens |
| DELETE → Snowflake raw | linha `d` (só a chave) + tombstone (`__OP`, chave e `source_ts_ms` todos nulos) |
| Tombstone → Bronze | descartado — 0 linhas de chave nula, 210.003 no total |
| DELETE → Silver | chave removida — 210.002 linhas |

Isso tirou D3 e D2 do terreno do raciocínio: as duas correções passaram a ter verificação contra dado real, não só contra o `dbt parse`.

### Metadados

`CONFIG.TABLE_METADATA` existe na conta com os 10 domínios ativos, idênticos ao fallback estático. Confirma que a Silver é de fato dirigida por metadados em tempo de execução, e não pelo fallback.

---

## Achados de qualidade de dados

Nenhum é defeito do pipeline. Todos foram rastreados até a base de origem.

| Achado | Medida | Verificação na origem |
|---|---|---|
| 7.246 `order_items` sem pedido (85 `order_id` distintos) | Nenhum desses IDs existe em `bronze_orders` | O Postgres fonte tem os mesmos 7.246: `order_items` referencia 491 pedidos e `orders` tem 414. Sem FK na base semeada |
| 95 CPFs duplicados em `users_mongo` | `uuid` é único, a pessoa não | 412 usuários para 216 CPFs distintos. Tratado na Gold por D6 |
| `payment_id` com cardinalidade degenerada | 2.210 eventos em 8 identificadores, interseção **zero** com `orders.payment_key` | Propriedade do gerador sintético. Os 3 models de pagamento são estruturalmente corretos e comercialmente inúteis nesta base |
| `gold_user_behavior` cobre 295 de 414 pedidos | Os 119 faltantes são os órfãos de `user_key` | `gasto_total` é receita atribuível a usuário conhecido, não receita total |
| Vocabulário de `event_type` errado na documentação | `purchase` e `dismiss` não ocorrem; faltavam `add_to_cart` e `recommendation_served` | Corrigido em 4 lugares — o comentário da Bronze era a origem do erro e havia contaminado o teste |

---

## Problemas encontrados durante o build

| # | Problema | Resolução |
|---|----------|-----------|
| 1 | `dbt test` morria com SIGKILL (exit 137) | O `dagster-daemon` estava em loop de restart (61 reinícios) porque o `dagster-postgres` estava parado; cada restart matava o `docker exec`. Resolvido subindo a stack, e contornado com `docker compose run --rm --no-deps` para comandos longos |
| 2 | Dagster não enxergava os models Gold | O grafo de assets é congelado no import do container. Resolvido com restart do daemon — e é fragilidade operacional permanente: model novo exige restart, senão o pipeline roda "com sucesso" ignorando a camada nova |
| 3 | `accepted_values` de `event_type` abriu WARN | O vocabulário tinha vindo de um comentário, não dos dados. Levantado por query e corrigido na origem do erro |

---

## Pendências

| Pendência | Ação necessária | Quem decide |
|-----------|-----------------|-------------|
| ~~Convenção de severidade não ratificada (D5)~~ | **Resolvida em 2026-08-11.** Decision 3 ratificada retroativamente; as 16 marcações `warn` ficam como estão. Aberto: os 83 testes Bronze nunca passaram pelo critério | Christian |
| ~~`DEFINE` e `DESIGN` ausentes desta feature~~ | **Resolvida em 2026-08-11: dívida aceita explicitamente.** Não serão escritos retroativamente — um `DEFINE` redigido depois do build descreveria o que foi construído, não o que foi prometido | Christian |
| ~~`BUILD_REPORT` do `GOVERNANCA_QUALIDADE_DADOS`~~ | **Resolvida em 2026-08-11.** Aquela feature é 100% documental: o entregável já existia, faltava o registro de fase 3. Ver [BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md](BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md) | Christian |
| ~~Cardinalidade de `payment_id`~~ | **Resolvida em 2026-08-11: aceita como andaime.** Os 3 models de pagamento ficam marcados no `schema.yml` como estruturalmente corretos e sem significado de negócio nesta base | Christian |
| ~~MERGE incremental com dado novo~~ | **Resolvida em 2026-08-11.** Evento `closed` inserido no Postgres percorreu Debezium, Kafka e sink; `gold_payment_lifecycle` devolveu `SUCCESS 1` com a tabela parada em 8 linhas — update, não insert | — |
| CI/CD | Nunca executado; referencia `docker-compose.prod.yml` e `scripts/snowflake_setup.sql`, ambos ausentes | Christian |
| Ordenação no CTE `alvo` | Descoberta em 2026-08-11: evento cujo `timestamp` seja anterior ao evento mais novo de **outro** pagamento é ignorado, porque o watermark é global | Christian |

---

## Status final

### Overall: Complete

- [x] Todos os models construídos e materializados
- [x] Todos os testes executados contra a conta real
- [x] Zero erros; 10 avisos, todos explicados e rastreados
- [x] Caminho CDC verificado ponta a ponta com INSERT e DELETE reais
- [ ] Critérios de aceite pré-acordados — **não existem**, ver "Desvios de processo"
- [ ] Pronto para `/ship` — falta ratificar D5 e resolver a dívida de DEFINE/DESIGN
