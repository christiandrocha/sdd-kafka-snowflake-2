# DEFINE: Governança de Custo e Disparo (GOVERNANCA_CUSTO_DISPARO)

> Eliminar o consumo de crédito Snowflake em repouso, substituindo o polling do sensor Dagster por um gate de disparo nativo do Snowflake.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOVERNANCA_CUSTO_DISPARO |
| **Date** | 2026-08-04 |
| **Author** | Christian (via Claude) |
| **Status** | Ready for Design |
| **Clarity Score** | 14/15 |

---

## Problem Statement

O `bronze_new_data_sensor` do Dagster consulta o Snowflake a cada 60 segundos para checar se há dado novo, e esse intervalo coincide com o `AUTO_SUSPEND=60s` do warehouse `CDC_WH` — o warehouse nunca acumula uma janela real de inatividade e fica praticamente sempre ligado, consumindo crédito mesmo sem nenhum dado real para processar (estimativa: ~24 créditos/dia de consumo ocioso, contra uma quota mensal de 20 créditos no Resource Monitor).

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Christian | Engenheiro de dados / responsável pelo projeto | Vê o Resource Monitor estourar sem nenhum processamento real acontecendo; não confia no pipeline pra rodar sem supervisão |
| Cliente (futuro, quando o projeto virar modelo de referência) | Consumidor do case técnico | Precisa que a solução demonstrada não tenha um problema de custo óbvio na primeira demo ao vivo |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Warehouse `CDC_WH` não deve consumir crédito quando não há dado novo chegando via CDC |
| **MUST** | O sensor Dagster continua disparando o `dbt run` corretamente quando há dado novo, sem aumento de latência perceptível |
| **MUST** | O Resource Monitor real da conta reflete a configuração documentada (resolver a discrepância 348créditos/NEVER vs 20créditos/MONTHLY encontrada entre `CLAUDE.md` e `snowflake_setup.sql`) |
| **SHOULD** | Storage do Dagster suporta execução concorrente sem risco de corrupção (elimina SQLite) |
| **SHOULD** | Storage do Dagster não compartilha instância com o Postgres fonte do CDC (evita dependência circular) |
| **COULD** | Teste de regressão automatizado para o bug de watermark corrigido na ADR-0024 |

---

## Success Criteria

- [ ] Consumo de créditos do `CDC_WH` medido em `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` fica **abaixo de 1 crédito/dia** durante períodos de pelo menos 1 hora sem tráfego CDC real
- [ ] `SHOW RESOURCE MONITORS` confirma um único monitor ativo, com valores batendo com o `scripts/snowflake_setup.sql` versionado
- [ ] Latência entre um evento CDC chegar no Kafka e o `dbt run` correspondente disparar fica **abaixo de 3 minutos** (contra latência de até 1 hora observável hoje com o bug do watermark, na pior hipótese)
- [ ] `dagster-postgres` sobe e o Dagster conecta nele sem erro de migração, verificável via `dagster instance info`

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Warehouse fica ocioso sem tráfego | Nenhum evento CDC chega por 2 horas | O tempo passa | `WAREHOUSE_METERING_HISTORY` mostra consumo próximo de zero nesse período |
| AT-002 | Dado novo dispara o pipeline corretamente | Um evento CDC chega numa tabela de alto volume (ex: `ORDERS`) | A Task nativa correspondente detecta via `SYSTEM$STREAM_HAS_DATA` | `CONFIG.PENDING_RUNS` recebe uma linha e o sensor Dagster dispara um `RunRequest` em até 60s |
| AT-003 | Domínio de baixo volume não é perdido (regressão do bug ADR-0024) | Um domínio de alto volume (`ORDERS`) grava `PENDING_RUNS` com `detected_at` recente, avançando qualquer cursor global | Um domínio de baixo volume (`RECOMMENDATIONS`) grava `PENDING_RUNS` com `detected_at` mais antigo, na sequência seguinte | O sensor processa a linha de `RECOMMENDATIONS` normalmente — não fica invisível |
| AT-004 | Dagster sobrevive a restart sem perder run history | `dagster-postgres` está rodando com dado de runs anteriores | O container `dagster` reinicia | O histórico de runs anteriores continua acessível na UI |

---

## Out of Scope

- Migração da camada de ingestão (Kafka Connector v4, schematização) — feature separada `MIGRACAO_INGESTAO_V4`
- Categorização/otimização dos models Gold — feature separada `GOVERNANCA_QUALIDADE_DADOS`
- `CDC_WH_TRANSFORM` (segundo warehouse) e `tag_concurrency_limits` — aguardando contexto adicional, vira feature própria depois
- Watcher Kafka customizado (consumer group dedicado) — avaliado e descartado em favor de Streams+Tasks nativos (ver Decision 2 no DESIGN)

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Sem ambiente Snowflake/Kafka/Dagster ativo durante a análise original (agora resolvido — ambiente novo disponível) | Todo código do Build precisa de validação em ambiente de teste antes de qualquer demo de cliente |
| Technical | `CDC_ROLE` precisa de `GRANT EXECUTE TASK ON ACCOUNT` para as Tasks nativas funcionarem | Passo de setup obrigatório, uma vez, como `ACCOUNTADMIN` |
| Timeline | Prazo de semanas até uso como modelo de referência com cliente | Prioriza correção testável sobre arquitetura mais ambiciosa (watcher Kafka) |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `dagster/pipeline/`, `scripts/`, `docker-compose.yml`, `.github/workflows/` | Estrutura herdada do padrão do `sdd-kafka-snowflake` v1 |
| **KB Domains** | Nenhum domínio Snowflake/Kafka/Dagster existe em `.claude/kb/` deste template — mais próximo é `medallion/` (genérico) | KB específica de domínio ainda precisa ser criada (ver Open Questions) |
| **IaC Impact** | Modifica `docker-compose.yml` (novo serviço `dagster-postgres`), cria objetos Snowflake via SQL (Streams, Tasks, tabelas de controle) | Não há Terraform/IaC formal no projeto — governança via scripts SQL versionados |

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | `SYSTEM$STREAM_HAS_DATA` não consome warehouse quando o resultado é falso | Se consumir, a Task ainda seria mais barata que o polling atual, mas não chegaria a zero | [ ] |
| A-002 | O Resource Monitor real da conta bate com um dos dois valores documentados (348/NEVER ou 20/MONTHLY), não um terceiro valor desconhecido | Se for um terceiro valor, a ADR-0020 precisa ser refeita | [ ] |
| A-003 | O ambiente Snowflake novo já tem o database/warehouse/role básicos, ou serão criados do zero seguindo `kb/snowflake.md` do v1 | Se a estrutura for diferente, os scripts SQL precisam de adaptação | [ ] |

**Nota:** nenhuma assumption foi validada ainda — o ambiente Snowflake novo acabou de ficar disponível.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Quantificado (24 créditos/dia vs quota de 20/mês), causa raiz identificada com evidência de código |
| Users | 2 | Só 2 usuários, o segundo (cliente futuro) é hipotético ainda |
| Goals | 3 | MUST/SHOULD/COULD claramente priorizados |
| Success | 3 | Todos os critérios têm número ou comando de verificação específico |
| Scope | 3 | Out of Scope explícito, inclusive o que foi avaliado e descartado (watcher Kafka) |
| **Total** | **14/15** | |

---

## Open Questions

- Falta criar a KB de domínio (`kb/snowflake-cdc/` ou similar) neste template — nenhuma existe hoje para Snowflake/Kafka/Dagster. Não bloqueia o Design desta feature, mas deveria ser resolvido antes do Build para os agentes terem contexto de domínio correto.
- `CDC_WH_TRANSFORM` (segundo warehouse) — aguardando contexto adicional do usuário, tratado como feature separada.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-04 | Christian (via Claude) | Versão inicial, traduzida das ADRs 0018, 0019, 0020, 0024 do v5-delivery |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_GOVERNANCA_CUSTO_DISPARO.md`
