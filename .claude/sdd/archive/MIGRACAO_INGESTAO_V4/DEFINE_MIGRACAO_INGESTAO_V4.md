# DEFINE: Migração de Ingestão Kafka Connector V4 (MIGRACAO_INGESTAO_V4)

> Migrar de Snowpipe clássico (conector v2.1.2) para Kafka Connector V4 nativo Snowpipe Streaming, e reduzir o escopo de ingestão aos domínios que efetivamente alimentam Silver/Gold.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | MIGRACAO_INGESTAO_V4 |
| **Date** | 2026-08-04 |
| **Author** | Christian (via Claude) |
| **Status** | Shipped (2026-08-07) |
| **Clarity Score** | 13/15 |

---

## Problem Statement

O projeto usa `snowflake-kafka-connector` v2.1.2 com `snowflake.ingestion.method=SNOWPIPE` (Snowpipe clássico, baseado em arquivo), apesar de ter sido descrito como "Snowpipe Streaming" em toda a documentação — e essa versão está numa geração que a Snowflake está descontinuando (v3 e anteriores, 18 meses de janela até fim de vida). Além disso, metade dos 20 domínios ingeridos (10 de 20) nunca alimenta nenhum modelo Silver ou Gold, sendo volume pago e processado sem consumo analítico real.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Christian | Engenheiro de dados | Vai apresentar o projeto como modelo de referência; não quer demonstrar um conector que a própria Snowflake já classificou como legado |
| Cliente (futuro) | Consumidor do case | Espera ver a stack mais atual, não uma versão prestes a ser descontinuada |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Conector migrado para v4 (`SnowflakeStreamingSinkConnector`), sem quebrar a ingestão dos 10 domínios Tier 1 |
| **MUST** | Escopo de ingestão reduzido aos 10 domínios que alimentam Silver/Gold (PAYMENTS e os outros 9 domínios Tier 2 removidos) |
| **MUST** | Colunas tipadas nativamente (schematização), normalizadas para MAIÚSCULO — consistente com o padrão do resto do projeto |
| **SHOULD** | Os dois conectores Sink (`sink`/`sinkitems`) consolidados em um só, já que a razão original (buffer client-side) desaparece no v4 |
| **COULD** | Avaliar Iceberg tables como evolução futura (fora de escopo desta feature) |

---

## Success Criteria

- [ ] Os 10 domínios Tier 1 continuam chegando em Bronze sem gap nem duplicata após a migração
- [ ] Latência ponta a ponta (Kafka → Bronze) cai de 60-120s (buffer clássico) para **5-10 segundos** (v4)
- [ ] Os 10 modelos Bronze reescritos compilam e passam nos testes dbt existentes
- [ ] Um único conector Sink registrado (não mais 2)

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Ingestão dos 10 domínios Tier 1 funciona no v4 | Conector v4 registrado e rodando | Um evento CDC chega em `ORDERS` | A linha aparece em `BRONZE.ORDERS` com colunas tipadas em MAIÚSCULO, sem VARIANT |
| AT-002 | Domínios Tier 2 não são mais ingeridos | `debezium.json` atualizado sem os 10 domínios removidos | O Postgres fonte recebe um evento em `payments` (Tier 2) | Nenhum tópico Kafka novo é criado para esse domínio |
| AT-003 | `order_items` continua funcionando após consolidação de conectores | Conector único (não mais `sink`+`sinkitems`) | Volume alto chega em `order_items` | Ingestão não atrasa os demais domínios (validar se buffer server-side do v4 realmente resolve isso) |

---

## Out of Scope

- Schematização com validação `server_side` (optou-se por `client_side`, usando o Schema Registry como fonte de tipo)
- Migração dos modelos Silver/Gold em si
- Avaliação de Iceberg tables
- PAYMENTS permanece removido (confirmado como órfão pelo responsável do projeto — não há reversão nesta feature)

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | v4 exige Java 11+ | Confirmado atendido — `debezium/connect:2.4` (imagem stock, sem base customizada) embarca Java 17 |
| Technical | Combinação `schematization=true` + `validation=client_side` não tem exemplo documentado oficialmente | Precisa validação empírica antes de qualquer demo |
| Timeline | Reescrita de 10 modelos Bronze é o item de maior volume de trabalho desta feature | Maior risco de atraso dentro do prazo de semanas |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `connectors/`, `dbt/models/bronze/`, `Dockerfile.connect` | Estrutura herdada do v1 |
| **KB Domains** | Nenhuma KB de Snowflake/Kafka Connect existe neste template ainda | Mesma lacuna da feature `GOVERNANCA_CUSTO_DISPARO` |
| **IaC Impact** | Reescreve `connectors/snowflake_sink.json` (consolidado), `Dockerfile.connect` (versão do JAR) | Sem Terraform formal |

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | `schematization=true` + `validation=client_side` são combináveis (dois eixos independentes, conforme doc oficial) | Se não forem, precisa cair para o combo padrão v4 (`server_side`) ou compat v3 | [ ] |
| A-002 | O nome das colunas geradas pelo schema Avro bate 1:1 com os campos usados hoje nos modelos Bronze (`RECORD_CONTENT:campo`) | Se não bater, os 10 modelos Bronze precisam de mapeamento adicional | [ ] |

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Quantificado e verificado contra doc oficial da Snowflake |
| Users | 2 | Mesma limitação da feature anterior |
| Goals | 3 | MUST/SHOULD/COULD claros |
| Success | 2 | Critério de latência é bom, mas "compilar e passar testes" não tem número de quantos testes |
| Scope | 3 | Out of Scope explícito, inclusive decisões revertidas (case das colunas) |
| **Total** | **13/15** | |

---

## Open Questions

- Nenhuma bloqueante — todas as decisões de configuração (schematização, validação, case dos identificadores, consolidação de conectores) já foram fechadas nesta conversa.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-04 | Christian (via Claude) | Versão inicial, traduzida das ADRs 0021 e 0022 do v5-delivery |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_MIGRACAO_INGESTAO_V4.md`
