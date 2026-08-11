# DEFINE: Governança de Qualidade de Dados (GOVERNANCA_QUALIDADE_DADOS)

> Formalizar invariantes estruturais e convenções de qualidade que hoje existem implicitamente no código, mas não estão documentadas em lugar nenhum.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOVERNANCA_QUALIDADE_DADOS |
| **Date** | 2026-08-04 |
| **Author** | Christian (via Claude) |
| **Status** | Accepted — convenção de severidade ratificada em 2026-08-11 |
| **Clarity Score** | 11/15 — **abaixo do gate de 12/15 da fase Define**, ver nota |

---

## Problem Statement

Várias partes do pipeline dependem de premissas não documentadas (Bronze é append-only) ou de decisões de design não categorizadas (quais models Gold são incrementalizáveis e por quê) — o que torna fácil violar essas premissas sem perceber, e obriga qualquer pessoa nova no projeto a inferir essas regras lendo código em vez de ler documentação.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Christian | Engenheiro de dados | Corre o risco de "corrigir" algo manualmente em Bronze (ex: um UPDATE de emergência) sem saber que isso quebra o comportamento dos Streams |
| Futuro mantenedor / cliente | Quem herdar o projeto | Sem essas invariantes escritas, não tem como saber o que pode e o que não pode mudar sem reler todo o código-fonte |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | A invariante "Bronze é append-only" está documentada, com todas as partes do sistema que dependem dela listadas |
| **MUST** | Os 6 models Gold estão categorizados por padrão de agregação, com nota de qual materialização usam hoje |
| **SHOULD** | Existe uma convenção proposta de severidade (error vs warn) para testes dbt e logging Dagster |
| **COULD** | Convenção de severidade aplicada retroativamente aos 162 testes dbt existentes (fora de escopo desta feature — é trabalho de Build separado) |

---

## Success Criteria

- [ ] Documento de invariante Bronze append-only existe e lista pelo menos as 4 partes do sistema que dependem dela (Kafka Connect Sink, Streams `APPEND_ONLY=TRUE`, filtro `op != 'd'` na Silver, Time Travel de 1 dia)
- [ ] Tabela de categorização dos 6 Gold está baseada no SQL real (não em suposição pelo nome do model) — cada linha cita o `materialized=` real do arquivo
- [ ] Convenção de severidade proposta cobre pelo menos os 3 contextos identificados (testes dbt, logging Dagster, TRIGGER_ACTION do Resource Monitor)

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Invariante Bronze é consultável | Alguém (humano ou agente) considera fazer um `UPDATE` manual em uma tabela Bronze | Consulta a documentação do projeto | Encontra a invariante explícita e as consequências de violá-la |
| AT-002 | Categorização Gold orienta decisão de incrementalização | Alguém decide se vale a pena incrementalizar `gold_revenue_per_restaurant` | Consulta a tabela de categorização | Vê que já é "aditivo-particionável, não implementado como tal" e o padrão de referência (`gold_payment_lifecycle`) |

---

## Out of Scope

- Aplicar a convenção de severidade proposta aos testes dbt existentes
- Implementar a incrementalização de nenhum model Gold (só documentar a categorização)
- `REVOKE UPDATE/DELETE` do `CDC_ROLE` sobre Bronze (mencionado como possível enforcement técnico, não implementado)

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Não existe convenção de severidade no projeto hoje (`grep severity` só retorna código do pacote `dbt_utils`, não nosso) | A convenção proposta é nova, não documentação de algo existente — precisa de revisão humana antes de virar "Accepted" |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `dbt/models/gold/*.sql` (só leitura, para categorização), documentação em `.claude/sdd/` | Nenhum código novo, só documentos |
| **KB Domains** | Nenhuma KB de domínio dbt/Snowflake existe neste template | Mesma lacuna das outras 2 features |
| **IaC Impact** | Nenhum | Feature é 100% documentação |

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | A convenção de severidade proposta (baseada em "a violação torna a métrica objetivamente errada" vs "só degrada qualidade") é aceitável para o time | Se não for, a tabela de critério precisa ser refeita | [ ] |

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 2 | Real, mas menos urgente/quantificável que as outras 2 features (não hesita em nenhum número de crédito ou latência) |
| Users | 2 | Genérico ("futuro mantenedor") |
| Goals | 3 | MUST/SHOULD/COULD claros |
| Success | 2 | Critérios são verificáveis mas qualitativos, não numéricos como as outras features |
| Scope | 2 | Out of Scope claro, mas a fronteira entre "documentar" e "implementar enforcement" é um pouco nebulosa |
| **Total** | **11/15** | Abaixo do mínimo de 12 — ver nota abaixo |

**Nota:** esta feature fica levemente abaixo do gate de 12/15 porque é fundamentalmente diferente das outras duas — não tem números de negócio pra medir, é trabalho de documentação. Registrar isso como um limite honesto do framework Clarity Score para features não-técnicas, não forçar uma pontuação artificial pra passar do gate.

---

## Open Questions

- Convenção de severidade (Decision no DESIGN) ainda não foi revisada/aceita por você — está marcada como proposta.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-04 | Christian (via Claude) | Versão inicial, traduzida das ADRs 0025, 0026, 0027 do v5-delivery |
| 1.1 | 2026-08-11 | Christian | Status para Accepted; nota sobre o gate de clareza |

---

## Nota sobre o gate de clareza (2026-08-11)

O Clarity Score deste documento é 11/15. O quality gate da fase Define, em `.claude/sdd/_index.md`, é **≥ 12/15**. O documento não passou no próprio portão e mesmo assim gerou um `DESIGN`, que gerou a convenção de severidade da Decision 3, que foi aplicada a 102 testes dbt em produção.

Isso ficou sem registro por uma semana. A cadeia inteira — Define reprovado, Design derivado dele, convenção aplicada antes da ratificação — foi construída abaixo da linha de corte que o próprio repositório publica. Nenhum passo isolado causou dano: as três decisões se sustentam, e a convenção de severidade se mostrou correta na prática. O ponto do registro é outro: o gate não segurou nada, e ninguém percebeu até alguém ir conferir.

A decisão de 2026-08-11 foi aceitar o documento como está, sem reescrevê-lo para inflar a nota. Um `DEFINE` corrigido depois do fato marcaria 15/15 sem que nada tivesse ficado mais claro.

---

## Next Step

**Concluída.** Design em [DESIGN_GOVERNANCA_QUALIDADE_DADOS.md](./DESIGN_GOVERNANCA_QUALIDADE_DADOS.md), build registrado em [BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md](../reports/BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md).
