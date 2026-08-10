# DESIGN: Governança de Qualidade de Dados (GOVERNANCA_QUALIDADE_DADOS)

> Design (documental) das invariantes e convenções de qualidade formalizadas.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOVERNANCA_QUALIDADE_DADOS |
| **Date** | 2026-08-04 |
| **Author** | design-agent (via Claude) |
| **DEFINE** | [DEFINE_GOVERNANCA_QUALIDADE_DADOS.md](./DEFINE_GOVERNANCA_QUALIDADE_DADOS.md) |
| **Status** | Draft — Decision 3 pendente de aceite humano |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│              GOVERNANÇA DE QUALIDADE — SÓ DOCUMENTAÇÃO                   │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  Nenhum componente de runtime novo — esta feature produz 3 documentos:   │
│                                                                            │
│  [Invariante Bronze append-only] ──consultado por──▶ qualquer humano/    │
│                                                        agente antes de    │
│                                                        tocar em Bronze    │
│                                                                            │
│  [Categorização Gold] ──orienta──▶ decisão futura de incrementalização   │
│                                                                            │
│  [Convenção de severidade] ──proposta para──▶ testes dbt + logging       │
│                                                Dagster + TRIGGER_ACTION   │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Documento de invariante Bronze | Registra a premissa append-only e suas dependências | Markdown |
| Tabela de categorização Gold | Classifica os 6 models por padrão de agregação | Markdown |
| Proposta de convenção de severidade | Critério error/warn para testes e logging | Markdown |

---

## Key Decisions

### Decision 1: Bronze append-only como invariante explícita (substitui ADR-0025)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** Streams `APPEND_ONLY=TRUE` (feature `GOVERNANCA_CUSTO_DISPARO`), o filtro `op != 'd'` na Silver, e o Time Travel de 1 dia no Bronze dependem, sem declarar isso em lugar nenhum, de que Bronze só recebe `INSERT`.

**Choice:** Declarar formalmente que nenhum processo deve executar `UPDATE`/`DELETE` direto em qualquer tabela `BRONZE.*`. Única escrita válida é `INSERT` via Snowpipe.

**Rationale:** Torna explícita uma premissa que já era verdadeira na prática, prevenindo violação acidental (ex: correção manual "rápida" que quebraria o comportamento do Stream `APPEND_ONLY`).

**Alternatives Rejected:**
1. **Não documentar, confiar que ninguém vai violar** — rejeitado, é exatamente o tipo de premissa implícita que já causou problema demonstrado nesta análise (o próprio bug de watermark do sensor, embora diferente, também nasceu de uma premissa implícita não verificada).

**Consequences:**
- Não há enforcement técnico automático ainda — é convenção documentada, não trava.
- Item futuro sugerido: `REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA BRONZE FROM ROLE CDC_ROLE`.

---

### Decision 2: Categorização dos 6 models Gold por padrão de agregação (substitui ADR-0026)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-04 |

**Context:** Necessidade de decidir quais models Gold valem a pena incrementalizar, com base real (SQL lido), não suposição pelo nome.

**Choice:**

| Model | Materialização atual | Categoria |
|---|---|---|
| `gold_payment_lifecycle` | incremental (já correto) | Aditivo-particionável — referência de padrão |
| `gold_payments_by_status` | incremental (já correto) | Razão-global limitada por baixa cardinalidade |
| `gold_payment_funnel` | table (full refresh, intencional) | Razão-global |
| `gold_driver_performance` | table | Aditivo-particionável (não incrementalizado ainda) |
| `gold_revenue_per_restaurant` | table | Aditivo-particionável (não incrementalizado ainda) |
| `gold_user_behavior` | table | Aditivo-particionável por usuário (não "janela-deslizante" — é cumulativo, não janela de tempo fixa) |

**Rationale:** Ler o SQL real corrigiu duas categorizações que uma análise anterior (baseada em nome/heurística) tinha proposto incorretamente.

**Alternatives Rejected:**
1. **Categorizar por nome/heurística sem ler o SQL** — rejeitado, gerou pelo menos 2 categorizações erradas na tentativa anterior.

**Consequences:**
- Nenhuma incrementalização implementada nesta feature — só a categorização, como base para decisão futura gatilhada por volume real.

---

### Decision 3: Convenção de severidade (substitui ADR-0027) — PROPOSTA, NÃO ACEITA

| Attribute | Value |
|-----------|-------|
| **Status** | **Proposed — requer revisão humana antes de "Accepted"** |
| **Date** | 2026-08-04 |

**Context:** Não existe convenção de severidade documentada no projeto (`grep severity` só retorna código de um pacote externo). Três contextos precisam de critério: testes dbt, logging Dagster, `TRIGGER_ACTION` do Resource Monitor.

**Choice (proposto):** error/`SUSPEND_IMMEDIATE` quando a violação corrompe dado ou deixa estado inconsistente sem recuperação segura; warn/`log.warning` quando o dado está degradado mas o sistema continua previsível; info/`log.info` para operação normal.

**Rationale:** Critério prático: um teste vira `error` se a violação, propagada, tornaria uma métrica de negócio objetivamente errada.

**Alternatives Rejected:** Nenhuma avaliada ainda — é a primeira proposta, não uma escolha entre alternativas conhecidas.

**Consequences:**
- **Não aplicar aos 162 testes dbt existentes até esta Decision ser explicitamente aceita.**

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `.claude/sdd/features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md` | Create | Este próprio documento — as 3 decisões já estão inline aqui, não há arquivo de código a gerar | (general) | None |

**Total Files:** 1 (esta é uma feature 100% documental — nenhum código de produção é gerado)

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|-----------------|-----------------|
| (general) | 1 | Feature documental, sem código — não há especialista técnico a delegar |

---

## Code Patterns

Não aplicável — feature sem código de produção.

---

## Data Flow

Não aplicável — feature documental.

---

## Integration Points

Não aplicável.

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Revisão humana | Decision 3 (severidade) | Este documento | Leitura humana | Aceite explícito antes de aplicar a qualquer teste real |

---

## Error Handling

Não aplicável.

---

## Configuration

Não aplicável.

---

## Security Considerations

- O enforcement técnico sugerido (`REVOKE UPDATE, DELETE`) não foi implementado — Bronze continua tecnicamente gravável por `UPDATE`/`DELETE` pelo `CDC_ROLE`, mesmo com a invariante documentada.

---

## Observability

Não aplicável — feature documental.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-04 | design-agent (via Claude) | Versão inicial, traduzida das ADRs 0025/0026/0027 do v5-delivery |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md` — mas Decision 3 precisa de aceite humano explícito antes de qualquer aplicação prática.
