# BUILD REPORT: Governança de Qualidade de Dados

> Relatório de fase 3 de uma feature cujo entregável é documental. Escrito sete dias depois do DESIGN, para fechar o ciclo e registrar o que aconteceu no intervalo.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOVERNANCA_QUALIDADE_DADOS |
| **Date** | 2026-08-11 |
| **Author** | Christian (via Claude) |
| **DEFINE** | [DEFINE_GOVERNANCA_QUALIDADE_DADOS.md](../features/DEFINE_GOVERNANCA_QUALIDADE_DADOS.md) — Clarity Score 11/15, abaixo do gate |
| **DESIGN** | [DESIGN_GOVERNANCA_QUALIDADE_DADOS.md](../features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md) — 3 decisões, todas `Accepted` |
| **Status** | Complete |

---

## Por que este relatório parecia não fazer sentido

O README listava esta feature como "tem `DEFINE` e `DESIGN` e nunca foi construída". A descrição está errada de um jeito instrutivo.

O File Manifest do `DESIGN` lista **um** arquivo: o próprio `DESIGN`. A nota abaixo dele diz "esta é uma feature 100% documental — nenhum código de produção é gerado". O entregável já existia no instante em que o `DESIGN` foi salvo. Não havia build a executar, e por isso nenhum foi executado.

O que faltava não era construção. Era o registro de fase 3 — este documento — e, principalmente, o aceite humano que a Decision 3 exigia em letras maiúsculas e que ninguém tinha dado. A feature ficou uma semana num limbo em que parecia incompleta por falta de código, quando estava incompleta por falta de uma resposta.

---

## Summary

| Metric | Value |
|--------|-------|
| **Arquivos criados neste build** | 1 (este relatório) |
| **Arquivos de código gerados** | 0 — previsto pelo DESIGN |
| **Decisões ratificadas** | 3 de 3 |
| **Testes afetados** | 0 novos; 102 já aplicados desde 2026-08-10 |
| **Agentes usados** | 0 |

---

## Verificação dos Success Criteria do DEFINE

| Critério | Status | Evidência |
|----------|--------|-----------|
| Invariante Bronze append-only documentada, com as 4 partes que dependem dela | Atendido | Decision 1 lista Kafka Connect Sink, Streams `APPEND_ONLY=TRUE`, filtro `op != 'd'` na Silver e Time Travel de 1 dia |
| Categorização dos 6 Gold baseada no SQL real, citando o `materialized=` de cada arquivo | Atendido | Decision 2; a tabela confere com os arquivos em `dbt/models/gold/` |
| Convenção de severidade cobrindo os 3 contextos | Atendido com ressalva | Decision 3 cobre testes dbt, logging Dagster e `TRIGGER_ACTION`. Os dois primeiros estão exercitados; o terceiro nunca foi verificado — exige `ACCOUNTADMIN` |

O Time Travel de 1 dia citado na Decision 1 foi verificado em 2026-08-11: os 20 objetos de `CDC_POC.BRONZE` retornam `retention_time = 1`. A verificação também mostrou que o valor vem de default (o dbt materializa como `TRANSIENT`, cujo teto é 1 dia) e não de configuração — nenhum `retention` é declarado no projeto. A invariante da Decision 1 continua verdadeira, mas se apoia num default, não numa trava.

---

## Decisões

### Ratificação retroativa da Decision 3

A convenção de severidade foi aceita em 2026-08-11, um dia **depois** de já estar aplicada a 102 testes. A Decision 3 dizia, em negrito, para não aplicá-la antes do aceite; o build das camadas Silver e Gold aplicou assim mesmo, e registrou o fato em texto. As duas frases conviveram no repositório por um dia sem que nada as reconciliasse.

A ratificação legaliza o estado atual em vez de revertê-lo, porque a convenção se mostrou correta na prática: 16 testes em `warn`, todos por integridade referencial entre domínios CDC independentes, que é latência normal e não defeito. Reverter 16 marcações para provar um ponto de processo custaria mais do que registrar o que houve.

**O que segue aberto:** os 83 testes da Bronze nunca passaram pelo critério. Estão em `error` por omissão do default, não por análise. Aplicar a convenção a eles é trabalho de um build futuro.

### Aceite da dívida de processo das camadas Silver e Gold

Decisão tomada na mesma sessão, sobre feature vizinha: os `DEFINE` e `DESIGN` ausentes das camadas Silver e Gold **não** serão escritos retroativamente. O `BUILD_REPORT_CAMADAS_SILVER_GOLD.md` já documenta o que existe e admite o desvio no primeiro parágrafo. Um `DEFINE` redigido depois do build descreveria exatamente o que foi construído — forma sem controle — e enfraqueceria a credibilidade dos `DEFINE` escritos antes.

### Não arquivamento

A feature não foi movida para `.claude/sdd/archive/`. Duas razões: `GOVERNANCA_CUSTO_DISPARO` está construída e permanece em `features/`, e mover estes dois documentos quebraria o link relativo em `BUILD_REPORT_CAMADAS_SILVER_GOLD.md:13`. Duas features construídas guardadas de formas diferentes seria pior que qualquer das duas convenções.

---

## Problemas encontrados

| # | Problema | Resolução |
|---|----------|-----------|
| 1 | O gate de clareza da fase Define (≥ 12/15) não segurou um documento de 11/15, que gerou `DESIGN` e convenção aplicada em produção | Registrado no `DEFINE`, sem reescrever o documento para inflar a nota |
| 2 | A frase "não aplicar até o aceite" e o `BUILD_REPORT` que registrava a aplicação coexistiram por um dia sem reconciliação | Ratificação retroativa; a frase original ficou preservada no texto da Decision 3 em vez de apagada |
| 3 | O README descrevia a feature como "nunca construída", o que sugeria código faltando | Corrigido: o entregável é documental e já existia; faltava o registro de fase 3 |

---

## Pendências

| Pendência | Ação necessária | Quem decide |
|-----------|-----------------|-------------|
| 83 testes Bronze fora da convenção | Classificar por severidade num build futuro, ou declarar que `error` universal é a escolha para a camada | Christian |
| `TRIGGER_ACTION` do Resource Monitor | Terceiro contexto da Decision 3, nunca verificado; depende de `scripts/verify_governance.sql` como `ACCOUNTADMIN` | Christian |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-11 | Christian (via Claude) | Relatório retroativo; ratificação das 3 decisões |
