#!/bin/bash
# infra/scripts/register_connectors.sh
# Registers 2 connectors: Debezium (source) + Snowflake Sink v4 (single sink).
# O segundo sink do projeto anterior (`sinkitems`, dedicado a order_items)
# desapareceu com o conector v4: ele existia por causa do buffer client-side
# (buffer.count.records/flush.time/size.bytes), configuracao que o JAR 4.1.0
# nao reconhece mais. Verificado no proprio JAR antes de consolidar.
# Usage: ./scripts/register_connectors.sh [--env local|prod]
#
# NOTA (v5): a retrospectiva do projeto (TD-25) registrava que "curl -sf +
# set -e sai em HTTP 409 antes do case tratar". Verificado o código atual:
# isso NÃO reproduz, porque `HTTP=$(...)` é uma atribuição — no bash, `set -e`
# ignora falhas de comandos usados em substituição de comando dentro de uma
# atribuição (uma peculiaridade conhecida e mal-documentada do -e). Ou seja,
# o case já tratava 409 corretamente antes desta mudança.
# Endurecido mesmo assim: removido o `-f` do curl na chamada de registro,
# para não depender dessa peculiaridade implícita — se alguém no futuro
# refatorar isso para fora de uma atribuição (ex: um pipe direto), o script
# pararia de funcionar silenciosamente sem essa mudança.
set -euo pipefail

ENV_FILE=".env"
CONNECT_URL="http://localhost:8083"
REGISTRY_URL="http://localhost:8081"
CONNECTORS_DIR="$(dirname "$0")/../connectors"

GREEN="\033[92m"; YELLOW="\033[93m"; RED="\033[91m"
CYAN="\033[96m"; GRAY="\033[90m"; RESET="\033[0m"

while [[ $# -gt 0 ]]; do
    case $1 in
        --env) ENV_FILE=".env.${2}"; shift 2 ;;
        *) shift ;;
    esac
done

if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}✖   $ENV_FILE not found. Copy .env.example to .env and fill values.${RESET}"
    exit 1
fi
set -a; source "$ENV_FILE"; set +a

# ── Fonte única do material da chave ─────────────────────────────────────────
#
# O connector do Snowflake precisa da chave privada como corpo base64 PKCS8
# DER numa variável; o dbt e o Dagster precisam dela como arquivo .p8. Até
# 2026-08-11 o `.env` guardava as DUAS coisas, e o custo apareceu de duas
# formas no mesmo dia: um `grep` no `.env` vazou a chave inteira, e a rotação
# virou um procedimento de cinco lugares porque o mesmo segredo estava escrito
# em dois formatos.
#
# Agora o arquivo é a fonte e a variável é derivada dele. `SNOWFLAKE_PRIVATE_KEY`
# no `.env` passa a ser opcional — se estiver lá, continua valendo, para não
# quebrar ambiente de quem ainda não migrou.
# ATENÇÃO AO CONTEXTO DE EXECUÇÃO. `SNOWFLAKE_PRIVATE_KEY_PATH` no `.env` é o
# caminho DENTRO DO CONTAINER (`./keys:/secrets:ro` no compose), porque quem o
# consome é o dbt e o Dagster. Este script roda no HOST, onde `/secrets` não
# existe — e era essa incompatibilidade que justificava manter a chave também
# como variável inline. A derivação abaixo tenta o caminho literal e, se ele
# não existir, procura o mesmo arquivo em `keys/` a partir da raiz do repo.
if [ -z "${SNOWFLAKE_PRIVATE_KEY:-}" ]; then
    KEY_FILE=""
    if [ -r "${SNOWFLAKE_PRIVATE_KEY_PATH:-/dev/null}" ]; then
        KEY_FILE="$SNOWFLAKE_PRIVATE_KEY_PATH"
    elif [ -n "${SNOWFLAKE_PRIVATE_KEY_PATH:-}" ] \
         && [ -r "$(dirname "$0")/../keys/$(basename "$SNOWFLAKE_PRIVATE_KEY_PATH")" ]; then
        KEY_FILE="$(dirname "$0")/../keys/$(basename "$SNOWFLAKE_PRIVATE_KEY_PATH")"
    fi

    if [ -n "$KEY_FILE" ]; then
        SNOWFLAKE_PRIVATE_KEY=$(grep -v '^-----' "$KEY_FILE" | tr -d '\n')
        export SNOWFLAKE_PRIVATE_KEY
        echo -e "${GRAY}    chave derivada de ${KEY_FILE}${RESET}"
    else
        echo -e "${RED}✖   Sem material de chave utilizável."
        echo -e "    SNOWFLAKE_PRIVATE_KEY_PATH='${SNOWFLAKE_PRIVATE_KEY_PATH:-}' não existe no host,"
        echo -e "    e não há keys/$(basename "${SNOWFLAKE_PRIVATE_KEY_PATH:-sem_caminho}") no repositório.${RESET}"
        exit 1
    fi
fi
echo -e "${GREEN}✅  Loaded credentials from ${ENV_FILE}${RESET}"

echo -e "\n${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${CYAN}  sdd-kafka-snowflake v2 — Register Connectors${RESET}"
echo -e "${CYAN}  10 domains (Tier 1) | 2 connectors${RESET}"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}"

# ── Wait for Schema Registry ──────────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for Schema Registry...${RESET}"
for i in $(seq 1 30); do
    if curl -sf "${REGISTRY_URL}/subjects" > /dev/null 2>&1; then
        echo -e "${GREEN}✅  Schema Registry ready (attempt ${i})${RESET}"; break
    fi
    [ "$i" -eq 30 ] && echo -e "${RED}✖   Timeout: Schema Registry${RESET}" && exit 1
    printf "${GRAY}    waiting... %d/30\r${RESET}" "$i"; sleep 5
done

# ── Set BACKWARD compatibility ────────────────────────────────────────────────
echo -e "\n${YELLOW}🔒  Setting global BACKWARD compatibility...${RESET}"
curl -sf -X PUT "${REGISTRY_URL}/config" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"compatibility": "BACKWARD"}' > /dev/null
echo -e "${GREEN}✅  Compatibility: BACKWARD${RESET}"

# ── Wait for Kafka Connect ────────────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for Kafka Connect...${RESET}"
for i in $(seq 1 40); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo -e "${GREEN}✅  Kafka Connect ready (attempt ${i})${RESET}"; break
    fi
    [ "$i" -eq 40 ] && echo -e "${RED}✖   Timeout: Kafka Connect${RESET}" && exit 1
    printf "${GRAY}    waiting... %d/40\r${RESET}" "$i"; sleep 5
done

# ── Register connector ────────────────────────────────────────────────────────
# PUT /connectors/{name}/config, não POST /connectors.
#
# A versão anterior usava POST, que só CRIA. Num conector já existente ela
# recebia HTTP 409, imprimia "already exists" e seguia — ou seja, rodar este
# script depois de mudar `connectors/*.json` ou qualquer variável do `.env`
# não propagava nada. A config velha continuava valendo no config topic do
# Connect, em silêncio.
#
# Isso não era teoria: em 2026-08-11, depois da rotação da chave do
# DAGSTER_SERVICE_USER, os 4 tasks do sink ficaram em FAILED com "JWT token is
# invalid" — a chave antiga estava embutida na config registrada. Rodar este
# script "para corrigir" devolveu 409 duas vezes e declarou sucesso com o sink
# quebrado. O conserto real foi um PUT manual.
#
# PUT é idempotente: cria se não existe, atualiza se existe. É o verbo certo
# para um script de registro que também serve de script de atualização.
#
# NOTA SOBRE O CORPO: POST /connectors recebe {"name": ..., "config": {...}};
# PUT /connectors/{name}/config recebe SÓ o objeto de config. Daí a extração
# do campo `config` abaixo.
register_connector() {
    local name="$1" file="$2"
    echo -e "\n${YELLOW}📡  Registering/updating: ${name}${RESET}"

    RESOLVED=$(envsubst < "$file" | python3 -c \
        "import sys, json; d = json.load(sys.stdin); print(json.dumps(d.get('config', d)))")

    # Sem -f: o código HTTP é sempre capturado explicitamente pelo -w,
    # independente do status. Não dependemos do exit code do curl.
    HTTP=$(echo "$RESOLVED" | curl -s -o /tmp/connect_resp.json -w "%{http_code}" \
        -X PUT "${CONNECT_URL}/connectors/${name}/config" \
        -H "Content-Type: application/json" -d @-) || true

    case "$HTTP" in
        201) echo -e "${GREEN}✅  ${name} created (HTTP 201)${RESET}" ;;
        200) echo -e "${GREEN}✅  ${name} updated (HTTP 200)${RESET}" ;;
        *)   echo -e "${RED}✖   Failed ${name} (HTTP ${HTTP})${RESET}"
             cat /tmp/connect_resp.json 2>/dev/null; exit 1 ;;
    esac
}

register_connector "debezium-postgres-cdc" "${CONNECTORS_DIR}/debezium.json"
register_connector "sink"                   "${CONNECTORS_DIR}/snowflake_sink.json"

# ── Status check ─────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for connectors to stabilize (15s)...${RESET}"
sleep 15

# O estado do CONECTOR não basta: um conector pode reportar RUNNING com todos
# os tasks em FAILED. Foi assim que o sink quebrado passou despercebido em
# 2026-08-11 -- e é o modo de falha mais traiçoeiro aqui, porque o Debezium
# segue saudável e metade do pipeline parece viva enquanto nada chega ao
# destino.
echo -e "\n${CYAN}── Connector status ──────────────────────────────────────${RESET}"
UNHEALTHY=0
for connector in debezium-postgres-cdc sink; do
    REPORT=$(curl -sf "${CONNECT_URL}/connectors/${connector}/status" \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
tasks = d.get('tasks', [])
bad = [t for t in tasks if t.get('state') != 'RUNNING']
ok = d['connector']['state'] == 'RUNNING' and not bad and tasks
print(('OK' if ok else 'BAD'),
      d['connector']['state'],
      f\"{len(tasks) - len(bad)}/{len(tasks)} tasks\",
      (bad[0].get('trace','').splitlines() or [''])[0][:90] if bad else '')
" 2>/dev/null || echo "BAD UNKNOWN 0/0 sem resposta do Connect")

    case "$REPORT" in
        OK*)  echo -e "  ${GREEN}✅  ${connector}: ${REPORT#OK }${RESET}" ;;
        *)    echo -e "  ${RED}✖   ${connector}: ${REPORT#BAD }${RESET}"; UNHEALTHY=1 ;;
    esac
done

if [ "$UNHEALTHY" -ne 0 ]; then
    echo -e "\n${RED}✖   Registro concluído, mas há conector ou task fora de RUNNING.${RESET}"
    echo -e "${GRAY}    Detalhes: ${CONNECT_URL}/connectors/{nome}/status${RESET}\n"
    exit 1
fi

echo -e "\n${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${GREEN}  All connectors registered and healthy! (10 domains → 2 connectors)${RESET}\n"
echo -e "  ${GRAY}Kafka UI    →${RESET} http://localhost:8080"
echo -e "  ${GRAY}Connect     →${RESET} http://localhost:8083/connectors"
echo -e "  ${GRAY}Registry    →${RESET} http://localhost:8081/subjects"
echo -e "  ${GRAY}Dagster     →${RESET} http://localhost:3000"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}\n"
