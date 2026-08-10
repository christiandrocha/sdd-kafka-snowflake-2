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
register_connector() {
    local name="$1" file="$2"
    echo -e "\n${YELLOW}📡  Registering: ${name}${RESET}"

    RESOLVED=$(envsubst < "$file")

    # Sem -f: o código HTTP é sempre capturado explicitamente pelo -w,
    # independente do status. Não dependemos do exit code do curl.
    HTTP=$(echo "$RESOLVED" | curl -s -o /tmp/connect_resp.json -w "%{http_code}" \
        -X POST "${CONNECT_URL}/connectors" \
        -H "Content-Type: application/json" -d @-) || true

    case "$HTTP" in
        201) echo -e "${GREEN}✅  ${name} created (HTTP 201)${RESET}" ;;
        409) echo -e "${YELLOW}⚠️   ${name} already exists (HTTP 409)${RESET}" ;;
        *)   echo -e "${RED}✖   Failed ${name} (HTTP ${HTTP})${RESET}"
             cat /tmp/connect_resp.json 2>/dev/null; exit 1 ;;
    esac
}

register_connector "debezium-postgres-cdc" "${CONNECTORS_DIR}/debezium.json"
register_connector "sink"                   "${CONNECTORS_DIR}/snowflake_sink.json"

# ── Status check ─────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for connectors to stabilize (15s)...${RESET}"
sleep 15

echo -e "\n${CYAN}── Connector status ──────────────────────────────────────${RESET}"
for connector in debezium-postgres-cdc sink; do
    STATUS=$(curl -sf "${CONNECT_URL}/connectors/${connector}/status" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
    [ "$STATUS" = "RUNNING" ] \
        && echo -e "  ${GREEN}✅  ${connector}: ${STATUS}${RESET}" \
        || echo -e "  ${RED}✖   ${connector}: ${STATUS}${RESET}"
done

echo -e "\n${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${GREEN}  All connectors registered! (10 domains → 2 connectors)${RESET}\n"
echo -e "  ${GRAY}Kafka UI    →${RESET} http://localhost:8080"
echo -e "  ${GRAY}Connect     →${RESET} http://localhost:8083/connectors"
echo -e "  ${GRAY}Registry    →${RESET} http://localhost:8081/subjects"
echo -e "  ${GRAY}Dagster     →${RESET} http://localhost:3000"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}\n"
