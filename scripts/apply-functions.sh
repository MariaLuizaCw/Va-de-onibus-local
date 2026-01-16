#!/usr/bin/env bash

set -e

# Uso: ./apply-functions.sh [container|remote]
#   container - executa via docker exec (padrão)
#   remote    - executa via psql conectando a uma máquina remota na rede

MODE="${1:-container}"

if [[ "$MODE" != "container" && "$MODE" != "remote" ]]; then
  echo "❌ Modo inválido: $MODE"
  echo "Uso: $0 [container|remote]"
  echo "  container - executa via docker exec (padrão)"
  echo "  remote    - executa via psql conectando a uma máquina remota na rede"
  exit 1
fi

echo "▶ Modo de execução: $MODE"
echo "▶ Carregando variáveis do .env..."

if [ ! -f .env ]; then
  echo "❌ Arquivo .env não encontrado"
  exit 1
fi

export $(grep -v '^#' .env | xargs)

if [[ "$MODE" == "container" ]]; then
  REQUIRED_VARS=(DB_NAME DB_USER DB_PASSWORD)
else
  REQUIRED_VARS=(DB_NAME DB_USER DB_PASSWORD DB_HOST DB_PORT)
fi

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "❌ Variável $VAR não definida no .env"
    exit 1
  fi
done

# ⚠️ ajuste se o nome do container for outro
POSTGRES_CONTAINER_NAME=vadeonibus-db

SQL_DIR="database/functions"

if [ ! -d "$SQL_DIR" ]; then
  echo "❌ Diretório $SQL_DIR não encontrado"
  exit 1
fi

FILES=$(ls "$SQL_DIR"/*.sql 2>/dev/null | sort)

if [ -z "$FILES" ]; then
  echo "⚠ Nenhum arquivo .sql encontrado em $SQL_DIR"
  exit 0
fi

if [[ "$MODE" == "container" ]]; then
  echo "▶ Usando container Postgres: $POSTGRES_CONTAINER_NAME"
else
  echo "▶ Conectando ao Postgres remoto: $DB_HOST:$DB_PORT"
fi
echo ""

for FILE in $FILES; do
  echo "▶ Executando $(basename "$FILE")"

  if [[ "$MODE" == "container" ]]; then
    sudo docker exec -i \
      -e PGPASSWORD="$DB_PASSWORD" \
      "$POSTGRES_CONTAINER_NAME" \
      psql \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -v ON_ERROR_STOP=1 \
        < "$FILE"
  else
    PGPASSWORD="$DB_PASSWORD" psql \
      -h "$DB_HOST" \
      -p "$DB_PORT" \
      -U "$DB_USER" \
      -d "$DB_NAME" \
      -v ON_ERROR_STOP=1 \
      < "$FILE"
  fi
done

echo ""
echo "✅ Todos os scripts SQL foram aplicados com sucesso!"
