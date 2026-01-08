#!/usr/bin/env bash

set -e

echo "▶ Carregando variáveis do .env..."

if [ ! -f .env ]; then
  echo "❌ Arquivo .env não encontrado"
  exit 1
fi

export $(grep -v '^#' .env | xargs)

REQUIRED_VARS=(DB_NAME DB_USER DB_PASSWORD)

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "❌ Variável $VAR não definida no .env"
    exit 1
  fi
done

# ⚠️ ajuste se o nome do container for outro
POSTGRES_CONTAINER_NAME=vadeonibus-db

SQL_DIR="backend/sql/functions"

if [ ! -d "$SQL_DIR" ]; then
  echo "❌ Diretório $SQL_DIR não encontrado"
  exit 1
fi

FILES=$(ls "$SQL_DIR"/*.sql 2>/dev/null | sort)

if [ -z "$FILES" ]; then
  echo "⚠ Nenhum arquivo .sql encontrado em $SQL_DIR"
  exit 0
fi

echo "▶ Usando container Postgres: $POSTGRES_CONTAINER_NAME"
echo ""

for FILE in $FILES; do
  echo "▶ Executando $(basename "$FILE")"

  sudo docker exec -i \
    -e PGPASSWORD="$DB_PASSWORD" \
    "$POSTGRES_CONTAINER_NAME" \
    psql \
      -U "$DB_USER" \
      -d "$DB_NAME" \
      -v ON_ERROR_STOP=1 \
      < "$FILE"
done

echo ""
echo "✅ Todos os scripts SQL foram aplicados com sucesso!"
