#!/usr/bin/env bash

set -e

# Uso: ./dump-clusters-parada.sh [container|remote]
#   container - executa via docker exec (padrão)
#   remote    - executa via psql conectando a uma máquina remota na rede

MODE="${1:-container}"

if [[ "$MODE" != "container" && "$MODE" != "remote" ]]; then
  echo "❌ Modo inválido: $MODE"
  echo "Uso: $0 [container|remote]"
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

POSTGRES_CONTAINER_NAME=vadeonibus-db
OUTPUT_FILE="database/seeds/clusters_parada_resultado_data.sql"

# Criar diretório seeds se não existir
mkdir -p database/seeds

echo "▶ Fazendo dump da tabela clusters_parada_resultado..."

if [[ "$MODE" == "container" ]]; then
  sudo docker exec \
    -e PGPASSWORD="$DB_PASSWORD" \
    "$POSTGRES_CONTAINER_NAME" \
    pg_dump \
      -U "$DB_USER" \
      -d "$DB_NAME" \
      --data-only \
      --column-inserts \
      --table=clusters_parada_resultado \
    > "$OUTPUT_FILE"
else
  PGPASSWORD="$DB_PASSWORD" pg_dump \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    --data-only \
    --column-inserts \
    --table=clusters_parada_resultado \
    > "$OUTPUT_FILE"
fi

# Verificar se o arquivo foi criado e tem conteúdo
if [ -s "$OUTPUT_FILE" ]; then
  LINES=$(wc -l < "$OUTPUT_FILE")
  echo "✅ Dump criado com sucesso: $OUTPUT_FILE ($LINES linhas)"
else
  echo "⚠ Tabela vazia ou não encontrada. Arquivo de dump vazio."
fi
