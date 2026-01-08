#!/bin/bash

set -e

# Load environment variables from .env
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Apply functions
echo "Applying GPS Backend functions..."
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f backend/sql/00_master.sql
echo "Done!"
