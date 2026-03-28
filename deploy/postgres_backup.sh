#!/usr/bin/env bash

set -Eeuo pipefail

APP_ROOT="/opt/sales-marketplaces"
ENV_FILE="$APP_ROOT/.env"
BACKUP_ROOT="/opt/backups/sales-marketplaces"
STAMP="$(date -u +%Y%m%d_%H%M%S)"
TARGET_DIR="$BACKUP_ROOT/$STAMP"

mkdir -p "$TARGET_DIR"

read_env_value() {
  local key="$1"
  [[ -f "$ENV_FILE" ]] || return 0
  grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2-
}

dump_db() {
  local url="$1"
  local name="$2"
  if [[ -z "$url" ]]; then
    echo "Missing database url for $name" >&2
    exit 1
  fi
  pg_dump "$url" -Fc -f "$TARGET_DIR/$name.dump"
}

dump_db "$(read_env_value APP_SYSTEM_DATABASE_URL)" "data_analytics_system"
dump_db "$(read_env_value APP_DATABASE_URL)" "data_analytics_hot"
dump_db "$(read_env_value APP_HISTORY_DATABASE_URL)" "data_analytics_history"

find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d | sort | head -n -7 | xargs -r rm -rf
