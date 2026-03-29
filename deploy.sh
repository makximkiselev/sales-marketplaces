#!/usr/bin/env bash

set -Eeuo pipefail

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_SERVICE="${BACKEND_SERVICE:-sales-marketplaces-backend}"
NGINX_SERVICE="${NGINX_SERVICE:-nginx}"
BACKEND_HEALTH_URL="${BACKEND_HEALTH_URL:-http://127.0.0.1:18000/api/health}"
PUBLIC_HEALTH_URL="${PUBLIC_HEALTH_URL:-https://sales.id-smart.ru/api/health}"
FRONTEND_DIST_DIR="${FRONTEND_DIST_DIR:-$APP_ROOT/frontend/dist}"
FRONTEND_DIST_BUILD_DIR="${FRONTEND_DIST_BUILD_DIR:-$APP_ROOT/frontend/dist-build}"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

wait_for_url() {
  local url="$1"
  local label="$2"
  local attempts="${3:-60}"
  local delay="${4:-2}"
  local i

  for ((i = 1; i <= attempts; i++)); do
    if curl --fail --silent --show-error "$url"; then
      printf '\n'
      return 0
    fi
    sleep "$delay"
  done

  echo "$label did not become healthy: $url" >&2
  return 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd git
require_cmd python3
require_cmd npm
require_cmd npx
require_cmd curl
require_cmd systemctl

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "Missing required file: $path" >&2
    exit 1
  fi
}

require_glob_match() {
  local pattern="$1"
  shopt -s nullglob
  local matches=($pattern)
  shopt -u nullglob
  if (( ${#matches[@]} == 0 )); then
    echo "Missing required build artifacts matching: $pattern" >&2
    exit 1
  fi
}

cd "$APP_ROOT"

log "Pulling latest code"
git pull --ff-only

if [[ ! -d .venv ]]; then
  log "Creating Python virtualenv"
  python3 -m venv .venv
fi

log "Installing backend dependencies"
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

log "Installing frontend dependencies"
cd "$APP_ROOT/frontend"
if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi

log "Building frontend"
rm -rf "$FRONTEND_DIST_BUILD_DIR"
npx tsc -b
VITE_API_BASE="${VITE_API_BASE:-/api}" npx vite build --outDir "$FRONTEND_DIST_BUILD_DIR"
require_file "$FRONTEND_DIST_BUILD_DIR/index.html"
require_glob_match "$FRONTEND_DIST_BUILD_DIR/assets/index-*.js"
require_glob_match "$FRONTEND_DIST_BUILD_DIR/assets/index-*.css"
rm -rf "$FRONTEND_DIST_DIR"
mv "$FRONTEND_DIST_BUILD_DIR" "$FRONTEND_DIST_DIR"

cd "$APP_ROOT"

log "Restarting backend service"
systemctl restart "$BACKEND_SERVICE"

log "Reloading nginx"
systemctl reload "$NGINX_SERVICE"

log "Checking backend health"
wait_for_url "$BACKEND_HEALTH_URL" "Backend health"

log "Checking public health"
wait_for_url "$PUBLIC_HEALTH_URL" "Public health"

log "Deploy completed"
