# Data Analytics Web

Веб-приложение на FastAPI + React/Vite для аналитики продаж, прайсинга и интеграций с маркетплейсами и внешними таблицами.

## Что внутри

- FastAPI backend (`backend/`)
- React + Vite + TypeScript frontend (`frontend/`)
- локальное хранилище на SQLite в `data/`
- поддержка PostgreSQL через `APP_DB_BACKEND=postgres`
- интеграции с Яндекс Маркетом, внешними таблицами и Google Sheets

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Для frontend:

```bash
cd frontend
npm install
```

## Настройка

1. Создать `.env` на основе `.env.example`.
2. При необходимости создать `frontend/.env.local` на основе `frontend/.env.local.example`.
3. Ключи маркетплейсов и Google-таблиц настраивать через интерфейс приложения или локальные конфиги вне Git.

Важно:
- `.env` не хранится в репозитории
- `data/` не хранится в репозитории
- `data/config/integrations.json` не хранится в репозитории
- безопасный шаблон интеграций лежит в `data/config/integrations.example.json`

## Локальный запуск

```bash
python3 main.py
```

Открыть frontend: `http://127.0.0.1:3000`
Backend API: `http://127.0.0.1:8000`

`main.py` поднимает:
- FastAPI backend на `:8000`
- Vite frontend (autostart) на `:3000`
- и перенаправляет `/` с backend на frontend.

## Отдельный запуск frontend

Из папки `frontend/`:

```bash
cp .env.local.example .env.local
npm run dev
```

Открыть: `http://127.0.0.1:3000`

Важно:
- backend (FastAPI) должен быть запущен на `127.0.0.1:8000`;
- frontend ходит в API backend;
- dev-перезапуск backend и frontend настроен через `main.py`.

## Production на сервере

Текущий production-стек:

- `nginx` раздаёт `frontend/dist` и проксирует `/api`
- `systemd` держит backend-сервис `sales-marketplaces-backend`
- `PostgreSQL` запущен локально на сервере

Обновление проекта на сервере:

```bash
cd /opt/sales-marketplaces
./deploy.sh
```

Что делает `deploy.sh`:

- подтягивает `main` из GitHub
- обновляет Python-зависимости в `.venv`
- обновляет frontend-зависимости
- собирает `frontend/dist`
- перезапускает backend
- делает `health-check` backend и домена

Важно:

- `deploy.sh` рассчитан на уже подготовленный сервер
- backend-сервис по умолчанию называется `sales-marketplaces-backend`
- при необходимости сервисы можно переопределить через `BACKEND_SERVICE` и `NGINX_SERVICE`

## База данных

- По умолчанию используется SQLite: `data/analytics.db`
- Служебные файлы `data/analytics.db-shm` и `data/analytics.db-wal` являются нормальной частью SQLite в режиме `WAL`.
- Для миграции на PostgreSQL:

```bash
export APP_DB_BACKEND=postgres
export APP_DATABASE_URL=postgresql://user:pass@host:5432/dbname
python3 tools/migrate_sqlite_to_postgres.py
```

Сейчас PostgreSQL-поддержка уже добавлена в `backend/services/db.py` для core DB-слоя и подготовлен мигратор hot-таблиц, но полный runtime store-data контура ещё переносится поэтапно.

## Что не хранится в Git

- локальные базы данных из `data/*.db`
- recovery-архивы и логи
- `.env` и прочие локальные env-файлы
- `data/config/integrations.json` с боевыми ключами
- `frontend/node_modules`, `frontend/dist`, `frontend/.next`

## Лицензия

Проект закрытый. Детали в [LICENSE](LICENSE).
