# Data Analytics Web

Веб-приложение на FastAPI + React/Vite для аналитики продаж, прайсинга и интеграций с маркетплейсами и внешними таблицами.

## Что внутри

- FastAPI (API-only backend)
- React + Vite + TypeScript frontend (`frontend/`)
- Backend API (`backend/`)
- Next.js frontend (`frontend/`)
- Текущее локальное хранилище: SQLite (`data/analytics.db`)
- Подготовлен переход на PostgreSQL через `APP_DB_BACKEND=postgres`

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Настройка

1. Заполнить `.env` базовыми переменными приложения.
2. Ключи маркетплейсов и Google-таблиц настраиваются через интерфейс приложения.
3. Файлы Google Service Account сохраняются в `data/config/google_keys/`.

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

## Фронтенд (React + Vite)

Фронтенд живёт в `frontend/` (React + Vite + TypeScript).

1. Установка:

```bash
cd frontend
npm install
cp .env.local.example .env.local
```

2. Запуск:

```bash
npm run dev
```

Открыть: `http://127.0.0.1:3000`

Важно:
- backend (FastAPI) должен быть запущен на `127.0.0.1:8000`;
- frontend ходит в API backend;
- dev-перезапуск backend и frontend настроен через `main.py`.

## Production через Docker Compose

```bash
docker compose build
docker compose up -d
```

Сервисы:
- frontend: `http://localhost:3000`
- backend API: `http://localhost:8000`

## База данных

- По умолчанию используется SQLite: `data/analytics.db`
- Служебные файлы `data/analytics.db-shm` и `data/analytics.db-wal` являются нормальной частью SQLite в режиме `WAL`.
- Для начала миграции на PostgreSQL:

```bash
export APP_DB_BACKEND=postgres
export APP_DATABASE_URL=postgresql://user:pass@host:5432/dbname
python3 tools/migrate_sqlite_to_postgres.py
```

Сейчас PostgreSQL-поддержка уже добавлена в `backend/services/db.py` для core DB-слоя и подготовлен мигратор hot-таблиц, но полный runtime store-data контура ещё переносится поэтапно.
