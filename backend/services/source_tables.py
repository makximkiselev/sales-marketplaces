from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from backend.services.db import (
    BASE_DIR,
    DATABASE_URL,
    SYSTEM_DATABASE_URL,
    is_postgres_backend,
    rebuild_db_explorer_views,
)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    if not is_postgres_backend():
        raise RuntimeError("SQLite runtime отключен. Используй PostgreSQL backend.")
    import psycopg
    from psycopg.rows import dict_row

    if not DATABASE_URL:
        raise RuntimeError("APP_DATABASE_URL/DATABASE_URL не задан для PostgreSQL backend")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _connect_registry():
    if not is_postgres_backend():
        raise RuntimeError("SQLite runtime отключен. Используй PostgreSQL backend.")
    import psycopg
    from psycopg.rows import dict_row

    dsn = SYSTEM_DATABASE_URL or DATABASE_URL
    if not dsn:
        raise RuntimeError("APP_SYSTEM_DATABASE_URL/SYSTEM_DATABASE_URL не задан для PostgreSQL backend")
    return psycopg.connect(dsn, row_factory=dict_row)


def _ph() -> str:
    return "%s" if is_postgres_backend() else "?"


def _registry_ph() -> str:
    return "%s" if is_postgres_backend() and SYSTEM_DATABASE_URL else "?"


def _table_exists(conn, table_name: str) -> bool:
    if is_postgres_backend():
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = %s
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return row is not None
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _list_tables(conn) -> set[str]:
    if is_postgres_backend():
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
            """
        ).fetchall()
        return {str(row["table_name"] if isinstance(row, dict) else row[0]) for row in rows}
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _slugify(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "source"
    raw = re.sub(r"[^a-z0-9_-]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw or "source"


def _table_name_for_source(source_id: str) -> str:
    sid = str(source_id or "").strip()
    slug = _slugify(sid)
    if slug:
        return f"source_items__{slug}"
    digest = hashlib.sha1(sid.encode("utf-8")).hexdigest()[:8]
    return f"source_items__{digest}"


def cleanup_legacy_source_tables(registry_conn, data_conn) -> bool:
    rows = registry_conn.execute(
        """
        SELECT source_id, table_name, source_type
        FROM source_tables_registry
        WHERE source_type = 'yandex_market'
          AND source_id LIKE 'yandex_market:%'
        ORDER BY source_id
        """
    ).fetchall()
    changed = False
    for row in rows:
        source_id = str(row["source_id"] or "").strip()
        table_name = str(row["table_name"] or "").strip()
        if not source_id or not table_name:
            continue
        exists = _table_exists(data_conn, table_name)
        if not exists:
            registry_conn.execute(f"DELETE FROM source_tables_registry WHERE source_id = {_registry_ph()}", (source_id,))
            changed = True
            continue
        count = data_conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        if int(count or 0) != 0:
            continue
        data_conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        registry_conn.execute(f"DELETE FROM source_tables_registry WHERE source_id = {_registry_ph()}", (source_id,))
        changed = True
    return changed


def migrate_source_table_names() -> None:
    with _connect_registry() as registry_conn, _connect() as data_conn:
        rows = registry_conn.execute(
            """
            SELECT source_id, table_name
            FROM source_tables_registry
            ORDER BY source_id
            """
        ).fetchall()
        existing = _list_tables(data_conn)
        changed = False
        for row in rows:
            source_id = str(row["source_id"] or "").strip()
            current_name = str(row["table_name"] or "").strip()
            desired_name = _table_name_for_source(source_id)
            if not source_id or not current_name or current_name == desired_name:
                continue
            if current_name not in existing:
                continue
            if desired_name in existing:
                registry_conn.execute(
                    f"UPDATE source_tables_registry SET table_name = {_registry_ph()}, folder_path = '', updated_at = {_registry_ph()} WHERE source_id = {_registry_ph()}",
                    (desired_name, _now_iso(), source_id),
                )
                changed = True
                continue
            data_conn.execute(f'ALTER TABLE "{current_name}" RENAME TO "{desired_name}"')
            registry_conn.execute(
                f"UPDATE source_tables_registry SET table_name = {_registry_ph()}, folder_path = '', updated_at = {_registry_ph()} WHERE source_id = {_registry_ph()}",
                (desired_name, _now_iso(), source_id),
            )
            existing.discard(current_name)
            existing.add(desired_name)
            changed = True
        if cleanup_legacy_source_tables(registry_conn, data_conn):
            changed = True
        if changed:
            rebuild_db_explorer_views(data_conn)
            registry_conn.commit()
            data_conn.commit()


def init_source_registry() -> None:
    with _connect_registry() as registry_conn, _connect() as data_conn:
        registry_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_tables_registry (
                source_id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                table_name TEXT NOT NULL UNIQUE,
                folder_path TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        registry_conn.execute("UPDATE source_tables_registry SET folder_path = '' WHERE folder_path <> ''")
        cleanup_legacy_source_tables(registry_conn, data_conn)
        rebuild_db_explorer_views(data_conn)
        registry_conn.commit()
        data_conn.commit()


def ensure_source_table(source_id: str, *, source_type: str = "", title: str = "") -> str:
    sid = str(source_id or "").strip()
    if not sid:
        raise ValueError("source_id обязателен")

    init_source_registry()
    migrate_source_table_names()
    table_name = _table_name_for_source(sid)
    now = _now_iso()

    with _connect() as data_conn, _connect_registry() as registry_conn:
        data_conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                sku TEXT PRIMARY KEY,
                sku_original TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                subcategory TEXT NOT NULL DEFAULT '',
                price REAL NULL,
                currency TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{{}}',
                updated_at TEXT NOT NULL
            )
            """
        )
        registry_conn.execute(
            f"""
            INSERT INTO source_tables_registry (
                source_id, source_type, title, table_name, folder_path, created_at, updated_at
            )
            VALUES ({_registry_ph()}, {_registry_ph()}, {_registry_ph()}, {_registry_ph()}, {_registry_ph()}, {_registry_ph()}, {_registry_ph()})
            ON CONFLICT(source_id) DO UPDATE SET
                source_type = excluded.source_type,
                title = excluded.title,
                table_name = excluded.table_name,
                folder_path = excluded.folder_path,
                updated_at = excluded.updated_at
            """,
            (
                sid,
                str(source_type or "").strip(),
                str(title or "").strip(),
                table_name,
                "",
                now,
                now,
            ),
        )
        data_conn.commit()
        registry_conn.commit()

    return table_name


def get_registered_source_table(source_id: str) -> str:
    sid = str(source_id or "").strip()
    if not sid:
        return ""
    candidates = [sid]
    if not sid.startswith("gsheets:"):
        candidates.append(f"gsheets:{sid}")
    init_source_registry()
    with _connect_registry() as conn:
        for candidate in candidates:
            row = conn.execute(
                f"SELECT table_name FROM source_tables_registry WHERE source_id = {_registry_ph()}",
                (candidate,),
            ).fetchone()
            if not row:
                continue
            table_name = str(row["table_name"] or "").strip()
            if table_name:
                return table_name
    # Fallback for partially migrated environments: if the deterministic table
    # already exists in data DB but registry row is missing, still resolve it.
    for candidate in candidates:
        table_name = _table_name_for_source(candidate)
        try:
            with _connect() as data_conn:
                if _table_exists(data_conn, table_name):
                    return table_name
        except Exception:
            continue
    return ""


def replace_source_rows(
    source_id: str,
    rows: list[dict[str, Any]],
    *,
    source_type: str = "",
    title: str = "",
) -> int:
    table_name = ensure_source_table(source_id, source_type=source_type, title=title)
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        price_val = row.get("price")
        try:
            price_num = float(str(price_val).replace(",", ".")) if price_val not in (None, "") else None
        except Exception:
            price_num = None
        prepared.append(
            (
                sku,
                str(row.get("sku_original") or sku),
                str(row.get("name") or "").strip(),
                str(row.get("category") or "").strip(),
                str(row.get("subcategory") or "").strip(),
                price_num,
                str(row.get("currency") or "").strip(),
                json.dumps(row.get("payload") if isinstance(row.get("payload"), dict) else {}, ensure_ascii=False),
                now,
            )
        )

    with _connect() as conn:
        conn.execute(f'DELETE FROM "{table_name}"')
        if prepared:
            values_sql = ", ".join([_ph()] * 9)
            sql = f"""
                INSERT INTO "{table_name}" (
                    sku, sku_original, name, category, subcategory, price, currency, payload_json, updated_at
                ) VALUES ({values_sql})
                """
            if is_postgres_backend():
                with conn.cursor() as cur:
                    cur.executemany(sql, prepared)
            else:
                conn.executemany(sql, prepared)
        conn.commit()
    return len(prepared)
