from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[2]
DB_DIR = BASE_DIR / "data"
DB_PATH = DB_DIR / "analytics.db"
OPERATIONAL_DB_PATH = DB_PATH
SYSTEM_DB_PATH = DB_DIR / "analytics_system.db"
HISTORY_DB_PATH = DB_DIR / "analytics_history.db"
DATABASE_URL = str(os.getenv("APP_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
OPERATIONAL_DATABASE_URL = DATABASE_URL
SYSTEM_DATABASE_URL = str(os.getenv("APP_SYSTEM_DATABASE_URL") or os.getenv("SYSTEM_DATABASE_URL") or "").strip()
HISTORY_DATABASE_URL = str(os.getenv("APP_HISTORY_DATABASE_URL") or os.getenv("HISTORY_DATABASE_URL") or "").strip()
DB_BACKEND = str(
    os.getenv("APP_DB_BACKEND")
    or ("postgres" if (DATABASE_URL or SYSTEM_DATABASE_URL or HISTORY_DATABASE_URL) else "sqlite")
).strip().lower()
_DB_INIT_LOCK = threading.Lock()
_DB_INIT_DONE = False

SQLITE_BUSY_TIMEOUT_MS = 30_000
SQLITE_WAL_AUTOCHECKPOINT_PAGES = 1_000
SQLITE_JOURNAL_SIZE_LIMIT = 64 * 1024 * 1024
SQLITE_CACHE_SIZE_KIB = 20_000
SQLITE_MMAP_SIZE = 256 * 1024 * 1024


def _slugify_identifier(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "item"
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw or "item"


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sql_text(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def is_postgres_backend() -> bool:
    return DB_BACKEND in {"postgres", "postgresql"}


def apply_sqlite_pragmas(conn: sqlite3.Connection, *, history: bool = False) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute(f"PRAGMA wal_autocheckpoint={SQLITE_WAL_AUTOCHECKPOINT_PAGES};")
    conn.execute(f"PRAGMA journal_size_limit={SQLITE_JOURNAL_SIZE_LIMIT};")
    conn.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_SIZE_KIB};")
    conn.execute(f"PRAGMA mmap_size={SQLITE_MMAP_SIZE};")
    conn.execute(f"PRAGMA foreign_keys={'OFF' if history else 'ON'};")
    return conn


def run_sqlite_maintenance(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA optimize;")
    except Exception:
        pass


def _connect_postgres(*, history: bool = False, system: bool = False):
    if system and SYSTEM_DATABASE_URL:
        dsn = SYSTEM_DATABASE_URL
    elif history and HISTORY_DATABASE_URL:
        dsn = HISTORY_DATABASE_URL
    else:
        dsn = DATABASE_URL
    if not dsn:
        raise RuntimeError("APP_DATABASE_URL/DATABASE_URL не задан для PostgreSQL backend")
    try:
        import psycopg
        from psycopg.rows import tuple_row
    except Exception as exc:
        raise RuntimeError("Для PostgreSQL backend требуется psycopg[binary]") from exc
    conn = psycopg.connect(dsn, row_factory=tuple_row)
    conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
    return conn


def _connect_sqlite(*, history: bool = False, system: bool = False) -> sqlite3.Connection:
    if system:
        db_path = SYSTEM_DB_PATH
    elif history:
        db_path = HISTORY_DB_PATH
    else:
        db_path = OPERATIONAL_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    return apply_sqlite_pragmas(conn, history=history)


def _connect() -> sqlite3.Connection:
    if is_postgres_backend():
        return _connect_postgres(history=False)
    return _connect_sqlite(history=False)


def _connect_history() -> sqlite3.Connection:
    if is_postgres_backend():
        return _connect_postgres(history=True)
    return _connect_sqlite(history=True)


def _connect_system():
    if is_postgres_backend():
        return _connect_postgres(system=True)
    return _connect_sqlite(system=True)


def cleanup_legacy_kv_store(conn: sqlite3.Connection) -> bool:
    legacy_keys = (
        "core.catalog_cache",
        "core.sales_cache",
        "yamarket.attractiveness",
        "yamarket.pricing_decisions",
        "yamarket.promo_pricing_table",
        "yamarket.promos",
        "yamarket.promos_full",
    )
    placeholder = "%s" if is_postgres_backend() else "?"
    removed = 0
    for key in legacy_keys:
        cur = conn.execute(f"DELETE FROM kv_store WHERE key = {placeholder}", (key,))
        removed += int(getattr(cur, "rowcount", 0) or 0)
    return removed > 0


def rebuild_db_explorer_views(conn: sqlite3.Connection | None = None) -> None:
    owns_conn = conn is None
    if conn is None:
        conn = _connect()
    try:
        if is_postgres_backend():
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS db_explorer_catalog (
                    object_name TEXT PRIMARY KEY,
                    object_type TEXT NOT NULL DEFAULT 'view',
                    group_name TEXT NOT NULL DEFAULT '',
                    source_name TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
            return
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS db_explorer_catalog (
                object_name TEXT PRIMARY KEY,
                object_type TEXT NOT NULL DEFAULT 'view',
                group_name TEXT NOT NULL DEFAULT '',
                source_name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        existing_objects = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        system_existing_objects: set[str] = set()
        source_registry_rows: list[Any] = []
        store_ref_rows: list[Any] = []
        try:
            with _connect_system() as system_conn:
                if hasattr(system_conn, "execute"):
                    if is_postgres_backend():
                        system_existing_objects = {
                            str(row[0] if not isinstance(row, dict) else row["table_name"] or "")
                            for row in system_conn.execute(
                                """
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = current_schema()
                                """
                            ).fetchall()
                        }
                    else:
                        system_existing_objects = {
                            row[0]
                            for row in system_conn.execute(
                                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
                            ).fetchall()
                        }
                    if "source_tables_registry" in system_existing_objects:
                        source_registry_rows = system_conn.execute(
                            "SELECT source_id, source_type, title, table_name FROM source_tables_registry ORDER BY source_id"
                        ).fetchall()
                    if "stores" in system_existing_objects:
                        store_ref_rows = system_conn.execute(
                            "SELECT store_uid, platform, store_id, store_name, currency_code, fulfillment_model FROM stores ORDER BY platform, store_id"
                        ).fetchall()
        except Exception:
            source_registry_rows = []
            store_ref_rows = []

        managed_views = [
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='view'
                  AND (
                    name LIKE 'v_%'
                    OR name LIKE 'boost__%'
                    OR name LIKE 'strategy__%'
                    OR name LIKE 'prices__%'
                    OR name LIKE 'attractiveness__%'
                    OR name LIKE 'promos__%'
                    OR name LIKE 'promo_offers__%'
                    OR name LIKE 'store_settings__%'
                    OR name LIKE 'logistics_store__%'
                    OR name LIKE 'logistics_products__%'
                    OR name LIKE 'category_tree__%'
                    OR name LIKE 'category_settings__%'
                    OR name LIKE 'store_ref__%'
                  )
                """
            ).fetchall()
        ]
        for name in managed_views:
            conn.execute(f"DROP VIEW IF EXISTS {_quote_ident(name)}")
        conn.execute("DELETE FROM db_explorer_catalog")

        def register_object(name: str, object_type: str, group_name: str, source_name: str, description: str) -> None:
            conn.execute(
                """
                INSERT INTO db_explorer_catalog (object_name, object_type, group_name, source_name, description, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (name, object_type, group_name, source_name, description, now),
            )

        def create_view(name: str, select_sql: str, group_name: str, source_name: str, description: str) -> None:
            conn.execute(f"CREATE VIEW {_quote_ident(name)} AS {select_sql}")
            register_object(name, "view", group_name, source_name, description)

        table_descriptions = {
            "stores": ("ref", "Справочник магазинов"),
            "store_datasets": ("ref", "Наборы данных магазинов"),
            "source_tables_registry": ("sources", "Реестр источников данных"),
            "pricing_store_settings": ("pricing", "Общие настройки ценообразования"),
            "pricing_logistics_store_settings": ("pricing", "Настройки логистики магазинов"),
            "pricing_logistics_product_settings": ("pricing", "Логистические параметры товаров"),
            "pricing_category_tree": ("pricing", "Дерево категорий ценообразования"),
            "pricing_category_settings": ("pricing", "Настройки по категориям"),
            "pricing_price_results": ("pricing", "Рассчитанные цены"),
            "pricing_boost_results": ("pricing", "Рекомендованные ставки буста"),
            "pricing_strategy_results": ("pricing", "Результаты стратегии ценообразования"),
            "pricing_attractiveness_results": ("pricing", "Рассчитанная привлекательность"),
            "pricing_promo_results": ("pricing", "Сводные результаты промо"),
            "pricing_promo_offer_results": ("pricing", "Детальные результаты по акциям"),
            "fx_rates_cache": ("cache", "Кэш курсов валют"),
            "category_tree_cache_nodes": ("cache", "Кэш деревьев категорий"),
            "kv_store": ("system", "KV-хранилище JSON"),
            "db_explorer_catalog": ("system", "Каталог объектов БД"),
        }
        for object_name, (group_name, description) in table_descriptions.items():
            if object_name in existing_objects:
                register_object(object_name, "table", group_name, object_name, description)

        static_views: list[tuple[str, str, str, str, str]] = [
            ("v_ref_stores", "SELECT * FROM stores", "ref", "stores", "Справочник магазинов"),
            ("v_ref_store_datasets", "SELECT * FROM store_datasets", "ref", "store_datasets", "Наборы данных магазинов"),
            ("v_cache_fx_rates", "SELECT * FROM fx_rates_cache", "cache", "fx_rates_cache", "Кэш курсов валют"),
            ("v_cache_category_tree_nodes", "SELECT * FROM category_tree_cache_nodes", "cache", "category_tree_cache_nodes", "Кэш деревьев категорий"),
            ("v_source_registry", "SELECT * FROM source_tables_registry", "sources", "source_tables_registry", "Реестр источников данных"),
        ]
        for name, sql, group_name, source_name, description in static_views:
            if source_name in existing_objects:
                create_view(name, sql, group_name, source_name, description)

        if source_registry_rows:
            for row in source_registry_rows:
                source_id = str(row[0] if not isinstance(row, dict) else row["source_id"] or "").strip()
                source_type = str(row[1] if not isinstance(row, dict) else row["source_type"] or "").strip()
                title = str(row[2] if not isinstance(row, dict) else row["title"] or "").strip()
                table_name = str(row[3] if not isinstance(row, dict) else row["table_name"] or "").strip()
                if not table_name or table_name not in existing_objects:
                    continue
                view_name = f"v_source__{_slugify_identifier(source_id)}"
                description = title or source_id or table_name
                create_view(
                    view_name,
                    f"SELECT * FROM {_quote_ident(table_name)}",
                    "sources",
                    table_name,
                    f"Источник данных: {description} ({source_type or 'source'})",
                )

        if store_ref_rows and "pricing_price_results" in existing_objects:
            for row in store_ref_rows:
                store_uid = str(row[0] if not isinstance(row, dict) else row["store_uid"] or "").strip()
                platform = str(row[1] if not isinstance(row, dict) else row["platform"] or "").strip()
                store_id = str(row[2] if not isinstance(row, dict) else row["store_id"] or "").strip()
                store_name = str(row[3] if not isinstance(row, dict) else row["store_name"] or "").strip()
                currency_code = str(row[4] if not isinstance(row, dict) else row["currency_code"] or "").strip().upper() or "RUB"
                fulfillment_model = str(row[5] if not isinstance(row, dict) else row["fulfillment_model"] or "").strip().upper() or "FBO"
                if not store_uid or not platform or not store_id:
                    continue
                suffix = f"{_slugify_identifier(platform)}__{_slugify_identifier(store_id)}"
                store_label = store_name or store_uid
                source_label = f"{platform}:{store_id}"
                display_suffix = f"{platform} / {store_id} / {currency_code} / {fulfillment_model}"
                create_view(
                    f"store_settings__{suffix}",
                    f"SELECT * FROM pricing_store_settings WHERE store_uid = {_sql_text(store_uid)}",
                    "pricing",
                    "pricing_store_settings",
                    f"Настройки магазина: {store_label} [{display_suffix}]",
                )
                if "pricing_logistics_store_settings" in existing_objects:
                    create_view(
                        f"logistics_store__{suffix}",
                        f"SELECT * FROM pricing_logistics_store_settings WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_logistics_store_settings",
                        f"Логистика магазина: {store_label} [{display_suffix}]",
                    )
                if "pricing_logistics_product_settings" in existing_objects:
                    create_view(
                        f"logistics_products__{suffix}",
                        f"SELECT * FROM pricing_logistics_product_settings WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_logistics_product_settings",
                        f"Логистика товаров: {store_label} [{display_suffix}]",
                    )
                if "pricing_category_tree" in existing_objects:
                    create_view(
                        f"category_tree__{suffix}",
                        f"SELECT * FROM pricing_category_tree WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_category_tree",
                        f"Дерево категорий: {store_label} [{display_suffix}]",
                    )
                if "pricing_category_settings" in existing_objects:
                    create_view(
                        f"category_settings__{suffix}",
                        f"SELECT * FROM pricing_category_settings WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_category_settings",
                        f"Настройки категорий: {store_label} [{display_suffix}]",
                    )
                    create_view(
                        f"prices__{suffix}",
                        f"SELECT * FROM pricing_price_results WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_price_results",
                        f"Цены: {store_label} [{display_suffix}]",
                    )
                if "pricing_boost_results" in existing_objects:
                    create_view(
                        f"boost__{suffix}",
                        f"SELECT * FROM pricing_boost_results WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_boost_results",
                        f"Буст: {store_label} [{display_suffix}]",
                    )
                if "pricing_strategy_results" in existing_objects:
                    create_view(
                        f"strategy__{suffix}",
                        f"SELECT * FROM pricing_strategy_results WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_strategy_results",
                        f"Стратегия ценообразования: {store_label} [{display_suffix}]",
                    )
                if "pricing_attractiveness_results" in existing_objects:
                    create_view(
                        f"attractiveness__{suffix}",
                        f"SELECT * FROM pricing_attractiveness_results WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_attractiveness_results",
                        f"Привлекательность: {store_label} [{display_suffix}]",
                    )
                if "pricing_promo_results" in existing_objects:
                    create_view(
                        f"promos__{suffix}",
                        f"SELECT * FROM pricing_promo_results WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_promo_results",
                        f"Промо: {store_label} [{display_suffix}]",
                    )
                if "pricing_promo_offer_results" in existing_objects:
                    create_view(
                        f"promo_offers__{suffix}",
                        f"SELECT * FROM pricing_promo_offer_results WHERE store_uid = {_sql_text(store_uid)}",
                        "pricing",
                        "pricing_promo_offer_results",
                        f"Промо-акции: {store_label} [{display_suffix}]",
                    )
                create_view(
                    f"store_ref__{suffix}",
                    f"SELECT * FROM stores WHERE store_uid = {_sql_text(store_uid)}",
                    "ref",
                    "stores",
                    f"Справочник магазина: {store_label} [{source_label}]",
                )

        if "db_explorer_catalog" in existing_objects or True:
            create_view(
                "v_db_catalog",
                "SELECT * FROM db_explorer_catalog ORDER BY group_name, object_name",
                "system",
                "db_explorer_catalog",
                "Каталог объектов БД",
            )
        conn.commit()
    finally:
        if owns_conn:
            conn.close()


def init_db() -> None:
    global _DB_INIT_DONE
    if _DB_INIT_DONE:
        return
    with _DB_INIT_LOCK:
        if _DB_INIT_DONE:
            return
        conn = _connect()
        conn.execute("DROP TABLE IF EXISTS lost_and_found")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cleanup_legacy_kv_store(conn)
        rebuild_db_explorer_views(conn)
        if not is_postgres_backend():
            run_sqlite_maintenance(conn)
        conn.commit()
        conn.close()
        _DB_INIT_DONE = True


def load_json(key: str, default: Any = None) -> Any:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT payload FROM kv_store WHERE key = %s" if is_postgres_backend() else "SELECT payload FROM kv_store WHERE key = ?",
            (key,),
        ).fetchone()
    if not row:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return default


def save_json(key: str, payload: Any) -> None:
    init_db()
    raw = json.dumps(payload, ensure_ascii=False)
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        if is_postgres_backend():
            conn.execute(
                """
                INSERT INTO kv_store (key, payload, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT(key) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at
                """,
                (key, raw, now),
            )
        else:
            conn.execute(
                """
                INSERT INTO kv_store (key, payload, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (key, raw, now),
            )
        conn.commit()


def has_key(key: str) -> bool:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM kv_store WHERE key = %s LIMIT 1" if is_postgres_backend() else "SELECT 1 FROM kv_store WHERE key = ? LIMIT 1",
            (key,),
        ).fetchone()
    return row is not None


def migrate_legacy_json_if_missing(key: str, path: Path, default: Any) -> Any:
    current = load_json(key, None)
    if current is not None:
        return current
    if path.exists():
        try:
            legacy = json.loads(path.read_text(encoding="utf-8"))
            save_json(key, legacy)
            return legacy
        except Exception:
            pass
    save_json(key, default)
    return default
