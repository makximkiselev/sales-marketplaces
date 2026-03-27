from __future__ import annotations

import os
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

HOT_SQLITE_PATH = DATA_DIR / "analytics.db"
SYSTEM_SQLITE_PATH = DATA_DIR / "analytics_system.db"
HISTORY_SQLITE_PATH = DATA_DIR / "analytics_history.db"

HOT_EXPLICIT_TABLES = {
    "kv_store",
    "stores",
    "store_datasets",
    "source_tables_registry",
    "pricing_store_settings",
    "pricing_logistics_store_settings",
    "pricing_logistics_product_settings",
    "pricing_category_tree",
    "pricing_category_settings",
    "pricing_price_results",
    "pricing_boost_results",
    "pricing_strategy_results",
    "pricing_attractiveness_results",
    "pricing_promo_results",
    "pricing_promo_offer_results",
    "pricing_promo_campaign_raw",
    "pricing_promo_offer_raw",
    "pricing_promo_coinvest_settings",
    "refresh_jobs",
    "refresh_job_runs",
    "fx_rates_cache",
    "category_tree_cache_nodes",
    "sales_market_order_items",
    "sales_overview_cogs_source_rows",
    "sales_overview_order_rows",
    "sales_shelfs_statistics_report_rows",
    "sales_shows_boost_report_rows",
    "sales_united_netting_report_rows",
    "sales_united_order_transactions",
    "yandex_goods_price_report_items",
}

SYSTEM_EXPLICIT_TABLES = {
    "stores",
    "store_settings",
    "refresh_jobs",
}

HISTORY_EXPLICIT_TABLES = {
    "pricing_market_price_export_history",
    "pricing_strategy_history",
    "pricing_strategy_iteration_history",
    "pricing_cogs_snapshots",
    "sales_market_order_items",
    "sales_overview_order_rows",
    "yandex_goods_price_report_history",
}

SKIP_ON_ERROR_TABLES = {
    "pricing_strategy_history",
    "pricing_strategy_iteration_history",
}

SKIP_AFTER_INITIAL_FAILURES = 100


def _pg_type(sqlite_type: str) -> str:
    raw = str(sqlite_type or "").strip().upper()
    if "INT" in raw:
        return "BIGINT"
    if any(token in raw for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE PRECISION"
    if "BLOB" in raw:
        return "BYTEA"
    return "TEXT"


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _normalize_sqlite_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _coerce_numeric_value(value, *, integer: bool):
    normalized = _normalize_sqlite_value(value)
    if normalized in (None, ""):
        return None
    if isinstance(normalized, bool):
        return int(normalized) if integer else float(normalized)
    if isinstance(normalized, (int, float)):
        return int(normalized) if integer else float(normalized)
    text = str(normalized).strip().replace(",", ".")
    if not text:
        return None
    try:
        return int(float(text)) if integer else float(text)
    except Exception:
        return None


def _iter_tables(sqlite_conn: sqlite3.Connection, *, explicit_tables: set[str]) -> list[str]:
    names = [
        str(_normalize_sqlite_value(row[0]) or "")
        for row in sqlite_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    source_tables = [name for name in names if name.startswith("source_items__")]
    base_tables = [name for name in names if name in explicit_tables]
    return sorted(set(base_tables + source_tables))


def _create_table_sql(sqlite_conn: sqlite3.Connection, table_name: str) -> tuple[str, list[str], dict[str, str]]:
    pragma_rows = sqlite_conn.execute(f"PRAGMA table_info({_quote_ident(table_name)})").fetchall()
    column_defs: list[str] = []
    pk_columns: list[str] = []
    column_names: list[str] = []
    pg_types: dict[str, str] = {}
    for _, name, col_type, notnull, default_value, pk in pragma_rows:
        norm_name = str(_normalize_sqlite_value(name) or "")
        norm_type = str(_normalize_sqlite_value(col_type) or "")
        norm_default = _normalize_sqlite_value(default_value)
        pg_type = _pg_type(norm_type)
        column_names.append(norm_name)
        pg_types[norm_name] = pg_type
        parts = [f'{_quote_ident(norm_name)} {pg_type}']
        if int(notnull or 0):
            parts.append("NOT NULL")
        if norm_default is not None:
            parts.append(f"DEFAULT {norm_default}")
        column_defs.append(" ".join(parts))
        if int(pk or 0):
            pk_columns.append(norm_name)
    pk_sql = ""
    if pk_columns:
        pk_sql = ", PRIMARY KEY (" + ", ".join(_quote_ident(name) for name in pk_columns) + ")"
    create_sql = f"CREATE TABLE IF NOT EXISTS {_quote_ident(table_name)} ({', '.join(column_defs)}{pk_sql})"
    return create_sql, column_names, pg_types


def _migrate_database(*, label: str, sqlite_path: Path, dsn: str, explicit_tables: set[str]) -> None:
    if not dsn:
        print(f"{label}: skipped (dsn not set)")
        return
    if not sqlite_path.exists():
        print(f"{label}: skipped ({sqlite_path.name} not found)")
        return

    try:
        import psycopg
    except Exception as exc:
        raise SystemExit("psycopg[binary] is required. Install dependencies first.") from exc

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_conn.text_factory = bytes
    pg_conn = psycopg.connect(dsn)
    pg_conn.autocommit = False
    try:
        tables = _iter_tables(sqlite_conn, explicit_tables=explicit_tables)
        print(f"{label}: migrating {len(tables)} tables from {sqlite_path.name}")
        for table_name in tables:
            create_sql, column_names, pg_types = _create_table_sql(sqlite_conn, table_name)
            rows = sqlite_conn.execute(f"SELECT * FROM {_quote_ident(table_name)}").fetchall()
            insert_sql = (
                f"INSERT INTO {_quote_ident(table_name)} "
                f"({', '.join(_quote_ident(name) for name in column_names)}) "
                f"VALUES ({', '.join(['%s'] * len(column_names))})"
            )
            payload = [
                tuple(
                    _coerce_numeric_value(row[name], integer=(pg_types.get(name) == "BIGINT"))
                    if pg_types.get(name) in {"BIGINT", "DOUBLE PRECISION"}
                    else _normalize_sqlite_value(row[name])
                    for name in column_names
                )
                for row in rows
            ]

            try:
                with pg_conn.cursor() as cur:
                    cur.execute(create_sql)
                    cur.execute(f"TRUNCATE TABLE {_quote_ident(table_name)}")
                    if payload:
                        cur.executemany(insert_sql, payload)
                pg_conn.commit()
                print(f"  {table_name}: {len(payload)}")
                continue
            except Exception as exc:
                pg_conn.rollback()
                sample = payload[0] if payload else ()
                print(f"{label}: failed table={table_name}")
                print(f"columns={column_names}")
                print(f"sample={sample}")
                if not payload:
                    print(f"  {table_name}: 0")
                    continue

                skipped = 0
                inserted = 0
                with pg_conn.cursor() as cur:
                    cur.execute(create_sql)
                    cur.execute(f"TRUNCATE TABLE {_quote_ident(table_name)}")
                pg_conn.commit()

                for index, row_payload in enumerate(payload, start=1):
                    try:
                        with pg_conn.cursor() as cur:
                            cur.execute(insert_sql, row_payload)
                        pg_conn.commit()
                        inserted += 1
                    except Exception:
                        pg_conn.rollback()
                        skipped += 1
                        if (
                            table_name in SKIP_ON_ERROR_TABLES
                            and inserted == 0
                            and skipped >= SKIP_AFTER_INITIAL_FAILURES
                        ):
                            print(
                                f"{label}: skipped corrupted table={table_name} "
                                f"after first {index} rows all failed"
                            )
                            break
                print(f"{label}: fallback table={table_name} inserted={inserted} skipped={skipped}")
                if inserted == 0:
                    if table_name in SKIP_ON_ERROR_TABLES:
                        print(f"{label}: skipped corrupted table={table_name}")
                        continue
                    raise exc
    finally:
        sqlite_conn.close()
        pg_conn.close()


def main() -> None:
    hot_dsn = str(os.getenv("APP_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
    system_dsn = str(os.getenv("APP_SYSTEM_DATABASE_URL") or os.getenv("SYSTEM_DATABASE_URL") or hot_dsn).strip()
    history_dsn = str(os.getenv("APP_HISTORY_DATABASE_URL") or os.getenv("HISTORY_DATABASE_URL") or hot_dsn).strip()

    if not hot_dsn and not system_dsn and not history_dsn:
        raise SystemExit(
            "Set APP_DATABASE_URL / APP_SYSTEM_DATABASE_URL / APP_HISTORY_DATABASE_URL for PostgreSQL migration"
        )

    _migrate_database(
        label="hot",
        sqlite_path=HOT_SQLITE_PATH,
        dsn=hot_dsn,
        explicit_tables=HOT_EXPLICIT_TABLES,
    )
    _migrate_database(
        label="system",
        sqlite_path=SYSTEM_SQLITE_PATH,
        dsn=system_dsn,
        explicit_tables=SYSTEM_EXPLICIT_TABLES,
    )
    _migrate_database(
        label="history",
        sqlite_path=HISTORY_SQLITE_PATH,
        dsn=history_dsn,
        explicit_tables=HISTORY_EXPLICIT_TABLES,
    )


if __name__ == "__main__":
    main()
