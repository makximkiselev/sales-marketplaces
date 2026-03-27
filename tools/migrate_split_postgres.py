from __future__ import annotations

import os
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OPERATIONAL_SQLITE = DATA_DIR / "analytics.db"
SYSTEM_SQLITE = DATA_DIR / "analytics_system.db"
HISTORY_SQLITE = DATA_DIR / "analytics_history.db"

SYSTEM_TABLES = {
    "stores",
    "store_settings",
    "store_datasets",
    "source_tables_registry",
    "refresh_jobs",
    "pricing_category_tree",
    "pricing_category_settings",
    "pricing_logistics_product_settings",
    "fx_rates_cache",
    "category_tree_cache_nodes",
}

OPERATIONAL_TABLES = {
    "pricing_price_results",
    "pricing_boost_results",
    "pricing_strategy_results",
    "pricing_attractiveness_results",
    "pricing_promo_results",
    "pricing_promo_offer_results",
    "sales_overview_order_rows",
    "sales_overview_cogs_source_rows",
    "yandex_goods_price_report_items",
    "refresh_job_runs",
    "pricing_autopilot_decisions",
    "db_explorer_catalog",
}

HISTORY_TABLES = {
    "pricing_strategy_history",
    "pricing_strategy_iteration_history",
    "pricing_market_price_export_history",
    "pricing_cogs_snapshots",
    "pricing_autopilot_snapshots",
    "sales_market_order_items",
    "sales_united_order_transactions",
    "sales_united_netting_report_rows",
    "sales_shelfs_statistics_report_rows",
    "sales_shows_boost_report_rows",
    "yandex_goods_price_report_history",
    "pricing_promo_campaign_raw",
    "pricing_promo_offer_raw",
}


def _sqlite_connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _iter_source_tables(conn: sqlite3.Connection) -> list[str]:
    return [
        str(row[0] or "")
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'source_items__%' ORDER BY name"
        ).fetchall()
    ]


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


def _create_table_sql(sqlite_conn: sqlite3.Connection, table_name: str) -> tuple[str, list[str]]:
    pragma_rows = sqlite_conn.execute(f"PRAGMA table_info({_quote_ident(table_name)})").fetchall()
    column_defs: list[str] = []
    pk_columns: list[str] = []
    column_names: list[str] = []
    for _, name, col_type, notnull, default_value, pk in pragma_rows:
        column_names.append(str(name))
        parts = [f'{_quote_ident(name)} {_pg_type(str(col_type or ""))}']
        if int(notnull or 0):
            parts.append("NOT NULL")
        if default_value is not None:
            parts.append(f"DEFAULT {default_value}")
        column_defs.append(" ".join(parts))
        if int(pk or 0):
            pk_columns.append(str(name))
    pk_sql = ""
    if pk_columns:
        pk_sql = ", PRIMARY KEY (" + ", ".join(_quote_ident(name) for name in pk_columns) + ")"
    create_sql = f"CREATE TABLE IF NOT EXISTS {_quote_ident(table_name)} ({', '.join(column_defs)}{pk_sql})"
    return create_sql, column_names


def _copy_table(sqlite_conn: sqlite3.Connection, pg_cur, table_name: str) -> int:
    create_sql, column_names = _create_table_sql(sqlite_conn, table_name)
    pg_cur.execute(create_sql)
    pg_cur.execute(f"TRUNCATE TABLE {_quote_ident(table_name)}")
    rows = sqlite_conn.execute(f"SELECT * FROM {_quote_ident(table_name)}").fetchall()
    if not rows:
        return 0
    insert_sql = (
        f"INSERT INTO {_quote_ident(table_name)} "
        f"({', '.join(_quote_ident(name) for name in column_names)}) "
        f"VALUES ({', '.join(['%s'] * len(column_names))})"
    )
    payload = [tuple(row[name] for name in column_names) for row in rows]
    pg_cur.executemany(insert_sql, payload)
    return len(payload)


def _copy_group(*, sqlite_path: Path, tables: set[str], dsn: str, include_source_tables: bool = False, label: str) -> None:
    if not dsn:
        print(f"{label}: skipped (dsn not set)")
        return
    if not sqlite_path.exists():
        print(f"{label}: skipped (sqlite source missing)")
        return
    try:
        import psycopg
    except Exception as exc:
        raise SystemExit("psycopg[binary] is required. Install dependencies first.") from exc

    sqlite_conn = _sqlite_connect(sqlite_path)
    pg_conn = psycopg.connect(dsn)
    try:
        table_list = [t for t in sorted(tables) if _table_exists(sqlite_conn, t)]
        if include_source_tables:
            table_list.extend(_iter_source_tables(sqlite_conn))
        with pg_conn:
            with pg_conn.cursor() as cur:
                for table_name in table_list:
                    copied = _copy_table(sqlite_conn, cur, table_name)
                    print(f"{label}:{table_name}:{copied}")
    finally:
        sqlite_conn.close()
        pg_conn.close()


def main() -> None:
    operational_dsn = str(os.getenv("APP_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
    system_dsn = str(os.getenv("APP_SYSTEM_DATABASE_URL") or os.getenv("SYSTEM_DATABASE_URL") or "").strip()
    history_dsn = str(os.getenv("APP_HISTORY_DATABASE_URL") or os.getenv("HISTORY_DATABASE_URL") or "").strip()

    _copy_group(
        sqlite_path=SYSTEM_SQLITE if SYSTEM_SQLITE.exists() else OPERATIONAL_SQLITE,
        tables=SYSTEM_TABLES,
        dsn=system_dsn or operational_dsn,
        include_source_tables=False,
        label="system",
    )
    _copy_group(
        sqlite_path=OPERATIONAL_SQLITE,
        tables=OPERATIONAL_TABLES,
        dsn=operational_dsn,
        include_source_tables=True,
        label="operational",
    )
    _copy_group(
        sqlite_path=HISTORY_SQLITE if HISTORY_SQLITE.exists() else OPERATIONAL_SQLITE,
        tables=HISTORY_TABLES,
        dsn=history_dsn or operational_dsn,
        include_source_tables=False,
        label="history",
    )


if __name__ == "__main__":
    main()
