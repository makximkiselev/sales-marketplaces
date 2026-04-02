from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from backend.services.db import (
    DATABASE_URL,
    HISTORY_DATABASE_URL,
    SYSTEM_DATABASE_URL,
    is_postgres_backend,
    rebuild_db_explorer_views,
    run_sqlite_maintenance,
)


_INIT_LOCK = threading.Lock()
_INIT_DONE = False
_MAINTENANCE_DONE = False
_SYSTEM_INIT_LOCK = threading.Lock()
_SYSTEM_INIT_DONE = False

_HOT_RETENTION_RULES: tuple[tuple[str, str, int], ...] = (
    ("pricing_strategy_history", "captured_at", 2),
    ("pricing_strategy_iteration_history", "captured_at", 2),
    ("pricing_cogs_snapshots", "snapshot_at", 7),
    ("pricing_market_price_export_history", "requested_at", 7),
    ("pricing_autopilot_snapshots", "snapshot_at", 7),
    ("pricing_autopilot_decisions", "created_at", 90),
    ("sales_united_netting_report_rows", "loaded_at", 30),
)

_HISTORY_RETENTION_RULES: tuple[tuple[str, str, int], ...] = (
    ("pricing_strategy_history", "captured_at", 7),
    ("pricing_strategy_iteration_history", "captured_at", 7),
    ("pricing_cogs_snapshots", "snapshot_at", 7),
    ("pricing_market_price_export_history", "requested_at", 7),
    ("sales_market_order_items", "loaded_at", 90),
    ("sales_overview_order_rows", "calculated_at", 90),
    ("yandex_goods_price_report_history", "captured_at", 30),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _placeholders(count: int) -> str:
    token = "%s" if is_postgres_backend() else "?"
    return ", ".join([token] * max(0, int(count)))


def _system_placeholders(count: int) -> str:
    token = "%s" if is_postgres_backend() and SYSTEM_DATABASE_URL else "?"
    return ", ".join([token] * max(0, int(count)))


def _executemany(conn: Any, sql: str, params_seq: list[tuple[Any, ...]] | list[list[Any]] | list[dict[str, Any]]) -> None:
    if is_postgres_backend():
        with conn.cursor() as cur:
            cur.executemany(sql, params_seq)
        return
    conn.executemany(sql, params_seq)


def _connect() -> sqlite3.Connection:
    if not is_postgres_backend():
        raise RuntimeError("SQLite runtime отключен. Используй PostgreSQL backend.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as exc:
        raise RuntimeError("Для PostgreSQL backend требуется psycopg[binary]") from exc
    if not DATABASE_URL:
        raise RuntimeError("APP_DATABASE_URL/DATABASE_URL не задан для PostgreSQL backend")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _connect_history() -> sqlite3.Connection:
    if not is_postgres_backend():
        raise RuntimeError("SQLite runtime отключен. Используй PostgreSQL backend.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as exc:
        raise RuntimeError("Для PostgreSQL backend требуется psycopg[binary]") from exc
    return psycopg.connect(HISTORY_DATABASE_URL, row_factory=dict_row)


def _connect_system() -> sqlite3.Connection:
    if not is_postgres_backend():
        raise RuntimeError("SQLite runtime отключен. Используй PostgreSQL backend.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as exc:
        raise RuntimeError("Для PostgreSQL backend требуется psycopg[binary]") from exc
    return psycopg.connect(SYSTEM_DATABASE_URL, row_factory=dict_row)


def _run_retention_cleanup() -> None:
    cutoff_cache: dict[int, str] = {}

    def _cutoff(days: int) -> str:
        cached = cutoff_cache.get(days)
        if cached:
            return cached
        value = (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat()
        cutoff_cache[days] = value
        return value

    with _connect() as conn:
        existing = _table_names(conn)
        for table_name, column_name, days in _HOT_RETENTION_RULES:
            if table_name not in existing:
                continue
            conn.execute(
                f"DELETE FROM {_quote_ident(table_name)} WHERE {_quote_ident(column_name)} < {'%s' if is_postgres_backend() else '?'}",
                (_cutoff(days),),
            )
        if not is_postgres_backend():
            run_sqlite_maintenance(conn)
        conn.commit()

    with _connect_history() as conn:
        existing = _table_names(conn)
        for table_name, column_name, days in _HISTORY_RETENTION_RULES:
            if table_name not in existing:
                continue
            conn.execute(
                f"DELETE FROM {_quote_ident(table_name)} WHERE {_quote_ident(column_name)} < {'%s' if is_postgres_backend() else '?'}",
                (_cutoff(days),),
            )
        if not is_postgres_backend():
            run_sqlite_maintenance(conn)
        conn.commit()


def _init_history_tables() -> None:
    with _connect_history() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pricing_market_price_export_history (
                store_uid TEXT NOT NULL,
                sku TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                campaign_id TEXT NOT NULL DEFAULT '',
                price REAL NULL,
                source TEXT NOT NULL DEFAULT 'strategy',
                PRIMARY KEY (store_uid, sku, requested_at)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pricing_market_price_export_history_store_sku_time "
            "ON pricing_market_price_export_history(store_uid, sku, requested_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pricing_strategy_history (
                store_uid TEXT NOT NULL,
                sku TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                cycle_started_at TEXT NOT NULL DEFAULT '',
                strategy_code TEXT NOT NULL DEFAULT 'base',
                strategy_label TEXT NOT NULL DEFAULT 'Базовая',
                installed_price REAL NULL,
                installed_profit_abs REAL NULL,
                installed_profit_pct REAL NULL,
                boost_bid_percent REAL NULL,
                market_boost_bid_percent REAL NULL,
                boost_share REAL NULL,
                decision_code TEXT NOT NULL DEFAULT 'observe',
                decision_label TEXT NOT NULL DEFAULT 'Наблюдать',
                decision_tone TEXT NOT NULL DEFAULT 'warning',
                hypothesis TEXT NOT NULL DEFAULT '',
                hypothesis_started_at TEXT NOT NULL DEFAULT '',
                hypothesis_expires_at TEXT NOT NULL DEFAULT '',
                control_state TEXT NOT NULL DEFAULT 'stable',
                control_state_started_at TEXT NOT NULL DEFAULT '',
                attractiveness_status TEXT NOT NULL DEFAULT '',
                promo_count INTEGER NOT NULL DEFAULT 0,
                coinvest_pct REAL NULL,
                selected_iteration_code TEXT NOT NULL DEFAULT '',
                uses_promo INTEGER NOT NULL DEFAULT 0,
                uses_attractiveness INTEGER NOT NULL DEFAULT 0,
                uses_boost INTEGER NOT NULL DEFAULT 0,
                market_promo_status TEXT NOT NULL DEFAULT '',
                market_promo_checked_at TEXT NOT NULL DEFAULT '',
                market_promo_message TEXT NOT NULL DEFAULT '',
                source_updated_at TEXT NULL,
                PRIMARY KEY (store_uid, sku, captured_at)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pricing_strategy_history_store_sku_time "
            "ON pricing_strategy_history(store_uid, sku, captured_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pricing_strategy_iteration_history (
                store_uid TEXT NOT NULL,
                sku TEXT NOT NULL,
                cycle_started_at TEXT NOT NULL,
                iteration_code TEXT NOT NULL,
                iteration_label TEXT NOT NULL DEFAULT '',
                tested_price REAL NULL,
                tested_boost_pct REAL NULL,
                market_boost_bid_percent REAL NULL,
                boost_share REAL NULL,
                promo_count INTEGER NOT NULL DEFAULT 0,
                attractiveness_status TEXT NOT NULL DEFAULT '',
                coinvest_pct REAL NULL,
                on_display_price REAL NULL,
                promo_details_json TEXT NOT NULL DEFAULT '[]',
                market_promo_status TEXT NOT NULL DEFAULT '',
                market_promo_message TEXT NOT NULL DEFAULT '',
                source_updated_at TEXT NULL,
                captured_at TEXT NOT NULL,
                PRIMARY KEY (store_uid, sku, cycle_started_at, iteration_code)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pricing_strategy_iteration_history_store_cycle "
            "ON pricing_strategy_iteration_history(store_uid, cycle_started_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pricing_strategy_iteration_history_store_sku "
            "ON pricing_strategy_iteration_history(store_uid, sku)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_market_order_items (
                store_uid TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'yandex_market',
                order_id TEXT NOT NULL,
                order_item_id TEXT NOT NULL,
                order_status TEXT NOT NULL DEFAULT '',
                order_created_at TEXT NOT NULL,
                order_created_date TEXT NOT NULL,
                sku TEXT NOT NULL,
                item_name TEXT NOT NULL DEFAULT '',
                sale_price REAL NULL,
                payment_price REAL NULL,
                subsidy_amount REAL NULL,
                item_count INTEGER NOT NULL DEFAULT 1,
                line_revenue REAL NULL,
                loaded_at TEXT NOT NULL,
                PRIMARY KEY (store_uid, order_item_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_market_order_items_store_date ON sales_market_order_items(store_uid, order_created_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_market_order_items_store_sku ON sales_market_order_items(store_uid, sku)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_market_order_items_order ON sales_market_order_items(store_uid, order_id)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_united_order_transactions (
                store_uid TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'yandex_market',
                order_id TEXT NOT NULL,
                order_created_at TEXT NOT NULL,
                order_created_date TEXT NOT NULL,
                shipment_date TEXT NOT NULL DEFAULT '',
                delivery_date TEXT NOT NULL DEFAULT '',
                sku TEXT NOT NULL,
                item_name TEXT NOT NULL DEFAULT '',
                item_status TEXT NOT NULL DEFAULT '',
                source_updated_at TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                loaded_at TEXT NOT NULL,
                PRIMARY KEY (store_uid, order_id, sku)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_store_date ON sales_united_order_transactions(store_uid, order_created_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_store_sku ON sales_united_order_transactions(store_uid, sku)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_order ON sales_united_order_transactions(store_uid, order_id)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_overview_order_rows (
                store_uid TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'yandex_market',
                order_created_date TEXT NOT NULL,
                order_created_at TEXT NOT NULL DEFAULT '',
                shipment_date TEXT NOT NULL DEFAULT '',
                delivery_date TEXT NOT NULL DEFAULT '',
                order_id TEXT NOT NULL,
                item_status TEXT NOT NULL DEFAULT '',
                sku TEXT NOT NULL,
                item_name TEXT NOT NULL DEFAULT '',
                sale_price REAL NULL,
                gross_profit REAL NULL,
                cogs_price REAL NULL,
                commission REAL NULL,
                acquiring REAL NULL,
                delivery REAL NULL,
                ads REAL NULL,
                tax REAL NULL,
                profit REAL NULL,
                sale_price_with_coinvest REAL NULL,
                strategy_cycle_started_at TEXT NOT NULL DEFAULT '',
                strategy_market_boost_bid_percent REAL NULL,
                strategy_boost_share REAL NULL,
                strategy_boost_bid_percent REAL NULL,
                strategy_snapshot_at TEXT NOT NULL DEFAULT '',
                strategy_installed_price REAL NULL,
                strategy_decision_code TEXT NOT NULL DEFAULT '',
                strategy_decision_label TEXT NOT NULL DEFAULT '',
                strategy_control_state TEXT NOT NULL DEFAULT '',
                strategy_attractiveness_status TEXT NOT NULL DEFAULT '',
                strategy_promo_count INTEGER NOT NULL DEFAULT 0,
                strategy_coinvest_pct REAL NULL,
                strategy_selected_iteration_code TEXT NOT NULL DEFAULT '',
                strategy_uses_promo INTEGER NOT NULL DEFAULT 0,
                strategy_market_promo_status TEXT NOT NULL DEFAULT '',
                uses_planned_costs INTEGER NOT NULL DEFAULT 0,
                source_updated_at TEXT NOT NULL DEFAULT '',
                calculated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (store_uid, order_id, sku)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_store_date "
            "ON sales_overview_order_rows(store_uid, order_created_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_store_status "
            "ON sales_overview_order_rows(store_uid, item_status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_store_sku "
            "ON sales_overview_order_rows(store_uid, sku)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pricing_cogs_snapshots (
                snapshot_at TEXT NOT NULL,
                store_uid TEXT NOT NULL,
                sku TEXT NOT NULL,
                cogs_value REAL NULL,
                source_id TEXT NOT NULL DEFAULT '',
                loaded_at TEXT NOT NULL,
                PRIMARY KEY (snapshot_at, store_uid, sku)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pricing_cogs_snapshots_store_time "
            "ON pricing_cogs_snapshots(store_uid, snapshot_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pricing_cogs_snapshots_sku "
            "ON pricing_cogs_snapshots(store_uid, sku)"
        )
        if not is_postgres_backend():
            try:
                with _connect() as hot_conn:
                    hot_tables = _table_names(hot_conn)
                    if "sales_united_order_transactions" in hot_tables:
                        hot_rows = hot_conn.execute(
                            """
                            SELECT
                                store_uid, platform, order_id, order_created_at, order_created_date,
                                shipment_date, delivery_date, sku, item_name, item_status,
                                source_updated_at, payload_json, loaded_at
                            FROM sales_united_order_transactions
                            """
                        ).fetchall()
                        prepared = [
                            (
                                str(row["store_uid"] or "").strip(),
                                str(row["platform"] or "yandex_market").strip() or "yandex_market",
                                str(row["order_id"] or "").strip(),
                                str(row["order_created_at"] or "").strip(),
                                str(row["order_created_date"] or "").strip(),
                                str(row["shipment_date"] or "").strip(),
                                str(row["delivery_date"] or "").strip(),
                                str(row["sku"] or "").strip(),
                                str(row["item_name"] or "").strip(),
                                str(row["item_status"] or "").strip(),
                                str(row["source_updated_at"] or "").strip(),
                                str(row["payload_json"] or "{}").strip() or "{}",
                                str(row["loaded_at"] or "").strip(),
                            )
                            for row in hot_rows
                            if str(row["store_uid"] or "").strip()
                            and str(row["order_id"] or "").strip()
                            and str(row["sku"] or "").strip()
                            and str(row["order_created_at"] or "").strip()
                            and str(row["order_created_date"] or "").strip()
                        ]
                        if prepared:
                            values_sql = _placeholders(13)
                            _executemany(
                                conn,
                                f"""
                                INSERT INTO sales_united_order_transactions (
                                    store_uid, platform, order_id, order_created_at, order_created_date,
                                    shipment_date, delivery_date, sku, item_name, item_status,
                                    source_updated_at, payload_json, loaded_at
                                ) VALUES ({values_sql})
                                ON CONFLICT(store_uid, order_id, sku) DO UPDATE SET
                                    platform = excluded.platform,
                                    order_created_at = excluded.order_created_at,
                                    order_created_date = excluded.order_created_date,
                                    shipment_date = excluded.shipment_date,
                                    delivery_date = excluded.delivery_date,
                                    item_name = excluded.item_name,
                                    item_status = excluded.item_status,
                                    source_updated_at = excluded.source_updated_at,
                                    payload_json = excluded.payload_json,
                                    loaded_at = excluded.loaded_at
                                """,
                                prepared,
                            )
                    if "sales_overview_order_rows" in hot_tables:
                        hot_rows = hot_conn.execute(
                            """
                            SELECT
                                store_uid, platform, order_created_date, order_created_at, shipment_date, delivery_date,
                                order_id, item_status, sku, item_name, sale_price, gross_profit, cogs_price, commission,
                                acquiring, delivery, ads, tax, profit, sale_price_with_coinvest,
                                strategy_cycle_started_at, strategy_market_boost_bid_percent, strategy_boost_share,
                                strategy_boost_bid_percent, strategy_snapshot_at, strategy_installed_price,
                                strategy_decision_code, strategy_decision_label, strategy_control_state,
                                strategy_attractiveness_status, strategy_promo_count, strategy_coinvest_pct,
                                strategy_selected_iteration_code, strategy_uses_promo, strategy_market_promo_status,
                                uses_planned_costs, source_updated_at, calculated_at
                            FROM sales_overview_order_rows
                            """
                        ).fetchall()
                        prepared = [
                            (
                                str(row["store_uid"] or "").strip(),
                                str(row["platform"] or "yandex_market").strip() or "yandex_market",
                                str(row["order_created_date"] or "").strip(),
                                str(row["order_created_at"] or "").strip(),
                                str(row["shipment_date"] or "").strip(),
                                str(row["delivery_date"] or "").strip(),
                                str(row["order_id"] or "").strip(),
                                str(row["item_status"] or "").strip(),
                                str(row["sku"] or "").strip(),
                                str(row["item_name"] or "").strip(),
                                row["sale_price"],
                                row["gross_profit"],
                                row["cogs_price"],
                                row["commission"],
                                row["acquiring"],
                                row["delivery"],
                                row["ads"],
                                row["tax"],
                                row["profit"],
                                row["sale_price_with_coinvest"],
                                str(row["strategy_cycle_started_at"] or "").strip(),
                                row["strategy_market_boost_bid_percent"],
                                row["strategy_boost_share"],
                                row["strategy_boost_bid_percent"],
                                str(row["strategy_snapshot_at"] or "").strip(),
                                row["strategy_installed_price"],
                                str(row["strategy_decision_code"] or "").strip(),
                                str(row["strategy_decision_label"] or "").strip(),
                                str(row["strategy_control_state"] or "").strip(),
                                str(row["strategy_attractiveness_status"] or "").strip(),
                                int(row["strategy_promo_count"] or 0),
                                row["strategy_coinvest_pct"],
                                str(row["strategy_selected_iteration_code"] or "").strip(),
                                int(row["strategy_uses_promo"] or 0),
                                str(row["strategy_market_promo_status"] or "").strip(),
                                int(row["uses_planned_costs"] or 0),
                                str(row["source_updated_at"] or "").strip(),
                                str(row["calculated_at"] or "").strip(),
                            )
                            for row in hot_rows
                            if str(row["store_uid"] or "").strip()
                            and str(row["order_id"] or "").strip()
                            and str(row["sku"] or "").strip()
                            and str(row["order_created_date"] or "").strip()
                        ]
                        if prepared:
                            values_sql = _placeholders(38)
                            _executemany(
                                conn,
                                f"""
                                INSERT INTO sales_overview_order_rows (
                                    store_uid, platform, order_created_date, order_created_at, shipment_date, delivery_date,
                                    order_id, item_status, sku, item_name, sale_price, gross_profit, cogs_price, commission,
                                    acquiring, delivery, ads, tax, profit, sale_price_with_coinvest,
                                    strategy_cycle_started_at, strategy_market_boost_bid_percent, strategy_boost_share,
                                    strategy_boost_bid_percent, strategy_snapshot_at, strategy_installed_price,
                                    strategy_decision_code, strategy_decision_label, strategy_control_state,
                                    strategy_attractiveness_status, strategy_promo_count, strategy_coinvest_pct, strategy_selected_iteration_code,
                                    strategy_uses_promo, strategy_market_promo_status, uses_planned_costs,
                                    source_updated_at, calculated_at
                                ) VALUES ({values_sql})
                                ON CONFLICT(store_uid, order_id, sku) DO UPDATE SET
                                    platform = excluded.platform,
                                    order_created_date = excluded.order_created_date,
                                    order_created_at = excluded.order_created_at,
                                    shipment_date = excluded.shipment_date,
                                    delivery_date = excluded.delivery_date,
                                    item_status = excluded.item_status,
                                    item_name = excluded.item_name,
                                    sale_price = excluded.sale_price,
                                    gross_profit = excluded.gross_profit,
                                    cogs_price = excluded.cogs_price,
                                    commission = excluded.commission,
                                    acquiring = excluded.acquiring,
                                    delivery = excluded.delivery,
                                    ads = excluded.ads,
                                    tax = excluded.tax,
                                    profit = excluded.profit,
                                    sale_price_with_coinvest = excluded.sale_price_with_coinvest,
                                    strategy_cycle_started_at = excluded.strategy_cycle_started_at,
                                    strategy_market_boost_bid_percent = excluded.strategy_market_boost_bid_percent,
                                    strategy_boost_share = excluded.strategy_boost_share,
                                    strategy_boost_bid_percent = excluded.strategy_boost_bid_percent,
                                    strategy_snapshot_at = excluded.strategy_snapshot_at,
                                    strategy_installed_price = excluded.strategy_installed_price,
                                    strategy_decision_code = excluded.strategy_decision_code,
                                    strategy_decision_label = excluded.strategy_decision_label,
                                    strategy_control_state = excluded.strategy_control_state,
                                    strategy_attractiveness_status = excluded.strategy_attractiveness_status,
                                    strategy_promo_count = excluded.strategy_promo_count,
                                    strategy_coinvest_pct = excluded.strategy_coinvest_pct,
                                    strategy_selected_iteration_code = excluded.strategy_selected_iteration_code,
                                    strategy_uses_promo = excluded.strategy_uses_promo,
                                    strategy_market_promo_status = excluded.strategy_market_promo_status,
                                    uses_planned_costs = excluded.uses_planned_costs,
                                    source_updated_at = excluded.source_updated_at,
                                    calculated_at = excluded.calculated_at
                                """,
                                prepared,
                            )
            except Exception:
                pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS yandex_goods_price_report_history (
                store_uid TEXT NOT NULL,
                offer_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                offer_name TEXT NOT NULL DEFAULT '',
                currency TEXT NOT NULL DEFAULT '',
                shop_price REAL NULL,
                basic_price REAL NULL,
                on_display_raw TEXT NOT NULL DEFAULT '',
                on_display_price REAL NULL,
                price_value_outside_market REAL NULL,
                price_value_on_market REAL NULL,
                price_green_threshold REAL NULL,
                price_red_threshold REAL NULL,
                source_updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (store_uid, offer_id, captured_at)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_yandex_goods_price_report_history_store_offer_time "
            "ON yandex_goods_price_report_history(store_uid, offer_id, captured_at)"
        )
        conn.commit()


def _table_columns(conn: Any, table_name: str) -> set[str]:
    if is_postgres_backend():
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            """,
            (str(table_name or "").strip(),),
        ).fetchall()
        return {str(row["column_name"]) for row in rows}
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _table_names(conn: Any) -> set[str]:
    if is_postgres_backend():
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_type = 'BASE TABLE'
            """
        ).fetchall()
        return {str(row["table_name"]) for row in rows}
    return {
        str(row[0] or "")
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }


def _rebuild_sales_overview_cogs_source_rows_if_needed(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "sales_overview_cogs_source_rows")
    if not cols:
        return
    pk_rows = conn.execute("PRAGMA table_info(sales_overview_cogs_source_rows)").fetchall()
    pk_cols = [str(row[1]) for row in pk_rows if int(row[5] or 0) > 0]
    if "sku_key" in cols and pk_cols == ["store_uid", "order_key", "sku_key"]:
        return
    conn.execute("ALTER TABLE sales_overview_cogs_source_rows RENAME TO sales_overview_cogs_source_rows_old")
    conn.execute(
        """
        CREATE TABLE sales_overview_cogs_source_rows (
            store_uid TEXT NOT NULL,
            order_key TEXT NOT NULL,
            sku_key TEXT NOT NULL DEFAULT '',
            cogs_value REAL NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, order_key, sku_key),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    old_cols = _table_columns(conn, "sales_overview_cogs_source_rows_old")
    if "sku_key" in old_cols:
        conn.execute(
            """
            INSERT INTO sales_overview_cogs_source_rows (
                store_uid, order_key, sku_key, cogs_value, loaded_at
            )
            SELECT store_uid, order_key, coalesce(sku_key, ''), cogs_value, loaded_at
            FROM sales_overview_cogs_source_rows_old
            """
        )
    else:
        conn.execute(
            """
            INSERT INTO sales_overview_cogs_source_rows (
                store_uid, order_key, sku_key, cogs_value, loaded_at
            )
            SELECT store_uid, order_key, '', cogs_value, loaded_at
            FROM sales_overview_cogs_source_rows_old
            """
        )
    conn.execute("DROP TABLE sales_overview_cogs_source_rows_old")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_overview_cogs_source_rows_store "
        "ON sales_overview_cogs_source_rows(store_uid)"
    )


def _rename_column_if_exists(conn: sqlite3.Connection, table_name: str, old_name: str, new_name: str) -> None:
    cols = _table_columns(conn, table_name)
    if old_name in cols and new_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")


def _rebuild_pricing_price_results_if_needed(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "pricing_price_results")
    if not cols:
        return
    legacy_cols = {"logistics_cost", "handling_text", "market_price", "market_profit_abs", "market_profit_pct"}
    if not any(col in cols for col in legacy_cols):
        return
    conn.execute("ALTER TABLE pricing_price_results RENAME TO pricing_price_results_old")
    conn.execute(
        """
        CREATE TABLE pricing_price_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            cogs_price REAL NULL,
            rrc_no_ads_price REAL NULL,
            rrc_no_ads_profit_abs REAL NULL,
            rrc_no_ads_profit_pct REAL NULL,
            mrc_price REAL NULL,
            mrc_profit_abs REAL NULL,
            mrc_profit_pct REAL NULL,
            mrc_with_boost_price REAL NULL,
            mrc_with_boost_profit_abs REAL NULL,
            mrc_with_boost_profit_pct REAL NULL,
            target_price REAL NULL,
            target_profit_abs REAL NULL,
            target_profit_pct REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO pricing_price_results (
            store_uid, sku, cogs_price, rrc_no_ads_price, rrc_no_ads_profit_abs, rrc_no_ads_profit_pct,
            mrc_price, mrc_profit_abs, mrc_profit_pct,
            mrc_with_boost_price, mrc_with_boost_profit_abs, mrc_with_boost_profit_pct,
            target_price, target_profit_abs, target_profit_pct, source_updated_at, calculated_at
        )
        SELECT store_uid, sku, cogs_price, NULL, NULL, NULL,
               mrc_price, mrc_profit_abs, mrc_profit_pct,
               NULL, NULL, NULL,
               target_price, target_profit_abs, target_profit_pct, source_updated_at, calculated_at
        FROM pricing_price_results_old
        """
    )
    conn.execute("DROP TABLE pricing_price_results_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_price_results_store ON pricing_price_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_price_results_sku ON pricing_price_results(sku)")


def _row_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _with_price_result_aliases(item: dict[str, Any]) -> dict[str, Any]:
    item["rrc"] = item.get("target_price")
    item["rrc_no_ads_price"] = item.get("rrc_no_ads_price")
    item["rrc_no_ads_profit_abs"] = item.get("rrc_no_ads_profit_abs")
    item["rrc_no_ads_profit_pct"] = item.get("rrc_no_ads_profit_pct")
    item["cogs"] = item.get("cogs_price")
    item["mrc_price"] = item.get("mrc_price")
    item["mrc_profit_abs"] = item.get("mrc_profit_abs")
    item["mrc_profit_pct"] = item.get("mrc_profit_pct")
    item["mrc_with_boost_price"] = item.get("mrc_with_boost_price")
    item["mrc_with_boost_profit_abs"] = item.get("mrc_with_boost_profit_abs")
    item["mrc_with_boost_profit_pct"] = item.get("mrc_with_boost_profit_pct")
    item["current_profit_abs"] = item.get("target_profit_abs")
    item["current_profit_pct"] = item.get("target_profit_pct")
    return item


def _with_attractiveness_aliases(item: dict[str, Any]) -> dict[str, Any]:
    item["non_profitable_price"] = item.get("attractiveness_overpriced_price")
    item["moderate_price"] = item.get("attractiveness_moderate_price")
    item["profitable_price"] = item.get("attractiveness_profitable_price")
    item["chosen_price"] = item.get("attractiveness_chosen_price")
    item["chosen_boost_bid_percent"] = item.get("attractiveness_chosen_boost_bid_percent")
    return item


def _rebuild_pricing_attractiveness_results_if_needed(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "pricing_attractiveness_results")
    if not cols:
        return
    legacy_cols = {
        "base_price",
        "base_profit_abs",
        "base_profit_pct",
        "attractiveness_set_price",
        "attractiveness_set_profit_abs",
        "attractiveness_set_profit_pct",
        "attractiveness_overpriced_profit_abs",
        "attractiveness_overpriced_profit_pct",
        "attractiveness_moderate_profit_abs",
        "attractiveness_moderate_profit_pct",
        "attractiveness_profitable_profit_abs",
        "attractiveness_profitable_profit_pct",
        "ozon_competitor_profit_abs",
        "ozon_competitor_profit_pct",
        "external_competitor_profit_abs",
        "external_competitor_profit_pct",
        "attractiveness_chosen_profit_abs",
        "attractiveness_chosen_profit_pct",
    }
    needs_rebuild = any(col in cols for col in legacy_cols) or "attractiveness_chosen_boost_bid_percent" not in cols
    if not needs_rebuild:
        return
    conn.execute("ALTER TABLE pricing_attractiveness_results RENAME TO pricing_attractiveness_results_old")
    conn.execute(
        """
        CREATE TABLE pricing_attractiveness_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            attractiveness_overpriced_price REAL NULL,
            attractiveness_moderate_price REAL NULL,
            attractiveness_profitable_price REAL NULL,
            ozon_competitor_price REAL NULL,
            external_competitor_price REAL NULL,
            attractiveness_chosen_price REAL NULL,
            attractiveness_chosen_boost_bid_percent REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO pricing_attractiveness_results (
            store_uid, sku,
            attractiveness_overpriced_price,
            attractiveness_moderate_price,
            attractiveness_profitable_price,
            ozon_competitor_price,
            external_competitor_price,
            attractiveness_chosen_price,
            attractiveness_chosen_boost_bid_percent,
            source_updated_at, calculated_at
        )
        SELECT store_uid, sku,
               attractiveness_overpriced_price,
               attractiveness_moderate_price,
               attractiveness_profitable_price,
               ozon_competitor_price,
               external_competitor_price,
               attractiveness_chosen_price,
               attractiveness_chosen_boost_bid_percent,
               source_updated_at, calculated_at
        FROM pricing_attractiveness_results_old
        """
    )
    conn.execute("DROP TABLE pricing_attractiveness_results_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_attractiveness_results_store ON pricing_attractiveness_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_attractiveness_results_sku ON pricing_attractiveness_results(sku)")


def _with_promo_aliases(item: dict[str, Any]) -> dict[str, Any]:
    item["selected_promo_items_json"] = item.get("promo_selected_items_json")
    item["selected_promo_items"] = item.get("promo_selected_items")
    item["selected_promo_price"] = item.get("promo_selected_price")
    item["selected_promo_boost_bid_percent"] = item.get("promo_selected_boost_bid_percent")
    item["selected_promo_profit_abs"] = item.get("promo_selected_profit_abs")
    item["selected_promo_profit_pct"] = item.get("promo_selected_profit_pct")
    return item


def _rebuild_pricing_promo_results_if_needed(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "pricing_promo_results")
    if not cols:
        return
    expected_cols = {
        "store_uid",
        "sku",
        "promo_selected_items_json",
        "promo_selected_price",
        "promo_selected_boost_bid_percent",
        "promo_selected_profit_abs",
        "promo_selected_profit_pct",
        "source_updated_at",
        "calculated_at",
    }
    legacy_cols = {
        "base_price",
        "base_profit_abs",
        "base_profit_pct",
        "promo_selected_names_json",
        "promo_selected_drr_percent",
    }
    if cols == expected_cols and not any(col in cols for col in legacy_cols):
        return
    conn.execute("ALTER TABLE pricing_promo_results RENAME TO pricing_promo_results_old")
    conn.execute(
        """
        CREATE TABLE pricing_promo_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            promo_selected_items_json TEXT NOT NULL DEFAULT '[]',
            promo_selected_price REAL NULL,
            promo_selected_boost_bid_percent REAL NULL,
            promo_selected_profit_abs REAL NULL,
            promo_selected_profit_pct REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO pricing_promo_results (
            store_uid, sku, promo_selected_items_json, promo_selected_price,
            promo_selected_boost_bid_percent, promo_selected_profit_abs, promo_selected_profit_pct,
            source_updated_at, calculated_at
        )
        SELECT store_uid, sku, promo_selected_items_json, promo_selected_price,
               promo_selected_boost_bid_percent, promo_selected_profit_abs, promo_selected_profit_pct,
               source_updated_at, calculated_at
        FROM pricing_promo_results_old
        """
    )
    conn.execute("DROP TABLE pricing_promo_results_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_results_store ON pricing_promo_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_results_sku ON pricing_promo_results(sku)")


def _rebuild_pricing_promo_offer_results_if_needed(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "pricing_promo_offer_results")
    if not cols:
        return
    legacy_cols = {
        "promo_drr_percent",
        "promo_target_price_no_ads",
        "promo_profit_abs_no_ads",
        "promo_profit_pct_no_ads",
    }
    if not any(col in cols for col in legacy_cols):
        return
    conn.execute("ALTER TABLE pricing_promo_offer_results RENAME TO pricing_promo_offer_results_old")
    conn.execute(
        """
        CREATE TABLE pricing_promo_offer_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            promo_id TEXT NOT NULL,
            promo_name TEXT NOT NULL DEFAULT '',
            promo_price REAL NULL,
            promo_profit_abs REAL NULL,
            promo_profit_pct REAL NULL,
            promo_fit_mode TEXT NOT NULL DEFAULT 'rejected',
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku, promo_id),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO pricing_promo_offer_results (
            store_uid, sku, promo_id, promo_name, promo_price, promo_profit_abs, promo_profit_pct,
            promo_fit_mode, source_updated_at, calculated_at
        )
        SELECT store_uid, sku, promo_id, promo_name, promo_price, promo_profit_abs, promo_profit_pct,
               promo_fit_mode, source_updated_at, calculated_at
        FROM pricing_promo_offer_results_old
        """
    )
    conn.execute("DROP TABLE pricing_promo_offer_results_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_results_store ON pricing_promo_offer_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_results_sku ON pricing_promo_offer_results(sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_results_promo ON pricing_promo_offer_results(store_uid, promo_id)")


def _rebuild_sales_united_order_transactions_if_needed(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "sales_united_order_transactions")
    if not cols:
        return
    pk_rows = conn.execute("PRAGMA table_info(sales_united_order_transactions)").fetchall()
    pk_cols = [str(row[1]) for row in sorted((r for r in pk_rows if int(r[5] or 0) > 0), key=lambda r: int(r[5]))]
    if pk_cols == ["store_uid", "order_id", "sku"]:
        return
    conn.execute("ALTER TABLE sales_united_order_transactions RENAME TO sales_united_order_transactions_old")
    conn.execute(
        """
        CREATE TABLE sales_united_order_transactions (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            order_id TEXT NOT NULL,
            order_created_at TEXT NOT NULL,
            order_created_date TEXT NOT NULL,
            shipment_date TEXT NOT NULL DEFAULT '',
            delivery_date TEXT NOT NULL DEFAULT '',
            sku TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            item_status TEXT NOT NULL DEFAULT '',
            source_updated_at TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, order_id, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO sales_united_order_transactions (
            store_uid, platform, order_id, order_created_at, order_created_date, shipment_date, delivery_date,
            sku, item_name, item_status, source_updated_at, payload_json, loaded_at
        )
        SELECT old.store_uid, old.platform, old.order_id, old.order_created_at, old.order_created_date,
               COALESCE(old.shipment_date, ''), COALESCE(old.delivery_date, ''),
               old.sku, COALESCE(old.item_name, ''), old.item_status, old.source_updated_at, old.payload_json, old.loaded_at
        FROM sales_united_order_transactions_old old
        JOIN (
            SELECT store_uid, order_id, sku, MAX(rowid) AS max_rowid
            FROM sales_united_order_transactions_old
            GROUP BY store_uid, order_id, sku
        ) dedup
          ON dedup.store_uid = old.store_uid
         AND dedup.order_id = old.order_id
         AND dedup.sku = old.sku
         AND dedup.max_rowid = old.rowid
        """
    )
    conn.execute("DROP TABLE sales_united_order_transactions_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_store_date ON sales_united_order_transactions(store_uid, order_created_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_store_sku ON sales_united_order_transactions(store_uid, sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_order ON sales_united_order_transactions(store_uid, order_id)")


def _drop_legacy_tables_if_exist(conn: sqlite3.Connection) -> None:
    for table_name in (
        "pricing_attractiveness_results_old",
        "pricing_promo_results_old",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")


def _rebuild_logical_views(conn: sqlite3.Connection) -> None:
    views: dict[str, str] = {
        "pricing_prices_results": "SELECT * FROM pricing_price_results",
        "pricing_promos_results": "SELECT * FROM pricing_promo_results",
        "pricing_promos_offers": "SELECT * FROM pricing_promo_offer_results",
        "pricing_promos_campaigns_raw": "SELECT * FROM pricing_promo_campaign_raw",
        "pricing_promos_offers_raw": "SELECT * FROM pricing_promo_offer_raw",
        "pricing_settings_store": "SELECT * FROM pricing_store_settings",
        "pricing_settings_categories": "SELECT * FROM pricing_category_settings",
        "pricing_settings_logistics_store": "SELECT * FROM pricing_logistics_store_settings",
        "pricing_settings_logistics_products": "SELECT * FROM pricing_logistics_product_settings",
        "pricing_settings_cogs_snapshots": "SELECT * FROM pricing_cogs_snapshots",
        "settings_monitoring_jobs": "SELECT * FROM refresh_jobs",
        "settings_monitoring_runs": "SELECT * FROM refresh_job_runs",
        "sales_overview_orders": "SELECT * FROM sales_united_order_transactions",
        "sales_overview_netting": "SELECT * FROM sales_united_netting_report_rows",
        "sales_overview_shelfs": "SELECT * FROM sales_shelfs_statistics_report_rows",
        "sales_overview_shows_boost": "SELECT * FROM sales_shows_boost_report_rows",
        "sales_overview_cogs_source": "SELECT * FROM sales_overview_cogs_source_rows",
        "sales_elasticity_orders": "SELECT * FROM sales_market_order_items",
    }
    for view_name, query in views.items():
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
        conn.execute(f"CREATE VIEW {view_name} AS {query}")


def _normalize_json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, list) else []
        except Exception:
            return []
    return []


def _default_store_settings_document() -> dict[str, Any]:
    return {
        "pricing": {},
        "logistics": {},
        "sources": {},
        "export": {},
        "sales_plan": {},
    }


def _init_system_store_tables() -> None:
    global _SYSTEM_INIT_DONE
    if _SYSTEM_INIT_DONE:
        return
    with _SYSTEM_INIT_LOCK:
        if _SYSTEM_INIT_DONE:
            return
        with _connect_system() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stores (
                    store_uid TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    store_id TEXT NOT NULL,
                    store_name TEXT NOT NULL DEFAULT '',
                    currency_code TEXT NOT NULL DEFAULT 'RUB',
                    fulfillment_model TEXT NOT NULL DEFAULT 'FBO',
                    business_id TEXT NOT NULL DEFAULT '',
                    seller_id TEXT NOT NULL DEFAULT '',
                    account_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_system_stores_platform ON stores(platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_system_stores_store_id ON stores(store_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS store_settings (
                    store_uid TEXT PRIMARY KEY,
                    pricing_json TEXT NOT NULL DEFAULT '{}',
                    logistics_json TEXT NOT NULL DEFAULT '{}',
                    sources_json TEXT NOT NULL DEFAULT '{}',
                    export_json TEXT NOT NULL DEFAULT '{}',
                    sales_plan_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_jobs (
                    job_code TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    schedule_kind TEXT NOT NULL DEFAULT 'interval',
                    interval_minutes INTEGER NULL,
                    time_of_day TEXT NULL,
                    date_from TEXT NULL,
                    date_to TEXT NULL,
                    stores_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_snapshots (
                    cache_key TEXT PRIMARY KEY,
                    snapshot_name TEXT NOT NULL,
                    scope_id TEXT NOT NULL DEFAULT '',
                    period TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    response_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_name_scope_period ON dashboard_snapshots(snapshot_name, scope_id, period)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_updated_at ON dashboard_snapshots(updated_at)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_users (
                    user_id TEXT PRIMARY KEY,
                    identifier TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL DEFAULT '',
                    password_hash TEXT NOT NULL DEFAULT '',
                    role TEXT NOT NULL DEFAULT 'viewer',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_app_users_role ON app_users(role)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_access_links (
                    link_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    label TEXT NOT NULL DEFAULT '',
                    token_hash TEXT NOT NULL UNIQUE,
                    created_by TEXT NOT NULL DEFAULT '',
                    use_count INTEGER NOT NULL DEFAULT 0,
                    last_used_at TEXT NULL,
                    expires_at TEXT NULL,
                    revoked_at TEXT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES app_users(user_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_app_access_links_user_id ON app_access_links(user_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE,
                    user_agent TEXT NOT NULL DEFAULT '',
                    ip_address TEXT NOT NULL DEFAULT '',
                    last_seen_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES app_users(user_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_app_sessions_user_id ON app_sessions(user_id)")
            if not is_postgres_backend():
                user_cols = {row[1] for row in conn.execute("PRAGMA table_info(app_users)").fetchall()}
                if "password_hash" not in user_cols:
                    conn.execute("ALTER TABLE app_users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''")
            conn.commit()
        _SYSTEM_INIT_DONE = True


def _load_system_store_settings_document(*, store_uid: str) -> dict[str, Any]:
    suid = str(store_uid or "").strip()
    if not suid:
        return _default_store_settings_document()
    _init_system_store_tables()
    with _connect_system() as conn:
        row = conn.execute(
            f"SELECT pricing_json, logistics_json, sources_json, export_json, sales_plan_json FROM store_settings WHERE store_uid = {'%s' if is_postgres_backend() and SYSTEM_DATABASE_URL else '?'}",
            (suid,),
        ).fetchone()
    if not row:
        return _default_store_settings_document()
    out = _default_store_settings_document()
    for key, column in (
        ("pricing", "pricing_json"),
        ("logistics", "logistics_json"),
        ("sources", "sources_json"),
        ("export", "export_json"),
        ("sales_plan", "sales_plan_json"),
    ):
        try:
            out[key] = json.loads(str(row[column] or "{}") or "{}")
            if not isinstance(out[key], dict):
                out[key] = {}
        except Exception:
            out[key] = {}
    return out


def _save_system_store_settings_document(*, store_uid: str, document: dict[str, Any], updated_at: str | None = None) -> None:
    suid = str(store_uid or "").strip()
    if not suid:
        return
    _init_system_store_tables()
    doc = _default_store_settings_document()
    if isinstance(document, dict):
        for section in doc:
            value = document.get(section)
            doc[section] = value if isinstance(value, dict) else {}
    ts = str(updated_at or _now_iso()).strip() or _now_iso()
    with _connect_system() as conn:
        _save_system_store_settings_document_with_conn(
            conn=conn,
            store_uid=suid,
            document=doc,
            updated_at=ts,
        )
        conn.commit()


def _save_system_store_settings_document_with_conn(
    *,
    conn,
    store_uid: str,
    document: dict[str, Any],
    updated_at: str | None = None,
) -> None:
    suid = str(store_uid or "").strip()
    if not suid:
        return
    doc = _default_store_settings_document()
    if isinstance(document, dict):
        for section in doc:
            value = document.get(section)
            doc[section] = value if isinstance(value, dict) else {}
    ts = str(updated_at or _now_iso()).strip() or _now_iso()
    conn.execute(
        f"""
        INSERT INTO store_settings (
            store_uid, pricing_json, logistics_json, sources_json, export_json, sales_plan_json, updated_at
        ) VALUES ({_system_placeholders(7)})
        ON CONFLICT(store_uid) DO UPDATE SET
            pricing_json = excluded.pricing_json,
            logistics_json = excluded.logistics_json,
            sources_json = excluded.sources_json,
            export_json = excluded.export_json,
            sales_plan_json = excluded.sales_plan_json,
            updated_at = excluded.updated_at
        """,
        (
            suid,
            json.dumps(doc["pricing"], ensure_ascii=False),
            json.dumps(doc["logistics"], ensure_ascii=False),
            json.dumps(doc["sources"], ensure_ascii=False),
            json.dumps(doc["export"], ensure_ascii=False),
            json.dumps(doc["sales_plan"], ensure_ascii=False),
            ts,
        ),
    )


def get_dashboard_snapshot(*, snapshot_name: str, cache_key: str) -> dict[str, Any] | None:
    name = str(snapshot_name or "").strip()
    key = str(cache_key or "").strip()
    if not name or not key:
        return None
    _init_system_store_tables()
    marker = "%s" if is_postgres_backend() and SYSTEM_DATABASE_URL else "?"
    with _connect_system() as conn:
        row = conn.execute(
            f"""
            SELECT snapshot_name, scope_id, period, payload_json, response_json, updated_at
            FROM dashboard_snapshots
            WHERE snapshot_name = {marker} AND cache_key = {marker}
            """,
            (name, key),
        ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(str(row["payload_json"] or "{}") or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    try:
        response = json.loads(str(row["response_json"] or "{}") or "{}")
        if not isinstance(response, dict):
            response = {}
    except Exception:
        response = {}
    return {
        "snapshot_name": str(row["snapshot_name"] or "").strip(),
        "scope_id": str(row["scope_id"] or "").strip(),
        "period": str(row["period"] or "").strip(),
        "payload": payload,
        "response": response,
        "updated_at": str(row["updated_at"] or "").strip(),
    }


def upsert_dashboard_snapshot(
    *,
    snapshot_name: str,
    cache_key: str,
    scope_id: str = "",
    period: str = "",
    payload: dict[str, Any] | None = None,
    response: dict[str, Any] | None = None,
    updated_at: str | None = None,
) -> None:
    name = str(snapshot_name or "").strip()
    key = str(cache_key or "").strip()
    if not name or not key:
        return
    _init_system_store_tables()
    ts = str(updated_at or _now_iso()).strip() or _now_iso()
    conn_payload = payload if isinstance(payload, dict) else {}
    conn_response = response if isinstance(response, dict) else {}
    with _connect_system() as conn:
        conn.execute(
            f"""
            INSERT INTO dashboard_snapshots (
                cache_key, snapshot_name, scope_id, period, payload_json, response_json, updated_at
            ) VALUES ({_system_placeholders(7)})
            ON CONFLICT(cache_key) DO UPDATE SET
                snapshot_name = excluded.snapshot_name,
                scope_id = excluded.scope_id,
                period = excluded.period,
                payload_json = excluded.payload_json,
                response_json = excluded.response_json,
                updated_at = excluded.updated_at
            """,
            (
                key,
                name,
                str(scope_id or "").strip(),
                str(period or "").strip(),
                json.dumps(conn_payload, ensure_ascii=False, default=str),
                json.dumps(conn_response, ensure_ascii=False, default=str),
                ts,
            ),
        )
        conn.commit()


def delete_dashboard_snapshots(*, snapshot_name: str) -> None:
    name = str(snapshot_name or "").strip()
    if not name:
        return
    _init_system_store_tables()
    marker = "%s" if is_postgres_backend() and SYSTEM_DATABASE_URL else "?"
    with _connect_system() as conn:
        conn.execute(
            f"DELETE FROM dashboard_snapshots WHERE snapshot_name = {marker}",
            (name,),
        )
        conn.commit()


def _merge_system_store_settings_sections(*, store_uid: str, sections: dict[str, dict[str, Any]], updated_at: str | None = None) -> None:
    document = _load_system_store_settings_document(store_uid=store_uid)
    for section_name, payload in sections.items():
        if section_name not in document or not isinstance(payload, dict):
            continue
        document[section_name].update(payload)
    _save_system_store_settings_document(store_uid=store_uid, document=document, updated_at=updated_at)


def _backfill_system_settings_from_legacy() -> None:
    with _connect() as conn:
        existing = _table_names(conn)
        if "stores" not in existing:
            return
        pricing_join = ""
        logistics_join = ""
        pricing_select = """
                NULL AS earning_mode,
                NULL AS earning_unit,
                NULL AS strategy_mode,
                NULL AS planned_revenue,
                NULL AS target_profit_rub,
                NULL AS target_profit_percent,
                NULL AS minimum_profit_percent,
                NULL AS target_margin_rub,
                NULL AS target_margin_percent,
                NULL AS target_drr_percent,
                NULL AS cogs_source_type,
                NULL AS cogs_source_id,
                NULL AS cogs_source_name,
                NULL AS cogs_sku_column,
                NULL AS cogs_value_column,
                NULL AS stock_source_type,
                NULL AS stock_source_id,
                NULL AS stock_source_name,
                NULL AS stock_sku_column,
                NULL AS stock_value_column,
                NULL AS overview_cogs_source_type,
                NULL AS overview_cogs_source_id,
                NULL AS overview_cogs_source_name,
                NULL AS overview_cogs_order_column,
                NULL AS overview_cogs_sku_column,
                NULL AS overview_cogs_value_column,
                NULL AS export_prices_source_type,
                NULL AS export_prices_source_id,
                NULL AS export_prices_source_name,
                NULL AS export_prices_sku_column,
                NULL AS export_prices_value_column,
                NULL AS export_ads_source_type,
                NULL AS export_ads_source_id,
                NULL AS export_ads_source_name,
                NULL AS export_ads_sku_column,
                NULL AS export_ads_value_column,
        """
        logistics_select = """
                NULL AS handling_mode,
                NULL AS handling_fixed_amount,
                NULL AS handling_percent,
                NULL AS handling_min_amount,
                NULL AS handling_max_amount,
                NULL AS delivery_cost_per_kg,
                NULL AS return_processing_cost,
                NULL AS disposal_cost
        """
        if "pricing_store_settings" in existing:
            pricing_join = "LEFT JOIN pricing_store_settings ps ON ps.store_uid = s.store_uid"
            pricing_select = """
                ps.earning_mode,
                ps.earning_unit,
                ps.strategy_mode,
                ps.planned_revenue,
                ps.target_profit_rub,
                ps.target_profit_percent,
                ps.minimum_profit_percent,
                ps.target_margin_rub,
                ps.target_margin_percent,
                ps.target_drr_percent,
                ps.cogs_source_type,
                ps.cogs_source_id,
                ps.cogs_source_name,
                ps.cogs_sku_column,
                ps.cogs_value_column,
                ps.stock_source_type,
                ps.stock_source_id,
                ps.stock_source_name,
                ps.stock_sku_column,
                ps.stock_value_column,
                ps.overview_cogs_source_type,
                ps.overview_cogs_source_id,
                ps.overview_cogs_source_name,
                ps.overview_cogs_order_column,
                ps.overview_cogs_sku_column,
                ps.overview_cogs_value_column,
                ps.export_prices_source_type,
                ps.export_prices_source_id,
                ps.export_prices_source_name,
                ps.export_prices_sku_column,
                ps.export_prices_value_column,
                ps.export_ads_source_type,
                ps.export_ads_source_id,
                ps.export_ads_source_name,
                ps.export_ads_sku_column,
                ps.export_ads_value_column,
            """
        if "pricing_logistics_store_settings" in existing:
            logistics_join = "LEFT JOIN pricing_logistics_store_settings ls ON ls.store_uid = s.store_uid"
            logistics_select = """
                ls.handling_mode,
                ls.handling_fixed_amount,
                ls.handling_percent,
                ls.handling_min_amount,
                ls.handling_max_amount,
                ls.delivery_cost_per_kg,
                ls.return_processing_cost,
                ls.disposal_cost
            """
        store_rows = conn.execute(
            f"""
            SELECT
                s.store_uid,
                s.platform,
                s.store_id,
                s.store_name,
                s.currency_code,
                s.fulfillment_model,
                s.business_id,
                s.seller_id,
                s.account_id,
                s.created_at,
                s.updated_at,
                {pricing_select}
                {logistics_select}
            FROM stores s
            {pricing_join}
            {logistics_join}
            """
        ).fetchall()
        job_rows: list[Any] = []
        if "refresh_jobs" in existing:
            job_rows = conn.execute(
                """
                SELECT job_code, title, enabled, schedule_kind, interval_minutes, time_of_day, date_from, date_to, stores_json, updated_at
                FROM refresh_jobs
                """
            ).fetchall()
    with _connect_system() as conn:
        for row in store_rows:
            item = dict(row)
            store_uid = str(item.get("store_uid") or "").strip()
            if not store_uid:
                continue
            conn.execute(
                f"""
                INSERT INTO stores (
                    store_uid, platform, store_id, store_name, currency_code, fulfillment_model, business_id, seller_id, account_id, created_at, updated_at
                ) VALUES ({_system_placeholders(11)})
                ON CONFLICT(store_uid) DO UPDATE SET
                    store_name = excluded.store_name,
                    currency_code = excluded.currency_code,
                    fulfillment_model = excluded.fulfillment_model,
                    business_id = excluded.business_id,
                    seller_id = excluded.seller_id,
                    account_id = excluded.account_id,
                    updated_at = excluded.updated_at
                """,
                (
                    store_uid,
                    str(item.get("platform") or "").strip(),
                    str(item.get("store_id") or "").strip(),
                    str(item.get("store_name") or "").strip(),
                    str(item.get("currency_code") or "RUB").strip().upper() or "RUB",
                    str(item.get("fulfillment_model") or "FBO").strip().upper() or "FBO",
                    str(item.get("business_id") or "").strip(),
                    str(item.get("seller_id") or "").strip(),
                    str(item.get("account_id") or "").strip(),
                    str(item.get("created_at") or _now_iso()).strip(),
                    str(item.get("updated_at") or _now_iso()).strip(),
                ),
            )
            _save_system_store_settings_document_with_conn(
                conn=conn,
                store_uid=store_uid,
                updated_at=str(item.get("updated_at") or _now_iso()).strip(),
                document={
                    "pricing": {
                        "earning_mode": item.get("earning_mode"),
                        "earning_unit": item.get("earning_unit"),
                        "strategy_mode": item.get("strategy_mode"),
                        "planned_revenue": item.get("planned_revenue"),
                        "target_profit_rub": item.get("target_profit_rub"),
                        "target_profit_percent": item.get("target_profit_percent"),
                        "minimum_profit_percent": item.get("minimum_profit_percent"),
                        "target_margin_rub": item.get("target_margin_rub"),
                        "target_margin_percent": item.get("target_margin_percent"),
                        "target_drr_percent": item.get("target_drr_percent"),
                    },
                    "logistics": {
                        "fulfillment_model": item.get("fulfillment_model"),
                        "handling_mode": item.get("handling_mode"),
                        "handling_fixed_amount": item.get("handling_fixed_amount"),
                        "handling_percent": item.get("handling_percent"),
                        "handling_min_amount": item.get("handling_min_amount"),
                        "handling_max_amount": item.get("handling_max_amount"),
                        "delivery_cost_per_kg": item.get("delivery_cost_per_kg"),
                        "return_processing_cost": item.get("return_processing_cost"),
                        "disposal_cost": item.get("disposal_cost"),
                    },
                    "sources": {
                        "cogs_source_type": item.get("cogs_source_type"),
                        "cogs_source_id": item.get("cogs_source_id"),
                        "cogs_source_name": item.get("cogs_source_name"),
                        "cogs_sku_column": item.get("cogs_sku_column"),
                        "cogs_value_column": item.get("cogs_value_column"),
                        "stock_source_type": item.get("stock_source_type"),
                        "stock_source_id": item.get("stock_source_id"),
                        "stock_source_name": item.get("stock_source_name"),
                        "stock_sku_column": item.get("stock_sku_column"),
                        "stock_value_column": item.get("stock_value_column"),
                        "overview_cogs_source_type": item.get("overview_cogs_source_type"),
                        "overview_cogs_source_id": item.get("overview_cogs_source_id"),
                        "overview_cogs_source_name": item.get("overview_cogs_source_name"),
                        "overview_cogs_order_column": item.get("overview_cogs_order_column"),
                        "overview_cogs_sku_column": item.get("overview_cogs_sku_column"),
                        "overview_cogs_value_column": item.get("overview_cogs_value_column"),
                    },
                    "export": {
                        "export_prices_source_type": item.get("export_prices_source_type"),
                        "export_prices_source_id": item.get("export_prices_source_id"),
                        "export_prices_source_name": item.get("export_prices_source_name"),
                        "export_prices_sku_column": item.get("export_prices_sku_column"),
                        "export_prices_value_column": item.get("export_prices_value_column"),
                        "export_ads_source_type": item.get("export_ads_source_type"),
                        "export_ads_source_id": item.get("export_ads_source_id"),
                        "export_ads_source_name": item.get("export_ads_source_name"),
                        "export_ads_sku_column": item.get("export_ads_sku_column"),
                        "export_ads_value_column": item.get("export_ads_value_column"),
                    },
                    "sales_plan": {
                        "planned_revenue": item.get("planned_revenue"),
                        "target_profit_rub": item.get("target_profit_rub"),
                        "target_profit_percent": item.get("target_profit_percent"),
                        "minimum_profit_percent": item.get("minimum_profit_percent"),
                        "target_margin_rub": item.get("target_margin_rub"),
                        "target_margin_percent": item.get("target_margin_percent"),
                        "target_drr_percent": item.get("target_drr_percent"),
                        "strategy_mode": item.get("strategy_mode"),
                    },
                },
            )
        for row in job_rows:
            item = dict(row)
            conn.execute(
                f"""
                INSERT INTO refresh_jobs (
                    job_code, title, enabled, schedule_kind, interval_minutes, time_of_day, date_from, date_to, stores_json, updated_at
                ) VALUES ({_system_placeholders(10)})
                ON CONFLICT(job_code) DO UPDATE SET
                    title = excluded.title,
                    enabled = excluded.enabled,
                    schedule_kind = excluded.schedule_kind,
                    interval_minutes = excluded.interval_minutes,
                    time_of_day = excluded.time_of_day,
                    date_from = excluded.date_from,
                    date_to = excluded.date_to,
                    stores_json = excluded.stores_json,
                    updated_at = excluded.updated_at
                """,
                (
                    str(item.get("job_code") or "").strip(),
                    str(item.get("title") or "").strip(),
                    int(item.get("enabled") or 0),
                    str(item.get("schedule_kind") or "interval").strip() or "interval",
                    item.get("interval_minutes"),
                    str(item.get("time_of_day") or "").strip() or None,
                    str(item.get("date_from") or "").strip() or None,
                    str(item.get("date_to") or "").strip() or None,
                    str(item.get("stores_json") or "[]").strip() or "[]",
                    str(item.get("updated_at") or _now_iso()).strip(),
                ),
            )
        conn.commit()


def _init_store_data_model_postgres() -> None:
    global _INIT_DONE, _MAINTENANCE_DONE
    if _INIT_DONE:
        return
    with _INIT_LOCK:
        if _INIT_DONE:
            return
        with _connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stores (
                    store_uid TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    store_id TEXT NOT NULL,
                    store_name TEXT NOT NULL DEFAULT '',
                    currency_code TEXT NOT NULL DEFAULT 'RUB',
                    fulfillment_model TEXT NOT NULL DEFAULT 'FBO',
                    business_id TEXT NOT NULL DEFAULT '',
                    seller_id TEXT NOT NULL DEFAULT '',
                    account_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_stores_platform ON stores(platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_stores_store_id ON stores(store_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS store_datasets (
                    dataset_key TEXT PRIMARY KEY,
                    store_uid TEXT NOT NULL,
                    task_code TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'ready',
                    row_count INTEGER NOT NULL DEFAULT 0,
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_store_datasets_store_uid ON store_datasets(store_uid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_store_datasets_task_code ON store_datasets(task_code)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_store_settings (
                    store_uid TEXT PRIMARY KEY,
                    earning_mode TEXT NOT NULL DEFAULT 'margin',
                    earning_unit TEXT NOT NULL DEFAULT 'percent',
                    strategy_mode TEXT NOT NULL DEFAULT 'mix',
                    planned_revenue DOUBLE PRECISION NULL,
                    target_profit_rub DOUBLE PRECISION NULL,
                    target_profit_percent DOUBLE PRECISION NULL,
                    minimum_profit_percent DOUBLE PRECISION NULL,
                    target_margin_rub DOUBLE PRECISION NULL,
                    target_margin_percent DOUBLE PRECISION NULL,
                    target_drr_percent DOUBLE PRECISION NULL,
                    cogs_source_type TEXT NULL,
                    cogs_source_id TEXT NULL,
                    cogs_source_name TEXT NULL,
                    cogs_sku_column TEXT NULL,
                    cogs_value_column TEXT NULL,
                    stock_source_type TEXT NULL,
                    stock_source_id TEXT NULL,
                    stock_source_name TEXT NULL,
                    stock_sku_column TEXT NULL,
                    stock_value_column TEXT NULL,
                    overview_cogs_source_type TEXT NULL,
                    overview_cogs_source_id TEXT NULL,
                    overview_cogs_source_name TEXT NULL,
                    overview_cogs_order_column TEXT NULL,
                    overview_cogs_sku_column TEXT NULL,
                    overview_cogs_value_column TEXT NULL,
                    export_prices_source_type TEXT NULL,
                    export_prices_source_id TEXT NULL,
                    export_prices_source_name TEXT NULL,
                    export_prices_sku_column TEXT NULL,
                    export_prices_value_column TEXT NULL,
                    export_ads_source_type TEXT NULL,
                    export_ads_source_id TEXT NULL,
                    export_ads_source_name TEXT NULL,
                    export_ads_sku_column TEXT NULL,
                    export_ads_value_column TEXT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_jobs (
                    job_code TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    schedule_kind TEXT NOT NULL DEFAULT 'interval',
                    interval_minutes INTEGER NULL,
                    time_of_day TEXT NULL,
                    date_from TEXT NULL,
                    date_to TEXT NULL,
                    stores_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_job_runs (
                    run_id BIGSERIAL PRIMARY KEY,
                    job_code TEXT NOT NULL,
                    trigger_source TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL,
                    finished_at TEXT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    message TEXT NOT NULL DEFAULT '',
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY (job_code) REFERENCES refresh_jobs(job_code) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE SEQUENCE IF NOT EXISTS refresh_job_runs_run_id_seq")
            conn.execute(
                """
                ALTER TABLE refresh_job_runs
                ALTER COLUMN run_id SET DEFAULT nextval('refresh_job_runs_run_id_seq')
                """
            )
            conn.execute(
                """
                SELECT setval(
                    'refresh_job_runs_run_id_seq',
                    GREATEST(COALESCE((SELECT MAX(run_id) FROM refresh_job_runs), 0), 1),
                    COALESCE((SELECT MAX(run_id) FROM refresh_job_runs), 0) > 0
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_refresh_job_runs_job_code ON refresh_job_runs(job_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_refresh_job_runs_status ON refresh_job_runs(status)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_category_tree (
                    id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
                    dataset_key TEXT NOT NULL,
                    store_uid TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT '',
                    subcategory_1 TEXT NOT NULL DEFAULT '',
                    subcategory_2 TEXT NOT NULL DEFAULT '',
                    subcategory_3 TEXT NOT NULL DEFAULT '',
                    subcategory_4 TEXT NOT NULL DEFAULT '',
                    subcategory_5 TEXT NOT NULL DEFAULT '',
                    leaf_path TEXT NOT NULL DEFAULT '',
                    items_count BIGINT NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    UNIQUE (
                        dataset_key,
                        category,
                        subcategory_1,
                        subcategory_2,
                        subcategory_3,
                        subcategory_4,
                        subcategory_5
                    )
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_category_tree_dataset ON pricing_category_tree(dataset_key)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_category_tree_store_uid ON pricing_category_tree(store_uid)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_catalog_sku_paths (
                    priority_platform TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    anchor_store_uid TEXT NOT NULL DEFAULT '',
                    source_store_uid TEXT NOT NULL DEFAULT '',
                    resolved_category TEXT NOT NULL DEFAULT '',
                    resolved_subcategory_1 TEXT NOT NULL DEFAULT '',
                    resolved_subcategory_2 TEXT NOT NULL DEFAULT '',
                    resolved_subcategory_3 TEXT NOT NULL DEFAULT '',
                    resolved_subcategory_4 TEXT NOT NULL DEFAULT '',
                    resolved_subcategory_5 TEXT NOT NULL DEFAULT '',
                    leaf_path TEXT NOT NULL DEFAULT '',
                    resolution_kind TEXT NOT NULL DEFAULT 'undefined',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (priority_platform, sku)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_catalog_sku_paths_platform ON pricing_catalog_sku_paths(priority_platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_catalog_sku_paths_leaf ON pricing_catalog_sku_paths(priority_platform, leaf_path)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_category_settings (
                    id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
                    dataset_key TEXT NOT NULL,
                    store_uid TEXT NOT NULL,
                    leaf_path TEXT NOT NULL,
                    commission_percent DOUBLE PRECISION NULL,
                    acquiring_percent DOUBLE PRECISION NULL,
                    logistics_rub DOUBLE PRECISION NULL,
                    ads_percent DOUBLE PRECISION NULL,
                    returns_percent DOUBLE PRECISION NULL,
                    tax_percent DOUBLE PRECISION NULL,
                    other_expenses_rub DOUBLE PRECISION NULL,
                    other_expenses_percent DOUBLE PRECISION NULL,
                    cogs_rub DOUBLE PRECISION NULL,
                    target_profit_rub DOUBLE PRECISION NULL,
                    target_profit_percent DOUBLE PRECISION NULL,
                    target_margin_rub DOUBLE PRECISION NULL,
                    target_margin_percent DOUBLE PRECISION NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            id_meta = conn.execute(
                """
                SELECT is_identity
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'pricing_category_settings'
                  AND column_name = 'id'
                """
            ).fetchone()
            if str((id_meta or {}).get("is_identity", "")).upper() != "YES":
                conn.execute("CREATE SEQUENCE IF NOT EXISTS pricing_category_settings_id_seq")
                conn.execute(
                    """
                    ALTER TABLE pricing_category_settings
                    ALTER COLUMN id SET DEFAULT nextval('pricing_category_settings_id_seq')
                    """
                )
                conn.execute(
                    """
                    SELECT setval(
                        'pricing_category_settings_id_seq',
                        GREATEST(COALESCE((SELECT MAX(id) FROM pricing_category_settings), 0), 1),
                        COALESCE((SELECT MAX(id) FROM pricing_category_settings), 0) > 0
                    )
                    """
                )
            pcs_cols = {row["column_name"] for row in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'pricing_category_settings'
                """
            ).fetchall()}
            if "tax_percent" not in pcs_cols:
                conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN tax_percent DOUBLE PRECISION NULL")
            if "other_expenses_rub" not in pcs_cols:
                conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN other_expenses_rub DOUBLE PRECISION NULL")
            if "other_expenses_percent" not in pcs_cols:
                conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN other_expenses_percent DOUBLE PRECISION NULL")
            if "target_profit_percent" not in pcs_cols:
                conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN target_profit_percent DOUBLE PRECISION NULL")
            if "target_margin_rub" not in pcs_cols:
                conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN target_margin_rub DOUBLE PRECISION NULL")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_category_settings_dataset ON pricing_category_settings(dataset_key)")
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_pricing_category_settings_dataset_leaf
                ON pricing_category_settings(dataset_key, leaf_path)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sales_overview_order_rows_hot (
                    store_uid TEXT NOT NULL,
                    platform TEXT NOT NULL DEFAULT 'yandex_market',
                    order_created_date TEXT NOT NULL,
                    order_created_at TEXT NOT NULL DEFAULT '',
                    shipment_date TEXT NOT NULL DEFAULT '',
                    delivery_date TEXT NOT NULL DEFAULT '',
                    order_id TEXT NOT NULL,
                    item_status TEXT NOT NULL DEFAULT '',
                    sku TEXT NOT NULL,
                    item_name TEXT NOT NULL DEFAULT '',
                    sale_price DOUBLE PRECISION NULL,
                    gross_profit DOUBLE PRECISION NULL,
                    cogs_price DOUBLE PRECISION NULL,
                    commission DOUBLE PRECISION NULL,
                    acquiring DOUBLE PRECISION NULL,
                    delivery DOUBLE PRECISION NULL,
                    ads DOUBLE PRECISION NULL,
                    tax DOUBLE PRECISION NULL,
                    profit DOUBLE PRECISION NULL,
                    sale_price_with_coinvest DOUBLE PRECISION NULL,
                    strategy_cycle_started_at TEXT NOT NULL DEFAULT '',
                    strategy_market_boost_bid_percent DOUBLE PRECISION NULL,
                    strategy_boost_share DOUBLE PRECISION NULL,
                    strategy_boost_bid_percent DOUBLE PRECISION NULL,
                    strategy_snapshot_at TEXT NOT NULL DEFAULT '',
                    strategy_installed_price DOUBLE PRECISION NULL,
                    strategy_decision_code TEXT NOT NULL DEFAULT '',
                    strategy_decision_label TEXT NOT NULL DEFAULT '',
                    strategy_control_state TEXT NOT NULL DEFAULT '',
                    strategy_attractiveness_status TEXT NOT NULL DEFAULT '',
                    strategy_promo_count INTEGER NOT NULL DEFAULT 0,
                    strategy_coinvest_pct DOUBLE PRECISION NULL,
                    strategy_selected_iteration_code TEXT NOT NULL DEFAULT '',
                    strategy_uses_promo INTEGER NOT NULL DEFAULT 0,
                    strategy_market_promo_status TEXT NOT NULL DEFAULT '',
                    uses_planned_costs INTEGER NOT NULL DEFAULT 0,
                    source_updated_at TEXT NOT NULL DEFAULT '',
                    calculated_at TEXT NOT NULL,
                    PRIMARY KEY (store_uid, order_id, sku)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_hot_store_date "
                "ON sales_overview_order_rows_hot(store_uid, order_created_date)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_hot_store_status "
                "ON sales_overview_order_rows_hot(store_uid, item_status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_hot_store_sku "
                "ON sales_overview_order_rows_hot(store_uid, sku)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_attractiveness_recommendations_raw (
                    store_uid TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    attractiveness_overpriced_price DOUBLE PRECISION NULL,
                    attractiveness_moderate_price DOUBLE PRECISION NULL,
                    attractiveness_profitable_price DOUBLE PRECISION NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    source_updated_at TEXT NULL,
                    loaded_at TEXT NOT NULL,
                    PRIMARY KEY (store_uid, sku),
                    FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pricing_attr_recommendations_raw_store "
                "ON pricing_attractiveness_recommendations_raw(store_uid)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pricing_attr_recommendations_raw_sku "
                "ON pricing_attractiveness_recommendations_raw(sku)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_autopilot_snapshots (
                    snapshot_id BIGSERIAL PRIMARY KEY,
                    snapshot_at TEXT NOT NULL,
                    time_bucket_start TEXT NOT NULL,
                    time_bucket_end TEXT NOT NULL,
                    store_uid TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE (time_bucket_start, store_uid, sku)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_snapshots_bucket ON pricing_autopilot_snapshots(time_bucket_start)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_snapshots_store_sku ON pricing_autopilot_snapshots(store_uid, sku)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_autopilot_decisions (
                    decision_id BIGSERIAL PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    review_after TEXT NOT NULL,
                    reviewed_at TEXT NULL,
                    store_uid TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    decision_status TEXT NOT NULL DEFAULT 'pending',
                    decision_mode TEXT NOT NULL DEFAULT 'simulate',
                    action_code TEXT NOT NULL DEFAULT '',
                    action_unit TEXT NOT NULL DEFAULT '',
                    action_value DOUBLE PRECISION NULL,
                    previous_value DOUBLE PRECISION NULL,
                    proposed_value DOUBLE PRECISION NULL,
                    baseline_snapshot_id BIGINT NULL,
                    review_snapshot_id BIGINT NULL,
                    reason_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_decisions_status ON pricing_autopilot_decisions(decision_status, review_after)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_decisions_store_sku ON pricing_autopilot_decisions(store_uid, sku, created_at)")
            rebuild_db_explorer_views(conn)
            conn.commit()
            _MAINTENANCE_DONE = True
            _INIT_DONE = True


def init_store_data_model() -> None:
    global _INIT_DONE, _MAINTENANCE_DONE
    if _INIT_DONE:
        return
    _init_system_store_tables()
    if is_postgres_backend():
        _init_store_data_model_postgres()
        _backfill_system_settings_from_legacy()
        return
    with _INIT_LOCK:
        if _INIT_DONE:
            return
        _init_history_tables()
        if not _MAINTENANCE_DONE:
            _run_retention_cleanup()
            _MAINTENANCE_DONE = True
        conn = _connect()
        _init_store_data_model_sqlite_reference_tables(conn)
        _init_store_data_model_sqlite_pricing_core_tables(conn)

        _init_store_data_model_sqlite_strategy_tables(conn)

        _init_store_data_model_sqlite_attractiveness_promo_tables(conn)

        _init_store_data_model_sqlite_sales_tables(conn)

        _init_store_data_model_sqlite_cogs_tables(conn)
        _init_store_data_model_sqlite_operational_tables(conn)
        conn.commit()
        conn.close()
        _backfill_system_settings_from_legacy()
        _INIT_DONE = True


def _init_store_data_model_sqlite_cogs_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_overview_cogs_source_rows (
            store_uid TEXT NOT NULL,
            order_key TEXT NOT NULL,
            sku_key TEXT NOT NULL DEFAULT '',
            cogs_value REAL NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, order_key, sku_key),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_overview_cogs_source_rows_store "
        "ON sales_overview_cogs_source_rows(store_uid)"
    )
    _rebuild_sales_overview_cogs_source_rows_if_needed(conn)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_cogs_snapshots (
            snapshot_at TEXT NOT NULL,
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            cogs_value REAL NULL,
            source_id TEXT NOT NULL DEFAULT '',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (snapshot_at, store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_cogs_snapshots_store_time "
        "ON pricing_cogs_snapshots(store_uid, snapshot_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_cogs_snapshots_sku "
        "ON pricing_cogs_snapshots(store_uid, sku)"
    )


def _init_store_data_model_sqlite_pricing_core_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_price_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            cogs_price REAL NULL,
            rrc_no_ads_price REAL NULL,
            rrc_no_ads_profit_abs REAL NULL,
            rrc_no_ads_profit_pct REAL NULL,
            mrc_price REAL NULL,
            mrc_profit_abs REAL NULL,
            mrc_profit_pct REAL NULL,
            mrc_with_boost_price REAL NULL,
            mrc_with_boost_profit_abs REAL NULL,
            mrc_with_boost_profit_pct REAL NULL,
            target_price REAL NULL,
            target_profit_abs REAL NULL,
            target_profit_pct REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    _rename_column_if_exists(conn, "pricing_price_results", "cogs", "cogs_price")
    _rebuild_pricing_price_results_if_needed(conn)
    price_cols = _table_columns(conn, "pricing_price_results")
    if "rrc_no_ads_price" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN rrc_no_ads_price REAL NULL")
    if "rrc_no_ads_profit_abs" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN rrc_no_ads_profit_abs REAL NULL")
    if "rrc_no_ads_profit_pct" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN rrc_no_ads_profit_pct REAL NULL")
    if "mrc_price" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN mrc_price REAL NULL")
    if "mrc_profit_abs" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN mrc_profit_abs REAL NULL")
    if "mrc_profit_pct" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN mrc_profit_pct REAL NULL")
    if "mrc_with_boost_price" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN mrc_with_boost_price REAL NULL")
    if "mrc_with_boost_profit_abs" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN mrc_with_boost_profit_abs REAL NULL")
    if "mrc_with_boost_profit_pct" not in price_cols:
        conn.execute("ALTER TABLE pricing_price_results ADD COLUMN mrc_with_boost_profit_pct REAL NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_price_results_store ON pricing_price_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_price_results_sku ON pricing_price_results(sku)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_boost_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            recommended_bid REAL NULL,
            bid_30 REAL NULL,
            bid_60 REAL NULL,
            bid_80 REAL NULL,
            bid_95 REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    boost_cols = _table_columns(conn, "pricing_boost_results")
    if "bid_30" not in boost_cols:
        conn.execute("ALTER TABLE pricing_boost_results ADD COLUMN bid_30 REAL NULL")
    if "bid_60" not in boost_cols:
        conn.execute("ALTER TABLE pricing_boost_results ADD COLUMN bid_60 REAL NULL")
    if "bid_80" not in boost_cols:
        conn.execute("ALTER TABLE pricing_boost_results ADD COLUMN bid_80 REAL NULL")
    if "bid_95" not in boost_cols:
        conn.execute("ALTER TABLE pricing_boost_results ADD COLUMN bid_95 REAL NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_boost_results_store ON pricing_boost_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_boost_results_sku ON pricing_boost_results(sku)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_market_price_export_history (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            campaign_id TEXT NOT NULL DEFAULT '',
            price REAL NULL,
            source TEXT NOT NULL DEFAULT 'strategy',
            PRIMARY KEY (store_uid, sku, requested_at),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_market_price_export_history_store_sku_time "
        "ON pricing_market_price_export_history(store_uid, sku, requested_at)"
    )


def _init_store_data_model_sqlite_strategy_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_strategy_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            strategy_code TEXT NOT NULL DEFAULT 'base',
            strategy_label TEXT NOT NULL DEFAULT 'Базовая',
            rrc_price REAL NULL,
            rrc_profit_abs REAL NULL,
            rrc_profit_pct REAL NULL,
            mrc_price REAL NULL,
            mrc_profit_abs REAL NULL,
            mrc_profit_pct REAL NULL,
            mrc_with_boost_price REAL NULL,
            mrc_with_boost_profit_abs REAL NULL,
            mrc_with_boost_profit_pct REAL NULL,
            promo_price REAL NULL,
            promo_profit_abs REAL NULL,
            promo_profit_pct REAL NULL,
            attractiveness_price REAL NULL,
            attractiveness_profit_abs REAL NULL,
            attractiveness_profit_pct REAL NULL,
            installed_price REAL NULL,
            installed_profit_abs REAL NULL,
            installed_profit_pct REAL NULL,
            boost_bid_percent REAL NULL,
            decision_code TEXT NOT NULL DEFAULT 'observe',
            decision_label TEXT NOT NULL DEFAULT 'Наблюдать',
            decision_tone TEXT NOT NULL DEFAULT 'warning',
            hypothesis TEXT NOT NULL DEFAULT '',
            hypothesis_started_at TEXT NOT NULL DEFAULT '',
            hypothesis_expires_at TEXT NOT NULL DEFAULT '',
            attractiveness_status TEXT NOT NULL DEFAULT '',
            promo_items_json TEXT NOT NULL DEFAULT '[]',
            uses_promo INTEGER NOT NULL DEFAULT 0,
            uses_attractiveness INTEGER NOT NULL DEFAULT 0,
            uses_boost INTEGER NOT NULL DEFAULT 0,
            selected_iteration_code TEXT NOT NULL DEFAULT '',
            scenario_matrix_json TEXT NOT NULL DEFAULT '{}',
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    strategy_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_strategy_results)").fetchall()}
    if "mrc_price" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN mrc_price REAL NULL")
    if "mrc_profit_abs" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN mrc_profit_abs REAL NULL")
    if "mrc_profit_pct" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN mrc_profit_pct REAL NULL")
    if "mrc_with_boost_price" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN mrc_with_boost_price REAL NULL")
    if "mrc_with_boost_profit_abs" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN mrc_with_boost_profit_abs REAL NULL")
    if "mrc_with_boost_profit_pct" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN mrc_with_boost_profit_pct REAL NULL")
    if "promo_price" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN promo_price REAL NULL")
    if "promo_profit_abs" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN promo_profit_abs REAL NULL")
    if "promo_profit_pct" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN promo_profit_pct REAL NULL")
    if "attractiveness_price" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN attractiveness_price REAL NULL")
    if "attractiveness_profit_abs" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN attractiveness_profit_abs REAL NULL")
    if "attractiveness_profit_pct" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN attractiveness_profit_pct REAL NULL")
    if "decision_code" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN decision_code TEXT NOT NULL DEFAULT 'observe'")
    if "decision_label" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN decision_label TEXT NOT NULL DEFAULT 'Наблюдать'")
    if "decision_tone" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN decision_tone TEXT NOT NULL DEFAULT 'warning'")
    if "hypothesis" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN hypothesis TEXT NOT NULL DEFAULT ''")
    if "hypothesis_started_at" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN hypothesis_started_at TEXT NOT NULL DEFAULT ''")
    if "hypothesis_expires_at" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN hypothesis_expires_at TEXT NOT NULL DEFAULT ''")
    if "control_state" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN control_state TEXT NOT NULL DEFAULT 'stable'")
    if "control_state_started_at" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN control_state_started_at TEXT NOT NULL DEFAULT ''")
    if "market_promo_status" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN market_promo_status TEXT NOT NULL DEFAULT ''")
    if "market_promo_checked_at" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN market_promo_checked_at TEXT NOT NULL DEFAULT ''")
    if "market_promo_message" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN market_promo_message TEXT NOT NULL DEFAULT ''")
    if "selected_iteration_code" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN selected_iteration_code TEXT NOT NULL DEFAULT ''")
    if "scenario_matrix_json" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN scenario_matrix_json TEXT NOT NULL DEFAULT '{}'")
    if "cycle_started_at" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN cycle_started_at TEXT NOT NULL DEFAULT ''")
    if "market_boost_bid_percent" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN market_boost_bid_percent REAL NULL")
    if "boost_share" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN boost_share REAL NULL")
    if "promo_count" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN promo_count INTEGER NOT NULL DEFAULT 0")
    if "coinvest_pct" not in strategy_cols:
        conn.execute("ALTER TABLE pricing_strategy_results ADD COLUMN coinvest_pct REAL NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_strategy_results_store ON pricing_strategy_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_strategy_results_sku ON pricing_strategy_results(sku)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_strategy_history (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            cycle_started_at TEXT NOT NULL DEFAULT '',
            strategy_code TEXT NOT NULL DEFAULT 'base',
            strategy_label TEXT NOT NULL DEFAULT 'Базовая',
            installed_price REAL NULL,
            installed_profit_abs REAL NULL,
            installed_profit_pct REAL NULL,
            boost_bid_percent REAL NULL,
            market_boost_bid_percent REAL NULL,
            boost_share REAL NULL,
            decision_code TEXT NOT NULL DEFAULT 'observe',
            decision_label TEXT NOT NULL DEFAULT 'Наблюдать',
            decision_tone TEXT NOT NULL DEFAULT 'warning',
            hypothesis TEXT NOT NULL DEFAULT '',
            hypothesis_started_at TEXT NOT NULL DEFAULT '',
            hypothesis_expires_at TEXT NOT NULL DEFAULT '',
            control_state TEXT NOT NULL DEFAULT 'stable',
            control_state_started_at TEXT NOT NULL DEFAULT '',
            attractiveness_status TEXT NOT NULL DEFAULT '',
            promo_count INTEGER NOT NULL DEFAULT 0,
            coinvest_pct REAL NULL,
            selected_iteration_code TEXT NOT NULL DEFAULT '',
            uses_promo INTEGER NOT NULL DEFAULT 0,
            uses_attractiveness INTEGER NOT NULL DEFAULT 0,
            uses_boost INTEGER NOT NULL DEFAULT 0,
            market_promo_status TEXT NOT NULL DEFAULT '',
            market_promo_checked_at TEXT NOT NULL DEFAULT '',
            market_promo_message TEXT NOT NULL DEFAULT '',
            source_updated_at TEXT NULL,
            PRIMARY KEY (store_uid, sku, captured_at),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_strategy_history_store_sku_time "
        "ON pricing_strategy_history(store_uid, sku, captured_at)"
    )
    strategy_history_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_strategy_history)").fetchall()}
    if "cycle_started_at" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN cycle_started_at TEXT NOT NULL DEFAULT ''")
    if "market_boost_bid_percent" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN market_boost_bid_percent REAL NULL")
    if "boost_share" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN boost_share REAL NULL")
    if "decision_code" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN decision_code TEXT NOT NULL DEFAULT 'observe'")
    if "decision_label" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN decision_label TEXT NOT NULL DEFAULT 'Наблюдать'")
    if "decision_tone" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN decision_tone TEXT NOT NULL DEFAULT 'warning'")
    if "hypothesis" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN hypothesis TEXT NOT NULL DEFAULT ''")
    if "hypothesis_started_at" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN hypothesis_started_at TEXT NOT NULL DEFAULT ''")
    if "hypothesis_expires_at" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN hypothesis_expires_at TEXT NOT NULL DEFAULT ''")
    if "control_state" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN control_state TEXT NOT NULL DEFAULT 'stable'")
    if "control_state_started_at" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN control_state_started_at TEXT NOT NULL DEFAULT ''")
    if "promo_count" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN promo_count INTEGER NOT NULL DEFAULT 0")
    if "coinvest_pct" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN coinvest_pct REAL NULL")
    if "selected_iteration_code" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN selected_iteration_code TEXT NOT NULL DEFAULT ''")
    if "market_promo_status" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN market_promo_status TEXT NOT NULL DEFAULT ''")
    if "market_promo_checked_at" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN market_promo_checked_at TEXT NOT NULL DEFAULT ''")
    if "market_promo_message" not in strategy_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_history ADD COLUMN market_promo_message TEXT NOT NULL DEFAULT ''")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_strategy_iteration_history (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            cycle_started_at TEXT NOT NULL,
            iteration_code TEXT NOT NULL,
            iteration_label TEXT NOT NULL DEFAULT '',
            tested_price REAL NULL,
            tested_boost_pct REAL NULL,
            market_boost_bid_percent REAL NULL,
            boost_share REAL NULL,
            promo_count INTEGER NOT NULL DEFAULT 0,
            attractiveness_status TEXT NOT NULL DEFAULT '',
            coinvest_pct REAL NULL,
            on_display_price REAL NULL,
            promo_details_json TEXT NOT NULL DEFAULT '[]',
            market_promo_status TEXT NOT NULL DEFAULT '',
            market_promo_message TEXT NOT NULL DEFAULT '',
            source_updated_at TEXT NULL,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku, cycle_started_at, iteration_code),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_strategy_iteration_history_store_cycle "
        "ON pricing_strategy_iteration_history(store_uid, cycle_started_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_strategy_iteration_history_store_sku "
        "ON pricing_strategy_iteration_history(store_uid, sku)"
    )
    iteration_history_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_strategy_iteration_history)").fetchall()}
    if "market_boost_bid_percent" not in iteration_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_iteration_history ADD COLUMN market_boost_bid_percent REAL NULL")
    if "boost_share" not in iteration_history_cols:
        conn.execute("ALTER TABLE pricing_strategy_iteration_history ADD COLUMN boost_share REAL NULL")


def _init_store_data_model_sqlite_attractiveness_promo_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_attractiveness_recommendations_raw (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            attractiveness_overpriced_price REAL NULL,
            attractiveness_moderate_price REAL NULL,
            attractiveness_profitable_price REAL NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            source_updated_at TEXT NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_attr_recommendations_raw_store ON pricing_attractiveness_recommendations_raw(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_attr_recommendations_raw_sku ON pricing_attractiveness_recommendations_raw(sku)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_attractiveness_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            attractiveness_overpriced_price REAL NULL,
            attractiveness_moderate_price REAL NULL,
            attractiveness_profitable_price REAL NULL,
            ozon_competitor_price REAL NULL,
            external_competitor_price REAL NULL,
            attractiveness_chosen_price REAL NULL,
            attractiveness_chosen_boost_bid_percent REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "rrc", "base_price")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "current_profit_abs", "base_profit_abs")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "current_profit_pct", "base_profit_pct")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "set_price", "attractiveness_set_price")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "set_profit_abs", "attractiveness_set_profit_abs")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "set_profit_pct", "attractiveness_set_profit_pct")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "non_profitable_price", "attractiveness_overpriced_price")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "non_profitable_profit_abs", "attractiveness_overpriced_profit_abs")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "non_profitable_profit_pct", "attractiveness_overpriced_profit_pct")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "moderate_price", "attractiveness_moderate_price")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "moderate_profit_abs", "attractiveness_moderate_profit_abs")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "moderate_profit_pct", "attractiveness_moderate_profit_pct")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "profitable_price", "attractiveness_profitable_price")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "profitable_profit_abs", "attractiveness_profitable_profit_abs")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "profitable_profit_pct", "attractiveness_profitable_profit_pct")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "chosen_price", "attractiveness_chosen_price")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "chosen_boost_bid_percent", "attractiveness_chosen_boost_bid_percent")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "total_profit_abs", "attractiveness_chosen_profit_abs")
    _rename_column_if_exists(conn, "pricing_attractiveness_results", "total_profit_pct", "attractiveness_chosen_profit_pct")
    _rebuild_pricing_attractiveness_results_if_needed(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_attractiveness_results_store ON pricing_attractiveness_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_attractiveness_results_sku ON pricing_attractiveness_results(sku)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_promo_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            promo_selected_items_json TEXT NOT NULL DEFAULT '[]',
            promo_selected_price REAL NULL,
            promo_selected_boost_bid_percent REAL NULL,
            promo_selected_profit_abs REAL NULL,
            promo_selected_profit_pct REAL NULL,
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    _rename_column_if_exists(conn, "pricing_promo_results", "selected_promo_price", "promo_selected_price")
    promo_results_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_promo_results)").fetchall()}
    if "promo_selected_items_json" not in promo_results_cols:
        conn.execute("ALTER TABLE pricing_promo_results ADD COLUMN promo_selected_items_json TEXT NOT NULL DEFAULT '[]'")
    if "promo_selected_boost_bid_percent" not in promo_results_cols:
        conn.execute("ALTER TABLE pricing_promo_results ADD COLUMN promo_selected_boost_bid_percent REAL NULL")
    if "promo_selected_profit_abs" not in promo_results_cols:
        conn.execute("ALTER TABLE pricing_promo_results ADD COLUMN promo_selected_profit_abs REAL NULL")
    if "promo_selected_profit_pct" not in promo_results_cols:
        conn.execute("ALTER TABLE pricing_promo_results ADD COLUMN promo_selected_profit_pct REAL NULL")
    _rebuild_pricing_promo_results_if_needed(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_results_store ON pricing_promo_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_results_sku ON pricing_promo_results(sku)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_promo_offer_results (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            promo_id TEXT NOT NULL,
            promo_name TEXT NOT NULL DEFAULT '',
            promo_price REAL NULL,
            promo_profit_abs REAL NULL,
            promo_profit_pct REAL NULL,
            promo_fit_mode TEXT NOT NULL DEFAULT 'rejected',
            source_updated_at TEXT NULL,
            calculated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku, promo_id),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    promo_offer_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_promo_offer_results)").fetchall()}
    if "promo_fit_mode" not in promo_offer_cols:
        conn.execute("ALTER TABLE pricing_promo_offer_results ADD COLUMN promo_fit_mode TEXT NOT NULL DEFAULT 'rejected'")
    _rebuild_pricing_promo_offer_results_if_needed(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_results_store ON pricing_promo_offer_results(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_results_sku ON pricing_promo_offer_results(sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_results_promo ON pricing_promo_offer_results(store_uid, promo_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_promo_campaign_raw (
            store_uid TEXT NOT NULL,
            promo_id TEXT NOT NULL,
            promo_name TEXT NOT NULL DEFAULT '',
            date_time_from TEXT NULL,
            date_time_to TEXT NULL,
            source_updated_at TEXT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, promo_id),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_campaign_raw_store ON pricing_promo_campaign_raw(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_campaign_raw_dates ON pricing_promo_campaign_raw(store_uid, date_time_from, date_time_to)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_promo_coinvest_settings (
            store_uid TEXT NOT NULL,
            promo_id TEXT NOT NULL,
            promo_name TEXT NOT NULL DEFAULT '',
            max_discount_percent REAL NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, promo_id),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_promo_coinvest_settings_store "
        "ON pricing_promo_coinvest_settings(store_uid)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_promo_offer_raw (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            promo_id TEXT NOT NULL,
            promo_name TEXT NOT NULL DEFAULT '',
            date_time_from TEXT NULL,
            date_time_to TEXT NULL,
            promo_price REAL NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            source_updated_at TEXT NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku, promo_id),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_raw_store ON pricing_promo_offer_raw(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_raw_sku ON pricing_promo_offer_raw(store_uid, sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_promo_offer_raw_promo ON pricing_promo_offer_raw(store_uid, promo_id)")


def _init_store_data_model_sqlite_sales_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_market_order_items (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            order_id TEXT NOT NULL,
            order_item_id TEXT NOT NULL,
            order_status TEXT NOT NULL DEFAULT '',
            order_created_at TEXT NOT NULL,
            order_created_date TEXT NOT NULL,
            sku TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            sale_price REAL NULL,
            payment_price REAL NULL,
            subsidy_amount REAL NULL,
            item_count INTEGER NOT NULL DEFAULT 1,
            line_revenue REAL NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, order_item_id),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    sales_cols = {row[1] for row in conn.execute("PRAGMA table_info(sales_market_order_items)").fetchall()}
    if "payment_price" not in sales_cols:
        conn.execute("ALTER TABLE sales_market_order_items ADD COLUMN payment_price REAL NULL")
    if "subsidy_amount" not in sales_cols:
        conn.execute("ALTER TABLE sales_market_order_items ADD COLUMN subsidy_amount REAL NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_market_order_items_store_date ON sales_market_order_items(store_uid, order_created_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_market_order_items_store_sku ON sales_market_order_items(store_uid, sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_market_order_items_order ON sales_market_order_items(store_uid, order_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_united_order_transactions (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            order_id TEXT NOT NULL,
            order_created_at TEXT NOT NULL,
            order_created_date TEXT NOT NULL,
            shipment_date TEXT NOT NULL DEFAULT '',
            delivery_date TEXT NOT NULL DEFAULT '',
            sku TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            item_status TEXT NOT NULL DEFAULT '',
            source_updated_at TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, order_id, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    united_cols = {row[1] for row in conn.execute("PRAGMA table_info(sales_united_order_transactions)").fetchall()}
    if "item_name" not in united_cols:
        conn.execute("ALTER TABLE sales_united_order_transactions ADD COLUMN item_name TEXT NOT NULL DEFAULT ''")
    if "shipment_date" not in united_cols:
        conn.execute("ALTER TABLE sales_united_order_transactions ADD COLUMN shipment_date TEXT NOT NULL DEFAULT ''")
    if "delivery_date" not in united_cols:
        conn.execute("ALTER TABLE sales_united_order_transactions ADD COLUMN delivery_date TEXT NOT NULL DEFAULT ''")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_store_date "
        "ON sales_united_order_transactions(store_uid, order_created_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_store_sku "
        "ON sales_united_order_transactions(store_uid, sku)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_united_order_transactions_order "
        "ON sales_united_order_transactions(store_uid, order_id)"
    )
    _rebuild_sales_united_order_transactions_if_needed(conn)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_united_netting_report_rows (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            report_date_from TEXT NOT NULL,
            report_date_to TEXT NOT NULL,
            report_id TEXT NOT NULL DEFAULT '',
            sheet_name TEXT NOT NULL DEFAULT '',
            row_index INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            source_updated_at TEXT NOT NULL DEFAULT '',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, report_date_from, report_date_to, sheet_name, row_index),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_united_netting_store_period "
        "ON sales_united_netting_report_rows(store_uid, report_date_from, report_date_to)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_united_netting_sheet "
        "ON sales_united_netting_report_rows(store_uid, sheet_name)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_shelfs_statistics_report_rows (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            report_date_from TEXT NOT NULL,
            report_date_to TEXT NOT NULL,
            report_id TEXT NOT NULL DEFAULT '',
            sheet_name TEXT NOT NULL DEFAULT '',
            row_index INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            source_updated_at TEXT NOT NULL DEFAULT '',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, report_date_from, report_date_to, sheet_name, row_index),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_shelfs_statistics_store_period "
        "ON sales_shelfs_statistics_report_rows(store_uid, report_date_from, report_date_to)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_shelfs_statistics_sheet "
        "ON sales_shelfs_statistics_report_rows(store_uid, sheet_name)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_shows_boost_report_rows (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            report_date_from TEXT NOT NULL,
            report_date_to TEXT NOT NULL,
            report_id TEXT NOT NULL DEFAULT '',
            sheet_name TEXT NOT NULL DEFAULT '',
            row_index INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL DEFAULT '{}',
            source_updated_at TEXT NOT NULL DEFAULT '',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, report_date_from, report_date_to, sheet_name, row_index),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_shows_boost_store_period "
        "ON sales_shows_boost_report_rows(store_uid, report_date_from, report_date_to)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_shows_boost_sheet "
        "ON sales_shows_boost_report_rows(store_uid, sheet_name)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_overview_order_rows (
            store_uid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'yandex_market',
            order_created_date TEXT NOT NULL,
            order_created_at TEXT NOT NULL DEFAULT '',
            shipment_date TEXT NOT NULL DEFAULT '',
            delivery_date TEXT NOT NULL DEFAULT '',
            order_id TEXT NOT NULL,
            item_status TEXT NOT NULL DEFAULT '',
            sku TEXT NOT NULL,
            item_name TEXT NOT NULL DEFAULT '',
            sale_price REAL NULL,
            gross_profit REAL NULL,
            cogs_price REAL NULL,
            commission REAL NULL,
            acquiring REAL NULL,
            delivery REAL NULL,
            ads REAL NULL,
            tax REAL NULL,
            profit REAL NULL,
            sale_price_with_coinvest REAL NULL,
            strategy_cycle_started_at TEXT NOT NULL DEFAULT '',
            strategy_market_boost_bid_percent REAL NULL,
            strategy_boost_share REAL NULL,
            strategy_boost_bid_percent REAL NULL,
            strategy_snapshot_at TEXT NOT NULL DEFAULT '',
            strategy_installed_price REAL NULL,
            strategy_decision_code TEXT NOT NULL DEFAULT '',
            strategy_decision_label TEXT NOT NULL DEFAULT '',
            strategy_control_state TEXT NOT NULL DEFAULT '',
            strategy_attractiveness_status TEXT NOT NULL DEFAULT '',
            strategy_promo_count INTEGER NOT NULL DEFAULT 0,
            strategy_coinvest_pct REAL NULL,
            strategy_selected_iteration_code TEXT NOT NULL DEFAULT '',
            strategy_uses_promo INTEGER NOT NULL DEFAULT 0,
            strategy_market_promo_status TEXT NOT NULL DEFAULT '',
            uses_planned_costs INTEGER NOT NULL DEFAULT 0,
            source_updated_at TEXT NOT NULL DEFAULT '',
            calculated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (store_uid, order_id, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    overview_cols = {row[1] for row in conn.execute("PRAGMA table_info(sales_overview_order_rows)").fetchall()}
    if "strategy_cycle_started_at" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_cycle_started_at TEXT NOT NULL DEFAULT ''")
    if "strategy_market_boost_bid_percent" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_market_boost_bid_percent REAL NULL")
    if "strategy_boost_share" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_boost_share REAL NULL")
    if "strategy_boost_bid_percent" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_boost_bid_percent REAL NULL")
    if "strategy_snapshot_at" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_snapshot_at TEXT NOT NULL DEFAULT ''")
    if "strategy_installed_price" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_installed_price REAL NULL")
    if "strategy_decision_code" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_decision_code TEXT NOT NULL DEFAULT ''")
    if "strategy_decision_label" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_decision_label TEXT NOT NULL DEFAULT ''")
    if "strategy_control_state" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_control_state TEXT NOT NULL DEFAULT ''")
    if "strategy_attractiveness_status" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_attractiveness_status TEXT NOT NULL DEFAULT ''")
    if "strategy_promo_count" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_promo_count INTEGER NOT NULL DEFAULT 0")
    if "strategy_coinvest_pct" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_coinvest_pct REAL NULL")
    if "strategy_selected_iteration_code" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_selected_iteration_code TEXT NOT NULL DEFAULT ''")
    if "strategy_uses_promo" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_uses_promo INTEGER NOT NULL DEFAULT 0")
    if "strategy_market_promo_status" not in overview_cols:
        conn.execute("ALTER TABLE sales_overview_order_rows ADD COLUMN strategy_market_promo_status TEXT NOT NULL DEFAULT ''")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_store_date "
        "ON sales_overview_order_rows(store_uid, order_created_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_store_status "
        "ON sales_overview_order_rows(store_uid, item_status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sales_overview_order_rows_store_sku "
        "ON sales_overview_order_rows(store_uid, sku)"
    )


def _init_store_data_model_sqlite_reference_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stores (
            store_uid TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            store_id TEXT NOT NULL,
            store_name TEXT NOT NULL DEFAULT '',
            currency_code TEXT NOT NULL DEFAULT 'RUB',
            fulfillment_model TEXT NOT NULL DEFAULT 'FBO',
            business_id TEXT NOT NULL DEFAULT '',
            seller_id TEXT NOT NULL DEFAULT '',
            account_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stores_platform ON stores(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stores_store_id ON stores(store_id)")
    store_cols = {row[1] for row in conn.execute("PRAGMA table_info(stores)").fetchall()}
    if "currency_code" not in store_cols:
        conn.execute("ALTER TABLE stores ADD COLUMN currency_code TEXT NOT NULL DEFAULT 'RUB'")
    if "fulfillment_model" not in store_cols:
        conn.execute("ALTER TABLE stores ADD COLUMN fulfillment_model TEXT NOT NULL DEFAULT 'FBO'")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS store_datasets (
            dataset_key TEXT PRIMARY KEY,
            store_uid TEXT NOT NULL,
            task_code TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'ready',
            row_count INTEGER NOT NULL DEFAULT 0,
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_store_datasets_store_uid ON store_datasets(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_store_datasets_task_code ON store_datasets(task_code)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_category_tree (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_key TEXT NOT NULL,
            store_uid TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT '',
            subcategory_1 TEXT NOT NULL DEFAULT '',
            subcategory_2 TEXT NOT NULL DEFAULT '',
            subcategory_3 TEXT NOT NULL DEFAULT '',
            subcategory_4 TEXT NOT NULL DEFAULT '',
            subcategory_5 TEXT NOT NULL DEFAULT '',
            leaf_path TEXT NOT NULL DEFAULT '',
            items_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (dataset_key) REFERENCES store_datasets(dataset_key) ON DELETE CASCADE,
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE,
            UNIQUE (
                dataset_key,
                category,
                subcategory_1,
                subcategory_2,
                subcategory_3,
                subcategory_4,
                subcategory_5
            )
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_category_tree_dataset ON pricing_category_tree(dataset_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_category_tree_store_uid ON pricing_category_tree(store_uid)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_catalog_sku_paths (
            priority_platform TEXT NOT NULL,
            sku TEXT NOT NULL,
            anchor_store_uid TEXT NOT NULL DEFAULT '',
            source_store_uid TEXT NOT NULL DEFAULT '',
            resolved_category TEXT NOT NULL DEFAULT '',
            resolved_subcategory_1 TEXT NOT NULL DEFAULT '',
            resolved_subcategory_2 TEXT NOT NULL DEFAULT '',
            resolved_subcategory_3 TEXT NOT NULL DEFAULT '',
            resolved_subcategory_4 TEXT NOT NULL DEFAULT '',
            resolved_subcategory_5 TEXT NOT NULL DEFAULT '',
            leaf_path TEXT NOT NULL DEFAULT '',
            resolution_kind TEXT NOT NULL DEFAULT 'undefined',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (priority_platform, sku)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_catalog_sku_paths_platform ON pricing_catalog_sku_paths(priority_platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_catalog_sku_paths_leaf ON pricing_catalog_sku_paths(priority_platform, leaf_path)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_category_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_key TEXT NOT NULL,
            store_uid TEXT NOT NULL,
            leaf_path TEXT NOT NULL,
            commission_percent REAL NULL,
            acquiring_percent REAL NULL,
            logistics_rub REAL NULL,
            ads_percent REAL NULL,
            returns_percent REAL NULL,
            tax_percent REAL NULL,
            other_expenses_rub REAL NULL,
            other_expenses_percent REAL NULL,
            cogs_rub REAL NULL,
            target_profit_rub REAL NULL,
            target_profit_percent REAL NULL,
            target_margin_rub REAL NULL,
            target_margin_percent REAL NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (dataset_key) REFERENCES store_datasets(dataset_key) ON DELETE CASCADE,
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE,
            UNIQUE (dataset_key, leaf_path)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_category_settings_dataset ON pricing_category_settings(dataset_key)")
    pcs_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_category_settings)").fetchall()}
    if "tax_percent" not in pcs_cols:
        conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN tax_percent REAL NULL")
    if "other_expenses_rub" not in pcs_cols:
        conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN other_expenses_rub REAL NULL")
    if "other_expenses_percent" not in pcs_cols:
        conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN other_expenses_percent REAL NULL")
    if "target_profit_percent" not in pcs_cols:
        conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN target_profit_percent REAL NULL")
    if "target_margin_rub" not in pcs_cols:
        conn.execute("ALTER TABLE pricing_category_settings ADD COLUMN target_margin_rub REAL NULL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS category_tree_cache_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key TEXT NOT NULL,
            platform TEXT NOT NULL,
            account_id TEXT NOT NULL,
            tree_code TEXT NOT NULL,
            node_kind TEXT NOT NULL DEFAULT 'category',
            category_id TEXT NOT NULL,
            type_id TEXT NOT NULL DEFAULT '',
            parent_category_id TEXT NOT NULL DEFAULT '',
            name TEXT NOT NULL DEFAULT '',
            type_name TEXT NOT NULL DEFAULT '',
            level INTEGER NOT NULL DEFAULT 0,
            path TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            UNIQUE (cache_key, category_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_category_tree_cache_key ON category_tree_cache_nodes(cache_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_category_tree_cache_platform_account ON category_tree_cache_nodes(platform, account_id, tree_code)")
    cache_cols = {row[1] for row in conn.execute("PRAGMA table_info(category_tree_cache_nodes)").fetchall()}
    if "node_kind" not in cache_cols:
        conn.execute("ALTER TABLE category_tree_cache_nodes ADD COLUMN node_kind TEXT NOT NULL DEFAULT 'category'")
    if "type_id" not in cache_cols:
        conn.execute("ALTER TABLE category_tree_cache_nodes ADD COLUMN type_id TEXT NOT NULL DEFAULT ''")
    if "type_name" not in cache_cols:
        conn.execute("ALTER TABLE category_tree_cache_nodes ADD COLUMN type_name TEXT NOT NULL DEFAULT ''")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_store_settings (
            store_uid TEXT PRIMARY KEY,
            earning_mode TEXT NOT NULL DEFAULT 'margin',
            earning_unit TEXT NOT NULL DEFAULT 'percent',
            strategy_mode TEXT NOT NULL DEFAULT 'mix',
            planned_revenue REAL NULL,
            target_profit_rub REAL NULL,
            target_profit_percent REAL NULL,
            minimum_profit_percent REAL NULL,
            target_margin_rub REAL NULL,
            target_margin_percent REAL NULL,
            target_drr_percent REAL NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    store_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_store_settings)").fetchall()}
    if "planned_revenue" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN planned_revenue REAL NULL")
    if "strategy_mode" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN strategy_mode TEXT NOT NULL DEFAULT 'mix'")
    if "cogs_source_type" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN cogs_source_type TEXT NULL")
    if "cogs_source_id" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN cogs_source_id TEXT NULL")
    if "cogs_source_name" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN cogs_source_name TEXT NULL")
    if "cogs_sku_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN cogs_sku_column TEXT NULL")
    if "cogs_value_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN cogs_value_column TEXT NULL")
    if "stock_source_type" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN stock_source_type TEXT NULL")
    if "stock_source_id" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN stock_source_id TEXT NULL")
    if "stock_source_name" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN stock_source_name TEXT NULL")
    if "stock_sku_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN stock_sku_column TEXT NULL")
    if "stock_value_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN stock_value_column TEXT NULL")
    if "overview_cogs_source_type" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN overview_cogs_source_type TEXT NULL")
    if "overview_cogs_source_id" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN overview_cogs_source_id TEXT NULL")
    if "overview_cogs_source_name" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN overview_cogs_source_name TEXT NULL")
    if "overview_cogs_order_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN overview_cogs_order_column TEXT NULL")
    if "overview_cogs_sku_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN overview_cogs_sku_column TEXT NULL")
    if "overview_cogs_value_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN overview_cogs_value_column TEXT NULL")
    if "export_prices_source_type" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_prices_source_type TEXT NULL")
    if "export_prices_source_id" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_prices_source_id TEXT NULL")
    if "export_prices_source_name" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_prices_source_name TEXT NULL")
    if "export_prices_sku_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_prices_sku_column TEXT NULL")
    if "export_prices_value_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_prices_value_column TEXT NULL")
    if "export_ads_source_type" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_ads_source_type TEXT NULL")
    if "export_ads_source_id" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_ads_source_id TEXT NULL")
    if "export_ads_source_name" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_ads_source_name TEXT NULL")
    if "export_ads_sku_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_ads_sku_column TEXT NULL")
    if "export_ads_value_column" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN export_ads_value_column TEXT NULL")
    if "target_profit_percent" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN target_profit_percent REAL NULL")
    if "minimum_profit_percent" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN minimum_profit_percent REAL NULL")
    if "target_margin_rub" not in store_cols:
        conn.execute("ALTER TABLE pricing_store_settings ADD COLUMN target_margin_rub REAL NULL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_logistics_store_settings (
            store_uid TEXT PRIMARY KEY,
            fulfillment_model TEXT NOT NULL DEFAULT 'FBO',
            handling_mode TEXT NOT NULL DEFAULT 'fixed',
            handling_fixed_amount REAL NULL,
            handling_percent REAL NULL,
            handling_min_amount REAL NULL,
            handling_max_amount REAL NULL,
            delivery_cost_per_kg REAL NULL,
            return_processing_cost REAL NULL,
            disposal_cost REAL NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    ls_cols = {row[1] for row in conn.execute("PRAGMA table_info(pricing_logistics_store_settings)").fetchall()}
    if "handling_mode" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN handling_mode TEXT NOT NULL DEFAULT 'fixed'")
    if "handling_fixed_amount" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN handling_fixed_amount REAL NULL")
    if "handling_percent" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN handling_percent REAL NULL")
    if "handling_min_amount" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN handling_min_amount REAL NULL")
    if "handling_max_amount" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN handling_max_amount REAL NULL")
    if "delivery_cost_per_kg" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN delivery_cost_per_kg REAL NULL")
    if "return_processing_cost" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN return_processing_cost REAL NULL")
    if "disposal_cost" not in ls_cols:
        conn.execute("ALTER TABLE pricing_logistics_store_settings ADD COLUMN disposal_cost REAL NULL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_logistics_product_settings (
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            width_cm REAL NULL,
            length_cm REAL NULL,
            height_cm REAL NULL,
            weight_kg REAL NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, sku),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_logistics_product_store ON pricing_logistics_product_settings(store_uid)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_rates_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            pair TEXT NOT NULL DEFAULT 'USD_RUB',
            rate_date TEXT NOT NULL,
            rate_value REAL NOT NULL,
            loaded_at TEXT NOT NULL,
            meta_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(source, pair, rate_date)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fx_rates_cache_source_date ON fx_rates_cache(source, pair, rate_date)")


def _init_store_data_model_sqlite_operational_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS refresh_jobs (
            job_code TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            schedule_kind TEXT NOT NULL DEFAULT 'interval',
            interval_minutes INTEGER NULL,
            time_of_day TEXT NULL,
            date_from TEXT NULL,
            date_to TEXT NULL,
            stores_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
        """
    )
    refresh_job_cols = _table_columns(conn, "refresh_jobs")
    if "stores_json" not in refresh_job_cols:
        conn.execute("ALTER TABLE refresh_jobs ADD COLUMN stores_json TEXT NOT NULL DEFAULT '[]'")
    if "date_from" not in refresh_job_cols:
        conn.execute("ALTER TABLE refresh_jobs ADD COLUMN date_from TEXT NULL")
    if "date_to" not in refresh_job_cols:
        conn.execute("ALTER TABLE refresh_jobs ADD COLUMN date_to TEXT NULL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS refresh_job_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_code TEXT NOT NULL,
            trigger_source TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL,
            finished_at TEXT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            message TEXT NOT NULL DEFAULT '',
            meta_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_refresh_job_runs_job_started "
        "ON refresh_job_runs(job_code, started_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_refresh_job_runs_status "
        "ON refresh_job_runs(status, started_at DESC)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS yandex_goods_price_report_items (
            store_uid TEXT NOT NULL,
            offer_id TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT '',
            on_display_raw TEXT NOT NULL DEFAULT '',
            on_display_price REAL NULL,
            source_updated_at TEXT NOT NULL DEFAULT '',
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (store_uid, offer_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_yandex_goods_price_report_store ON yandex_goods_price_report_items(store_uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_yandex_goods_price_report_offer ON yandex_goods_price_report_items(offer_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS yandex_goods_price_report_history (
            store_uid TEXT NOT NULL,
            offer_id TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT '',
            on_display_raw TEXT NOT NULL DEFAULT '',
            on_display_price REAL NULL,
            source_updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (store_uid, offer_id, captured_at),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_yandex_goods_price_report_history_store_offer_time "
        "ON yandex_goods_price_report_history(store_uid, offer_id, captured_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_daily_plan_history (
            store_uid TEXT NOT NULL,
            plan_date TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            planned_revenue_daily REAL NULL,
            planned_profit_daily REAL NULL,
            today_revenue REAL NULL,
            today_profit REAL NULL,
            weighted_day_profit_pct REAL NULL,
            minimum_profit_percent REAL NULL,
            experimental_floor_pct REAL NULL,
            PRIMARY KEY (store_uid, plan_date, captured_at),
            FOREIGN KEY (store_uid) REFERENCES stores(store_uid) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pricing_daily_plan_history_store_date_time "
        "ON pricing_daily_plan_history(store_uid, plan_date, captured_at)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_autopilot_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT NOT NULL,
            time_bucket_start TEXT NOT NULL,
            time_bucket_end TEXT NOT NULL,
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE (time_bucket_start, store_uid, sku)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_snapshots_bucket ON pricing_autopilot_snapshots(time_bucket_start)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_snapshots_store_sku ON pricing_autopilot_snapshots(store_uid, sku)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_autopilot_decisions (
            decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            review_after TEXT NOT NULL,
            reviewed_at TEXT NULL,
            store_uid TEXT NOT NULL,
            sku TEXT NOT NULL,
            decision_status TEXT NOT NULL DEFAULT 'pending',
            decision_mode TEXT NOT NULL DEFAULT 'simulate',
            action_code TEXT NOT NULL DEFAULT '',
            action_unit TEXT NOT NULL DEFAULT '',
            action_value REAL NULL,
            previous_value REAL NULL,
            proposed_value REAL NULL,
            baseline_snapshot_id INTEGER NULL,
            review_snapshot_id INTEGER NULL,
            reason_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_decisions_status ON pricing_autopilot_decisions(decision_status, review_after)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_autopilot_decisions_store_sku ON pricing_autopilot_decisions(store_uid, sku, created_at)")
    _drop_legacy_tables_if_exist(conn)
    _rebuild_logical_views(conn)
    rebuild_db_explorer_views(conn)


def replace_category_tree_cache_nodes(
    *,
    platform: str,
    account_id: str,
    tree_code: str,
    nodes: list[dict[str, Any]],
) -> str:
    init_store_data_model()
    p = str(platform or "").strip().lower()
    aid = str(account_id or "").strip()
    tcode = str(tree_code or "").strip()
    if not p or not aid or not tcode:
        raise ValueError("platform, account_id, tree_code обязательны")
    cache_key = f"{p}:{aid}:{tcode}"
    now = _now_iso()
    prepared_map: dict[str, tuple[Any, ...]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        raw_cid = str(node.get("category_id") or "").strip()
        node_kind = str(node.get("node_kind") or "category").strip() or "category"
        type_id = str(node.get("type_id") or "").strip()
        if not raw_cid and not type_id:
            continue
        # В таблице category_id участвует в UNIQUE(cache_key, category_id). Для type-узлов
        # используем внутренний ключ с префиксом, чтобы не конфликтовать с category_id категорий.
        db_cid = f"type:{type_id}" if node_kind == "type" and type_id else raw_cid
        if not db_cid:
            continue
        row_tuple = (
            cache_key,
            p,
            aid,
            tcode,
            node_kind,
            db_cid,
            type_id,
            str(node.get("parent_category_id") or "").strip(),
            str(node.get("name") or "").strip(),
            str(node.get("type_name") or "").strip(),
            int(node.get("level") or 0),
            str(node.get("path") or "").strip(),
            now,
        )
        prev = prepared_map.get(db_cid)
        # Если ключ повторился, оставляем запись с более длинным path (обычно она информативнее).
        if prev is None or len(str(row_tuple[11] or "")) >= len(str(prev[11] or "")):
            prepared_map[db_cid] = row_tuple
    prepared = list(prepared_map.values())
    with _connect_history() as conn:
        conn.execute(
            f"DELETE FROM category_tree_cache_nodes WHERE cache_key = {'%s' if is_postgres_backend() else '?'}",
            (cache_key,),
        )
        if prepared:
            values_sql = _placeholders(13)
            _executemany(conn, 
                f"""
                INSERT INTO category_tree_cache_nodes (
                    cache_key, platform, account_id, tree_code, node_kind, category_id, type_id, parent_category_id,
                    name, type_name, level, path, updated_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return cache_key


def get_category_tree_cache_paths(
    *,
    platform: str,
    account_id: str,
    tree_code: str,
) -> dict[str, list[str]]:
    init_store_data_model()
    p = str(platform or "").strip().lower()
    aid = str(account_id or "").strip()
    tcode = str(tree_code or "").strip()
    if not p or not aid or not tcode:
        raise ValueError("platform, account_id, tree_code обязательны")
    cache_key = f"{p}:{aid}:{tcode}"
    with _connect_history() as conn:
        rows = conn.execute(
            f"""
            SELECT node_kind, category_id, type_id, path
            FROM category_tree_cache_nodes
            WHERE cache_key = {'%s' if is_postgres_backend() else '?'}
            """,
            (cache_key,),
        ).fetchall()
    out: dict[str, list[str]] = {}
    for row in rows:
        node_kind = str(row["node_kind"] or "").strip()
        cid = str(row["type_id"] or "").strip() if node_kind == "type" and str(row["type_id"] or "").strip() else str(row["category_id"] or "").strip()
        path = str(row["path"] or "").strip()
        if not cid or not path:
            continue
        out[cid] = [p.strip() for p in path.split(" / ") if p.strip()]
    return out


def upsert_store(
    *,
    platform: str,
    store_id: str,
    store_name: str = "",
    currency_code: str = "RUB",
    fulfillment_model: str = "FBO",
    business_id: str = "",
    seller_id: str = "",
    account_id: str = "",
) -> str:
    init_store_data_model()
    p = str(platform or "").strip()
    sid = str(store_id or "").strip()
    if not p or not sid:
        raise ValueError("platform и store_id обязательны")
    store_uid = f"{p}:{sid}"
    now = _now_iso()
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO stores (
                store_uid, platform, store_id, store_name, currency_code, fulfillment_model, business_id, seller_id, account_id, created_at, updated_at
            ) VALUES ({_placeholders(11)})
            ON CONFLICT(store_uid) DO UPDATE SET
                store_name = excluded.store_name,
                currency_code = excluded.currency_code,
                fulfillment_model = excluded.fulfillment_model,
                business_id = excluded.business_id,
                seller_id = excluded.seller_id,
                account_id = excluded.account_id,
                updated_at = excluded.updated_at
            """,
            (
                store_uid,
                p,
                sid,
                str(store_name or "").strip(),
                (str(currency_code or "RUB").strip().upper() or "RUB"),
                (str(fulfillment_model or "FBO").strip().upper() or "FBO"),
                str(business_id or "").strip(),
                str(seller_id or "").strip(),
                str(account_id or "").strip(),
                now,
                now,
            ),
        )
        rebuild_db_explorer_views(conn)
        conn.commit()
    with _connect_system() as conn:
        conn.execute(
            f"""
            INSERT INTO stores (
                store_uid, platform, store_id, store_name, currency_code, fulfillment_model, business_id, seller_id, account_id, created_at, updated_at
            ) VALUES ({_system_placeholders(11)})
            ON CONFLICT(store_uid) DO UPDATE SET
                store_name = excluded.store_name,
                currency_code = excluded.currency_code,
                fulfillment_model = excluded.fulfillment_model,
                business_id = excluded.business_id,
                seller_id = excluded.seller_id,
                account_id = excluded.account_id,
                updated_at = excluded.updated_at
            """,
            (
                store_uid,
                p,
                sid,
                str(store_name or "").strip(),
                (str(currency_code or "RUB").strip().upper() or "RUB"),
                (str(fulfillment_model or "FBO").strip().upper() or "FBO"),
                str(business_id or "").strip(),
                str(seller_id or "").strip(),
                str(account_id or "").strip(),
                now,
                now,
            ),
        )
        conn.commit()
    return store_uid


def upsert_store_dataset(
    *,
    store_uid: str,
    store_id: str,
    task_code: str,
    title: str,
    status: str = "ready",
    row_count: int = 0,
    meta: dict[str, Any] | None = None,
) -> str:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    task = str(task_code or "").strip()
    sid = str(store_id or "").strip()
    if not suid or not task or not sid:
        raise ValueError("store_uid, store_id, task_code обязательны")
    dataset_key = f"{sid}:{task}"
    now = _now_iso()
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO store_datasets (
                dataset_key, store_uid, task_code, title, status, row_count, meta_json, created_at, updated_at
            ) VALUES ({_placeholders(9)})
            ON CONFLICT(dataset_key) DO UPDATE SET
                store_uid = excluded.store_uid,
                title = excluded.title,
                status = excluded.status,
                row_count = excluded.row_count,
                meta_json = excluded.meta_json,
                updated_at = excluded.updated_at
            """,
            (
                dataset_key,
                suid,
                task,
                str(title or "").strip(),
                str(status or "ready").strip(),
                int(row_count or 0),
                json.dumps(meta or {}, ensure_ascii=False),
                now,
                now,
            ),
        )
        conn.commit()
    return dataset_key


def replace_pricing_category_tree(
    *,
    dataset_key: str,
    store_uid: str,
    rows: list[dict[str, Any]],
) -> int:
    init_store_data_model()
    dkey = str(dataset_key or "").strip()
    suid = str(store_uid or "").strip()
    if not dkey or not suid:
        raise ValueError("dataset_key и store_uid обязательны")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or "").strip()
        levels = row.get("subcategory_levels")
        if not isinstance(levels, list):
            levels = []
        # Settings UI supports only 3 category levels; keep dataset leaf paths canonical.
        norm_levels = [str(v or "").strip() for v in levels if str(v or "").strip()][:3]
        norm_levels += [""] * (5 - len(norm_levels))
        leaf_path = " / ".join([x for x in [category, *norm_levels] if x])
        prepared.append(
            (
                dkey,
                suid,
                category,
                norm_levels[0],
                norm_levels[1],
                norm_levels[2],
                norm_levels[3],
                norm_levels[4],
                leaf_path,
                int(row.get("items_count") or 0),
                now,
            )
        )

    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_category_tree WHERE dataset_key = {'%s' if is_postgres_backend() else '?'}",
            (dkey,),
        )
        if prepared:
            values_sql = _placeholders(11)
            _executemany(conn, 
                f"""
                INSERT INTO pricing_category_tree (
                    dataset_key, store_uid, category, subcategory_1, subcategory_2, subcategory_3,
                    subcategory_4, subcategory_5, leaf_path, items_count, updated_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def get_pricing_category_tree(
    *,
    store_uid: str,
    dataset_key: str | None = None,
) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    with _connect() as conn:
        if dataset_key:
            dkey = str(dataset_key).strip()
        else:
            row = conn.execute(
                f"""
                SELECT dataset_key
                FROM store_datasets
                WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND task_code = 'pricing_categories'
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (suid,),
            ).fetchone()
            dkey = str(row["dataset_key"]) if row else ""
        if not dkey:
            return {"dataset_key": "", "rows": []}

        rows = conn.execute(
            f"""
            SELECT
                t.category,
                t.subcategory_1,
                t.subcategory_2,
                t.subcategory_3,
                t.subcategory_4,
                t.subcategory_5,
                t.leaf_path,
                t.items_count,
                s.commission_percent,
                s.acquiring_percent,
                s.logistics_rub,
                s.ads_percent,
                s.returns_percent,
                s.tax_percent,
                s.other_expenses_rub,
                s.other_expenses_percent,
                s.cogs_rub,
                s.target_profit_rub,
                s.target_profit_percent,
                s.target_margin_rub,
                s.target_margin_percent
            FROM pricing_category_tree t
            LEFT JOIN pricing_category_settings s
              ON s.dataset_key = t.dataset_key AND s.leaf_path = t.leaf_path
            WHERE t.dataset_key = {'%s' if is_postgres_backend() else '?'}
            ORDER BY t.category, t.subcategory_1, t.subcategory_2, t.subcategory_3, t.subcategory_4, t.subcategory_5
            """,
            (dkey,),
        ).fetchall()
        settings_rows = conn.execute(
            f"""
            SELECT
                leaf_path,
                commission_percent,
                acquiring_percent,
                logistics_rub,
                ads_percent,
                returns_percent,
                tax_percent,
                other_expenses_rub,
                other_expenses_percent,
                cogs_rub,
                target_profit_rub,
                target_profit_percent,
                target_margin_rub,
                target_margin_percent
            FROM pricing_category_settings
            WHERE dataset_key = {'%s' if is_postgres_backend() else '?'}
            """,
            (dkey,),
        ).fetchall()
    normalized_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        category = str(item.get("category") or "").strip()
        levels = [
            str(item.get("subcategory_1") or "").strip(),
            str(item.get("subcategory_2") or "").strip(),
            str(item.get("subcategory_3") or "").strip(),
        ]
        clean_levels = [value for value in levels if value]
        leaf_path = " / ".join([value for value in [category, *clean_levels] if value])
        if not leaf_path:
            continue
        existing = normalized_rows.get(leaf_path)
        if existing is None:
            normalized = dict(item)
            normalized["subcategory_1"] = clean_levels[0] if len(clean_levels) > 0 else ""
            normalized["subcategory_2"] = clean_levels[1] if len(clean_levels) > 1 else ""
            normalized["subcategory_3"] = clean_levels[2] if len(clean_levels) > 2 else ""
            normalized["subcategory_4"] = ""
            normalized["subcategory_5"] = ""
            normalized["leaf_path"] = leaf_path
            normalized_rows[leaf_path] = normalized
            continue
        existing["items_count"] = int(existing.get("items_count") or 0) + int(item.get("items_count") or 0)
        for field in (
            "commission_percent",
            "acquiring_percent",
            "logistics_rub",
            "ads_percent",
            "returns_percent",
            "tax_percent",
            "other_expenses_rub",
            "other_expenses_percent",
            "cogs_rub",
            "target_profit_rub",
            "target_profit_percent",
            "target_margin_rub",
            "target_margin_percent",
        ):
            if existing.get(field) is None and item.get(field) is not None:
                existing[field] = item.get(field)
    normalized_settings: dict[str, dict[str, Any]] = {}
    for row in settings_rows:
        item = dict(row)
        leaf = str(item.get("leaf_path") or "").strip()
        if not leaf:
            continue
        parts = [part.strip() for part in leaf.split(" / ") if part.strip()]
        normalized_leaf = " / ".join(parts[:4])
        if not normalized_leaf:
            continue
        existing = normalized_settings.get(normalized_leaf)
        if existing is None:
            normalized_settings[normalized_leaf] = item
            continue
        for field in (
            "commission_percent",
            "acquiring_percent",
            "logistics_rub",
            "ads_percent",
            "returns_percent",
            "tax_percent",
            "other_expenses_rub",
            "other_expenses_percent",
            "cogs_rub",
            "target_profit_rub",
            "target_profit_percent",
            "target_margin_rub",
            "target_margin_percent",
        ):
            if existing.get(field) is None and item.get(field) is not None:
                existing[field] = item.get(field)
    for leaf_path, settings in normalized_settings.items():
        if leaf_path in normalized_rows:
            continue
        parts = [part.strip() for part in str(leaf_path or "").split(" / ") if part.strip()]
        if not parts:
            continue
        category = parts[0]
        levels = parts[1:4]
        normalized_rows[leaf_path] = {
            "category": category,
            "subcategory_1": levels[0] if len(levels) > 0 else "",
            "subcategory_2": levels[1] if len(levels) > 1 else "",
            "subcategory_3": levels[2] if len(levels) > 2 else "",
            "subcategory_4": "",
            "subcategory_5": "",
            "leaf_path": leaf_path,
            "items_count": 0,
            "commission_percent": settings.get("commission_percent"),
            "acquiring_percent": settings.get("acquiring_percent"),
            "logistics_rub": settings.get("logistics_rub"),
            "ads_percent": settings.get("ads_percent"),
            "returns_percent": settings.get("returns_percent"),
            "tax_percent": settings.get("tax_percent"),
            "other_expenses_rub": settings.get("other_expenses_rub"),
            "other_expenses_percent": settings.get("other_expenses_percent"),
            "cogs_rub": settings.get("cogs_rub"),
            "target_profit_rub": settings.get("target_profit_rub"),
            "target_profit_percent": settings.get("target_profit_percent"),
            "target_margin_rub": settings.get("target_margin_rub"),
            "target_margin_percent": settings.get("target_margin_percent"),
        }
    def _pick_settings_for_leaf(leaf_path: str) -> dict[str, Any] | None:
        direct = normalized_settings.get(leaf_path)
        if direct:
            return direct
        parts = [part.strip() for part in str(leaf_path or "").split(" / ") if part.strip()]
        for size in range(len(parts) - 1, 0, -1):
            candidate = normalized_settings.get(" / ".join(parts[:size]))
            if candidate:
                return candidate
        return None

    for leaf_path, row in normalized_rows.items():
        settings = _pick_settings_for_leaf(leaf_path)
        if not settings:
            continue
        for field in (
            "commission_percent",
            "acquiring_percent",
            "logistics_rub",
            "ads_percent",
            "returns_percent",
            "tax_percent",
            "other_expenses_rub",
            "other_expenses_percent",
            "cogs_rub",
            "target_profit_rub",
            "target_profit_percent",
            "target_margin_rub",
            "target_margin_percent",
        ):
            if row.get(field) is None and settings.get(field) is not None:
                row[field] = settings.get(field)
    return {
        "dataset_key": dkey,
        "rows": sorted(
            normalized_rows.values(),
            key=lambda item: (
                str(item.get("category") or ""),
                str(item.get("subcategory_1") or ""),
                str(item.get("subcategory_2") or ""),
                str(item.get("subcategory_3") or ""),
            ),
        ),
    }


def get_pricing_category_settings_map(
    *,
    dataset_key: str,
    store_uid: str | None = None,
) -> dict[str, dict[str, Any]]:
    init_store_data_model()
    dkey = str(dataset_key or "").strip()
    if not dkey:
        return {}
    suid = str(store_uid or "").strip()
    query = f"""
        SELECT
            leaf_path,
            commission_percent,
            acquiring_percent,
            logistics_rub,
            ads_percent,
            returns_percent,
            tax_percent,
            other_expenses_rub,
            other_expenses_percent,
            cogs_rub,
            target_profit_rub,
            target_profit_percent,
            target_margin_rub,
            target_margin_percent
        FROM pricing_category_settings
        WHERE dataset_key = {'%s' if is_postgres_backend() else '?'}
    """
    args: list[Any] = [dkey]
    if suid:
        query += f" AND store_uid = {'%s' if is_postgres_backend() else '?'}"
        args.append(suid)
    out: dict[str, dict[str, Any]] = {}
    with _connect() as conn:
        rows = conn.execute(query, args).fetchall()
    for row in rows:
        leaf_path = str(row["leaf_path"] or "").strip()
        if not leaf_path:
            continue
        out[leaf_path] = dict(row)
    return out


def replace_pricing_catalog_sku_paths(*, priority_platform: str, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    platform = str(priority_platform or "").strip().lower()
    if not platform:
        raise ValueError("priority_platform обязателен")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        levels = row.get("resolved_subcategory_levels")
        if not isinstance(levels, list):
            levels = []
        norm_levels = [str(v or "").strip() for v in levels if str(v or "").strip()][:5]
        norm_levels += [""] * (5 - len(norm_levels))
        prepared.append(
            (
                platform,
                sku,
                str(row.get("anchor_store_uid") or "").strip(),
                str(row.get("source_store_uid") or "").strip(),
                str(row.get("resolved_category") or "").strip(),
                norm_levels[0],
                norm_levels[1],
                norm_levels[2],
                norm_levels[3],
                norm_levels[4],
                str(row.get("leaf_path") or "").strip(),
                str(row.get("resolution_kind") or "undefined").strip(),
                now,
            )
        )
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_catalog_sku_paths WHERE priority_platform = {'%s' if is_postgres_backend() else '?'}",
            (platform,),
        )
        if prepared:
            values_sql = _placeholders(13)
            _executemany(conn, 
                f"""
                INSERT INTO pricing_catalog_sku_paths (
                    priority_platform, sku, anchor_store_uid, source_store_uid,
                    resolved_category, resolved_subcategory_1, resolved_subcategory_2, resolved_subcategory_3,
                    resolved_subcategory_4, resolved_subcategory_5, leaf_path, resolution_kind, updated_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def get_pricing_catalog_sku_path_map(
    *,
    priority_platform: str,
    skus: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    init_store_data_model()
    platform = str(priority_platform or "").strip().lower()
    if not platform:
        return {}
    sku_list = [str(x or "").strip() for x in (skus or []) if str(x or "").strip()]
    out: dict[str, dict[str, Any]] = {}
    try:
        with _connect() as conn:
            if sku_list:
                placeholders = _placeholders(len(sku_list))
                rows = conn.execute(
                    f"""
                    SELECT priority_platform, sku, anchor_store_uid, source_store_uid,
                           resolved_category, resolved_subcategory_1, resolved_subcategory_2, resolved_subcategory_3,
                           resolved_subcategory_4, resolved_subcategory_5, leaf_path, resolution_kind, updated_at
                    FROM pricing_catalog_sku_paths
                    WHERE priority_platform = {'%s' if is_postgres_backend() else '?'} AND sku IN ({placeholders})
                    """,
                    [platform, *sku_list],
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT priority_platform, sku, anchor_store_uid, source_store_uid,
                           resolved_category, resolved_subcategory_1, resolved_subcategory_2, resolved_subcategory_3,
                           resolved_subcategory_4, resolved_subcategory_5, leaf_path, resolution_kind, updated_at
                    FROM pricing_catalog_sku_paths
                    WHERE priority_platform = {'%s' if is_postgres_backend() else '?'}
                    """,
                    (platform,),
                ).fetchall()
    except Exception as exc:
        if "pricing_catalog_sku_paths" in str(exc):
            return {}
        raise
    for row in rows:
        out[str(row["sku"])] = dict(row)
    return out


def get_pricing_store_settings(*, store_uid: str) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    default = {
        "earning_mode": "margin",
        "earning_unit": "percent",
        "strategy_mode": "mix",
        "planned_revenue": None,
        "target_profit_rub": None,
        "target_profit_percent": None,
        "minimum_profit_percent": None,
        "target_margin_rub": None,
        "target_margin_percent": None,
        "target_drr_percent": None,
        "commission_percent": None,
        "acquiring_percent": None,
        "tax_percent": None,
        "ads_percent": None,
        "logistics_rub": None,
        "cogs_source_type": None,
        "cogs_source_id": None,
        "cogs_source_name": None,
        "cogs_sku_column": None,
        "cogs_value_column": None,
        "stock_source_type": None,
        "stock_source_id": None,
        "stock_source_name": None,
        "stock_sku_column": None,
        "stock_value_column": None,
        "overview_cogs_source_type": None,
        "overview_cogs_source_id": None,
        "overview_cogs_source_name": None,
        "overview_cogs_order_column": None,
        "overview_cogs_sku_column": None,
        "overview_cogs_value_column": None,
        "export_prices_source_type": None,
        "export_prices_source_id": None,
        "export_prices_source_name": None,
        "export_prices_sku_column": None,
        "export_prices_value_column": None,
        "export_ads_source_type": None,
        "export_ads_source_id": None,
        "export_ads_source_name": None,
        "export_ads_sku_column": None,
        "export_ads_value_column": None,
        "updated_at": None,
    }
    system_doc = _load_system_store_settings_document(store_uid=suid)
    merged_system = dict(default)
    for section_name in ("pricing", "sources", "export", "sales_plan"):
        section = system_doc.get(section_name)
        if isinstance(section, dict):
            merged_system.update(section)
    if any(merged_system.get(key) not in (None, "", [], {}) for key in merged_system if key != "updated_at"):
        return merged_system
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT earning_mode, earning_unit, planned_revenue, target_profit_rub, target_profit_percent, minimum_profit_percent, target_margin_rub, target_margin_percent,
                   strategy_mode,
                   target_drr_percent, cogs_source_type, cogs_source_id, cogs_source_name,
                   cogs_sku_column, cogs_value_column, stock_source_type, stock_source_id, stock_source_name,
                   stock_sku_column, stock_value_column, overview_cogs_source_type, overview_cogs_source_id, overview_cogs_source_name,
                   overview_cogs_order_column, overview_cogs_sku_column, overview_cogs_value_column,
                   export_prices_source_type, export_prices_source_id, export_prices_source_name, export_prices_sku_column, export_prices_value_column,
                   export_ads_source_type, export_ads_source_id, export_ads_source_name, export_ads_sku_column, export_ads_value_column,
                   updated_at
            FROM pricing_store_settings
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
            """,
            (suid,),
        ).fetchone()
    if not row:
        return default
    data = dict(row)
    if "overview_cogs_source_type" not in data:
        data["overview_cogs_source_type"] = None
        data["overview_cogs_source_id"] = None
        data["overview_cogs_source_name"] = None
        data["overview_cogs_order_column"] = None
        data["overview_cogs_sku_column"] = None
        data["overview_cogs_value_column"] = None
    for key in (
        "export_prices_source_type",
        "export_prices_source_id",
        "export_prices_source_name",
        "export_prices_sku_column",
        "export_prices_value_column",
        "export_ads_source_type",
        "export_ads_source_id",
        "export_ads_source_name",
        "export_ads_sku_column",
        "export_ads_value_column",
    ):
        if key not in data:
            data[key] = None
    return data


def upsert_pricing_store_settings(*, store_uid: str, values: dict[str, Any]) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")

    current = get_pricing_store_settings(store_uid=suid)
    next_values = dict(current)

    if "earning_mode" in values:
        v = str(values.get("earning_mode") or "").strip().lower()
        if v in {"profit", "margin"}:
            next_values["earning_mode"] = v
    if "earning_unit" in values:
        v = str(values.get("earning_unit") or "").strip().lower()
        if v in {"rub", "percent"}:
            next_values["earning_unit"] = v
    if "strategy_mode" in values:
        v = str(values.get("strategy_mode") or "").strip().lower()
        if v in {"mix", "mrc"}:
            next_values["strategy_mode"] = v

    for key in ("planned_revenue", "target_profit_rub", "target_profit_percent", "minimum_profit_percent", "target_margin_rub", "target_margin_percent", "target_drr_percent"):
        if key not in values:
            continue
        raw = values.get(key)
        if raw in ("", None):
            next_values[key] = None
            continue
        try:
            next_values[key] = float(str(raw).replace(",", "."))
        except Exception:
            next_values[key] = None

    for key in (
        "cogs_source_type",
        "cogs_source_id",
        "cogs_source_name",
        "cogs_sku_column",
        "cogs_value_column",
        "stock_source_type",
        "stock_source_id",
        "stock_source_name",
        "stock_sku_column",
        "stock_value_column",
        "overview_cogs_source_type",
        "overview_cogs_source_id",
        "overview_cogs_source_name",
        "overview_cogs_order_column",
        "overview_cogs_sku_column",
        "overview_cogs_value_column",
        "export_prices_source_type",
        "export_prices_source_id",
        "export_prices_source_name",
        "export_prices_sku_column",
        "export_prices_value_column",
        "export_ads_source_type",
        "export_ads_source_id",
        "export_ads_source_name",
        "export_ads_sku_column",
        "export_ads_value_column",
    ):
        if key in values:
            v = values.get(key)
            next_values[key] = str(v).strip() if v not in ("", None) else None

    now = _now_iso()
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO pricing_store_settings (
                store_uid, earning_mode, earning_unit, strategy_mode, planned_revenue, target_profit_rub, target_profit_percent, minimum_profit_percent, target_margin_rub, target_margin_percent,
                target_drr_percent, cogs_source_type, cogs_source_id, cogs_source_name,
                cogs_sku_column, cogs_value_column, stock_source_type, stock_source_id, stock_source_name,
                stock_sku_column, stock_value_column, overview_cogs_source_type, overview_cogs_source_id, overview_cogs_source_name,
                overview_cogs_order_column, overview_cogs_sku_column, overview_cogs_value_column,
                export_prices_source_type, export_prices_source_id, export_prices_source_name, export_prices_sku_column, export_prices_value_column,
                export_ads_source_type, export_ads_source_id, export_ads_source_name, export_ads_sku_column, export_ads_value_column,
                updated_at
            ) VALUES ({_placeholders(38)})
            ON CONFLICT(store_uid) DO UPDATE SET
                earning_mode = excluded.earning_mode,
                earning_unit = excluded.earning_unit,
                strategy_mode = excluded.strategy_mode,
                planned_revenue = excluded.planned_revenue,
                target_profit_rub = excluded.target_profit_rub,
                target_profit_percent = excluded.target_profit_percent,
                minimum_profit_percent = excluded.minimum_profit_percent,
                target_margin_rub = excluded.target_margin_rub,
                target_margin_percent = excluded.target_margin_percent,
                target_drr_percent = excluded.target_drr_percent,
                cogs_source_type = excluded.cogs_source_type,
                cogs_source_id = excluded.cogs_source_id,
                cogs_source_name = excluded.cogs_source_name,
                cogs_sku_column = excluded.cogs_sku_column,
                cogs_value_column = excluded.cogs_value_column,
                stock_source_type = excluded.stock_source_type,
                stock_source_id = excluded.stock_source_id,
                stock_source_name = excluded.stock_source_name,
                stock_sku_column = excluded.stock_sku_column,
                stock_value_column = excluded.stock_value_column,
                overview_cogs_source_type = excluded.overview_cogs_source_type,
                overview_cogs_source_id = excluded.overview_cogs_source_id,
                overview_cogs_source_name = excluded.overview_cogs_source_name,
                overview_cogs_order_column = excluded.overview_cogs_order_column,
                overview_cogs_sku_column = excluded.overview_cogs_sku_column,
                overview_cogs_value_column = excluded.overview_cogs_value_column,
                export_prices_source_type = excluded.export_prices_source_type,
                export_prices_source_id = excluded.export_prices_source_id,
                export_prices_source_name = excluded.export_prices_source_name,
                export_prices_sku_column = excluded.export_prices_sku_column,
                export_prices_value_column = excluded.export_prices_value_column,
                export_ads_source_type = excluded.export_ads_source_type,
                export_ads_source_id = excluded.export_ads_source_id,
                export_ads_source_name = excluded.export_ads_source_name,
                export_ads_sku_column = excluded.export_ads_sku_column,
                export_ads_value_column = excluded.export_ads_value_column,
                updated_at = excluded.updated_at
            """,
            (
                suid,
                next_values.get("earning_mode") or "margin",
                next_values.get("earning_unit") or "percent",
                next_values.get("strategy_mode") or "mix",
                next_values.get("planned_revenue"),
                next_values.get("target_profit_rub"),
                next_values.get("target_profit_percent"),
                next_values.get("minimum_profit_percent"),
                next_values.get("target_margin_rub"),
                next_values.get("target_margin_percent"),
                next_values.get("target_drr_percent"),
                next_values.get("cogs_source_type"),
                next_values.get("cogs_source_id"),
                next_values.get("cogs_source_name"),
                next_values.get("cogs_sku_column"),
                next_values.get("cogs_value_column"),
                next_values.get("stock_source_type"),
                next_values.get("stock_source_id"),
                next_values.get("stock_source_name"),
                next_values.get("stock_sku_column"),
                next_values.get("stock_value_column"),
                next_values.get("overview_cogs_source_type"),
                next_values.get("overview_cogs_source_id"),
                next_values.get("overview_cogs_source_name"),
                next_values.get("overview_cogs_order_column"),
                next_values.get("overview_cogs_sku_column"),
                next_values.get("overview_cogs_value_column"),
                next_values.get("export_prices_source_type"),
                next_values.get("export_prices_source_id"),
                next_values.get("export_prices_source_name"),
                next_values.get("export_prices_sku_column"),
                next_values.get("export_prices_value_column"),
                next_values.get("export_ads_source_type"),
                next_values.get("export_ads_source_id"),
                next_values.get("export_ads_source_name"),
                next_values.get("export_ads_sku_column"),
                next_values.get("export_ads_value_column"),
                now,
            ),
        )
        conn.commit()
    next_values["updated_at"] = now
    pricing_payload = {
        "earning_mode": next_values.get("earning_mode"),
        "earning_unit": next_values.get("earning_unit"),
        "strategy_mode": next_values.get("strategy_mode"),
        "planned_revenue": next_values.get("planned_revenue"),
        "target_profit_rub": next_values.get("target_profit_rub"),
        "target_profit_percent": next_values.get("target_profit_percent"),
        "minimum_profit_percent": next_values.get("minimum_profit_percent"),
        "target_margin_rub": next_values.get("target_margin_rub"),
        "target_margin_percent": next_values.get("target_margin_percent"),
        "target_drr_percent": next_values.get("target_drr_percent"),
    }
    sources_payload = {
        "cogs_source_type": next_values.get("cogs_source_type"),
        "cogs_source_id": next_values.get("cogs_source_id"),
        "cogs_source_name": next_values.get("cogs_source_name"),
        "cogs_sku_column": next_values.get("cogs_sku_column"),
        "cogs_value_column": next_values.get("cogs_value_column"),
        "stock_source_type": next_values.get("stock_source_type"),
        "stock_source_id": next_values.get("stock_source_id"),
        "stock_source_name": next_values.get("stock_source_name"),
        "stock_sku_column": next_values.get("stock_sku_column"),
        "stock_value_column": next_values.get("stock_value_column"),
        "overview_cogs_source_type": next_values.get("overview_cogs_source_type"),
        "overview_cogs_source_id": next_values.get("overview_cogs_source_id"),
        "overview_cogs_source_name": next_values.get("overview_cogs_source_name"),
        "overview_cogs_order_column": next_values.get("overview_cogs_order_column"),
        "overview_cogs_sku_column": next_values.get("overview_cogs_sku_column"),
        "overview_cogs_value_column": next_values.get("overview_cogs_value_column"),
    }
    export_payload = {
        "export_prices_source_type": next_values.get("export_prices_source_type"),
        "export_prices_source_id": next_values.get("export_prices_source_id"),
        "export_prices_source_name": next_values.get("export_prices_source_name"),
        "export_prices_sku_column": next_values.get("export_prices_sku_column"),
        "export_prices_value_column": next_values.get("export_prices_value_column"),
        "export_ads_source_type": next_values.get("export_ads_source_type"),
        "export_ads_source_id": next_values.get("export_ads_source_id"),
        "export_ads_source_name": next_values.get("export_ads_source_name"),
        "export_ads_sku_column": next_values.get("export_ads_sku_column"),
        "export_ads_value_column": next_values.get("export_ads_value_column"),
    }
    sales_plan_payload = {
        "planned_revenue": next_values.get("planned_revenue"),
        "target_profit_rub": next_values.get("target_profit_rub"),
        "target_profit_percent": next_values.get("target_profit_percent"),
        "minimum_profit_percent": next_values.get("minimum_profit_percent"),
        "target_margin_rub": next_values.get("target_margin_rub"),
        "target_margin_percent": next_values.get("target_margin_percent"),
        "target_drr_percent": next_values.get("target_drr_percent"),
        "strategy_mode": next_values.get("strategy_mode"),
    }
    _merge_system_store_settings_sections(
        store_uid=suid,
        sections={
            "pricing": pricing_payload,
            "sources": sources_payload,
            "export": export_payload,
            "sales_plan": sales_plan_payload,
        },
        updated_at=now,
    )
    return next_values


def get_monitoring_export_snapshot() -> dict[str, Any]:
    init_store_data_model()
    rows: list[dict[str, Any]] = []
    with _connect() as conn:
        store_rows = conn.execute(
            """
            SELECT
                s.store_uid,
                s.platform,
                s.store_id,
                s.store_name,
                COALESCE(ps.export_prices_source_type, '') AS export_prices_source_type,
                COALESCE(ps.export_prices_source_id, '') AS export_prices_source_id,
                COALESCE(ps.export_prices_source_name, '') AS export_prices_source_name,
                COALESCE(ps.export_prices_sku_column, '') AS export_prices_sku_column,
                COALESCE(ps.export_prices_value_column, '') AS export_prices_value_column,
                COALESCE(ps.export_ads_source_type, '') AS export_ads_source_type,
                COALESCE(ps.export_ads_source_id, '') AS export_ads_source_id,
                COALESCE(ps.export_ads_source_name, '') AS export_ads_source_name,
                COALESCE(ps.export_ads_sku_column, '') AS export_ads_sku_column,
                COALESCE(ps.export_ads_value_column, '') AS export_ads_value_column
            FROM stores s
            LEFT JOIN pricing_store_settings ps ON ps.store_uid = s.store_uid
            WHERE TRIM(COALESCE(s.platform, '')) <> ''
            ORDER BY LOWER(COALESCE(s.platform, '')) ASC, LOWER(COALESCE(s.store_name, '')) ASC, LOWER(COALESCE(s.store_id, '')) ASC
            """
        ).fetchall()
    for row in store_rows:
        item = dict(row)
        rows.append(
            {
                "store_uid": str(item.get("store_uid") or "").strip(),
                "platform": str(item.get("platform") or "").strip(),
                "store_id": str(item.get("store_id") or "").strip(),
                "store_name": str(item.get("store_name") or item.get("store_id") or "").strip(),
                "export_prices": {
                    "type": str(item.get("export_prices_source_type") or "").strip() or None,
                    "sourceId": str(item.get("export_prices_source_id") or "").strip() or None,
                    "sourceName": str(item.get("export_prices_source_name") or "").strip() or None,
                    "skuColumn": str(item.get("export_prices_sku_column") or "").strip() or None,
                    "valueColumn": str(item.get("export_prices_value_column") or "").strip() or None,
                },
                "export_ads": {
                    "type": str(item.get("export_ads_source_type") or "").strip() or None,
                    "sourceId": str(item.get("export_ads_source_id") or "").strip() or None,
                    "sourceName": str(item.get("export_ads_source_name") or "").strip() or None,
                    "skuColumn": str(item.get("export_ads_sku_column") or "").strip() or None,
                    "valueColumn": str(item.get("export_ads_value_column") or "").strip() or None,
                },
            }
        )
    return {"ok": True, "rows": rows}


def get_pricing_logistics_store_settings(*, store_uid: str) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    default = {
        "fulfillment_model": "FBO",
        "handling_mode": "fixed",
        "handling_fixed_amount": None,
        "handling_percent": None,
        "handling_min_amount": None,
        "handling_max_amount": None,
        "delivery_cost_per_kg": None,
        "return_processing_cost": None,
        "disposal_cost": None,
        "updated_at": None,
    }
    system_doc = _load_system_store_settings_document(store_uid=suid)
    section = system_doc.get("logistics")
    if isinstance(section, dict) and any(section.get(key) not in (None, "", [], {}) for key in section):
        merged = dict(default)
        merged.update(section)
        return merged
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT fulfillment_model, handling_mode, handling_fixed_amount, handling_percent,
                   handling_min_amount, handling_max_amount, delivery_cost_per_kg,
                   return_processing_cost, disposal_cost, updated_at
            FROM pricing_logistics_store_settings
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
            """,
            (suid,),
        ).fetchone()
    if not row:
        return default
    return dict(row)


def upsert_pricing_price_results_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                _row_value(row, "cogs_price", "cogs_price", "cogs"),
                row.get("rrc_no_ads_price"),
                row.get("rrc_no_ads_profit_abs"),
                row.get("rrc_no_ads_profit_pct"),
                row.get("mrc_price"),
                row.get("mrc_profit_abs"),
                row.get("mrc_profit_pct"),
                row.get("mrc_with_boost_price"),
                row.get("mrc_with_boost_profit_abs"),
                row.get("mrc_with_boost_profit_pct"),
                row.get("target_price"),
                row.get("target_profit_abs"),
                row.get("target_profit_pct"),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(17)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_price_results (
                store_uid, sku, cogs_price, rrc_no_ads_price, rrc_no_ads_profit_abs, rrc_no_ads_profit_pct,
                mrc_price, mrc_profit_abs, mrc_profit_pct,
                mrc_with_boost_price, mrc_with_boost_profit_abs, mrc_with_boost_profit_pct,
                target_price, target_profit_abs, target_profit_pct, source_updated_at, calculated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                cogs_price = excluded.cogs_price,
                rrc_no_ads_price = excluded.rrc_no_ads_price,
                rrc_no_ads_profit_abs = excluded.rrc_no_ads_profit_abs,
                rrc_no_ads_profit_pct = excluded.rrc_no_ads_profit_pct,
                mrc_price = excluded.mrc_price,
                mrc_profit_abs = excluded.mrc_profit_abs,
                mrc_profit_pct = excluded.mrc_profit_pct,
                mrc_with_boost_price = excluded.mrc_with_boost_price,
                mrc_with_boost_profit_abs = excluded.mrc_with_boost_profit_abs,
                mrc_with_boost_profit_pct = excluded.mrc_with_boost_profit_pct,
                target_price = excluded.target_price,
                target_profit_abs = excluded.target_profit_abs,
                target_profit_pct = excluded.target_profit_pct,
                source_updated_at = excluded.source_updated_at,
                calculated_at = excluded.calculated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def get_pricing_price_results_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    with _connect() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            SELECT store_uid, sku, cogs_price, rrc_no_ads_price, rrc_no_ads_profit_abs, rrc_no_ads_profit_pct,
                   mrc_price, mrc_profit_abs, mrc_profit_pct,
                   mrc_with_boost_price, mrc_with_boost_profit_abs, mrc_with_boost_profit_pct,
                   target_price, target_profit_abs, target_profit_pct,
                   source_updated_at, calculated_at
            FROM pricing_price_results
            WHERE store_uid IN ({suid_placeholders}) AND sku IN ({sku_placeholders})
            """,
            [*suids, *sku_list],
        ).fetchall()
        for row in rows:
            item = _with_price_result_aliases(dict(row))
            suid = str(row["store_uid"])
            out.setdefault(suid, {})[str(row["sku"])] = item
    return out


def clear_pricing_price_results_for_store(*, store_uid: str) -> None:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_price_results WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        conn.commit()


def clear_pricing_boost_results_for_store(*, store_uid: str) -> None:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_boost_results WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        conn.commit()


def upsert_pricing_boost_results_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                row.get("recommended_bid"),
                row.get("bid_30"),
                row.get("bid_60"),
                row.get("bid_80"),
                row.get("bid_95"),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(9)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_boost_results (
                store_uid, sku, recommended_bid, bid_30, bid_60, bid_80, bid_95, source_updated_at, calculated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                recommended_bid = excluded.recommended_bid,
                bid_30 = excluded.bid_30,
                bid_60 = excluded.bid_60,
                bid_80 = excluded.bid_80,
                bid_95 = excluded.bid_95,
                source_updated_at = excluded.source_updated_at,
                calculated_at = excluded.calculated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)

def append_pricing_market_price_export_history_bulk(
    *,
    store_uid: str,
    campaign_id: str,
    rows: list[dict[str, Any]],
    requested_at: str | None = None,
    source: str = "strategy",
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    campaign = str(campaign_id or "").strip()
    when = str(requested_at or _now_iso()).strip() or _now_iso()
    src = str(source or "strategy").strip() or "strategy"
    if not suid or not rows:
        return 0
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        prepared.append((suid, sku, when, campaign, row.get("price"), src))
    if not prepared:
        return 0
    values_sql = _placeholders(6)
    with _connect_history() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_market_price_export_history (
                store_uid, sku, requested_at, campaign_id, price, source
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku, requested_at) DO UPDATE SET
                campaign_id = excluded.campaign_id,
                price = excluded.price,
                source = excluded.source
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def get_pricing_boost_results_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {}
    with _connect() as conn:
        for suid in suids:
            placeholders = _placeholders(len(sku_list))
            rows = conn.execute(
                f"""
                SELECT store_uid, sku, recommended_bid, bid_30, bid_60, bid_80, bid_95, source_updated_at, calculated_at
                FROM pricing_boost_results
                WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND sku IN ({placeholders})
                """,
                [suid, *sku_list],
            ).fetchall()
            local: dict[str, dict[str, Any]] = {}
            for row in rows:
                local[str(row["sku"])] = dict(row)
            out[suid] = local
    return out


def clear_pricing_strategy_results_for_store(*, store_uid: str) -> None:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_strategy_results WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        conn.commit()


def upsert_pricing_strategy_results_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                str(row.get("strategy_code") or "base").strip() or "base",
                str(row.get("strategy_label") or "Базовая").strip() or "Базовая",
                row.get("rrc_price"),
                row.get("rrc_profit_abs"),
                row.get("rrc_profit_pct"),
                row.get("mrc_price"),
                row.get("mrc_profit_abs"),
                row.get("mrc_profit_pct"),
                row.get("mrc_with_boost_price"),
                row.get("mrc_with_boost_profit_abs"),
                row.get("mrc_with_boost_profit_pct"),
                row.get("promo_price"),
                row.get("promo_profit_abs"),
                row.get("promo_profit_pct"),
                row.get("attractiveness_price"),
                row.get("attractiveness_profit_abs"),
                row.get("attractiveness_profit_pct"),
                row.get("installed_price"),
                row.get("installed_profit_abs"),
                row.get("installed_profit_pct"),
                row.get("boost_bid_percent"),
                str(row.get("decision_code") or "observe").strip() or "observe",
                str(row.get("decision_label") or "Наблюдать").strip() or "Наблюдать",
                str(row.get("decision_tone") or "warning").strip() or "warning",
                str(row.get("hypothesis") or "").strip(),
                str(row.get("hypothesis_started_at") or "").strip(),
                str(row.get("hypothesis_expires_at") or "").strip(),
                str(row.get("control_state") or "stable").strip() or "stable",
                str(row.get("control_state_started_at") or row.get("hypothesis_started_at") or "").strip(),
                str(row.get("attractiveness_status") or "").strip(),
                json.dumps(_normalize_json_array(row.get("promo_items")), ensure_ascii=False),
                1 if row.get("uses_promo") else 0,
                1 if row.get("uses_attractiveness") else 0,
                1 if row.get("uses_boost") else 0,
                str(row.get("market_promo_status") or "").strip(),
                str(row.get("market_promo_checked_at") or "").strip(),
                str(row.get("market_promo_message") or "").strip(),
                str(row.get("selected_iteration_code") or "").strip(),
                json.dumps(row.get("scenario_matrix") or {}, ensure_ascii=False),
                str(row.get("cycle_started_at") or "").strip(),
                row.get("market_boost_bid_percent"),
                row.get("boost_share"),
                int(row.get("promo_count") or 0),
                row.get("coinvest_pct"),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(48)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_strategy_results (
                store_uid, sku, strategy_code, strategy_label, rrc_price, rrc_profit_abs, rrc_profit_pct,
                mrc_price, mrc_profit_abs, mrc_profit_pct,
                mrc_with_boost_price, mrc_with_boost_profit_abs, mrc_with_boost_profit_pct,
                promo_price, promo_profit_abs, promo_profit_pct,
                attractiveness_price, attractiveness_profit_abs, attractiveness_profit_pct,
                installed_price, installed_profit_abs, installed_profit_pct, boost_bid_percent,
                decision_code, decision_label, decision_tone, hypothesis, hypothesis_started_at, hypothesis_expires_at,
                control_state, control_state_started_at,
                attractiveness_status, promo_items_json, uses_promo, uses_attractiveness, uses_boost,
                market_promo_status, market_promo_checked_at, market_promo_message,
                selected_iteration_code, scenario_matrix_json, cycle_started_at, market_boost_bid_percent, boost_share, promo_count, coinvest_pct,
                source_updated_at, calculated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                strategy_code = excluded.strategy_code,
                strategy_label = excluded.strategy_label,
                rrc_price = excluded.rrc_price,
                rrc_profit_abs = excluded.rrc_profit_abs,
                rrc_profit_pct = excluded.rrc_profit_pct,
                mrc_price = excluded.mrc_price,
                mrc_profit_abs = excluded.mrc_profit_abs,
                mrc_profit_pct = excluded.mrc_profit_pct,
                mrc_with_boost_price = excluded.mrc_with_boost_price,
                mrc_with_boost_profit_abs = excluded.mrc_with_boost_profit_abs,
                mrc_with_boost_profit_pct = excluded.mrc_with_boost_profit_pct,
                promo_price = excluded.promo_price,
                promo_profit_abs = excluded.promo_profit_abs,
                promo_profit_pct = excluded.promo_profit_pct,
                attractiveness_price = excluded.attractiveness_price,
                attractiveness_profit_abs = excluded.attractiveness_profit_abs,
                attractiveness_profit_pct = excluded.attractiveness_profit_pct,
                installed_price = excluded.installed_price,
                installed_profit_abs = excluded.installed_profit_abs,
                installed_profit_pct = excluded.installed_profit_pct,
                boost_bid_percent = excluded.boost_bid_percent,
                decision_code = excluded.decision_code,
                decision_label = excluded.decision_label,
                decision_tone = excluded.decision_tone,
                hypothesis = excluded.hypothesis,
                hypothesis_started_at = excluded.hypothesis_started_at,
                hypothesis_expires_at = excluded.hypothesis_expires_at,
                control_state = excluded.control_state,
                control_state_started_at = excluded.control_state_started_at,
                attractiveness_status = excluded.attractiveness_status,
                promo_items_json = excluded.promo_items_json,
                uses_promo = excluded.uses_promo,
                uses_attractiveness = excluded.uses_attractiveness,
                uses_boost = excluded.uses_boost,
                market_promo_status = excluded.market_promo_status,
                market_promo_checked_at = excluded.market_promo_checked_at,
                market_promo_message = excluded.market_promo_message,
                selected_iteration_code = excluded.selected_iteration_code,
                scenario_matrix_json = excluded.scenario_matrix_json,
                cycle_started_at = excluded.cycle_started_at,
                market_boost_bid_percent = excluded.market_boost_bid_percent,
                boost_share = excluded.boost_share,
                promo_count = excluded.promo_count,
                coinvest_pct = excluded.coinvest_pct,
                source_updated_at = excluded.source_updated_at,
                calculated_at = excluded.calculated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def append_pricing_strategy_history_bulk(*, rows: list[dict[str, Any]], captured_at: str | None = None) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = str(captured_at or _now_iso()).strip() or _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                now,
                str(row.get("cycle_started_at") or "").strip(),
                str(row.get("strategy_code") or "base").strip() or "base",
                str(row.get("strategy_label") or "Базовая").strip() or "Базовая",
                row.get("installed_price"),
                row.get("installed_profit_abs"),
                row.get("installed_profit_pct"),
                row.get("boost_bid_percent"),
                row.get("market_boost_bid_percent"),
                row.get("boost_share"),
                str(row.get("decision_code") or "observe").strip() or "observe",
                str(row.get("decision_label") or "Наблюдать").strip() or "Наблюдать",
                str(row.get("decision_tone") or "warning").strip() or "warning",
                str(row.get("hypothesis") or "").strip(),
                str(row.get("hypothesis_started_at") or "").strip(),
                str(row.get("hypothesis_expires_at") or "").strip(),
                str(row.get("control_state") or "stable").strip() or "stable",
                str(row.get("control_state_started_at") or row.get("hypothesis_started_at") or "").strip(),
                str(row.get("attractiveness_status") or "").strip(),
                int(row.get("promo_count") or 0),
                row.get("coinvest_pct"),
                str(row.get("selected_iteration_code") or "").strip(),
                1 if row.get("uses_promo") else 0,
                1 if row.get("uses_attractiveness") else 0,
                1 if row.get("uses_boost") else 0,
                str(row.get("market_promo_status") or "").strip(),
                str(row.get("market_promo_checked_at") or "").strip(),
                str(row.get("market_promo_message") or "").strip(),
                row.get("source_updated_at"),
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(31)
    with _connect_history() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_strategy_history (
                store_uid, sku, captured_at, cycle_started_at, strategy_code, strategy_label,
                installed_price, installed_profit_abs, installed_profit_pct, boost_bid_percent, market_boost_bid_percent, boost_share,
                decision_code, decision_label, decision_tone, hypothesis, hypothesis_started_at, hypothesis_expires_at,
                control_state, control_state_started_at,
                attractiveness_status, promo_count, coinvest_pct, selected_iteration_code,
                uses_promo, uses_attractiveness, uses_boost,
                market_promo_status, market_promo_checked_at, market_promo_message, source_updated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku, captured_at) DO UPDATE SET
                cycle_started_at = excluded.cycle_started_at,
                strategy_code = excluded.strategy_code,
                strategy_label = excluded.strategy_label,
                installed_price = excluded.installed_price,
                installed_profit_abs = excluded.installed_profit_abs,
                installed_profit_pct = excluded.installed_profit_pct,
                boost_bid_percent = excluded.boost_bid_percent,
                market_boost_bid_percent = excluded.market_boost_bid_percent,
                boost_share = excluded.boost_share,
                decision_code = excluded.decision_code,
                decision_label = excluded.decision_label,
                decision_tone = excluded.decision_tone,
                hypothesis = excluded.hypothesis,
                hypothesis_started_at = excluded.hypothesis_started_at,
                hypothesis_expires_at = excluded.hypothesis_expires_at,
                control_state = excluded.control_state,
                control_state_started_at = excluded.control_state_started_at,
                attractiveness_status = excluded.attractiveness_status,
                promo_count = excluded.promo_count,
                coinvest_pct = excluded.coinvest_pct,
                selected_iteration_code = excluded.selected_iteration_code,
                uses_promo = excluded.uses_promo,
                uses_attractiveness = excluded.uses_attractiveness,
                uses_boost = excluded.uses_boost,
                market_promo_status = excluded.market_promo_status,
                market_promo_checked_at = excluded.market_promo_checked_at,
                market_promo_message = excluded.market_promo_message,
                source_updated_at = excluded.source_updated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def get_pricing_strategy_history_rows(
    *,
    store_uid: str,
    skus: list[str],
    captured_at_to: str = "",
) -> list[dict[str, Any]]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suid or not sku_list:
        return []
    placeholders = _placeholders(len(sku_list))
    params: list[Any] = [suid, *sku_list]
    where_extra = ""
    cutoff = str(captured_at_to or "").strip()
    if cutoff:
        where_extra = f" AND captured_at <= {'%s' if is_postgres_backend() else '?'}"
        params.append(cutoff)
    with _connect_history() as conn:
        rows = conn.execute(
            f"""
            SELECT store_uid, sku, captured_at, strategy_code, strategy_label,
                   installed_price, installed_profit_abs, installed_profit_pct, boost_bid_percent,
                   cycle_started_at, market_boost_bid_percent, boost_share, promo_count, coinvest_pct, selected_iteration_code,
                   decision_code, decision_label, decision_tone, hypothesis, hypothesis_started_at, hypothesis_expires_at,
                   control_state, control_state_started_at,
                   attractiveness_status, uses_promo, uses_attractiveness, uses_boost,
                   market_promo_status, market_promo_checked_at, market_promo_message, source_updated_at
            FROM pricing_strategy_history
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
              AND sku IN ({placeholders})
              {where_extra}
            ORDER BY sku ASC, captured_at ASC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_pricing_strategy_results_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    with _connect() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            SELECT store_uid, sku, strategy_code, strategy_label, rrc_price, rrc_profit_abs, rrc_profit_pct,
                   mrc_price, mrc_profit_abs, mrc_profit_pct,
                   mrc_with_boost_price, mrc_with_boost_profit_abs, mrc_with_boost_profit_pct,
                   promo_price, promo_profit_abs, promo_profit_pct,
                   attractiveness_price, attractiveness_profit_abs, attractiveness_profit_pct,
                   installed_price, installed_profit_abs, installed_profit_pct, boost_bid_percent,
                   cycle_started_at, market_boost_bid_percent, boost_share, promo_count, coinvest_pct,
                   decision_code, decision_label, decision_tone, hypothesis, hypothesis_started_at, hypothesis_expires_at,
                   control_state, control_state_started_at,
                   attractiveness_status, promo_items_json, uses_promo, uses_attractiveness, uses_boost,
                   market_promo_status, market_promo_checked_at, market_promo_message,
                   selected_iteration_code, scenario_matrix_json,
                   source_updated_at, calculated_at
            FROM pricing_strategy_results
            WHERE store_uid IN ({suid_placeholders}) AND sku IN ({sku_placeholders})
            """,
            [*suids, *sku_list],
        ).fetchall()
        for row in rows:
            item = dict(row)
            try:
                item["promo_items"] = json.loads(item.get("promo_items_json") or "[]")
            except Exception:
                item["promo_items"] = []
            try:
                item["scenario_matrix"] = json.loads(item.get("scenario_matrix_json") or "{}")
            except Exception:
                item["scenario_matrix"] = {}
            suid = str(row["store_uid"])
            out.setdefault(suid, {})[str(row["sku"])] = item
    return out


def update_pricing_strategy_market_promo_feedback(
    *,
    store_uid: str,
    feedback_by_sku: dict[str, dict[str, Any]],
    checked_at: str | None = None,
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid or not isinstance(feedback_by_sku, dict):
        return 0
    checked = str(checked_at or _now_iso()).strip() or _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for raw_sku, payload in feedback_by_sku.items():
        sku = str(raw_sku or "").strip()
        if not sku or not isinstance(payload, dict):
            continue
        prepared.append(
            (
                str(payload.get("market_promo_status") or "").strip(),
                checked,
                str(payload.get("market_promo_message") or "").strip(),
                suid,
                sku,
            )
        )
    if not prepared:
        return 0
    with _connect() as conn:
        _executemany(conn, 
            f"""
            UPDATE pricing_strategy_results
            SET market_promo_status = {'%s' if is_postgres_backend() else '?'},
                market_promo_checked_at = {'%s' if is_postgres_backend() else '?'},
                market_promo_message = {'%s' if is_postgres_backend() else '?'}
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND sku = {'%s' if is_postgres_backend() else '?'}
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def append_pricing_strategy_iteration_history_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        cycle_started_at = str(row.get("cycle_started_at") or "").strip()
        iteration_code = str(row.get("iteration_code") or "").strip()
        if not suid or not sku or not cycle_started_at or not iteration_code:
            continue
        prepared.append(
            (
                suid,
                sku,
                cycle_started_at,
                iteration_code,
                str(row.get("iteration_label") or "").strip(),
                row.get("tested_price"),
                row.get("tested_boost_pct"),
                row.get("market_boost_bid_percent"),
                row.get("boost_share"),
                int(row.get("promo_count") or 0),
                str(row.get("attractiveness_status") or "").strip(),
                row.get("coinvest_pct"),
                row.get("on_display_price"),
                json.dumps(_normalize_json_array(row.get("promo_details")), ensure_ascii=False),
                str(row.get("market_promo_status") or "").strip(),
                str(row.get("market_promo_message") or "").strip(),
                row.get("source_updated_at"),
                str(row.get("captured_at") or _now_iso()).strip() or _now_iso(),
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(18)
    with _connect_history() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_strategy_iteration_history (
                store_uid, sku, cycle_started_at, iteration_code, iteration_label,
                tested_price, tested_boost_pct, market_boost_bid_percent, boost_share, promo_count, attractiveness_status,
                coinvest_pct, on_display_price, promo_details_json,
                market_promo_status, market_promo_message, source_updated_at, captured_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku, cycle_started_at, iteration_code) DO UPDATE SET
                iteration_label = excluded.iteration_label,
                tested_price = excluded.tested_price,
                tested_boost_pct = excluded.tested_boost_pct,
                market_boost_bid_percent = excluded.market_boost_bid_percent,
                boost_share = excluded.boost_share,
                promo_count = excluded.promo_count,
                attractiveness_status = excluded.attractiveness_status,
                coinvest_pct = excluded.coinvest_pct,
                on_display_price = excluded.on_display_price,
                promo_details_json = excluded.promo_details_json,
                market_promo_status = excluded.market_promo_status,
                market_promo_message = excluded.market_promo_message,
                source_updated_at = excluded.source_updated_at,
                captured_at = excluded.captured_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def get_pricing_strategy_iteration_latest_map(
    *,
    store_uids: list[str],
    skus: list[str],
) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, dict[str, Any]]]] = {suid: {} for suid in suids}
    with _connect_history() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        cycle_rows = conn.execute(
            f"""
            SELECT store_uid, sku, cycle_started_at, COUNT(DISTINCT iteration_code) AS iteration_count
            FROM pricing_strategy_iteration_history
            WHERE store_uid IN ({suid_placeholders})
              AND sku IN ({sku_placeholders})
            GROUP BY store_uid, sku, cycle_started_at
            ORDER BY store_uid, sku, cycle_started_at DESC
            """,
            [*suids, *sku_list],
        ).fetchall()
        latest_cycle_by_store_sku: dict[tuple[str, str], str] = {}
        for row in cycle_rows:
            item = dict(row)
            suid = str(item.get("store_uid") or "").strip()
            sku = str(item.get("sku") or "").strip()
            cycle_started_at = str(item.get("cycle_started_at") or "").strip()
            if not suid or not sku or not cycle_started_at:
                continue
            latest_cycle_by_store_sku.setdefault((suid, sku), cycle_started_at)
        if not latest_cycle_by_store_sku:
            return out
        rows = conn.execute(
            f"""
            SELECT outer_rows.store_uid, outer_rows.sku, outer_rows.cycle_started_at, outer_rows.iteration_code, outer_rows.iteration_label,
                   outer_rows.tested_price, outer_rows.tested_boost_pct, outer_rows.promo_count, outer_rows.attractiveness_status,
                   outer_rows.coinvest_pct, outer_rows.on_display_price, outer_rows.promo_details_json,
                   outer_rows.market_promo_status, outer_rows.market_promo_message, outer_rows.source_updated_at, outer_rows.captured_at
            FROM pricing_strategy_iteration_history AS outer_rows
            JOIN (
                SELECT store_uid, sku, cycle_started_at, iteration_code, MAX(captured_at) AS latest_captured_at
                FROM pricing_strategy_iteration_history
                WHERE store_uid IN ({suid_placeholders})
                  AND sku IN ({sku_placeholders})
                GROUP BY store_uid, sku, cycle_started_at, iteration_code
            ) AS latest
              ON latest.store_uid = outer_rows.store_uid
             AND latest.sku = outer_rows.sku
             AND latest.cycle_started_at = outer_rows.cycle_started_at
             AND latest.iteration_code = outer_rows.iteration_code
             AND latest.latest_captured_at = outer_rows.captured_at
            WHERE outer_rows.store_uid IN ({suid_placeholders})
              AND outer_rows.sku IN ({sku_placeholders})
            """,
            [*suids, *sku_list, *suids, *sku_list],
        ).fetchall()
        for row in rows:
            item = dict(row)
            suid = str(item.get("store_uid") or "").strip()
            sku = str(item.get("sku") or "").strip()
            cycle_started_at = str(item.get("cycle_started_at") or "").strip()
            if latest_cycle_by_store_sku.get((suid, sku)) != cycle_started_at:
                continue
            try:
                item["promo_details"] = json.loads(item.get("promo_details_json") or "[]")
            except Exception:
                item["promo_details"] = []
            out.setdefault(suid, {}).setdefault(sku, {})[str(item.get("iteration_code") or "").strip()] = item
    return out


def upsert_pricing_attractiveness_results_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                _row_value(row, "attractiveness_overpriced_price", "non_profitable_price"),
                _row_value(row, "attractiveness_moderate_price", "moderate_price"),
                _row_value(row, "attractiveness_profitable_price", "profitable_price"),
                row.get("ozon_competitor_price"),
                row.get("external_competitor_price"),
                _row_value(row, "attractiveness_chosen_price", "chosen_price"),
                row.get("attractiveness_chosen_boost_bid_percent"),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(11)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_attractiveness_results (
                store_uid, sku,
                attractiveness_overpriced_price,
                attractiveness_moderate_price,
                attractiveness_profitable_price,
                ozon_competitor_price,
                external_competitor_price,
                attractiveness_chosen_price,
                attractiveness_chosen_boost_bid_percent,
                source_updated_at, calculated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                attractiveness_overpriced_price = excluded.attractiveness_overpriced_price,
                attractiveness_moderate_price = excluded.attractiveness_moderate_price,
                attractiveness_profitable_price = excluded.attractiveness_profitable_price,
                ozon_competitor_price = excluded.ozon_competitor_price,
                external_competitor_price = excluded.external_competitor_price,
                attractiveness_chosen_price = excluded.attractiveness_chosen_price,
                attractiveness_chosen_boost_bid_percent = excluded.attractiveness_chosen_boost_bid_percent,
                source_updated_at = excluded.source_updated_at,
                calculated_at = excluded.calculated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def upsert_pricing_attractiveness_recommendations_raw_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                row.get("attractiveness_overpriced_price"),
                row.get("attractiveness_moderate_price"),
                row.get("attractiveness_profitable_price"),
                json.dumps(row.get("payload") or {}, ensure_ascii=False, default=str),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(8)
    with _connect() as conn:
        _executemany(
            conn,
            f"""
            INSERT INTO pricing_attractiveness_recommendations_raw (
                store_uid, sku,
                attractiveness_overpriced_price,
                attractiveness_moderate_price,
                attractiveness_profitable_price,
                payload_json,
                source_updated_at,
                loaded_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                attractiveness_overpriced_price = excluded.attractiveness_overpriced_price,
                attractiveness_moderate_price = excluded.attractiveness_moderate_price,
                attractiveness_profitable_price = excluded.attractiveness_profitable_price,
                payload_json = excluded.payload_json,
                source_updated_at = excluded.source_updated_at,
                loaded_at = excluded.loaded_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)

def get_pricing_attractiveness_results_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    with _connect() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            SELECT store_uid, sku,
                   attractiveness_overpriced_price,
                   attractiveness_moderate_price,
                   attractiveness_profitable_price,
                   ozon_competitor_price,
                   external_competitor_price,
                   attractiveness_chosen_price,
                   attractiveness_chosen_boost_bid_percent,
                   source_updated_at, calculated_at
            FROM pricing_attractiveness_results
            WHERE store_uid IN ({suid_placeholders}) AND sku IN ({sku_placeholders})
            """,
            [*suids, *sku_list],
        ).fetchall()
        for row in rows:
            item = _with_attractiveness_aliases(dict(row))
            suid = str(row["store_uid"])
            out.setdefault(suid, {})[str(row["sku"])] = item
    return out


def get_pricing_attractiveness_recommendations_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {suid: {} for suid in suids}
    out: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    with _connect() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            SELECT store_uid, sku,
                   attractiveness_overpriced_price,
                   attractiveness_moderate_price,
                   attractiveness_profitable_price,
                   payload_json,
                   source_updated_at,
                   loaded_at
            FROM pricing_attractiveness_recommendations_raw
            WHERE store_uid IN ({suid_placeholders}) AND sku IN ({sku_placeholders})
            """,
            [*suids, *sku_list],
        ).fetchall()
        for row in rows:
            item = dict(row)
            try:
                item["payload"] = json.loads(item.get("payload_json") or "{}")
            except Exception:
                item["payload"] = {}
            suid = str(row["store_uid"])
            out.setdefault(suid, {})[str(row["sku"])] = item
    return out


def clear_pricing_attractiveness_results_for_store(*, store_uid: str) -> None:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_attractiveness_results WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        conn.commit()


def clear_pricing_attractiveness_recommendations_for_store(*, store_uid: str) -> None:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM pricing_attractiveness_recommendations_raw WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        conn.commit()


def clear_pricing_promo_results_for_store(*, store_uid: str) -> None:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return
    with _connect() as conn:
        marker = '%s' if is_postgres_backend() else '?'
        conn.execute(f"DELETE FROM pricing_promo_offer_raw WHERE store_uid = {marker}", (suid,))
        conn.execute(f"DELETE FROM pricing_promo_campaign_raw WHERE store_uid = {marker}", (suid,))
        conn.execute(f"DELETE FROM pricing_promo_offer_results WHERE store_uid = {marker}", (suid,))
        conn.execute(f"DELETE FROM pricing_promo_results WHERE store_uid = {marker}", (suid,))
        conn.commit()


def upsert_pricing_promo_campaign_raw_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        promo_id = str(row.get("promo_id") or "").strip()
        if not suid or not promo_id:
            continue
        prepared.append(
            (
                suid,
                promo_id,
                str(row.get("promo_name") or "").strip(),
                row.get("date_time_from"),
                row.get("date_time_to"),
                row.get("source_updated_at"),
                json.dumps(row.get("payload") or {}, ensure_ascii=False, default=str),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(8)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_promo_campaign_raw (
                store_uid, promo_id, promo_name, date_time_from, date_time_to,
                source_updated_at, payload_json, loaded_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, promo_id) DO UPDATE SET
                promo_name = excluded.promo_name,
                date_time_from = excluded.date_time_from,
                date_time_to = excluded.date_time_to,
                source_updated_at = excluded.source_updated_at,
                payload_json = excluded.payload_json,
                loaded_at = excluded.loaded_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def upsert_pricing_promo_offer_raw_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        promo_id = str(row.get("promo_id") or "").strip()
        if not suid or not sku or not promo_id:
            continue
        prepared.append(
            (
                suid,
                sku,
                promo_id,
                str(row.get("promo_name") or "").strip(),
                row.get("date_time_from"),
                row.get("date_time_to"),
                row.get("promo_price"),
                json.dumps(row.get("payload") or {}, ensure_ascii=False, default=str),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(10)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_promo_offer_raw (
                store_uid, sku, promo_id, promo_name, date_time_from, date_time_to,
                promo_price, payload_json, source_updated_at, loaded_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku, promo_id) DO UPDATE SET
                promo_name = excluded.promo_name,
                date_time_from = excluded.date_time_from,
                date_time_to = excluded.date_time_to,
                promo_price = excluded.promo_price,
                payload_json = excluded.payload_json,
                source_updated_at = excluded.source_updated_at,
                loaded_at = excluded.loaded_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def upsert_pricing_promo_results_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        prepared.append(
            (
                suid,
                sku,
                json.dumps(
                    _normalize_json_array(
                        _row_value(
                            row,
                            "promo_selected_items",
                            "promo_selected_items_json",
                            "selected_promo_items",
                            "selected_promo_items_json",
                        )
                    ),
                    ensure_ascii=False,
                ),
                _row_value(row, "promo_selected_price", "selected_promo_price"),
                row.get("promo_selected_boost_bid_percent"),
                row.get("promo_selected_profit_abs"),
                row.get("promo_selected_profit_pct"),
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(9)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_promo_results (
                store_uid, sku, promo_selected_items_json, promo_selected_price,
                promo_selected_boost_bid_percent, promo_selected_profit_abs, promo_selected_profit_pct,
                source_updated_at, calculated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                promo_selected_items_json = excluded.promo_selected_items_json,
                promo_selected_price = excluded.promo_selected_price,
                promo_selected_boost_bid_percent = excluded.promo_selected_boost_bid_percent,
                promo_selected_profit_abs = excluded.promo_selected_profit_abs,
                promo_selected_profit_pct = excluded.promo_selected_profit_pct,
                source_updated_at = excluded.source_updated_at,
                calculated_at = excluded.calculated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def upsert_pricing_promo_offer_results_bulk(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        promo_id = str(row.get("promo_id") or "").strip()
        if not suid or not sku or not promo_id:
            continue
        prepared.append(
            (
                suid,
                sku,
                promo_id,
                str(row.get("promo_name") or "").strip(),
                row.get("promo_price"),
                row.get("promo_profit_abs"),
                row.get("promo_profit_pct"),
                str(row.get("promo_fit_mode") or "rejected").strip() or "rejected",
                row.get("source_updated_at"),
                now,
            )
        )
    if not prepared:
        return 0
    values_sql = _placeholders(10)
    with _connect() as conn:
        _executemany(conn, 
            f"""
            INSERT INTO pricing_promo_offer_results (
                store_uid, sku, promo_id, promo_name, promo_price, promo_profit_abs,
                promo_profit_pct,
                promo_fit_mode, source_updated_at, calculated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku, promo_id) DO UPDATE SET
                promo_name = excluded.promo_name,
                promo_price = excluded.promo_price,
                promo_profit_abs = excluded.promo_profit_abs,
                promo_profit_pct = excluded.promo_profit_pct,
                promo_fit_mode = excluded.promo_fit_mode,
                source_updated_at = excluded.source_updated_at,
                calculated_at = excluded.calculated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)

def get_pricing_promo_results_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    with _connect() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            SELECT store_uid, sku, promo_selected_items_json, promo_selected_price,
                   promo_selected_boost_bid_percent, promo_selected_profit_abs, promo_selected_profit_pct,
                   source_updated_at, calculated_at
            FROM pricing_promo_results
            WHERE store_uid IN ({suid_placeholders}) AND sku IN ({sku_placeholders})
            """,
            [*suids, *sku_list],
        ).fetchall()
        for row in rows:
            item = dict(row)
            try:
                item["promo_selected_items"] = json.loads(item.get("promo_selected_items_json") or "[]")
            except Exception:
                item["promo_selected_items"] = []
            item = _with_promo_aliases(item)
            suid = str(row["store_uid"])
            out.setdefault(suid, {})[str(row["sku"])] = item
    return out


def get_pricing_promo_offer_results_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, list[dict[str, Any]]]] = {suid: {} for suid in suids}
    with _connect() as conn:
        suid_placeholders = _placeholders(len(suids))
        sku_placeholders = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            SELECT store_uid, sku, promo_id, promo_name, promo_price, promo_profit_abs,
                   promo_profit_pct,
                   promo_fit_mode, source_updated_at, calculated_at
            FROM pricing_promo_offer_results
            WHERE store_uid IN ({suid_placeholders}) AND sku IN ({sku_placeholders})
            ORDER BY store_uid, sku, promo_name, promo_id
            """,
            [*suids, *sku_list],
        ).fetchall()
        for row in rows:
            suid = str(row["store_uid"])
            sku = str(row["sku"])
            out.setdefault(suid, {}).setdefault(sku, []).append(dict(row))
    return out


def get_pricing_promo_offer_raw_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, list[dict[str, Any]]]] = {}
    with _connect() as conn:
        for suid in suids:
            placeholders = _placeholders(len(sku_list))
            rows = conn.execute(
                f"""
                SELECT store_uid, sku, promo_id, promo_name, promo_price, payload_json,
                       source_updated_at, loaded_at
                FROM pricing_promo_offer_raw
                WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND sku IN ({placeholders})
                ORDER BY promo_name, promo_id
                """,
                [suid, *sku_list],
            ).fetchall()
            local: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                item = dict(row)
                payload_raw = str(item.pop("payload_json", "") or "").strip()
                payload = {}
                if payload_raw:
                    try:
                        payload = json.loads(payload_raw)
                    except Exception:
                        payload = {}
                item["payload"] = payload if isinstance(payload, dict) else {}
                sku = str(item.get("sku") or "").strip()
                if not sku:
                    continue
                local.setdefault(sku, []).append(item)
            out[suid] = local
    return out


def get_pricing_promo_columns(*, store_uid: str) -> list[dict[str, str]]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return []
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT promo_id, promo_name
            FROM pricing_promo_offer_results
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
            GROUP BY promo_id, promo_name
            ORDER BY promo_name, promo_id
            """,
            (suid,),
        ).fetchall()
    return [
        {
            "promo_id": str(row["promo_id"] or "").strip(),
            "promo_name": str(row["promo_name"] or "").strip() or str(row["promo_id"] or "").strip(),
        }
        for row in rows
        if str(row["promo_id"] or "").strip()
    ]


def get_active_pricing_promo_campaigns(*, store_uid: str, as_of: str | None = None) -> list[dict[str, Any]]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return []
    as_of_raw = str(as_of or _now_iso()).strip()
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT store_uid, promo_id, promo_name, date_time_from, date_time_to, source_updated_at, loaded_at
            FROM pricing_promo_campaign_raw
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
            ORDER BY promo_name, promo_id
            """,
            (suid,),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        start_raw = str(row["date_time_from"] or "").strip()
        end_raw = str(row["date_time_to"] or "").strip()
        try:
            start_ok = (not start_raw) or (start_raw <= as_of_raw)
            end_ok = (not end_raw) or (end_raw >= as_of_raw)
        except Exception:
            start_ok = True
            end_ok = True
        if not (start_ok and end_ok):
            continue
        out.append(
            {
                "store_uid": suid,
                "promo_id": str(row["promo_id"] or "").strip(),
                "promo_name": str(row["promo_name"] or "").strip() or str(row["promo_id"] or "").strip(),
                "date_time_from": start_raw,
                "date_time_to": end_raw,
                "source_updated_at": str(row["source_updated_at"] or "").strip(),
                "loaded_at": str(row["loaded_at"] or "").strip(),
            }
        )
    return [row for row in out if row.get("promo_id")]


def get_pricing_promo_coinvest_settings(*, store_uid: str) -> list[dict[str, Any]]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return []
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT store_uid, promo_id, promo_name, max_discount_percent, updated_at
            FROM pricing_promo_coinvest_settings
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
            ORDER BY promo_name, promo_id
            """,
            (suid,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_pricing_promo_coinvest_settings_map(*, store_uids: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    if not suids:
        return {}
    placeholders = _placeholders(len(suids))
    out: dict[str, dict[str, dict[str, Any]]] = {}
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT store_uid, promo_id, promo_name, max_discount_percent, updated_at
            FROM pricing_promo_coinvest_settings
            WHERE store_uid IN ({placeholders})
            ORDER BY store_uid, promo_name, promo_id
            """,
            suids,
        ).fetchall()
    for row in rows:
        suid = str(row["store_uid"] or "").strip()
        promo_id = str(row["promo_id"] or "").strip()
        if not suid or not promo_id:
            continue
        out.setdefault(suid, {})[promo_id] = dict(row)
    return out


def upsert_pricing_promo_coinvest_settings(*, store_uid: str, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return 0
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        promo_id = str(row.get("promo_id") or "").strip()
        if not promo_id:
            continue
        discount = row.get("max_discount_percent")
        try:
            discount_value = None if discount in (None, "") else float(discount)
        except Exception:
            discount_value = None
        prepared.append(
            (
                suid,
                promo_id,
                str(row.get("promo_name") or "").strip() or promo_id,
                discount_value,
                now,
            )
        )
    with _connect() as conn:
        if prepared:
            values_sql = _placeholders(5)
            _executemany(conn, 
                f"""
                INSERT INTO pricing_promo_coinvest_settings (
                    store_uid, promo_id, promo_name, max_discount_percent, updated_at
                ) VALUES ({values_sql})
                ON CONFLICT(store_uid, promo_id) DO UPDATE SET
                    promo_name = excluded.promo_name,
                    max_discount_percent = excluded.max_discount_percent,
                    updated_at = excluded.updated_at
                """,
                prepared,
            )
        promo_ids = {str(row.get("promo_id") or "").strip() for row in rows if isinstance(row, dict) and str(row.get("promo_id") or "").strip()}
        if promo_ids:
            placeholders = _placeholders(len(promo_ids))
            conn.execute(
                f"DELETE FROM pricing_promo_coinvest_settings WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND promo_id NOT IN ({placeholders})",
                [suid, *sorted(promo_ids)],
            )
        else:
            conn.execute(
                f"DELETE FROM pricing_promo_coinvest_settings WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
                (suid,),
            )
        conn.commit()
    return len(prepared)


def replace_sales_market_order_items_for_period(
    *,
    store_uid: str,
    platform: str,
    date_from: str,
    date_to: str,
    rows: list[dict[str, Any]],
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    platform_code = str(platform or "").strip().lower() or "yandex_market"
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    if not suid or not from_date or not to_date:
        raise ValueError("store_uid, date_from, date_to обязательны")

    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        order_id = str(row.get("order_id") or "").strip()
        order_item_id = str(row.get("order_item_id") or "").strip()
        created_at = str(row.get("order_created_at") or "").strip()
        created_date = str(row.get("order_created_date") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not order_id or not order_item_id or not created_at or not created_date or not sku:
            continue
        try:
            item_count = int(row.get("item_count") or 1)
        except Exception:
            item_count = 1
        if item_count <= 0:
            item_count = 1
        prepared.append(
            (
                suid,
                platform_code,
                order_id,
                order_item_id,
                str(row.get("order_status") or "").strip(),
                created_at,
                created_date,
                sku,
                str(row.get("item_name") or "").strip(),
                row.get("sale_price"),
                row.get("payment_price"),
                row.get("subsidy_amount"),
                item_count,
                row.get("line_revenue"),
                now,
            )
        )

    with _connect_history() as conn:
        conn.execute(
            f"""
            DELETE FROM sales_market_order_items
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND order_created_date >= {'%s' if is_postgres_backend() else '?'} AND order_created_date <= {'%s' if is_postgres_backend() else '?'}
            """,
            (suid, from_date, to_date),
        )
        if prepared:
            values_sql = _placeholders(15)
            _executemany(conn, 
                f"""
                INSERT INTO sales_market_order_items (
                    store_uid, platform, order_id, order_item_id, order_status,
                    order_created_at, order_created_date, sku, item_name, sale_price,
                    payment_price, subsidy_amount, item_count, line_revenue, loaded_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def replace_yandex_goods_price_report_items(*, store_uid: str, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        offer_id = str(row.get("offer_id") or row.get("sku") or "").strip()
        if not offer_id:
            continue
        prepared.append(
            (
                suid,
                offer_id,
                str(row.get("currency") or "").strip(),
                str(row.get("on_display_raw") or "").strip(),
                row.get("on_display_price"),
                str(row.get("source_updated_at") or "").strip(),
                now,
            )
        )
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM yandex_goods_price_report_items WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        if prepared:
            values_sql = _placeholders(7)
            _executemany(conn, 
                f"""
                INSERT INTO yandex_goods_price_report_items (
                    store_uid, offer_id, currency, on_display_raw, on_display_price,
                    source_updated_at, loaded_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def append_pricing_daily_plan_history_bulk(*, rows: list[dict[str, Any]], captured_at: str | None = None) -> int:
    init_store_data_model()
    if not rows:
        return 0
    now = str(captured_at or _now_iso()).strip() or _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        suid = str(row.get("store_uid") or "").strip()
        plan_date = str(row.get("plan_date") or "").strip()
        if not suid or not plan_date:
            continue
        prepared.append(
            (
                suid,
                plan_date,
                now,
                row.get("planned_revenue_daily"),
                row.get("planned_profit_daily"),
                row.get("today_revenue"),
                row.get("today_profit"),
                row.get("weighted_day_profit_pct"),
                row.get("minimum_profit_percent"),
                row.get("experimental_floor_pct"),
            )
        )
    if not prepared:
        return 0
    with _connect() as conn:
        values_sql = _placeholders(10)
        _executemany(conn, 
            f"""
            INSERT INTO pricing_daily_plan_history (
                store_uid, plan_date, captured_at, planned_revenue_daily, planned_profit_daily, today_revenue,
                today_profit, weighted_day_profit_pct, minimum_profit_percent, experimental_floor_pct
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, plan_date, captured_at) DO UPDATE SET
                planned_revenue_daily = excluded.planned_revenue_daily,
                planned_profit_daily = excluded.planned_profit_daily,
                today_revenue = excluded.today_revenue,
                today_profit = excluded.today_profit,
                weighted_day_profit_pct = excluded.weighted_day_profit_pct,
                minimum_profit_percent = excluded.minimum_profit_percent,
                experimental_floor_pct = excluded.experimental_floor_pct
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def replace_sales_united_order_transactions_for_period(
    *,
    store_uid: str,
    platform: str,
    date_from: str,
    date_to: str,
    rows: list[dict[str, Any]],
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    platform_code = str(platform or "").strip().lower() or "yandex_market"
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    if not suid or not from_date or not to_date:
        raise ValueError("store_uid, date_from, date_to обязательны")

    now = _now_iso()
    dedup_map: dict[tuple[str, str], tuple[Any, ...]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        order_id = str(row.get("order_id") or "").strip()
        created_at = str(row.get("order_created_at") or "").strip()
        created_date = str(row.get("order_created_date") or "").strip()
        shipment_date = str(row.get("shipment_date") or "").strip()
        delivery_date = str(row.get("delivery_date") or "").strip()
        sku = str(row.get("sku") or "").strip()
        item_name = str(row.get("item_name") or "").strip()
        item_status = str(row.get("item_status") or "").strip()
        if not order_id or not created_at or not created_date or not sku:
            continue
        dedup_map[(order_id, sku)] = (
            suid,
            platform_code,
            order_id,
            created_at,
            created_date,
            shipment_date,
            delivery_date,
            sku,
            item_name,
            item_status,
            str(row.get("source_updated_at") or "").strip(),
            json.dumps(row.get("payload") or {}, ensure_ascii=False),
            now,
        )
    prepared = list(dedup_map.values())
    with _connect_history() as conn:
        if prepared:
            values_sql = _placeholders(13)
            _executemany(conn, 
                f"""
                INSERT INTO sales_united_order_transactions (
                    store_uid, platform, order_id, order_created_at, order_created_date, shipment_date, delivery_date,
                    sku, item_name, item_status, source_updated_at, payload_json, loaded_at
                ) VALUES ({values_sql})
                ON CONFLICT(store_uid, order_id, sku) DO UPDATE SET
                    platform = excluded.platform,
                    order_created_at = excluded.order_created_at,
                    order_created_date = excluded.order_created_date,
                    shipment_date = excluded.shipment_date,
                    delivery_date = excluded.delivery_date,
                    item_name = excluded.item_name,
                    item_status = excluded.item_status,
                    source_updated_at = excluded.source_updated_at,
                    payload_json = excluded.payload_json,
                    loaded_at = excluded.loaded_at
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def replace_sales_united_netting_report_rows_for_period(
    *,
    store_uid: str,
    platform: str,
    date_from: str,
    date_to: str,
    report_id: str,
    rows: list[dict[str, Any]],
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    platform_code = str(platform or "").strip().lower() or "yandex_market"
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    rid = str(report_id or "").strip()
    if not suid or not from_date or not to_date:
        raise ValueError("store_uid, date_from, date_to обязательны")

    now = _now_iso()
    dedup_map: dict[tuple[str, str], tuple[Any, ...]] = {}
    next_idx = 1
    for idx, row in enumerate(rows or [], start=1):
        if not isinstance(row, dict):
            continue
        sheet_name = str(row.get("sheet_name") or "").strip()
        payload_json = json.dumps(row.get("payload") or {}, ensure_ascii=False, sort_keys=True)
        dedup_key = (sheet_name, payload_json)
        dedup_map[dedup_key] = (
                suid,
                platform_code,
                from_date,
                to_date,
                rid,
                sheet_name,
                int(row.get("row_index") or idx or next_idx),
                payload_json,
                str(row.get("source_updated_at") or rid).strip(),
                now,
            )
        next_idx += 1
    prepared = list(dedup_map.values())

    with _connect() as conn:
        conn.execute(
            f"""
            DELETE FROM sales_united_netting_report_rows
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
              AND NOT (report_date_to < {'%s' if is_postgres_backend() else '?'} OR report_date_from > {'%s' if is_postgres_backend() else '?'})
            """,
            (suid, from_date, to_date),
        )
        if prepared:
            values_sql = _placeholders(10)
            _executemany(conn, 
                f"""
                INSERT INTO sales_united_netting_report_rows (
                    store_uid, platform, report_date_from, report_date_to, report_id,
                    sheet_name, row_index, payload_json, source_updated_at, loaded_at
                ) VALUES ({values_sql})
                ON CONFLICT(store_uid, report_date_from, report_date_to, sheet_name, row_index) DO UPDATE SET
                    platform = excluded.platform,
                    report_id = excluded.report_id,
                    payload_json = excluded.payload_json,
                    source_updated_at = excluded.source_updated_at,
                    loaded_at = excluded.loaded_at
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def replace_sales_shelfs_statistics_report_rows_for_period(
    *,
    store_uid: str,
    platform: str = "yandex_market",
    date_from: str,
    date_to: str,
    report_id: str = "",
    rows: list[dict[str, Any]] | None = None,
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    report_code = str(report_id or "").strip()
    platform_code = str(platform or "yandex_market").strip() or "yandex_market"
    if not suid or not from_date or not to_date:
        raise ValueError("store_uid, date_from, date_to обязательны")

    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        prepared.append(
            (
                suid,
                platform_code,
                from_date,
                to_date,
                report_code,
                str(row.get("sheet_name") or "").strip(),
                int(row.get("row_index") or 0),
                json.dumps(row.get("payload") or {}, ensure_ascii=False),
                str(row.get("source_updated_at") or "").strip(),
                now,
            )
        )

    with _connect() as conn:
        conn.execute(
            f"""
            DELETE FROM sales_shelfs_statistics_report_rows
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND report_date_from = {'%s' if is_postgres_backend() else '?'} AND report_date_to = {'%s' if is_postgres_backend() else '?'}
            """,
            (suid, from_date, to_date),
        )
        if prepared:
            values_sql = _placeholders(10)
            _executemany(conn, 
                f"""
                INSERT INTO sales_shelfs_statistics_report_rows (
                    store_uid, platform, report_date_from, report_date_to, report_id,
                    sheet_name, row_index, payload_json, source_updated_at, loaded_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def replace_sales_shows_boost_report_rows_for_period(
    *,
    store_uid: str,
    platform: str = "yandex_market",
    date_from: str,
    date_to: str,
    report_id: str = "",
    rows: list[dict[str, Any]] | None = None,
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    report_code = str(report_id or "").strip()
    platform_code = str(platform or "yandex_market").strip() or "yandex_market"
    if not suid or not from_date or not to_date:
        raise ValueError("store_uid, date_from, date_to обязательны")

    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        prepared.append(
            (
                suid,
                platform_code,
                from_date,
                to_date,
                report_code,
                str(row.get("sheet_name") or "").strip(),
                int(row.get("row_index") or 0),
                json.dumps(row.get("payload") or {}, ensure_ascii=False),
                str(row.get("source_updated_at") or "").strip(),
                now,
            )
        )

    with _connect() as conn:
        conn.execute(
            f"""
            DELETE FROM sales_shows_boost_report_rows
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND report_date_from = {'%s' if is_postgres_backend() else '?'} AND report_date_to = {'%s' if is_postgres_backend() else '?'}
            """,
            (suid, from_date, to_date),
        )
        if prepared:
            values_sql = _placeholders(10)
            _executemany(conn, 
                f"""
                INSERT INTO sales_shows_boost_report_rows (
                    store_uid, platform, report_date_from, report_date_to, report_id,
                    sheet_name, row_index, payload_json, source_updated_at, loaded_at
                ) VALUES ({values_sql})
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def replace_sales_overview_order_rows(
    *,
    store_uid: str,
    rows: list[dict[str, Any]],
    replace_all: bool = True,
    date_from: str = "",
    date_to: str = "",
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        order_id = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        order_created_date = str(row.get("order_created_date") or "").strip()
        if not order_id or not sku or not order_created_date:
            continue
        prepared.append(
            (
                suid,
                str(row.get("platform") or "yandex_market").strip() or "yandex_market",
                order_created_date,
                str(row.get("order_created_at") or "").strip(),
                str(row.get("shipment_date") or "").strip(),
                str(row.get("delivery_date") or "").strip(),
                order_id,
                str(row.get("item_status") or "").strip(),
                sku,
                str(row.get("item_name") or "").strip(),
                row.get("sale_price"),
                row.get("gross_profit"),
                row.get("cogs_price"),
                row.get("commission"),
                row.get("acquiring"),
                row.get("delivery"),
                row.get("ads"),
                row.get("tax"),
                row.get("profit"),
                row.get("sale_price_with_coinvest"),
                str(row.get("strategy_cycle_started_at") or "").strip(),
                row.get("strategy_market_boost_bid_percent"),
                row.get("strategy_boost_share"),
                row.get("strategy_boost_bid_percent"),
                str(row.get("strategy_snapshot_at") or "").strip(),
                row.get("strategy_installed_price"),
                str(row.get("strategy_decision_code") or "").strip(),
                str(row.get("strategy_decision_label") or "").strip(),
                str(row.get("strategy_control_state") or "").strip(),
                str(row.get("strategy_attractiveness_status") or "").strip(),
                int(row.get("strategy_promo_count") or 0),
                row.get("strategy_coinvest_pct"),
                str(row.get("strategy_selected_iteration_code") or "").strip(),
                1 if bool(row.get("strategy_uses_promo")) else 0,
                str(row.get("strategy_market_promo_status") or "").strip(),
                1 if bool(row.get("uses_planned_costs")) else 0,
                str(row.get("source_updated_at") or "").strip(),
                now,
            )
        )
    with _connect_history() as conn:
        if replace_all:
            conn.execute(
                f"DELETE FROM sales_overview_order_rows WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
                (suid,),
            )
        elif from_date and to_date:
            conn.execute(
                f"""
                DELETE FROM sales_overview_order_rows
                WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
                  AND order_created_date >= {'%s' if is_postgres_backend() else '?'}
                  AND order_created_date <= {'%s' if is_postgres_backend() else '?'}
                """,
                (suid, from_date, to_date),
            )
        if prepared:
            values_sql = _placeholders(38)
            _executemany(conn, 
                f"""
                INSERT INTO sales_overview_order_rows (
                    store_uid, platform, order_created_date, order_created_at, shipment_date, delivery_date,
                    order_id, item_status, sku, item_name, sale_price, gross_profit, cogs_price, commission,
                    acquiring, delivery, ads, tax, profit, sale_price_with_coinvest,
                    strategy_cycle_started_at, strategy_market_boost_bid_percent, strategy_boost_share,
                    strategy_boost_bid_percent, strategy_snapshot_at, strategy_installed_price,
                    strategy_decision_code, strategy_decision_label, strategy_control_state,
                    strategy_attractiveness_status, strategy_promo_count, strategy_coinvest_pct, strategy_selected_iteration_code,
                    strategy_uses_promo, strategy_market_promo_status, uses_planned_costs,
                    source_updated_at, calculated_at
                ) VALUES ({values_sql})
                ON CONFLICT(store_uid, order_id, sku) DO UPDATE SET
                    platform = excluded.platform,
                    order_created_date = excluded.order_created_date,
                    order_created_at = excluded.order_created_at,
                    shipment_date = excluded.shipment_date,
                    delivery_date = excluded.delivery_date,
                    item_status = excluded.item_status,
                    item_name = excluded.item_name,
                    sale_price = excluded.sale_price,
                    gross_profit = excluded.gross_profit,
                    cogs_price = excluded.cogs_price,
                    commission = excluded.commission,
                    acquiring = excluded.acquiring,
                    delivery = excluded.delivery,
                    ads = excluded.ads,
                    tax = excluded.tax,
                    profit = excluded.profit,
                    sale_price_with_coinvest = excluded.sale_price_with_coinvest,
                    strategy_cycle_started_at = excluded.strategy_cycle_started_at,
                    strategy_market_boost_bid_percent = excluded.strategy_market_boost_bid_percent,
                    strategy_boost_share = excluded.strategy_boost_share,
                    strategy_boost_bid_percent = excluded.strategy_boost_bid_percent,
                    strategy_snapshot_at = excluded.strategy_snapshot_at,
                    strategy_installed_price = excluded.strategy_installed_price,
                    strategy_decision_code = excluded.strategy_decision_code,
                    strategy_decision_label = excluded.strategy_decision_label,
                    strategy_control_state = excluded.strategy_control_state,
                    strategy_attractiveness_status = excluded.strategy_attractiveness_status,
                    strategy_promo_count = excluded.strategy_promo_count,
                    strategy_coinvest_pct = excluded.strategy_coinvest_pct,
                    strategy_selected_iteration_code = excluded.strategy_selected_iteration_code,
                    strategy_uses_promo = excluded.strategy_uses_promo,
                    strategy_market_promo_status = excluded.strategy_market_promo_status,
                    uses_planned_costs = excluded.uses_planned_costs,
                    source_updated_at = excluded.source_updated_at,
                    calculated_at = excluded.calculated_at
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def replace_sales_overview_order_rows_hot(
    *,
    store_uid: str,
    rows: list[dict[str, Any]],
    replace_all: bool = True,
    date_from: str = "",
    date_to: str = "",
) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        order_id = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        order_created_date = str(row.get("order_created_date") or "").strip()
        if not order_id or not sku or not order_created_date:
            continue
        prepared.append(
            (
                suid,
                str(row.get("platform") or "yandex_market").strip() or "yandex_market",
                order_created_date,
                str(row.get("order_created_at") or "").strip(),
                str(row.get("shipment_date") or "").strip(),
                str(row.get("delivery_date") or "").strip(),
                order_id,
                str(row.get("item_status") or "").strip(),
                sku,
                str(row.get("item_name") or "").strip(),
                row.get("sale_price"),
                row.get("gross_profit"),
                row.get("cogs_price"),
                row.get("commission"),
                row.get("acquiring"),
                row.get("delivery"),
                row.get("ads"),
                row.get("tax"),
                row.get("profit"),
                row.get("sale_price_with_coinvest"),
                str(row.get("strategy_cycle_started_at") or "").strip(),
                row.get("strategy_market_boost_bid_percent"),
                row.get("strategy_boost_share"),
                row.get("strategy_boost_bid_percent"),
                str(row.get("strategy_snapshot_at") or "").strip(),
                row.get("strategy_installed_price"),
                str(row.get("strategy_decision_code") or "").strip(),
                str(row.get("strategy_decision_label") or "").strip(),
                str(row.get("strategy_control_state") or "").strip(),
                str(row.get("strategy_attractiveness_status") or "").strip(),
                int(row.get("strategy_promo_count") or 0),
                row.get("strategy_coinvest_pct"),
                str(row.get("strategy_selected_iteration_code") or "").strip(),
                1 if bool(row.get("strategy_uses_promo")) else 0,
                str(row.get("strategy_market_promo_status") or "").strip(),
                1 if bool(row.get("uses_planned_costs")) else 0,
                str(row.get("source_updated_at") or "").strip(),
                now,
            )
        )
    with _connect() as conn:
        if replace_all:
            conn.execute(
                f"DELETE FROM sales_overview_order_rows_hot WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
                (suid,),
            )
        elif from_date and to_date:
            conn.execute(
                f"""
                DELETE FROM sales_overview_order_rows_hot
                WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
                  AND order_created_date >= {'%s' if is_postgres_backend() else '?'}
                  AND order_created_date <= {'%s' if is_postgres_backend() else '?'}
                """,
                (suid, from_date, to_date),
            )
        if prepared:
            values_sql = _placeholders(38)
            _executemany(
                conn,
                f"""
                INSERT INTO sales_overview_order_rows_hot (
                    store_uid, platform, order_created_date, order_created_at, shipment_date, delivery_date,
                    order_id, item_status, sku, item_name, sale_price, gross_profit, cogs_price, commission,
                    acquiring, delivery, ads, tax, profit, sale_price_with_coinvest,
                    strategy_cycle_started_at, strategy_market_boost_bid_percent, strategy_boost_share,
                    strategy_boost_bid_percent, strategy_snapshot_at, strategy_installed_price,
                    strategy_decision_code, strategy_decision_label, strategy_control_state,
                    strategy_attractiveness_status, strategy_promo_count, strategy_coinvest_pct, strategy_selected_iteration_code,
                    strategy_uses_promo, strategy_market_promo_status, uses_planned_costs,
                    source_updated_at, calculated_at
                ) VALUES ({values_sql})
                ON CONFLICT(store_uid, order_id, sku) DO UPDATE SET
                    platform = excluded.platform,
                    order_created_date = excluded.order_created_date,
                    order_created_at = excluded.order_created_at,
                    shipment_date = excluded.shipment_date,
                    delivery_date = excluded.delivery_date,
                    item_status = excluded.item_status,
                    item_name = excluded.item_name,
                    sale_price = excluded.sale_price,
                    gross_profit = excluded.gross_profit,
                    cogs_price = excluded.cogs_price,
                    commission = excluded.commission,
                    acquiring = excluded.acquiring,
                    delivery = excluded.delivery,
                    ads = excluded.ads,
                    tax = excluded.tax,
                    profit = excluded.profit,
                    sale_price_with_coinvest = excluded.sale_price_with_coinvest,
                    strategy_cycle_started_at = excluded.strategy_cycle_started_at,
                    strategy_market_boost_bid_percent = excluded.strategy_market_boost_bid_percent,
                    strategy_boost_share = excluded.strategy_boost_share,
                    strategy_boost_bid_percent = excluded.strategy_boost_bid_percent,
                    strategy_snapshot_at = excluded.strategy_snapshot_at,
                    strategy_installed_price = excluded.strategy_installed_price,
                    strategy_decision_code = excluded.strategy_decision_code,
                    strategy_decision_label = excluded.strategy_decision_label,
                    strategy_control_state = excluded.strategy_control_state,
                    strategy_attractiveness_status = excluded.strategy_attractiveness_status,
                    strategy_promo_count = excluded.strategy_promo_count,
                    strategy_coinvest_pct = excluded.strategy_coinvest_pct,
                    strategy_selected_iteration_code = excluded.strategy_selected_iteration_code,
                    strategy_uses_promo = excluded.strategy_uses_promo,
                    strategy_market_promo_status = excluded.strategy_market_promo_status,
                    uses_planned_costs = excluded.uses_planned_costs,
                    source_updated_at = excluded.source_updated_at,
                    calculated_at = excluded.calculated_at
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def _current_month_start_iso() -> str:
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")).date()
    return now_msk.replace(day=1).isoformat()


def _load_sales_overview_order_rows_combined(
    *,
    store_uid: str,
    skus: list[str] | tuple[str, ...] | set[str] | None = None,
) -> list[dict[str, Any]]:
    suid = str(store_uid or "").strip()
    if not suid:
        return []
    sku_list = [str(sku or "").strip() for sku in (skus or []) if str(sku or "").strip()]
    current_month_start = _current_month_start_iso()
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    history_sql = f"""
        SELECT *
        FROM sales_overview_order_rows
        WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
          AND order_created_date < {'%s' if is_postgres_backend() else '?'}
    """
    history_params: list[Any] = [suid, current_month_start]
    if sku_list:
        history_sql += f" AND sku IN ({_placeholders(len(sku_list))})"
        history_params.extend(sku_list)
    with _connect_history() as conn:
        rows = conn.execute(history_sql, tuple(history_params)).fetchall()
    for row in rows:
        item = dict(row)
        key = (str(item.get("order_id") or "").strip(), str(item.get("sku") or "").strip())
        if all(key):
            merged[key] = item
    try:
        hot_sql = f"""
            SELECT *
            FROM sales_overview_order_rows_hot
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
        """
        hot_params: list[Any] = [suid]
        if sku_list:
            hot_sql += f" AND sku IN ({_placeholders(len(sku_list))})"
            hot_params.extend(sku_list)
        with _connect() as conn:
            hot_rows = conn.execute(hot_sql, tuple(hot_params)).fetchall()
        for row in hot_rows:
            item = dict(row)
            key = (str(item.get("order_id") or "").strip(), str(item.get("sku") or "").strip())
            if all(key):
                merged[key] = item
    except Exception:
        pass
    return list(merged.values())


def get_sales_overview_order_rows(
    *,
    store_uid: str,
    item_status: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = 1,
    page_size: int = 200,
) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    page_num = max(1, int(page or 1))
    size = max(1, min(int(page_size or 200), 1000))
    offset = (page_num - 1) * size
    if not suid:
        return {"rows": [], "total_count": 0, "page": page_num, "page_size": size, "available_statuses": [], "min_date": "", "max_date": "", "loaded_at": ""}
    status = str(item_status or "").strip()
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    all_rows = _load_sales_overview_order_rows_combined(store_uid=suid)
    available_statuses = sorted(
        {
            str(item.get("item_status") or "").strip()
            for item in all_rows
            if str(item.get("item_status") or "").strip()
        },
        key=lambda value: value.lower(),
    )
    filtered_rows: list[dict[str, Any]] = []
    min_date = ""
    max_date = ""
    loaded_at = ""
    for item in all_rows:
        row_status = str(item.get("item_status") or "").strip()
        row_date = str(item.get("order_created_date") or "").strip()
        row_loaded_at = str(item.get("calculated_at") or "").strip()
        if row_date:
            if not min_date or row_date < min_date:
                min_date = row_date
            if not max_date or row_date > max_date:
                max_date = row_date
        if row_loaded_at and row_loaded_at > loaded_at:
            loaded_at = row_loaded_at
        if status and row_status != status:
            continue
        if from_date and (not row_date or row_date < from_date):
            continue
        if to_date and (not row_date or row_date > to_date):
            continue
        filtered_rows.append(item)
    filtered_rows.sort(
        key=lambda item: (
            str(item.get("order_created_at") or item.get("order_created_date") or ""),
            str(item.get("order_id") or ""),
            str(item.get("sku") or ""),
        ),
        reverse=True,
    )
    total_count = len(filtered_rows)
    rows = filtered_rows[offset : offset + size]
    return {
        "rows": rows,
        "total_count": total_count,
        "page": page_num,
        "page_size": size,
        "available_statuses": available_statuses,
        "min_date": min_date,
        "max_date": max_date,
        "loaded_at": loaded_at,
    }


def get_sales_overview_order_rows_map(*, store_uid: str, order_skus: list[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    prepared_keys = [(str(order_id or "").strip(), str(sku or "").strip()) for order_id, sku in (order_skus or [])]
    prepared_keys = [(order_id, sku) for order_id, sku in prepared_keys if order_id and sku]
    if not suid or not prepared_keys:
        return {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    wanted = set(prepared_keys)
    for item in _load_sales_overview_order_rows_combined(store_uid=suid):
        key = (str(item.get("order_id") or "").strip(), str(item.get("sku") or "").strip())
        if key in wanted:
            out[key] = item
    return out


def replace_sales_overview_cogs_source_rows(*, store_uid: str, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        order_key = str(row.get("order_key") or "").strip()
        sku_key = str(row.get("sku_key") or "").strip()
        if not order_key:
            continue
        value = row.get("cogs_value")
        try:
            cogs_value = float(value) if value not in (None, "") else None
        except Exception:
            cogs_value = None
        prepared.append((suid, order_key, sku_key, cogs_value, now))
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM sales_overview_cogs_source_rows WHERE store_uid = {'%s' if is_postgres_backend() else '?'}",
            (suid,),
        )
        if prepared:
            values_sql = _placeholders(5)
            _executemany(conn, 
                f"""
                INSERT INTO sales_overview_cogs_source_rows (
                    store_uid, order_key, sku_key, cogs_value, loaded_at
                ) VALUES ({values_sql})
                ON CONFLICT(store_uid, order_key, sku_key) DO UPDATE SET
                    cogs_value = excluded.cogs_value,
                    loaded_at = excluded.loaded_at
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def get_sales_overview_cogs_source_map(*, store_uid: str) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return {"rows": [], "loaded_at": ""}
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT order_key, sku_key, cogs_value, loaded_at
            FROM sales_overview_cogs_source_rows
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
            ORDER BY order_key ASC
            """,
            (suid,),
        ).fetchall()
    data_rows = [dict(row) for row in rows]
    loaded_at = max((str(r.get("loaded_at") or "").strip() for r in data_rows), default="")
    return {"rows": data_rows, "loaded_at": loaded_at}


def replace_pricing_cogs_snapshot_rows(*, snapshot_at: str, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    snap = str(snapshot_at or "").strip()
    if not snap:
        raise ValueError("snapshot_at обязателен")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        store_uid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not store_uid or not sku:
            continue
        value = row.get("cogs_value")
        try:
            cogs_value = float(value) if value not in (None, "") else None
        except Exception:
            cogs_value = None
        prepared.append(
            (
                snap,
                store_uid,
                sku,
                cogs_value,
                str(row.get("source_id") or "").strip(),
                now,
            )
        )
    with _connect_history() as conn:
        conn.execute(
            f"DELETE FROM pricing_cogs_snapshots WHERE snapshot_at = {'%s' if is_postgres_backend() else '?'}",
            (snap,),
        )
        if prepared:
            values_sql = _placeholders(6)
            _executemany(conn, 
                f"""
                INSERT INTO pricing_cogs_snapshots (
                    snapshot_at, store_uid, sku, cogs_value, source_id, loaded_at
                ) VALUES ({values_sql})
                ON CONFLICT(snapshot_at, store_uid, sku) DO UPDATE SET
                    cogs_value = excluded.cogs_value,
                    source_id = excluded.source_id,
                    loaded_at = excluded.loaded_at
                """,
                prepared,
            )
        conn.commit()
    return len(prepared)


def get_pricing_cogs_snapshot_map(
    *,
    store_uids: list[str],
    as_of_msk: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    init_store_data_model()
    wanted = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    if not wanted:
        return {}
    now_msk = as_of_msk.astimezone(ZoneInfo("Europe/Moscow")) if isinstance(as_of_msk, datetime) else datetime.now(ZoneInfo("Europe/Moscow"))
    cutoff_utc = now_msk.astimezone(timezone.utc).isoformat()
    placeholders = _placeholders(len(wanted))
    params: list[Any] = [*wanted, cutoff_utc]
    with _connect_history() as conn:
        latest_rows = conn.execute(
            f"""
            SELECT store_uid, MAX(snapshot_at) AS snapshot_at
            FROM pricing_cogs_snapshots
            WHERE store_uid IN ({placeholders}) AND snapshot_at <= {'%s' if is_postgres_backend() else '?'}
            GROUP BY store_uid
            """,
            params,
        ).fetchall()
        latest_by_store = {
            str(row["store_uid"] or "").strip(): str(row["snapshot_at"] or "").strip()
            for row in latest_rows
            if str(row["store_uid"] or "").strip() and str(row["snapshot_at"] or "").strip()
        }
        if not latest_by_store:
            return {}
        marker = '%s' if is_postgres_backend() else '?'
        pairs_where = " OR ".join(f"(store_uid = {marker} AND snapshot_at = {marker})" for _ in latest_by_store)
        pair_params: list[Any] = []
        for store_uid, snapshot_at in latest_by_store.items():
            pair_params.extend([store_uid, snapshot_at])
        rows = conn.execute(
            f"""
            SELECT store_uid, snapshot_at, sku, cogs_value, source_id, loaded_at
            FROM pricing_cogs_snapshots
            WHERE {pairs_where}
            ORDER BY store_uid ASC, sku ASC
            """,
            pair_params,
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        store_uid = str(row["store_uid"] or "").strip()
        sku = str(row["sku"] or "").strip()
        if not store_uid or not sku:
            continue
        bucket = out.setdefault(
            store_uid,
            {
                "snapshot_at": str(row["snapshot_at"] or "").strip(),
                "source_id": str(row["source_id"] or "").strip(),
                "rows": {},
            },
        )
        if not bucket.get("source_id") and str(row["source_id"] or "").strip():
            bucket["source_id"] = str(row["source_id"] or "").strip()
        bucket["rows"][sku] = None if row["cogs_value"] in (None, "") else float(row["cogs_value"])
    return out


def prune_pricing_cogs_snapshots_for_store(*, store_uid: str, keep_recent_days: int = 3) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(keep_recent_days or 1)))).isoformat()
    with _connect_history() as conn:
        marker = '%s' if is_postgres_backend() else '?'
        cur = conn.execute(
            f"""
            DELETE FROM pricing_cogs_snapshots
            WHERE store_uid = {marker}
              AND snapshot_at < {marker}
              AND (store_uid, sku, snapshot_at) NOT IN (
                    SELECT store_uid, sku, MAX(snapshot_at)
                    FROM pricing_cogs_snapshots
                    WHERE store_uid = {marker}
                    GROUP BY store_uid, sku
              )
            """,
            (suid, cutoff, suid),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def get_refresh_jobs() -> list[dict[str, Any]]:
    init_store_data_model()
    with _connect_system() as conn:
        rows = conn.execute(
            """
            SELECT job_code, title, enabled, schedule_kind, interval_minutes, time_of_day, date_from, date_to, stores_json, updated_at
            FROM refresh_jobs
            ORDER BY lower(title) ASC, job_code ASC
            """
        ).fetchall()
    result = [dict(row) for row in rows]
    if result:
        return result
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT job_code, title, enabled, schedule_kind, interval_minutes, time_of_day, date_from, date_to, stores_json, updated_at
            FROM refresh_jobs
            ORDER BY lower(title) ASC, job_code ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_refresh_job(*, job_code: str, values: dict[str, Any]) -> dict[str, Any]:
    init_store_data_model()
    code = str(job_code or "").strip()
    if not code:
        raise ValueError("job_code обязателен")
    current = next((row for row in get_refresh_jobs() if str(row.get("job_code") or "").strip() == code), {})
    try:
        current_stores = json.loads(str(current.get("stores_json") or "[]") or "[]")
    except Exception:
        current_stores = []
    next_stores = values.get("stores") if "stores" in values else current_stores
    if not isinstance(next_stores, list):
        next_stores = []
    next_values = {
        "title": str((values.get("title") if "title" in values else current.get("title")) or "").strip(),
        "enabled": 1 if bool(values.get("enabled") if "enabled" in values else current.get("enabled", 1)) else 0,
        "schedule_kind": str((values.get("schedule_kind") if "schedule_kind" in values else current.get("schedule_kind") or "interval")).strip() or "interval",
        "interval_minutes": values.get("interval_minutes") if "interval_minutes" in values else current.get("interval_minutes"),
        "time_of_day": str((values.get("time_of_day") if "time_of_day" in values else current.get("time_of_day")) or "").strip() or None,
        "date_from": str((values.get("date_from") if "date_from" in values else current.get("date_from")) or "").strip() or None,
        "date_to": str((values.get("date_to") if "date_to" in values else current.get("date_to")) or "").strip() or None,
        "stores_json": json.dumps([str(x or "").strip() for x in next_stores if str(x or "").strip()], ensure_ascii=False),
        "updated_at": _now_iso(),
    }
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO refresh_jobs (
                job_code, title, enabled, schedule_kind, interval_minutes, time_of_day, date_from, date_to, stores_json, updated_at
            ) VALUES ({_placeholders(10)})
            ON CONFLICT(job_code) DO UPDATE SET
                title = excluded.title,
                enabled = excluded.enabled,
                schedule_kind = excluded.schedule_kind,
                interval_minutes = excluded.interval_minutes,
                time_of_day = excluded.time_of_day,
                date_from = excluded.date_from,
                date_to = excluded.date_to,
                stores_json = excluded.stores_json,
                updated_at = excluded.updated_at
            """,
            (
                code,
                next_values["title"],
                int(next_values["enabled"]),
                next_values["schedule_kind"],
                next_values["interval_minutes"],
                next_values["time_of_day"],
                next_values["date_from"],
                next_values["date_to"],
                next_values["stores_json"],
                next_values["updated_at"],
            ),
        )
        conn.commit()
    with _connect_system() as conn:
        conn.execute(
            f"""
            INSERT INTO refresh_jobs (
                job_code, title, enabled, schedule_kind, interval_minutes, time_of_day, date_from, date_to, stores_json, updated_at
            ) VALUES ({_system_placeholders(10)})
            ON CONFLICT(job_code) DO UPDATE SET
                title = excluded.title,
                enabled = excluded.enabled,
                schedule_kind = excluded.schedule_kind,
                interval_minutes = excluded.interval_minutes,
                time_of_day = excluded.time_of_day,
                date_from = excluded.date_from,
                date_to = excluded.date_to,
                stores_json = excluded.stores_json,
                updated_at = excluded.updated_at
            """,
            (
                code,
                next_values["title"],
                int(next_values["enabled"]),
                next_values["schedule_kind"],
                next_values["interval_minutes"],
                next_values["time_of_day"],
                next_values["date_from"],
                next_values["date_to"],
                next_values["stores_json"],
                next_values["updated_at"],
            ),
        )
        conn.commit()
    return {"job_code": code, **next_values}


def create_refresh_job_run(
    *,
    job_code: str,
    trigger_source: str = "",
    meta: dict[str, Any] | None = None,
    status: str = "running",
    message: str = "",
) -> int:
    init_store_data_model()
    code = str(job_code or "").strip()
    if not code:
        raise ValueError("job_code обязателен")
    with _connect() as conn:
        cur = conn.execute(
            f"""
            INSERT INTO refresh_job_runs (
                job_code, trigger_source, started_at, status, message, meta_json
            ) VALUES ({_placeholders(6)})
            {"RETURNING run_id" if is_postgres_backend() else ""}
            """,
            (
                code,
                str(trigger_source or "").strip(),
                _now_iso(),
                str(status or "running").strip() or "running",
                str(message or "").strip(),
                json.dumps(meta or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
        if is_postgres_backend():
            row = cur.fetchone()
            return int((row or {}).get("run_id") or 0)
        return int(cur.lastrowid or 0)


def finish_refresh_job_run(*, run_id: int, status: str, message: str = "", meta: dict[str, Any] | None = None) -> None:
    init_store_data_model()
    rid = int(run_id or 0)
    if rid <= 0:
        return
    with _connect() as conn:
        conn.execute(
            f"""
            UPDATE refresh_job_runs
            SET finished_at = {"%s" if is_postgres_backend() else "?"}, status = {"%s" if is_postgres_backend() else "?"}, message = {"%s" if is_postgres_backend() else "?"}, meta_json = {"%s" if is_postgres_backend() else "?"}
            WHERE run_id = {"%s" if is_postgres_backend() else "?"}
            """,
            (
                _now_iso(),
                str(status or "done").strip() or "done",
                str(message or "").strip(),
                json.dumps(meta or {}, ensure_ascii=False),
                rid,
            ),
        )
        conn.commit()


def update_refresh_job_run(*, run_id: int, status: str | None = None, message: str | None = None, meta: dict[str, Any] | None = None) -> None:
    init_store_data_model()
    rid = int(run_id or 0)
    if rid <= 0:
        return
    with _connect() as conn:
        current = conn.execute(
            f"SELECT status, message, meta_json FROM refresh_job_runs WHERE run_id = {'%s' if is_postgres_backend() else '?'}",
            (rid,),
        ).fetchone()
        if current is None:
            return
        try:
            current_meta = json.loads(str(current["meta_json"] or "{}") or "{}")
        except Exception:
            current_meta = {}
        next_meta = dict(current_meta)
        if isinstance(meta, dict):
            next_meta.update(meta)
        conn.execute(
            f"""
            UPDATE refresh_job_runs
            SET status = {"%s" if is_postgres_backend() else "?"}, message = {"%s" if is_postgres_backend() else "?"}, meta_json = {"%s" if is_postgres_backend() else "?"}
            WHERE run_id = {"%s" if is_postgres_backend() else "?"}
            """,
            (
                str(status if status is not None else current["status"] or "running").strip() or "running",
                str(message if message is not None else current["message"] or "").strip(),
                json.dumps(next_meta, ensure_ascii=False),
                rid,
            ),
        )
        conn.commit()


def abandon_incomplete_refresh_job_runs(*, message: str = "Прервано при перезапуске") -> int:
    init_store_data_model()
    with _connect() as conn:
        cur = conn.execute(
            f"""
            UPDATE refresh_job_runs
            SET finished_at = {"%s" if is_postgres_backend() else "?"}, status = 'error', message = {"%s" if is_postgres_backend() else "?"}
            WHERE status IN ('running', 'queued')
            """,
            (_now_iso(), str(message or "").strip() or "Прервано при перезапуске"),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def get_refresh_job_runs_latest() -> dict[str, dict[str, Any]]:
    init_store_data_model()
    with _connect() as conn:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    r.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.job_code
                        ORDER BY
                            CASE
                                WHEN lower(coalesce(r.status, '')) IN ('running', 'queued') THEN 0
                                ELSE 1
                            END,
                            r.run_id DESC
                    ) AS rn
                FROM refresh_job_runs r
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            """
        ).fetchall()
    return {str(row["job_code"]): dict(row) for row in rows}


def get_refresh_job_runs_latest_success() -> dict[str, dict[str, Any]]:
    init_store_data_model()
    with _connect() as conn:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    r.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.job_code
                        ORDER BY r.run_id DESC
                    ) AS rn
                FROM refresh_job_runs r
                WHERE lower(coalesce(r.status, '')) = 'success'
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            """
        ).fetchall()
    return {str(row["job_code"]): dict(row) for row in rows}


def get_yandex_goods_price_report_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {}
    with _connect() as conn:
        for suid in suids:
            placeholders = _placeholders(len(sku_list))
            rows = conn.execute(
                f"""
                SELECT store_uid, offer_id, currency, on_display_raw, on_display_price,
                       source_updated_at, loaded_at
                FROM yandex_goods_price_report_items
                WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND offer_id IN ({placeholders})
                """,
                [suid, *sku_list],
            ).fetchall()
            local: dict[str, dict[str, Any]] = {}
            for row in rows:
                local[str(row["offer_id"])] = dict(row)
            out[suid] = local
    return out


def get_yandex_goods_price_report_prev_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    out: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    with _connect_history() as conn:
        placeholders_suid = _placeholders(len(suids))
        placeholders_sku = _placeholders(len(sku_list))
        rows = conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    store_uid,
                    offer_id,
                    currency,
                    on_display_raw,
                    on_display_price,
                    source_updated_at,
                    captured_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY store_uid, offer_id
                        ORDER BY captured_at DESC
                    ) AS rn
                FROM yandex_goods_price_report_history
                WHERE store_uid IN ({placeholders_suid})
                  AND offer_id IN ({placeholders_sku})
            )
            SELECT *
            FROM ranked
            WHERE rn = 2
            """,
            [*suids, *sku_list],
        ).fetchall()
    for row in rows:
        suid = str(row["store_uid"] or "").strip()
        sku = str(row["offer_id"] or "").strip()
        if not suid or not sku:
            continue
        out.setdefault(suid, {})[sku] = dict(row)
    return out


def upsert_pricing_autopilot_snapshots(*, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    if not rows:
        return 0
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        snapshot_at = str(row.get("snapshot_at") or "").strip()
        bucket_start = str(row.get("time_bucket_start") or "").strip()
        bucket_end = str(row.get("time_bucket_end") or "").strip()
        store_uid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not snapshot_at or not bucket_start or not bucket_end or not store_uid or not sku:
            continue
        prepared.append((snapshot_at, bucket_start, bucket_end, store_uid, sku, json.dumps(payload, ensure_ascii=False)))
    if not prepared:
        return 0
    with _connect() as conn:
        values_sql = _placeholders(6)
        _executemany(conn, 
            f"""
            INSERT INTO pricing_autopilot_snapshots (
                snapshot_at, time_bucket_start, time_bucket_end, store_uid, sku, payload_json
            ) VALUES ({values_sql})
            ON CONFLICT(time_bucket_start, store_uid, sku) DO UPDATE SET
                snapshot_at = excluded.snapshot_at,
                time_bucket_end = excluded.time_bucket_end,
                payload_json = excluded.payload_json
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def get_latest_pricing_autopilot_snapshot_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    store_placeholders = _placeholders(len(suids))
    sku_placeholders = _placeholders(len(sku_list))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT s1.*
            FROM pricing_autopilot_snapshots s1
            JOIN (
                SELECT store_uid, sku, MAX(snapshot_id) AS max_snapshot_id
                FROM pricing_autopilot_snapshots
                WHERE store_uid IN ({store_placeholders})
                  AND sku IN ({sku_placeholders})
                GROUP BY store_uid, sku
            ) latest ON latest.max_snapshot_id = s1.snapshot_id
            """,
            [*suids, *sku_list],
        ).fetchall()
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        payload = {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            payload = {}
        out.setdefault(str(row["store_uid"]), {})[str(row["sku"])] = {
            "snapshot_id": int(row["snapshot_id"]),
            "snapshot_at": str(row["snapshot_at"] or "").strip(),
            "time_bucket_start": str(row["time_bucket_start"] or "").strip(),
            "time_bucket_end": str(row["time_bucket_end"] or "").strip(),
            "payload": payload,
        }
    return out


def create_pricing_autopilot_decision(
    *,
    created_at: str,
    review_after: str,
    store_uid: str,
    sku: str,
    decision_status: str,
    decision_mode: str,
    action_code: str,
    action_unit: str,
    action_value: float | None,
    previous_value: float | None,
    proposed_value: float | None,
    baseline_snapshot_id: int | None,
    reason: dict[str, Any] | None = None,
) -> int:
    init_store_data_model()
    with _connect() as conn:
        values_sql = _placeholders(14)
        cur = conn.execute(
            f"""
            INSERT INTO pricing_autopilot_decisions (
                created_at, review_after, store_uid, sku, decision_status, decision_mode,
                action_code, action_unit, action_value, previous_value, proposed_value,
                baseline_snapshot_id, reason_json, result_json
            ) VALUES ({values_sql})
            {"RETURNING decision_id" if is_postgres_backend() else ""}
            """,
            (
                str(created_at or "").strip(),
                str(review_after or "").strip(),
                str(store_uid or "").strip(),
                str(sku or "").strip(),
                str(decision_status or "pending").strip(),
                str(decision_mode or "simulate").strip(),
                str(action_code or "").strip(),
                str(action_unit or "").strip(),
                action_value,
                previous_value,
                proposed_value,
                baseline_snapshot_id,
                json.dumps(reason or {}, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
            ),
        )
        conn.commit()
        if is_postgres_backend():
            row = cur.fetchone()
            return int((row or {}).get("decision_id") or 0)
        return int(cur.lastrowid)


def get_due_pricing_autopilot_decisions(*, review_before: str) -> list[dict[str, Any]]:
    init_store_data_model()
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM pricing_autopilot_decisions
            WHERE decision_status = 'pending'
              AND review_after <= {'%s' if is_postgres_backend() else '?'}
            ORDER BY created_at ASC
            """,
            (str(review_before or "").strip(),),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["reason"] = json.loads(item.get("reason_json") or "{}")
        except Exception:
            item["reason"] = {}
        try:
            item["result"] = json.loads(item.get("result_json") or "{}")
        except Exception:
            item["result"] = {}
        out.append(item)
    return out


def get_open_pricing_autopilot_decision_map(*, store_uids: list[str], skus: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
    init_store_data_model()
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}
    store_placeholders = _placeholders(len(suids))
    sku_placeholders = _placeholders(len(sku_list))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT d1.*
            FROM pricing_autopilot_decisions d1
            JOIN (
                SELECT store_uid, sku, MAX(decision_id) AS max_decision_id
                FROM pricing_autopilot_decisions
                WHERE decision_status = 'pending'
                  AND store_uid IN ({store_placeholders})
                  AND sku IN ({sku_placeholders})
                GROUP BY store_uid, sku
            ) latest ON latest.max_decision_id = d1.decision_id
            """,
            [*suids, *sku_list],
        ).fetchall()
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        try:
            item["reason"] = json.loads(item.get("reason_json") or "{}")
        except Exception:
            item["reason"] = {}
        out.setdefault(str(row["store_uid"]), {})[str(row["sku"])] = item
    return out


def finalize_pricing_autopilot_decision(
    *,
    decision_id: int,
    decision_status: str,
    reviewed_at: str,
    review_snapshot_id: int | None,
    result: dict[str, Any] | None = None,
) -> None:
    init_store_data_model()
    with _connect() as conn:
        conn.execute(
            f"""
            UPDATE pricing_autopilot_decisions
            SET decision_status = {'%s' if is_postgres_backend() else '?'},
                reviewed_at = {'%s' if is_postgres_backend() else '?'},
                review_snapshot_id = {'%s' if is_postgres_backend() else '?'},
                result_json = {'%s' if is_postgres_backend() else '?'}
            WHERE decision_id = {'%s' if is_postgres_backend() else '?'}
            """,
            (
                str(decision_status or "").strip(),
                str(reviewed_at or "").strip(),
                review_snapshot_id,
                json.dumps(result or {}, ensure_ascii=False),
                int(decision_id),
            ),
        )
        conn.commit()


def upsert_pricing_logistics_store_settings(*, store_uid: str, values: dict[str, Any]) -> dict[str, Any]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")

    current = get_pricing_logistics_store_settings(store_uid=suid)
    next_values = dict(current)

    if "fulfillment_model" in values:
        v = str(values.get("fulfillment_model") or "").strip().upper()
        if v in {"FBO", "FBS", "DBS", "EXPRESS"}:
            next_values["fulfillment_model"] = v
    if "handling_mode" in values:
        v = str(values.get("handling_mode") or "").strip().lower()
        if v in {"fixed", "percent"}:
            next_values["handling_mode"] = v

    numeric_keys = (
        "handling_fixed_amount",
        "handling_percent",
        "handling_min_amount",
        "handling_max_amount",
        "delivery_cost_per_kg",
        "return_processing_cost",
        "disposal_cost",
    )
    for key in numeric_keys:
        if key not in values:
            continue
        raw = values.get(key)
        if raw in ("", None):
            next_values[key] = None
            continue
        try:
            next_values[key] = float(str(raw).replace(",", "."))
        except Exception:
            next_values[key] = None

    now = _now_iso()
    with _connect() as conn:
        values_sql = _placeholders(11)
        conn.execute(
            f"""
            INSERT INTO pricing_logistics_store_settings (
                store_uid, fulfillment_model, handling_mode, handling_fixed_amount, handling_percent,
                handling_min_amount, handling_max_amount, delivery_cost_per_kg,
                return_processing_cost, disposal_cost, updated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid) DO UPDATE SET
                fulfillment_model = excluded.fulfillment_model,
                handling_mode = excluded.handling_mode,
                handling_fixed_amount = excluded.handling_fixed_amount,
                handling_percent = excluded.handling_percent,
                handling_min_amount = excluded.handling_min_amount,
                handling_max_amount = excluded.handling_max_amount,
                delivery_cost_per_kg = excluded.delivery_cost_per_kg,
                return_processing_cost = excluded.return_processing_cost,
                disposal_cost = excluded.disposal_cost,
                updated_at = excluded.updated_at
            """,
            (
                suid,
                next_values.get("fulfillment_model") or "FBO",
                next_values.get("handling_mode") or "fixed",
                next_values.get("handling_fixed_amount"),
                next_values.get("handling_percent"),
                next_values.get("handling_min_amount"),
                next_values.get("handling_max_amount"),
                next_values.get("delivery_cost_per_kg"),
                next_values.get("return_processing_cost"),
                next_values.get("disposal_cost"),
                now,
            ),
        )
        conn.commit()
    next_values["updated_at"] = now
    _merge_system_store_settings_sections(
        store_uid=suid,
        sections={
            "logistics": {
                "fulfillment_model": next_values.get("fulfillment_model"),
                "handling_mode": next_values.get("handling_mode"),
                "handling_fixed_amount": next_values.get("handling_fixed_amount"),
                "handling_percent": next_values.get("handling_percent"),
                "handling_min_amount": next_values.get("handling_min_amount"),
                "handling_max_amount": next_values.get("handling_max_amount"),
                "delivery_cost_per_kg": next_values.get("delivery_cost_per_kg"),
                "return_processing_cost": next_values.get("return_processing_cost"),
                "disposal_cost": next_values.get("disposal_cost"),
            }
        },
        updated_at=now,
    )
    return next_values


def get_pricing_logistics_product_settings_map(*, store_uid: str, skus: list[str]) -> dict[str, dict[str, Any]]:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid or not skus:
        return {}
    norm_skus = [str(s).strip() for s in skus if str(s).strip()]
    if not norm_skus:
        return {}
    placeholders = _placeholders(len(norm_skus))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT sku, width_cm, length_cm, height_cm, weight_kg, updated_at
            FROM pricing_logistics_product_settings
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'} AND sku IN ({placeholders})
            """,
            [suid, *norm_skus],
        ).fetchall()
    return {str(r["sku"]): dict(r) for r in rows}


def upsert_pricing_logistics_product_settings_bulk(*, store_uid: str, rows: list[dict[str, Any]]) -> int:
    init_store_data_model()
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        vals = []
        for key in ("width_cm", "length_cm", "height_cm", "weight_kg"):
            raw = row.get(key)
            if raw in ("", None):
                vals.append(None)
                continue
            try:
                vals.append(float(str(raw).replace(",", ".")))
            except Exception:
                vals.append(None)
        prepared.append((suid, sku, vals[0], vals[1], vals[2], vals[3], now))
    if not prepared:
        return 0
    with _connect() as conn:
        values_sql = _placeholders(7)
        _executemany(conn, 
            f"""
            INSERT INTO pricing_logistics_product_settings (
                store_uid, sku, width_cm, length_cm, height_cm, weight_kg, updated_at
            ) VALUES ({values_sql})
            ON CONFLICT(store_uid, sku) DO UPDATE SET
                width_cm = excluded.width_cm,
                length_cm = excluded.length_cm,
                height_cm = excluded.height_cm,
                weight_kg = excluded.weight_kg,
                updated_at = excluded.updated_at
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def upsert_pricing_category_setting(
    *,
    dataset_key: str,
    store_uid: str,
    leaf_path: str,
    values: dict[str, Any],
) -> None:
    init_store_data_model()
    dkey = str(dataset_key or "").strip()
    suid = str(store_uid or "").strip()
    leaf = str(leaf_path or "").strip()
    if not dkey or not suid or not leaf:
        raise ValueError("dataset_key, store_uid, leaf_path обязательны")

    allowed = {
        "commission_percent",
        "acquiring_percent",
        "logistics_rub",
        "ads_percent",
        "returns_percent",
        "tax_percent",
        "other_expenses_rub",
        "other_expenses_percent",
        "cogs_rub",
        "target_profit_rub",
        "target_profit_percent",
        "target_margin_rub",
        "target_margin_percent",
    }
    update_values: dict[str, Any] = {}
    for key, val in (values or {}).items():
        if key not in allowed:
            continue
        if val in ("", None):
            update_values[key] = None
            continue
        try:
            update_values[key] = float(str(val).replace(",", "."))
        except Exception:
            update_values[key] = None

    if not update_values:
        return

    now = _now_iso()
    cols = list(update_values.keys())
    placeholders = _placeholders(3 + len(cols) + 1)
    insert_cols = ", ".join(["dataset_key", "store_uid", "leaf_path", *cols, "updated_at"])
    insert_vals = [dkey, suid, leaf, *[update_values[c] for c in cols], now]

    # On conflict update only provided columns
    set_sql = ", ".join([f"{c} = excluded.{c}" for c in cols] + ["updated_at = excluded.updated_at"])
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO pricing_category_settings ({insert_cols})
            VALUES ({placeholders})
            ON CONFLICT(dataset_key, leaf_path) DO UPDATE SET
                {set_sql}
            """,
            insert_vals,
        )
        conn.commit()


def seed_pricing_category_settings_if_null(
    *,
    dataset_key: str,
    store_uid: str,
    rows: list[dict[str, Any]],
) -> int:
    init_store_data_model()
    dkey = str(dataset_key or "").strip()
    suid = str(store_uid or "").strip()
    if not dkey or not suid:
        raise ValueError("dataset_key и store_uid обязательны")

    allowed = {
        "commission_percent",
        "acquiring_percent",
        "logistics_rub",
        "ads_percent",
        "returns_percent",
        "tax_percent",
        "other_expenses_rub",
        "other_expenses_percent",
        "cogs_rub",
        "target_profit_rub",
        "target_profit_percent",
        "target_margin_rub",
        "target_margin_percent",
    }
    now = _now_iso()
    inserted = 0
    with _connect() as conn:
        for row in rows:
            if not isinstance(row, dict):
                continue
            leaf = str(row.get("leaf_path") or "").strip()
            if not leaf:
                continue
            vals: dict[str, Any] = {}
            for key, val in row.items():
                if key not in allowed:
                    continue
                if val in ("", None):
                    continue
                try:
                    vals[key] = float(str(val).replace(",", "."))
                except Exception:
                    continue
            if not vals:
                continue
            cols = list(vals.keys())
            insert_cols = ", ".join(["dataset_key", "store_uid", "leaf_path", *cols, "updated_at"])
            placeholders = _placeholders(3 + len(cols) + 1)
            set_sql = ", ".join([f"{c}=COALESCE(pricing_category_settings.{c}, excluded.{c})" for c in cols] + ["updated_at=excluded.updated_at"])
            conn.execute(
                f"""
                INSERT INTO pricing_category_settings ({insert_cols})
                VALUES ({placeholders})
                ON CONFLICT(dataset_key, leaf_path) DO UPDATE SET {set_sql}
                """,
                [dkey, suid, leaf, *[vals[c] for c in cols], now],
            )
            inserted += 1
        conn.commit()
    return inserted


def bulk_apply_pricing_defaults(
    *,
    dataset_key: str,
    store_uid: str,
    commission_percent: Any | None = None,
    target_margin_percent: Any | None = None,
    target_margin_rub: Any | None = None,
    target_profit_rub: Any | None = None,
    target_profit_percent: Any | None = None,
    ads_percent: Any | None = None,
) -> int:
    init_store_data_model()
    dkey = str(dataset_key or "").strip()
    suid = str(store_uid or "").strip()
    if not dkey or not suid:
        raise ValueError("dataset_key и store_uid обязательны")

    def _num(v):
        if v in ("", None):
            return None
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return None

    commission_val = _num(commission_percent)
    margin_val = _num(target_margin_percent)
    margin_rub_val = _num(target_margin_rub)
    profit_val = _num(target_profit_rub)
    profit_percent_val = _num(target_profit_percent)
    ads_val = _num(ads_percent)
    if (
        commission_val is None
        and margin_val is None
        and margin_rub_val is None
        and profit_val is None
        and profit_percent_val is None
        and ads_val is None
    ):
        return 0

    now = _now_iso()
    updated = 0
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT leaf_path FROM pricing_category_tree WHERE dataset_key = {'%s' if is_postgres_backend() else '?'}",
            (dkey,),
        ).fetchall()
        for r in rows:
            leaf_path = str(r["leaf_path"] or "").strip()
            if not leaf_path:
                continue
            cols = []
            vals = []
            if commission_val is not None:
                cols.append("commission_percent")
                vals.append(commission_val)
            if margin_val is not None:
                cols.append("target_margin_percent")
                vals.append(margin_val)
            if margin_rub_val is not None:
                cols.append("target_margin_rub")
                vals.append(margin_rub_val)
            if profit_val is not None:
                cols.append("target_profit_rub")
                vals.append(profit_val)
            if profit_percent_val is not None:
                cols.append("target_profit_percent")
                vals.append(profit_percent_val)
            if ads_val is not None:
                cols.append("ads_percent")
                vals.append(ads_val)
            insert_cols = ", ".join(["dataset_key", "store_uid", "leaf_path", *cols, "updated_at"])
            placeholders = _placeholders(3 + len(cols) + 1)
            # Верхние глобальные параметры должны автоприменяться ко всем строкам.
            set_sql = ", ".join([f"{c}=excluded.{c}" for c in cols] + ["updated_at=excluded.updated_at"])
            conn.execute(
                f"""
                INSERT INTO pricing_category_settings ({insert_cols})
                VALUES ({placeholders})
                ON CONFLICT(dataset_key, leaf_path) DO UPDATE SET {set_sql}
                """,
                [dkey, suid, leaf_path, *vals, now],
            )
            updated += 1
        conn.commit()
    return updated


def replace_fx_rates_cache(
    *,
    source: str,
    rows: list[dict[str, Any]],
    pair: str = "USD_RUB",
    meta: dict[str, Any] | None = None,
) -> int:
    init_store_data_model()
    src = str(source or "").strip()
    pr = str(pair or "USD_RUB").strip()
    if not src or not pr:
        raise ValueError("source и pair обязательны")
    now = _now_iso()
    prepared: list[tuple[Any, ...]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        rate_date = str(row.get("date") or "").strip()
        if not rate_date:
            continue
        try:
            rate = float(row.get("rate"))
        except Exception:
            continue
        prepared.append((src, pr, rate_date, rate, now, json.dumps(meta or {}, ensure_ascii=False)))
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM fx_rates_cache WHERE source = {'%s' if is_postgres_backend() else '?'} AND pair = {'%s' if is_postgres_backend() else '?'}",
            (src, pr),
        )
        if prepared:
            if is_postgres_backend():
                next_id = 1
                with conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(MAX(id), 0) FROM fx_rates_cache")
                    row = cur.fetchone()
                    try:
                        next_id = int((row or [0])[0]) + 1
                    except Exception:
                        next_id = 1
                prepared_with_ids: list[tuple[Any, ...]] = []
                for offset, payload in enumerate(prepared):
                    prepared_with_ids.append((next_id + offset, *payload))
                values_sql = _placeholders(7)
                _executemany(
                    conn,
                    f"""
                    INSERT INTO fx_rates_cache (id, source, pair, rate_date, rate_value, loaded_at, meta_json)
                    VALUES ({values_sql})
                    """,
                    prepared_with_ids,
                )
            else:
                values_sql = _placeholders(6)
                _executemany(
                    conn,
                    f"""
                    INSERT INTO fx_rates_cache (source, pair, rate_date, rate_value, loaded_at, meta_json)
                    VALUES ({values_sql})
                    """,
                    prepared,
                )
        conn.commit()
    return len(prepared)


def get_fx_rates_cache(
    *,
    source: str,
    date_from: str | None = None,
    date_to: str | None = None,
    pair: str = "USD_RUB",
) -> dict[str, Any]:
    init_store_data_model()
    src = str(source or "").strip()
    pr = str(pair or "USD_RUB").strip()
    if not src or not pr:
        raise ValueError("source и pair обязательны")
    sql = """
        SELECT rate_date, rate_value, loaded_at, meta_json
        FROM fx_rates_cache
        WHERE source = {source_marker} AND pair = {pair_marker}
    """
    sql = sql.format(
        source_marker='%s' if is_postgres_backend() else '?',
        pair_marker='%s' if is_postgres_backend() else '?',
    )
    params: list[Any] = [src, pr]
    if date_from:
        sql += f" AND rate_date >= {'%s' if is_postgres_backend() else '?'}"
        params.append(str(date_from))
    if date_to:
        sql += f" AND rate_date <= {'%s' if is_postgres_backend() else '?'}"
        params.append(str(date_to))
    sql += " ORDER BY rate_date DESC"
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    data_rows = [{"date": str(r["rate_date"]), "rate": float(r["rate_value"])} for r in rows]
    loaded_at = str(rows[0]["loaded_at"]) if rows else ""
    meta = {}
    if rows:
        try:
            meta = json.loads(rows[0]["meta_json"] or "{}")
        except Exception:
            meta = {}
    return {"rows": data_rows, "loaded_at": loaded_at, "meta": meta}
