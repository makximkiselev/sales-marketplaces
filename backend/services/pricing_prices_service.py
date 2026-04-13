from __future__ import annotations

import datetime
import logging
import math
from typing import Any

from backend.routers._shared import (
    _catalog_marketplace_stores_context,
    _catalog_external_tree_sources_context,
    _catalog_tree_from_paths,
    _fetch_cbr_usd_rates,
    _read_source_rows,
)
from backend.services.store_data_model import (
    get_fx_rates_cache,
    get_pricing_cogs_snapshot_map,
    get_pricing_catalog_sku_path_map,
    get_pricing_category_settings_map,
    get_pricing_category_tree,
    get_pricing_logistics_product_settings_map,
    get_pricing_logistics_store_settings,
    get_pricing_price_results_map,
    get_pricing_store_settings,
    get_pricing_strategy_results_map,
    replace_fx_rates_cache,
    upsert_pricing_price_results_bulk,
)
from backend.services.pricing_catalog_helpers import (
    clamp_rate as _clamp_rate,
    compute_max_weight_kg as _compute_max_weight_kg,
    idx_of as _idx_of,
    leaf_path_from_row as _leaf_path_from_row,
    norm_col_name as _norm_col_name,
    num0 as _num0,
    profit_for_price as _profit_for_price,
    resolve_source_table_name as _resolve_source_table_name,
    to_num as _to_num,
)
from backend.services.service_cache_helpers import cache_get_copy, cache_set_copy, make_cache_key
from backend.services.storage import get_source_by_id, is_source_mode_enabled


def refresh_pricing_catalog_trees_from_sources(*args, **kwargs):
    from backend.services.pricing_catalog_tree_service import refresh_pricing_catalog_trees_from_sources as impl
    return impl(*args, **kwargs)


def read_sheet_all(*args, **kwargs):
    from backend.services.gsheets import read_sheet_all as impl
    return impl(*args, **kwargs)

_PRICES_CACHE: dict[str, dict] = {}
_PRICES_CACHE_GEN = 1
_PRICES_CACHE_MAX = 400
_PRICES_SNAPSHOT_CACHE: dict[str, dict] = {}
_PRICES_SNAPSHOT_CACHE_MAX = 24
_FX_USD_RUB_MEM: dict[str, float] = {}
logger = logging.getLogger("uvicorn.error")


def _cache_key(name: str, payload: dict) -> str:
    return make_cache_key(name, payload, _PRICES_CACHE_GEN)


def _cache_get(name: str, payload: dict):
    key = _cache_key(name, payload)
    return cache_get_copy(_PRICES_CACHE, key)


def _cache_set(name: str, payload: dict, value: dict):
    key = _cache_key(name, payload)
    cache_set_copy(_PRICES_CACHE, key, value, _PRICES_CACHE_MAX)


def _snapshot_payload_from_overview_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "scope": payload.get("scope"),
        "platform": payload.get("platform"),
        "store_id": payload.get("store_id"),
        "tree_mode": payload.get("tree_mode"),
        "tree_source_store_id": payload.get("tree_source_store_id"),
        "category_path": payload.get("category_path"),
        "search": payload.get("search"),
        "stock_filter": payload.get("stock_filter"),
    }


def _snapshot_get(payload: dict[str, Any]) -> dict[str, Any] | None:
    key = _cache_key("overview_full", payload)
    return cache_get_copy(_PRICES_SNAPSHOT_CACHE, key)


def _snapshot_set(payload: dict[str, Any], value: dict[str, Any]) -> None:
    key = _cache_key("overview_full", payload)
    cache_set_copy(_PRICES_SNAPSHOT_CACHE, key, value, _PRICES_SNAPSHOT_CACHE_MAX)


def _can_reuse_materialized_price_metrics(*, db_rec: dict[str, Any] | None, src_updated: str | None) -> bool:
    if not isinstance(db_rec, dict) or not db_rec:
        return False
    source_updated = str(src_updated or "").strip()
    if not source_updated:
        return False
    db_source_updated = str(db_rec.get("source_updated_at") or "").strip()
    return bool(db_source_updated) and db_source_updated == source_updated


def _build_prices_page_from_snapshot(snapshot: dict[str, Any], *, page: int, page_size: int) -> dict[str, Any]:
    rows = list(snapshot.get("rows") or [])
    total_count = int(snapshot.get("total_count") or len(rows))
    page_size_n = max(1, min(int(page_size or 50), 100000))
    page_n = max(1, int(page or 1))
    start = (page_n - 1) * page_size_n
    paged = rows[start:start + page_size_n]
    return {
        "ok": True,
        "scope": snapshot.get("scope"),
        "platform": snapshot.get("platform"),
        "store_id": snapshot.get("store_id"),
        "tree_mode": snapshot.get("tree_mode"),
        "tree_source": snapshot.get("tree_source"),
        "stores": list(snapshot.get("stores") or []),
        "rows": paged,
        "total_count": total_count,
        "page": page_n,
        "page_size": page_size_n,
    }


def invalidate_prices_cache():
    global _PRICES_CACHE_GEN
    _PRICES_CACHE.clear()
    _PRICES_SNAPSHOT_CACHE.clear()
    _PRICES_CACHE_GEN += 1


def _build_empty_prices_overview_response(*, scope_norm: str, page: int, page_size: int) -> dict[str, Any]:
    return {
        "ok": True,
        "scope": scope_norm,
        "rows": [],
        "total_count": 0,
        "page": max(1, page),
        "page_size": max(1, min(page_size, 100000)),
        "stores": [],
        "tree_source": None,
    }


def _resolve_prices_tree_source(
    *,
    stores: list[dict[str, Any]],
    target_stores: list[dict[str, Any]],
    scope_norm: str,
    platform_norm: str,
    store_norm: str,
    tree_mode_norm: str,
    tree_source_store_id: str,
) -> dict[str, Any] | None:
    tree_source = None
    if tree_mode_norm == "marketplaces":
        if scope_norm == "store":
            tree_source = next(
                (s for s in target_stores if s["platform"] == platform_norm and s["store_id"] == store_norm),
                None,
            )
        else:
            chosen = str(tree_source_store_id or "").strip()
            tree_source = _match_tree_source_store(stores, chosen) if chosen else None
        if not tree_source:
            tree_source = next((s for s in stores if s.get("table_name")), None)
    return tree_source


async def _get_cbr_usd_rub_rate_for_date(calc_date: datetime.date) -> float | None:
    key = calc_date.isoformat()
    if key in _FX_USD_RUB_MEM:
        return _FX_USD_RUB_MEM[key]

    def _prefer_latest_published(by_date: dict[str, float]) -> tuple[str | None, float | None]:
        if not by_date:
            return None, None
        latest_date = max(by_date.keys())
        latest_rate = float(by_date[latest_date])
        return latest_date, latest_rate

    cached_best_date: str | None = None
    cached_best_rate: float | None = None
    try:
        cached = get_fx_rates_cache(source="cbr", pair="USD_RUB")
        rows = cached.get("rows") if isinstance(cached, dict) else None
        if isinstance(rows, list) and rows:
            by_date: dict[str, float] = {}
            for r in rows:
                d = str(r.get("date") or "").strip()
                try:
                    v = float(r.get("rate"))
                except Exception:
                    continue
                if d:
                    by_date[d] = v
            if by_date:
                cached_best_date, cached_best_rate = _prefer_latest_published(by_date)
                if cached_best_date == key:
                    rate = float(cached_best_rate)
                    if rate > 0:
                        _FX_USD_RUB_MEM[key] = rate
                        return rate
                if cached_best_date and cached_best_date > key and cached_best_rate and cached_best_rate > 0:
                    logger.warning(
                        "[pricing_prices] using latest published CBR USD/RUB rate publish_date=%s target_date=%s rate=%s",
                        cached_best_date,
                        key,
                        cached_best_rate,
                    )
                    _FX_USD_RUB_MEM[key] = float(cached_best_rate)
                    return float(cached_best_rate)
    except Exception:
        logger.warning("[pricing_prices] failed to read CBR USD/RUB cache for date=%s", key, exc_info=True)

    try:
        start = calc_date - datetime.timedelta(days=60)
        end = calc_date + datetime.timedelta(days=1)
        fresh_rows = await _fetch_cbr_usd_rates(start, end)
        if fresh_rows:
            replace_fx_rates_cache(
                source="cbr",
                pair="USD_RUB",
                rows=fresh_rows,
                meta={"loaded_from": "pricing_prices_overview"},
            )
            by_date: dict[str, float] = {}
            for r in fresh_rows:
                d = str(r.get("date") or "").strip()
                try:
                    v = float(r.get("rate"))
                except Exception:
                    continue
                if d:
                    by_date[d] = v
            if by_date:
                best_date, rate = _prefer_latest_published(by_date)
                rate = float(rate or 0.0)
                if rate > 0:
                    if best_date != key:
                        logger.warning(
                            "[pricing_prices] using latest published CBR USD/RUB rate publish_date=%s target_date=%s rate=%s",
                            best_date,
                            key,
                            rate,
                        )
                    _FX_USD_RUB_MEM[key] = rate
                    return rate
    except Exception:
        logger.warning("[pricing_prices] failed to fetch CBR USD/RUB rates for date=%s", key, exc_info=True)
    if cached_best_rate and cached_best_rate > 0:
        logger.warning(
            "[pricing_prices] using stale cached CBR USD/RUB rate cache_date=%s target_date=%s rate=%s",
            cached_best_date,
            key,
            cached_best_rate,
        )
        return float(cached_best_rate)
    return None


def _resolve_category_settings_for_leaf(
    settings_map: dict[str, dict] | None,
    leaf_path: str,
) -> dict[str, Any]:
    src = settings_map if isinstance(settings_map, dict) else {}
    leaf = str(leaf_path or "").strip()
    if not src or not leaf:
        return {}
    parts = [part.strip() for part in leaf.split(" / ") if part.strip()]
    if not parts:
        return {}
    merged: dict[str, Any] = {}
    for depth in range(len(parts), 0, -1):
        candidate = " / ".join(parts[:depth])
        row = src.get(candidate)
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if key in {"leaf_path", "dataset_key", "store_uid", "updated_at"}:
                continue
            if merged.get(key) in (None, "") and value not in (None, ""):
                merged[key] = value
    return merged


def _extract_rrc_from_payload(payload: dict | None) -> float | None:
    p = payload if isinstance(payload, dict) else {}

    def _pick(path: list[str]) -> float | None:
        cur: Any = p
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur.get(part)
        return _to_num(cur)

    # Yandex Market
    for path in (
        ["campaign_offer", "basicPrice", "value"],
        ["offer_mapping", "offer", "basicPrice", "value"],
        ["offer", "basicPrice", "value"],
        ["basicPrice", "value"],
    ):
        v = _pick(path)
        if v is not None:
            return v

    # Ozon
    for path in (
        ["product_info", "price"],
        ["product_info", "old_price"],
        ["price"],
    ):
        v = _pick(path)
        if v is not None:
            return v

    return None


def _extract_rrc_currency_from_payload(payload: dict | None) -> str | None:
    p = payload if isinstance(payload, dict) else {}

    def _pick(path: list[str]) -> str | None:
        cur: Any = p
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur.get(part)
        val = str(cur or "").strip().upper()
        return val or None

    for path in (
        ["campaign_offer", "basicPrice", "currencyId"],
        ["offer_mapping", "offer", "basicPrice", "currencyId"],
        ["offer", "basicPrice", "currencyId"],
        ["basicPrice", "currencyId"],
        ["product_info", "currency_code"],
        ["currency"],
    ):
        v = _pick(path)
        if v:
            return v
    return None


def _ceil_price_to_threshold(
    *,
    estimated_price: float,
    meets: callable,
    max_steps: int = 50,
) -> float:
    candidate = max(1, int(math.ceil(float(estimated_price or 0.0))))
    for _ in range(max(0, int(max_steps))):
        if meets(float(candidate)):
            return float(candidate)
        candidate += 1
    return float(candidate)


def _load_numeric_map_from_gsheets_source(*, source_id: str, sku_column: str, value_column: str) -> dict[str, float]:
    source = get_source_by_id(source_id)
    if not isinstance(source, dict):
        return {}
    if str(source.get("type") or "").strip().lower() != "gsheets":
        return {}
    spreadsheet_id = str(source.get("spreadsheet_id") or "").strip()
    worksheet = str(source.get("worksheet") or "").strip() or None
    if not spreadsheet_id:
        return {}

    payload = read_sheet_all(spreadsheet_id, worksheet=worksheet)
    all_rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(all_rows, list) or not all_rows:
        return {}

    header = [str(x or "").strip() for x in (all_rows[0] if isinstance(all_rows[0], list) else [])]
    if not header:
        return {}
    h_norm = [_norm_col_name(h) for h in header]

    sku_idx = _idx_of(h_norm, str(sku_column or "").strip())
    value_idx = _idx_of(h_norm, str(value_column or "").strip())
    if sku_idx < 0 or value_idx < 0:
        return {}

    out: dict[str, float] = {}
    for row in all_rows[1:]:
        if not isinstance(row, list):
            continue
        sku = str(row[sku_idx] if sku_idx < len(row) else "").strip()
        if not sku:
            continue
        num = _to_num(row[value_idx] if value_idx < len(row) else None)
        if num is None:
            continue
        out[sku] = float(num)
    return out


def _match_tree_source_store(stores: list[dict[str, Any]], chosen: str) -> dict[str, Any] | None:
    selected = str(chosen or "").strip()
    if not selected:
        return None
    by_uid = next((s for s in stores if str(s.get("store_uid") or "").strip() == selected and s.get("table_name")), None)
    if by_uid:
        return by_uid
    return next((s for s in stores if str(s.get("store_id") or "").strip() == selected and s.get("table_name")), None)


def _load_numeric_map_from_source(*, source_id: str, sku_column: str, value_column: str) -> dict[str, float]:
    if not is_source_mode_enabled(source_id, "import", default=True):
        return {}
    source = get_source_by_id(source_id)
    if isinstance(source, dict) and str(source.get("type") or "").strip().lower() == "gsheets":
        try:
            live_rows = _load_numeric_map_from_gsheets_source(
                source_id=source_id,
                sku_column=sku_column,
                value_column=value_column,
            )
            if live_rows:
                return live_rows
        except Exception:
            # Fallback to the materialized source table if live Google read is unavailable.
            pass
    table_name = _resolve_source_table_name(source_id)
    if not table_name:
        return {}
    rows = _read_source_rows(table_name)
    sku_col = str(sku_column or "").strip()
    value_col = str(value_column or "").strip()
    sku_col_norm = _norm_col_name(sku_col)
    value_col_norm = _norm_col_name(value_col)
    out: dict[str, float] = {}
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        sku_raw = None
        if sku_col and payload:
            sku_raw = payload.get(sku_col)
            if sku_raw in (None, "") and sku_col_norm:
                for k, v in payload.items():
                    if _norm_col_name(str(k)) == sku_col_norm:
                        sku_raw = v
                        break
        if sku_raw in (None, ""):
            sku_raw = row.get("sku")
        sku = str(sku_raw or "").strip()
        if not sku:
            continue

        if value_col:
            val_raw = payload.get(value_col) if payload else None
            if val_raw in (None, "") and value_col_norm and payload:
                for k, v in payload.items():
                    if _norm_col_name(str(k)) == value_col_norm:
                        val_raw = v
                        break
            if val_raw in (None, "") and value_col.lower() == "price":
                val_raw = row.get("price")
        else:
            val_raw = row.get("price")

        num = _to_num(val_raw)
        if num is None:
            continue
        out[sku] = float(num)
    return out


def _load_cogs_map_from_source(*, source_id: str, sku_column: str, value_column: str) -> dict[str, float]:
    return _load_numeric_map_from_source(source_id=source_id, sku_column=sku_column, value_column=value_column)


def _load_stock_map_from_source(*, source_id: str, sku_column: str, value_column: str) -> dict[str, float]:
    return _load_numeric_map_from_source(source_id=source_id, sku_column=sku_column, value_column=value_column)


async def get_prices_context():
    cached = _cache_get("context", {})
    if cached:
        return cached
    resp = {
        "ok": True,
        "tree_mode_options": [
            {"id": "marketplaces", "label": "Маркетплейсы"},
            {"id": "external", "label": "Внешний источник"},
        ],
        "marketplace_stores": _catalog_marketplace_stores_context(),
        "external_tree_source_types": [
            {"id": "tables", "label": "Таблицы"},
            {"id": "external_systems", "label": "Внешние системы"},
        ],
        "external_sources": _catalog_external_tree_sources_context(),
    }
    _cache_set("context", {}, resp)
    return resp


async def get_prices_tree(
    *,
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
):
    cache_payload = {
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
    }
    cached = _cache_get("tree", cache_payload)
    if cached:
        return cached
    mode = str(tree_mode or "marketplaces").strip().lower()
    if mode != "marketplaces":
        resp = {"ok": True, "tree_mode": mode, "roots": [{"name": "Не определено", "children": []}], "source": None}
        _cache_set("tree", cache_payload, resp)
        return resp

    stores = _catalog_marketplace_stores_context()
    if str(scope or "all").strip().lower() == "store":
        src_store = next((s for s in stores if s["platform"] == str(platform or "").strip().lower() and s["store_id"] == str(store_id or "").strip()), None)
    else:
        chosen = str(tree_source_store_id or "").strip()
        src_store = _match_tree_source_store(stores, chosen) if chosen else None
    if not src_store:
        src_store = next((s for s in stores if s.get("table_name")), None)
    if not src_store or not src_store.get("table_name"):
        resp = {"ok": True, "tree_mode": mode, "roots": [{"name": "Не определено", "children": []}], "source": None}
        _cache_set("tree", cache_payload, resp)
        return resp

    priority_platform = str(src_store.get("platform") or "").strip().lower()
    cached_paths = get_pricing_catalog_sku_path_map(priority_platform=priority_platform)
    paths: list[list[str]] = []
    has_undefined = False
    for item in cached_paths.values():
        leaf = str((item or {}).get("leaf_path") or "").strip()
        if not leaf or leaf == "Не определено":
            has_undefined = True
            continue
        path = [part.strip() for part in leaf.split(" / ") if part.strip()]
        if path:
            paths.append(path)
        else:
            has_undefined = True
    roots = _catalog_tree_from_paths(paths)
    if has_undefined:
        roots = [{"name": "Не определено", "children": []}, *roots]
    resp = {"ok": True, "tree_mode": mode, "roots": roots, "source": src_store}
    _cache_set("tree", cache_payload, resp)
    return resp


async def get_prices_overview(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    stock_filter: str = "all",
    page: int = 1,
    page_size: int = 50,
    force_refresh: bool = False,
):
    cache_payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "stock_filter": stock_filter,
        "page": page,
        "page_size": page_size,
    }
    if not force_refresh:
        cached = _cache_get("overview", cache_payload)
        if cached:
            return cached
        snapshot_cached = _snapshot_get(_snapshot_payload_from_overview_payload(cache_payload))
        if snapshot_cached:
            resp = _build_prices_page_from_snapshot(
                snapshot_cached,
                page=page,
                page_size=page_size,
            )
            _cache_set("overview", cache_payload, resp)
            return resp

    stores = _catalog_marketplace_stores_context()
    scope_norm = str(scope or "all").strip().lower()
    platform_norm = str(platform or "").strip().lower()
    store_norm = str(store_id or "").strip()
    tree_mode_norm = str(tree_mode or "marketplaces").strip().lower()

    if scope_norm == "store":
        target_stores = [s for s in stores if s["platform"] == platform_norm and s["store_id"] == store_norm]
    else:
        target_stores = [s for s in stores if s.get("table_name")]
    target_stores = [s for s in target_stores if s.get("table_name")]

    if not target_stores:
        resp = _build_empty_prices_overview_response(scope_norm=scope_norm, page=page, page_size=page_size)
        _cache_set("overview", cache_payload, resp)
        return resp

    tree_source = _resolve_prices_tree_source(
        stores=stores,
        target_stores=target_stores,
        scope_norm=scope_norm,
        platform_norm=platform_norm,
        store_norm=store_norm,
        tree_mode_norm=tree_mode_norm,
        tree_source_store_id=tree_source_store_id,
    )

    source_rows_map: dict[str, list[dict]] = {}
    source_row_by_store_sku: dict[str, dict[str, dict]] = {}
    cogs_map_by_store_uid: dict[str, dict[str, float]] = {}
    stock_map_by_store_uid: dict[str, dict[str, float]] = {}
    rrc_map_by_store_uid: dict[str, dict[str, float]] = {}
    rrc_currency_by_store_uid: dict[str, dict[str, str]] = {}
    category_settings_by_store_uid: dict[str, dict[str, dict]] = {}
    store_settings_by_store_uid: dict[str, dict] = {}
    logistics_store_by_store_uid: dict[str, dict] = {}
    logistics_product_by_store_uid: dict[str, dict[str, dict]] = {}

    for s in target_stores:
        suid = str(s["store_uid"])
        try:
            source_rows_map[suid] = _read_source_rows(str(s["table_name"]))
            source_row_by_store_sku[suid] = {
                str(r.get("sku") or "").strip(): r for r in source_rows_map[suid] if str(r.get("sku") or "").strip()
            }
            rrc_local: dict[str, float] = {}
            rrc_currency_local: dict[str, str] = {}
            for row in source_rows_map[suid]:
                sku = str(row.get("sku") or "").strip()
                if not sku:
                    continue
                price_num = _to_num(row.get("price"))
                price_currency = str(row.get("currency") or "").strip().upper() or None
                if price_num is None:
                    price_num = _extract_rrc_from_payload(row.get("payload"))
                if not price_currency:
                    price_currency = _extract_rrc_currency_from_payload(row.get("payload"))
                if price_num is None:
                    continue
                rrc_local[sku] = float(price_num)
                if price_currency:
                    rrc_currency_local[sku] = price_currency
            rrc_map_by_store_uid[suid] = rrc_local
            rrc_currency_by_store_uid[suid] = rrc_currency_local
        except Exception:
            source_rows_map[suid] = []
            source_row_by_store_sku[suid] = {}
            rrc_map_by_store_uid[suid] = {}
            rrc_currency_by_store_uid[suid] = {}

        try:
            ps = get_pricing_store_settings(store_uid=suid) or {}
            stock_source_id = str(ps.get("stock_source_id") or "").strip()
            stock_sku_column = str(ps.get("stock_sku_column") or "").strip()
            stock_value_column = str(ps.get("stock_value_column") or "").strip()
            if stock_source_id:
                stock_map_by_store_uid[suid] = _load_stock_map_from_source(
                    source_id=stock_source_id,
                    sku_column=stock_sku_column,
                    value_column=stock_value_column,
                )
            else:
                stock_map_by_store_uid[suid] = {}
        except Exception:
            stock_map_by_store_uid[suid] = {}

        try:
            tree = get_pricing_category_tree(store_uid=suid)
            dataset_key = str(tree.get("dataset_key") or "").strip() if isinstance(tree, dict) else ""
            category_settings_by_store_uid[suid] = (
                get_pricing_category_settings_map(dataset_key=dataset_key, store_uid=suid)
                if dataset_key
                else {}
            )
        except Exception:
            category_settings_by_store_uid[suid] = {}

        try:
            store_settings_by_store_uid[suid] = get_pricing_store_settings(store_uid=suid) or {}
        except Exception:
            store_settings_by_store_uid[suid] = {}

        try:
            logistics_store_by_store_uid[suid] = get_pricing_logistics_store_settings(store_uid=suid) or {}
        except Exception:
            logistics_store_by_store_uid[suid] = {}

        try:
            skus = list(source_row_by_store_sku.get(suid, {}).keys())
            logistics_product_by_store_uid[suid] = (
                get_pricing_logistics_product_settings_map(store_uid=suid, skus=skus) if skus else {}
            )
        except Exception:
            logistics_product_by_store_uid[suid] = {}

    cogs_snapshot_map = get_pricing_cogs_snapshot_map(
        store_uids=[str(store.get("store_uid") or "").strip() for store in target_stores],
        as_of_msk=datetime.datetime.now(datetime.timezone.utc),
    )
    for s in target_stores:
        suid = str(s["store_uid"])
        snapshot_bucket = cogs_snapshot_map.get(suid) if isinstance(cogs_snapshot_map.get(suid), dict) else {}
        snapshot_rows = snapshot_bucket.get("rows") if isinstance(snapshot_bucket.get("rows"), dict) else {}
        snapshot_source_id = str(snapshot_bucket.get("source_id") or "").strip()
        store_settings = store_settings_by_store_uid.get(suid) or {}
        current_source_id = str(store_settings.get("cogs_source_id") or "").strip()
        current_sku_column = str(store_settings.get("cogs_sku_column") or "").strip()
        current_value_column = str(store_settings.get("cogs_value_column") or "").strip()
        if current_source_id and current_sku_column and current_value_column and snapshot_source_id != current_source_id:
            try:
                cogs_map_by_store_uid[suid] = _load_cogs_map_from_source(
                    source_id=current_source_id,
                    sku_column=current_sku_column,
                    value_column=current_value_column,
                )
                logger.warning(
                    "[pricing_prices] live cogs source used store_uid=%s snapshot_source_id=%s current_source_id=%s rows=%s",
                    suid,
                    snapshot_source_id or "-",
                    current_source_id,
                    len(cogs_map_by_store_uid[suid]),
                )
            except Exception:
                logger.warning(
                    "[pricing_prices] failed to load live cogs source store_uid=%s current_source_id=%s",
                    suid,
                    current_source_id,
                    exc_info=True,
                )
                cogs_map_by_store_uid[suid] = snapshot_rows if isinstance(snapshot_rows, dict) else {}
        else:
            cogs_map_by_store_uid[suid] = snapshot_rows if isinstance(snapshot_rows, dict) else {}

    tree_assignment_by_sku: dict[str, list[str]] = {}
    if tree_mode_norm == "marketplaces" and tree_source:
        priority_platform = str(tree_source.get("platform") or "").strip().lower()
        try:
            cached_path_map = get_pricing_catalog_sku_path_map(priority_platform=priority_platform)
            tree_assignment_by_sku = {
                sku: [part.strip() for part in str(item.get("leaf_path") or "").split(" / ") if part.strip()]
                for sku, item in cached_path_map.items()
                if str(item.get("leaf_path") or "").strip() and str(item.get("leaf_path") or "").strip() != "Не определено"
            }
        except Exception:
            tree_assignment_by_sku = {}

    merged: dict[str, dict] = {}
    for s in target_stores:
        suid = str(s["store_uid"])
        for row in source_rows_map.get(suid) or []:
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue
            item = merged.get(sku)
            if item is None:
                item = {"sku": sku, "name": str(row.get("name") or "").strip(), "placements": {}, "updated_at": ""}
                merged[sku] = item
            if not item.get("name") and str(row.get("name") or "").strip():
                item["name"] = str(row.get("name") or "").strip()
            item["placements"][suid] = True
            ru = str(row.get("updated_at") or "").strip()
            if ru and (not item["updated_at"] or ru > item["updated_at"]):
                item["updated_at"] = ru

    target_store_uids = [str(s.get("store_uid") or "").strip() for s in target_stores if str(s.get("store_uid") or "").strip()]
    merged_skus = list(merged.keys())
    db_price_map = get_pricing_price_results_map(store_uids=target_store_uids, skus=merged_skus) if merged_skus else {}

    selected_prefix = [p.strip() for p in str(category_path or "").split("/") if p.strip()]
    q = str(search or "").strip().lower()
    stock_filter_norm = str(stock_filter or "all").strip().lower()
    usd_rub_rate: float | None = None
    if any(str(s.get("currency_code") or "RUB").strip().upper() == "USD" for s in target_stores):
        usd_rub_rate = await _get_cbr_usd_rub_rate_for_date(datetime.date.today())

    rows_out: list[dict] = []
    for sku, item in merged.items():
        path = tree_assignment_by_sku.get(sku) or ["Не определено"]
        if selected_prefix and path[: len(selected_prefix)] != selected_prefix:
            continue
        if q:
            hay = f"{sku} {str(item.get('name') or '')}".lower()
            if q not in hay:
                continue

        cogs_price_by_store: dict[str, float | None] = {}
        stock_by_store: dict[str, float | None] = {}
        market_price_by_store: dict[str, float | None] = {}
        rrc_no_ads_price_by_store: dict[str, float | None] = {}
        mrc_price_by_store: dict[str, float | None] = {}
        mrc_with_boost_price_by_store: dict[str, float | None] = {}
        target_price_by_store: dict[str, float | None] = {}
        price_metrics_by_store: dict[str, dict] = {}
        for s in target_stores:
            suid = str(s["store_uid"])
            is_placed = bool((item.get("placements") or {}).get(suid))
            if not is_placed:
                cogs_price_by_store[suid] = None
                stock_by_store[suid] = None
                market_price_by_store[suid] = None
                rrc_no_ads_price_by_store[suid] = None
                mrc_price_by_store[suid] = None
                mrc_with_boost_price_by_store[suid] = None
                target_price_by_store[suid] = None
                price_metrics_by_store[suid] = {
                    "rrc_no_ads_price": None,
                    "rrc_no_ads_profit_abs": None,
                    "rrc_no_ads_profit_pct": None,
                    "mrc_price": None,
                    "mrc_profit_abs": None,
                    "mrc_profit_pct": None,
                    "mrc_with_boost_price": None,
                    "mrc_with_boost_profit_abs": None,
                    "mrc_with_boost_profit_pct": None,
                    "target_price": None,
                    "target_profit_abs": None,
                    "target_profit_pct": None,
                }
                continue

            raw_val = cogs_map_by_store_uid.get(suid, {}).get(sku)
            if raw_val is None:
                cogs_price_by_store[suid] = None
            else:
                cogs_price_by_store[suid] = float(int(round(float(raw_val))))

            raw_stock = stock_map_by_store_uid.get(suid, {}).get(sku)
            stock_by_store[suid] = None if raw_stock is None else float(raw_stock)

            raw_rrc = rrc_map_by_store_uid.get(suid, {}).get(sku)
            raw_rrc_currency = str(rrc_currency_by_store_uid.get(suid, {}).get(sku) or "").strip().upper()
            if raw_rrc is None:
                market_price_by_store[suid] = None
            elif (
                str(s.get("currency_code") or "RUB").strip().upper() == "USD"
                and raw_rrc_currency not in {"USD", "$"}
                and usd_rub_rate
                and usd_rub_rate > 0
            ):
                market_price_by_store[suid] = float(int(round(float(raw_rrc) / float(usd_rub_rate))))
            else:
                market_price_by_store[suid] = float(int(round(float(raw_rrc))))

            src_row = source_row_by_store_sku.get(suid, {}).get(sku) or {}
            src_updated = str(src_row.get("updated_at") or "").strip() or None
            leaf = _leaf_path_from_row(src_row)
            st = _resolve_category_settings_for_leaf(category_settings_by_store_uid.get(suid), leaf)
            store_st = store_settings_by_store_uid.get(suid, {}) or {}
            lg_store = logistics_store_by_store_uid.get(suid, {}) or {}
            lg_prod = logistics_product_by_store_uid.get(suid, {}).get(sku) or {}
            platform_id = str(s.get("platform") or "").strip().lower()
            currency_code = str(s.get("currency_code") or "RUB").strip().upper()

            cogs_val = cogs_price_by_store.get(suid)
            rrc_val = market_price_by_store.get(suid)
            earning_mode = str(store_st.get("earning_mode") or "profit").strip().lower()
            earning_unit = str(store_st.get("earning_unit") or "percent").strip().lower()
            apply_tax = earning_mode == "profit"

            commission_rate = _clamp_rate(_num0(st.get("commission_percent")) / 100.0)
            acquiring_rate = _clamp_rate(_num0(st.get("acquiring_percent")) / 100.0)
            ads_percent_raw = st.get("ads_percent")
            if ads_percent_raw in (None, ""):
                ads_percent_raw = store_st.get("target_drr_percent")
            ads_rate = _clamp_rate(_num0(ads_percent_raw) / 100.0)
            returns_rate = _clamp_rate(_num0(st.get("returns_percent")) / 100.0)
            other_rate = _clamp_rate(_num0(st.get("other_expenses_percent")) / 100.0)
            tax_rate = _clamp_rate(_num0(st.get("tax_percent")) / 100.0)
            dep_rate = _clamp_rate(commission_rate + acquiring_rate + ads_rate + returns_rate + other_rate)

            other_fixed = _num0(st.get("other_expenses_rub"))
            if currency_code == "USD" and usd_rub_rate and usd_rub_rate > 0:
                other_fixed = other_fixed / float(usd_rub_rate)

            max_weight = _compute_max_weight_kg(platform_id, lg_prod)
            per_kg = _num0(lg_store.get("delivery_cost_per_kg"))
            ret_cost = _num0(lg_store.get("return_processing_cost"))
            disp_cost = _num0(lg_store.get("disposal_cost"))
            handling_mode = str(lg_store.get("handling_mode") or "fixed").strip().lower()
            handling_fixed = _num0(lg_store.get("handling_fixed_amount"))
            handling_percent = _num0(lg_store.get("handling_percent"))
            handling_min = _num0(lg_store.get("handling_min_amount"))
            handling_max = _num0(lg_store.get("handling_max_amount"))

            delivery_to_client = (per_kg * max_weight) if max_weight is not None else 0.0
            logistics_cost = delivery_to_client + ret_cost + disp_cost
            cogs_cost = _num0(cogs_val)
            fixed_cost_core = other_fixed + logistics_cost
            fixed_cost = fixed_cost_core + (cogs_cost if earning_mode == "profit" else 0.0)
            calc_ctx = {
                "dep_rate": dep_rate,
                "ads_rate": ads_rate,
                "tax_rate": tax_rate,
                "fixed_cost": fixed_cost,
                "apply_tax": apply_tax,
                "handling_mode": handling_mode,
                "handling_fixed": handling_fixed,
                "handling_percent": handling_percent,
                "handling_min": handling_min,
                "handling_max": handling_max,
            }

            if handling_mode == "percent":
                handling_text = (
                    f"{int(round(handling_min))}{'$' if currency_code == 'USD' else '₽'} + "
                    f"{handling_percent:.2f}% от цены "
                    f"(мин {int(round(handling_min))}{'$' if currency_code == 'USD' else '₽'}, "
                    f"макс {int(round(handling_max))}{'$' if currency_code == 'USD' else '₽'})"
                )
            else:
                handling_text = f"{int(round(handling_fixed))}{'$' if currency_code == 'USD' else '₽'}"

            rrc_profit_abs = None
            rrc_profit_pct = None
            if rrc_val is not None:
                pa, pp = _profit_for_price(
                    price=float(rrc_val),
                    dep_rate=dep_rate,
                    tax_rate=tax_rate,
                    fixed_cost=fixed_cost,
                    apply_tax=apply_tax,
                    handling_mode=handling_mode,
                    handling_fixed=handling_fixed,
                    handling_percent=handling_percent,
                    handling_min=handling_min,
                    handling_max=handling_max,
                )
                rrc_profit_abs = float(int(round(pa)))
                rrc_profit_pct = round(pp * 100.0, 2)

            db_rec = ((db_price_map.get(suid) or {}).get(sku) or {}) if isinstance(db_price_map.get(suid), dict) else {}
            if _can_reuse_materialized_price_metrics(db_rec=db_rec, src_updated=src_updated):
                price_metrics_by_store[suid] = {
                    "rrc_no_ads_price": _to_num(db_rec.get("rrc_no_ads_price")),
                    "rrc_no_ads_profit_abs": _to_num(db_rec.get("rrc_no_ads_profit_abs")),
                    "rrc_no_ads_profit_pct": _to_num(db_rec.get("rrc_no_ads_profit_pct")),
                    "mrc_price": _to_num(db_rec.get("mrc_price")),
                    "mrc_profit_abs": _to_num(db_rec.get("mrc_profit_abs")),
                    "mrc_profit_pct": _to_num(db_rec.get("mrc_profit_pct")),
                    "mrc_with_boost_price": _to_num(db_rec.get("mrc_with_boost_price")),
                    "mrc_with_boost_profit_abs": _to_num(db_rec.get("mrc_with_boost_profit_abs")),
                    "mrc_with_boost_profit_pct": _to_num(db_rec.get("mrc_with_boost_profit_pct")),
                    "target_price": _to_num(db_rec.get("target_price")),
                    "target_profit_abs": _to_num(db_rec.get("target_profit_abs")),
                    "target_profit_pct": _to_num(db_rec.get("target_profit_pct")),
                    "calc_ctx": calc_ctx,
                }
                rrc_no_ads_price_by_store[suid] = _to_num(db_rec.get("rrc_no_ads_price"))
                mrc_price_by_store[suid] = _to_num(db_rec.get("mrc_price"))
                mrc_with_boost_price_by_store[suid] = _to_num(db_rec.get("mrc_with_boost_price"))
                target_price_by_store[suid] = _to_num(db_rec.get("target_price"))
                continue

            target_pct_raw = None
            target_abs_raw = None
            if earning_unit == "percent":
                if earning_mode == "margin":
                    target_pct_raw = store_st.get("target_margin_percent")
                    if target_pct_raw in (None, ""):
                        target_pct_raw = st.get("target_margin_percent")
                else:
                    target_pct_raw = store_st.get("target_profit_percent")
                    if target_pct_raw in (None, ""):
                        target_pct_raw = st.get("target_profit_percent")
                if target_pct_raw in (None, ""):
                    target_pct_raw = (
                        store_st.get("target_profit_percent")
                        or st.get("target_profit_percent")
                        or store_st.get("target_margin_percent")
                        or st.get("target_margin_percent")
                    )
            else:
                if earning_mode == "margin":
                    target_abs_raw = store_st.get("target_margin_rub")
                    if target_abs_raw in (None, ""):
                        target_abs_raw = st.get("target_margin_rub")
                else:
                    target_abs_raw = store_st.get("target_profit_rub")
                    if target_abs_raw in (None, ""):
                        target_abs_raw = st.get("target_profit_rub")
                if target_abs_raw in (None, ""):
                    target_abs_raw = (
                        store_st.get("target_profit_rub")
                        or st.get("target_profit_rub")
                        or store_st.get("target_margin_rub")
                        or st.get("target_margin_rub")
                    )

            target_pct = _num0(target_pct_raw)
            target_abs = _num0(target_abs_raw)
            minimum_profit_pct_raw = st.get("minimum_profit_percent")
            if minimum_profit_pct_raw in (None, ""):
                minimum_profit_pct_raw = store_st.get("minimum_profit_percent")
            minimum_profit_pct = _num0(minimum_profit_pct_raw)
            if minimum_profit_pct <= 0:
                minimum_profit_pct = 3.0
            floor_t = minimum_profit_pct / 100.0
            rrc_no_ads_price = None
            rrc_no_ads_profit_abs = None
            rrc_no_ads_profit_pct = None
            mrc_price = None
            mrc_profit_abs = None
            mrc_profit_pct = None
            mrc_with_boost_price = None
            mrc_with_boost_profit_abs = None
            mrc_with_boost_profit_pct = None
            target_price = None
            target_profit_abs = None
            target_profit_pct = None
            if target_pct > 0 or target_abs > 0:
                t = (target_pct / 100.0) if target_pct > 0 else None

                def _pp(price_val: float) -> float:
                    _, pp_v = _profit_for_price(
                        price=price_val,
                        dep_rate=dep_rate,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    return pp_v

                def _pa(price_val: float) -> float:
                    pa_v, _ = _profit_for_price(
                        price=price_val,
                        dep_rate=dep_rate,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    return pa_v

                lo = 1.0
                hi = max(float(rrc_val or 0.0), 1000.0)
                for _ in range(40):
                    if (t is not None and _pp(hi) >= t) or (t is None and _pa(hi) >= target_abs):
                        break
                    hi *= 2.0
                    if hi > 1e9:
                        break
                ok_hi = (t is not None and _pp(hi) >= t) or (t is None and _pa(hi) >= target_abs)
                if ok_hi:
                    for _ in range(60):
                        mid = (lo + hi) / 2.0
                        ok_mid = (t is not None and _pp(mid) >= t) or (t is None and _pa(mid) >= target_abs)
                        if ok_mid:
                            hi = mid
                        else:
                            lo = mid
                    target_price = _ceil_price_to_threshold(
                        estimated_price=hi,
                        meets=(lambda price_val: _pp(price_val) >= t) if t is not None else (lambda price_val: _pa(price_val) >= target_abs),
                    )
                    pa_t, pp_t = _profit_for_price(
                        price=float(target_price),
                        dep_rate=dep_rate,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    target_profit_abs = float(int(round(pa_t)))
                    target_profit_pct = round(pp_t * 100.0, 2)

                dep_rate_no_ads = _clamp_rate(max(0.0, dep_rate - ads_rate))

                def _pp_no_ads(price_val: float) -> float:
                    _, pp_v = _profit_for_price(
                        price=price_val,
                        dep_rate=dep_rate_no_ads,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    return pp_v

                def _pa_no_ads(price_val: float) -> float:
                    pa_v, _ = _profit_for_price(
                        price=price_val,
                        dep_rate=dep_rate_no_ads,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    return pa_v

                lo_no_ads = 1.0
                hi_no_ads = max(float(rrc_val or 0.0), 1000.0)
                for _ in range(40):
                    if (t is not None and _pp_no_ads(hi_no_ads) >= t) or (t is None and _pa_no_ads(hi_no_ads) >= target_abs):
                        break
                    hi_no_ads *= 2.0
                    if hi_no_ads > 1e9:
                        break
                ok_hi_no_ads = (t is not None and _pp_no_ads(hi_no_ads) >= t) or (t is None and _pa_no_ads(hi_no_ads) >= target_abs)
                if ok_hi_no_ads:
                    for _ in range(60):
                        mid = (lo_no_ads + hi_no_ads) / 2.0
                        ok_mid = (t is not None and _pp_no_ads(mid) >= t) or (t is None and _pa_no_ads(mid) >= target_abs)
                        if ok_mid:
                            hi_no_ads = mid
                        else:
                            lo_no_ads = mid
                    rrc_no_ads_price = _ceil_price_to_threshold(
                        estimated_price=hi_no_ads,
                        meets=(lambda price_val: _pp_no_ads(price_val) >= t) if t is not None else (lambda price_val: _pa_no_ads(price_val) >= target_abs),
                    )
                    pa_no_ads, pp_no_ads = _profit_for_price(
                        price=float(rrc_no_ads_price),
                        dep_rate=dep_rate_no_ads,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    rrc_no_ads_profit_abs = float(int(round(pa_no_ads)))
                    rrc_no_ads_profit_pct = round(pp_no_ads * 100.0, 2)

                lo_floor = 1.0
                hi_floor = max(float(rrc_val or 0.0), 1000.0)
                for _ in range(40):
                    if _pp_no_ads(hi_floor) >= floor_t:
                        break
                    hi_floor *= 2.0
                    if hi_floor > 1e9:
                        break
                if _pp_no_ads(hi_floor) >= floor_t:
                    for _ in range(60):
                        mid = (lo_floor + hi_floor) / 2.0
                        if _pp_no_ads(mid) >= floor_t:
                            hi_floor = mid
                        else:
                            lo_floor = mid
                    mrc_price = _ceil_price_to_threshold(
                        estimated_price=hi_floor,
                        meets=lambda price_val: _pp_no_ads(price_val) >= floor_t,
                    )
                    pa_m, pp_m = _profit_for_price(
                        price=float(mrc_price),
                        dep_rate=dep_rate_no_ads,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    mrc_profit_abs = float(int(round(pa_m)))
                    mrc_profit_pct = round(pp_m * 100.0, 2)

                lo_floor_boost = 1.0
                hi_floor_boost = max(float(rrc_val or 0.0), 1000.0)
                for _ in range(40):
                    if _pp(hi_floor_boost) >= floor_t:
                        break
                    hi_floor_boost *= 2.0
                    if hi_floor_boost > 1e9:
                        break
                if _pp(hi_floor_boost) >= floor_t:
                    for _ in range(60):
                        mid = (lo_floor_boost + hi_floor_boost) / 2.0
                        if _pp(mid) >= floor_t:
                            hi_floor_boost = mid
                        else:
                            lo_floor_boost = mid
                    mrc_with_boost_price = _ceil_price_to_threshold(
                        estimated_price=hi_floor_boost,
                        meets=lambda price_val: _pp(price_val) >= floor_t,
                    )
                    pa_mb, pp_mb = _profit_for_price(
                        price=float(mrc_with_boost_price),
                        dep_rate=dep_rate,
                        tax_rate=tax_rate,
                        fixed_cost=fixed_cost,
                        apply_tax=apply_tax,
                        handling_mode=handling_mode,
                        handling_fixed=handling_fixed,
                        handling_percent=handling_percent,
                        handling_min=handling_min,
                        handling_max=handling_max,
                    )
                    mrc_with_boost_profit_abs = float(int(round(pa_mb)))
                    mrc_with_boost_profit_pct = round(pp_mb * 100.0, 2)

            price_metrics_by_store[suid] = {
                "rrc_no_ads_price": rrc_no_ads_price,
                "rrc_no_ads_profit_abs": rrc_no_ads_profit_abs,
                "rrc_no_ads_profit_pct": rrc_no_ads_profit_pct,
                "mrc_price": mrc_price,
                "mrc_profit_abs": mrc_profit_abs,
                "mrc_profit_pct": mrc_profit_pct,
                "mrc_with_boost_price": mrc_with_boost_price,
                "mrc_with_boost_profit_abs": mrc_with_boost_profit_abs,
                "mrc_with_boost_profit_pct": mrc_with_boost_profit_pct,
                "target_price": target_price,
                "target_profit_abs": target_profit_abs,
                "target_profit_pct": target_profit_pct,
                # Внутренний контекст расчета для переиспользования на смежных страницах (Привлекательность и т.д.).
                "calc_ctx": calc_ctx,
            }
            rrc_no_ads_price_by_store[suid] = rrc_no_ads_price
            mrc_price_by_store[suid] = mrc_price
            mrc_with_boost_price_by_store[suid] = mrc_with_boost_price
            target_price_by_store[suid] = target_price

        if stock_filter_norm in {"in_stock", "out_of_stock"}:
            current_store_uid = f"{platform_norm}:{store_norm}" if scope_norm == "store" and platform_norm and store_norm else ""
            stock_values = (
                [stock_by_store.get(current_store_uid)]
                if current_store_uid
                else list(stock_by_store.values())
            )
            has_stock = any(value is not None and float(value) > 0 for value in stock_values)
            if stock_filter_norm == "in_stock" and not has_stock:
                continue
            if stock_filter_norm == "out_of_stock" and has_stock:
                continue

        rows_out.append(
            {
                "sku": sku,
                "name": str(item.get("name") or ""),
                "tree_path": path,
                "placements": item.get("placements") or {},
                "cogs_price_by_store": cogs_price_by_store,
                "stock_by_store": stock_by_store,
                "market_price_by_store": market_price_by_store,
                "rrc_no_ads_price_by_store": rrc_no_ads_price_by_store,
                "mrc_price_by_store": mrc_price_by_store,
                "mrc_with_boost_price_by_store": mrc_with_boost_price_by_store,
                "target_price_by_store": target_price_by_store,
                "price_metrics_by_store": price_metrics_by_store,
                "updated_at": str(item.get("updated_at") or ""),
            }
        )

    rows_out.sort(key=lambda r: (str(r.get("sku") or "")))
    # Materialized слой для страницы "Цены": сохраняем рассчитанные метрики в БД.
    try:
        materialized_rows: list[dict[str, Any]] = []
        for item in rows_out:
            sku = str(item.get("sku") or "").strip()
            if not sku:
                continue
            src_updated = str(item.get("updated_at") or "").strip() or None
            cogs_map = item.get("cogs_price_by_store") if isinstance(item.get("cogs_price_by_store"), dict) else {}
            pm_map = item.get("price_metrics_by_store") if isinstance(item.get("price_metrics_by_store"), dict) else {}
            rrc_map = item.get("market_price_by_store") if isinstance(item.get("market_price_by_store"), dict) else {}
            for suid, pm in pm_map.items():
                if not isinstance(pm, dict):
                    continue
                materialized_rows.append(
                    {
                        "store_uid": str(suid),
                        "sku": sku,
                        "cogs_price": cogs_map.get(suid),
                        "rrc_no_ads_price": pm.get("rrc_no_ads_price"),
                        "rrc_no_ads_profit_abs": pm.get("rrc_no_ads_profit_abs"),
                        "rrc_no_ads_profit_pct": pm.get("rrc_no_ads_profit_pct"),
                        "mrc_price": pm.get("mrc_price"),
                        "mrc_profit_abs": pm.get("mrc_profit_abs"),
                        "mrc_profit_pct": pm.get("mrc_profit_pct"),
                        "mrc_with_boost_price": pm.get("mrc_with_boost_price"),
                        "mrc_with_boost_profit_abs": pm.get("mrc_with_boost_profit_abs"),
                        "mrc_with_boost_profit_pct": pm.get("mrc_with_boost_profit_pct"),
                        "target_price": pm.get("target_price"),
                        "target_profit_abs": pm.get("target_profit_abs"),
                        "target_profit_pct": pm.get("target_profit_pct"),
                        "source_updated_at": src_updated,
                    }
                )
        if materialized_rows:
            upsert_pricing_price_results_bulk(rows=materialized_rows)
    except Exception:
        pass

    total_count = len(rows_out)
    page_size_n = max(1, min(int(page_size or 50), 100000))
    page_n = max(1, int(page or 1))
    start = (page_n - 1) * page_size_n
    paged = rows_out[start:start + page_size_n]

    paged_skus = [str(item.get("sku") or "").strip() for item in paged if str(item.get("sku") or "").strip()]
    strategy_map = get_pricing_strategy_results_map(store_uids=target_store_uids, skus=paged_skus) if paged_skus else {}
    for item in paged:
        sku = str(item.get("sku") or "").strip()
        installed_price_by_store: dict[str, float | None] = {}
        installed_profit_abs_by_store: dict[str, float | None] = {}
        installed_profit_pct_by_store: dict[str, float | None] = {}
        for suid in target_store_uids:
            strategy_item = (strategy_map.get(suid) or {}).get(sku) or {}
            installed_price_by_store[suid] = _to_num(strategy_item.get("installed_price"))
            installed_profit_abs_by_store[suid] = _to_num(strategy_item.get("installed_profit_abs"))
            installed_profit_pct_by_store[suid] = _to_num(strategy_item.get("installed_profit_pct"))
        item["installed_price_by_store"] = installed_price_by_store
        item["installed_profit_abs_by_store"] = installed_profit_abs_by_store
        item["installed_profit_pct_by_store"] = installed_profit_pct_by_store

    resp = {
        "ok": True,
        "scope": scope_norm,
        "platform": platform_norm,
        "store_id": store_norm,
        "tree_mode": tree_mode_norm,
        "tree_source": tree_source,
        "stores": [
            {
                "store_uid": s["store_uid"],
                "store_id": s["store_id"],
                "platform": s["platform"],
                "platform_label": s["platform_label"],
                "label": s["label"],
                "currency_code": str(s.get("currency_code") or "RUB"),
            }
            for s in target_stores
        ],
        "rows": paged,
        "total_count": total_count,
        "page": page_n,
        "page_size": page_size_n,
    }
    _snapshot_set(
        _snapshot_payload_from_overview_payload(cache_payload),
        {
            "ok": True,
            "scope": scope_norm,
            "platform": platform_norm,
            "store_id": store_norm,
            "tree_mode": tree_mode_norm,
            "tree_source": tree_source,
            "stores": [
                {
                    "store_uid": s["store_uid"],
                    "store_id": s["store_id"],
                    "platform": s["platform"],
                    "platform_label": s["platform_label"],
                    "label": s["label"],
                    "currency_code": str(s.get("currency_code") or "RUB"),
                }
                for s in target_stores
            ],
            "rows": rows_out,
            "total_count": total_count,
        },
    )
    if not force_refresh:
        _cache_set("overview", cache_payload, resp)
    return resp


async def get_prices_overview_full(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    stock_filter: str = "all",
    force_refresh: bool = False,
) -> dict[str, Any]:
    snapshot_payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "stock_filter": stock_filter,
    }
    if not force_refresh:
        cached = _snapshot_get(snapshot_payload)
        if cached:
            return _build_prices_page_from_snapshot(
                cached,
                page=1,
                page_size=max(1, int(cached.get("total_count") or len(cached.get("rows") or []) or 1)),
            )

    full = await get_prices_overview(
        scope=scope,
        platform=platform,
        store_id=store_id,
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        category_path=category_path,
        search=search,
        stock_filter=stock_filter,
        page=1,
        page_size=100000,
        force_refresh=force_refresh,
    )
    _snapshot_set(
        snapshot_payload,
        {
            "ok": True,
            "scope": full.get("scope"),
            "platform": full.get("platform"),
            "store_id": full.get("store_id"),
            "tree_mode": full.get("tree_mode"),
            "tree_source": full.get("tree_source"),
            "stores": list(full.get("stores") or []),
            "rows": list(full.get("rows") or []),
            "total_count": int(full.get("total_count") or len(full.get("rows") or [])),
        },
    )
    return full


async def refresh_prices_data(*, store_uids: list[str] | None = None):
    wanted_store_uids = [str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()]
    if wanted_store_uids:
        ctx = await get_prices_context()
        all_stores = list((ctx or {}).get("marketplace_stores") or [])
        stores = [s for s in all_stores if str(s.get("store_uid") or "").strip() in wanted_store_uids]
        overviews: list[dict[str, Any]] = []
        for store in stores:
            platform = str(store.get("platform") or "").strip()
            store_id = str(store.get("store_id") or "").strip()
            if not platform or not store_id:
                continue
            overviews.append(
                await get_prices_overview(
                    scope="store",
                    platform=platform,
                    store_id=store_id,
                    page=1,
                    page_size=1,
                    force_refresh=True,
                )
            )
        prices_materialized = sum(int((item or {}).get("total_count") or 0) for item in overviews if isinstance(item, dict))
    else:
        overview = await get_prices_overview(
            scope="all",
            page=1,
            page_size=1,
            force_refresh=True,
        )
        overviews = [overview] if isinstance(overview, dict) else []
        prices_materialized = int(overview.get("total_count") or 0) if isinstance(overview, dict) else 0
    stores_map: dict[str, dict[str, Any]] = {}
    for overview in overviews:
        for store in list((overview or {}).get("stores") or []):
            store_uid = str(store.get("store_uid") or "").strip()
            if store_uid:
                stores_map[store_uid] = dict(store)
    try:
        from backend.services.pricing_strategy_service import invalidate_strategy_cache
        invalidate_strategy_cache()
    except Exception:
        pass
    invalidate_prices_cache()
    stores = list(stores_map.values())
    store_statuses = [
        {
            "store_uid": str(store.get("store_uid") or "").strip(),
            "store_id": str(store.get("store_id") or "").strip(),
            "status": "success",
            "message": "Цены пересчитаны",
        }
        for store in stores
        if str(store.get("store_uid") or "").strip()
    ]
    return {
        "ok": True,
        "prices_materialized": prices_materialized,
        "stores_total": len(stores),
        "stores_updated": len(store_statuses),
        "store_statuses": store_statuses,
    }


async def prime_prices_cache() -> None:
    try:
        ctx = await get_prices_context()
        stores = list(ctx.get("marketplace_stores") or [])
        if not stores:
            return
        first_store_uid = str(stores[0].get("store_uid") or "").strip()
        first_store_id = str(stores[0].get("store_id") or "").strip()
        common_params = {
            "scope": "all",
            "tree_mode": "marketplaces",
            "tree_source_store_id": "",
        }
        await get_prices_tree(**common_params)
        await get_prices_overview_full(**common_params)
        if first_store_id:
            store_params = {
                "scope": "store",
                "platform": str(stores[0].get("platform") or "").strip(),
                "store_id": first_store_id,
                "tree_mode": "marketplaces",
                "tree_source_store_id": first_store_uid,
            }
            await get_prices_tree(**store_params)
            await get_prices_overview_full(**store_params)
    except Exception as exc:
        logger.warning("[pricing_prices] prime cache skipped error=%s", exc)
