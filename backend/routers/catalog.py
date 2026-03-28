from __future__ import annotations

import copy
import hashlib
import json
import datetime

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.routers._shared import (
    _catalog_db_connect,
    _build_catalog_import_options,
    _catalog_marketplace_stores_context,
    _catalog_external_tree_sources_context,
    _catalog_path_from_row,
    _catalog_tree_from_paths,
    _default_catalog_import_config,
    _evaluate_catalog_import_config,
    _get_catalog_import_config,
    _normalize_catalog_source_ids,
    _now_iso,
    _read_source_rows,
    _fetch_cbr_usd_rates,
    _sanitize_catalog_mapping,
    _safe_source_table_name,
    get_pricing_store_settings,
    load_sources,
    _read_source_rows,
    load_integrations,
    replace_source_rows,
    save_integrations,
)
from backend.services.gsheets import read_sheet_all
from backend.services.pricing_catalog_helpers import (
    clamp_rate as _clamp_rate,
    compute_max_weight_kg as _compute_max_weight_kg,
    leaf_path_from_row as _leaf_path_from_row,
    load_numeric_map_from_source as _load_numeric_map_from_source,
    norm_col_name as _norm_col_name,
    num0 as _num0,
    profit_for_price as _profit_for_price,
    resolve_source_table_name as _resolve_source_table_name,
    to_num as _to_num,
)
from backend.services.store_data_model import (
    get_fx_rates_cache,
    replace_fx_rates_cache,
    get_pricing_category_tree,
    get_pricing_store_settings,
    get_pricing_logistics_store_settings,
    get_pricing_logistics_product_settings_map,
)

router = APIRouter()

_CATALOG_CACHE: dict[str, dict] = {}
_CATALOG_CACHE_GEN = 1
_CATALOG_CACHE_MAX = 400
_FX_USD_RUB_MEM: dict[str, float] = {}


def _cache_key(name: str, payload: dict) -> str:
    raw = json.dumps({"name": name, "gen": _CATALOG_CACHE_GEN, "payload": payload}, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_get(name: str, payload: dict):
    key = _cache_key(name, payload)
    got = _CATALOG_CACHE.get(key)
    return copy.deepcopy(got) if isinstance(got, dict) else None


def _cache_set(name: str, payload: dict, value: dict):
    key = _cache_key(name, payload)
    if len(_CATALOG_CACHE) >= _CATALOG_CACHE_MAX:
        _CATALOG_CACHE.clear()
    _CATALOG_CACHE[key] = copy.deepcopy(value)


def _cache_invalidate_all():
    global _CATALOG_CACHE_GEN
    _CATALOG_CACHE.clear()
    _CATALOG_CACHE_GEN += 1


async def _get_cbr_usd_rub_rate_for_date(calc_date: datetime.date) -> float | None:
    key = calc_date.isoformat()
    if key in _FX_USD_RUB_MEM:
        return _FX_USD_RUB_MEM[key]

    def _prefer_latest_published(by_date: dict[str, float]) -> tuple[str, float] | None:
        if not by_date:
            return None
        best_date = max(by_date.keys())
        return best_date, float(by_date[best_date])

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
            latest = _prefer_latest_published(by_date)
            if latest is not None:
                best_date, rate = latest
                if rate > 0:
                    _FX_USD_RUB_MEM[key] = rate
                    return rate
    except Exception:
        pass

    try:
        start = calc_date - datetime.timedelta(days=60)
        end = calc_date + datetime.timedelta(days=1)
        fresh_rows = await _fetch_cbr_usd_rates(start, end)
        if fresh_rows:
            replace_fx_rates_cache(
                source="cbr",
                pair="USD_RUB",
                rows=fresh_rows,
                meta={"loaded_from": "catalog_overview"},
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
            latest = _prefer_latest_published(by_date)
            if latest is not None:
                best_date, rate = latest
                if rate > 0:
                    _FX_USD_RUB_MEM[key] = rate
                    return rate
    except Exception:
        pass
    return None


def _is_probably_rub_column(col_name: str) -> bool:
    n = str(col_name or "").strip().lower()
    if not n:
        return True
    if "usd" in n or "$" in n:
        return False
    if "руб" in n or "rub" in n or "₽" in n:
        return True
    return True


def _load_cogs_map_from_source(*, source_id: str, sku_column: str, value_column: str) -> dict[str, float]:
    return _load_numeric_map_from_source(
        source_id=source_id,
        sku_column=sku_column,
        value_column=value_column,
    )


@router.get("/api/catalog/products/context")
async def catalog_products_context():
    cached = _cache_get("context", {})
    if cached:
        return cached
    marketplace_stores = _catalog_marketplace_stores_context()
    external_sources = _catalog_external_tree_sources_context()
    resp = {
        "ok": True,
        "tree_mode_options": [
            {"id": "marketplaces", "label": "Маркетплейсы"},
            {"id": "external", "label": "Внешний источник"},
        ],
        "marketplace_stores": marketplace_stores,
        "external_tree_source_types": [
            {"id": "tables", "label": "Таблицы"},
            {"id": "external_systems", "label": "Внешние системы"},
        ],
        "external_sources": external_sources,
    }
    _cache_set("context", {}, resp)
    return resp


@router.get("/api/catalog/products/tree")
async def catalog_products_tree(
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
        src_store = next((s for s in stores if s["store_id"] == chosen), None) if chosen else None
    if not src_store:
        src_store = next((s for s in stores if s.get("table_name")), None)
    if not src_store or not src_store.get("table_name"):
        resp = {"ok": True, "tree_mode": mode, "roots": [{"name": "Не определено", "children": []}], "source": None}
        _cache_set("tree", cache_payload, resp)
        return resp

    rows = _read_source_rows(str(src_store["table_name"]))
    paths: list[list[str]] = []
    has_undefined = False
    for row in rows:
        path = _catalog_path_from_row(row)
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


@router.get("/api/catalog/products/overview")
async def catalog_products_overview(
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    page: int = 1,
    page_size: int = 50,
):
    cache_payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "page": page,
        "page_size": page_size,
    }
    cached = _cache_get("overview", cache_payload)
    if cached:
        return cached
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
        resp = {
            "ok": True,
            "scope": scope_norm,
            "rows": [],
            "total_count": 0,
            "page": max(1, page),
            "page_size": max(1, min(page_size, 500)),
            "stores": [],
            "tree_source": None,
        }
        _cache_set("overview", cache_payload, resp)
        return resp

    # Источник дерева каталога (для таба "Все товары") или текущий магазин (для таба магазина)
    tree_source = None
    if tree_mode_norm == "marketplaces":
        if scope_norm == "store":
            tree_source = next((s for s in target_stores if s["platform"] == platform_norm and s["store_id"] == store_norm), None)
        else:
            chosen = str(tree_source_store_id or "").strip()
            tree_source = next((s for s in stores if s["store_id"] == chosen and s.get("table_name")), None) if chosen else None
        if not tree_source:
            tree_source = next((s for s in stores if s.get("table_name")), None)

    source_rows_map: dict[str, list[dict]] = {}
    source_row_by_store_sku: dict[str, dict[str, dict]] = {}
    cogs_map_by_store_uid: dict[str, dict[str, float]] = {}
    cogs_rub_to_usd_by_store_uid: dict[str, bool] = {}
    rrc_map_by_store_uid: dict[str, dict[str, float]] = {}
    category_settings_by_store_uid: dict[str, dict[str, dict]] = {}
    store_settings_by_store_uid: dict[str, dict] = {}
    logistics_store_by_store_uid: dict[str, dict] = {}
    logistics_product_by_store_uid: dict[str, dict[str, dict]] = {}
    for s in target_stores:
        try:
            source_rows_map[s["store_uid"]] = _read_source_rows(str(s["table_name"]))
            source_row_by_store_sku[str(s["store_uid"])] = {
                str(r.get("sku") or "").strip(): r
                for r in source_rows_map[s["store_uid"]]
                if str(r.get("sku") or "").strip()
            }
            rrc_local: dict[str, float] = {}
            for row in source_rows_map[s["store_uid"]]:
                sku = str(row.get("sku") or "").strip()
                if not sku:
                    continue
                price_num = _to_num(row.get("price"))
                if price_num is None:
                    continue
                rrc_local[sku] = float(price_num)
            rrc_map_by_store_uid[str(s["store_uid"])] = rrc_local
        except Exception:
            source_rows_map[s["store_uid"]] = []
            source_row_by_store_sku[str(s["store_uid"])] = {}
            rrc_map_by_store_uid[str(s["store_uid"])] = {}
        try:
            ps = get_pricing_store_settings(store_uid=str(s["store_uid"]))
            cogs_source_id = str(ps.get("cogs_source_id") or "").strip()
            cogs_sku_column = str(ps.get("cogs_sku_column") or "").strip()
            cogs_value_column = str(ps.get("cogs_value_column") or "").strip()
            if cogs_source_id:
                # Поддержка alias по сохраненному mapping источника (если колонки переименовали/подтягиваются иначе).
                if not cogs_sku_column or not cogs_value_column:
                    for src in (load_sources() or []):
                        if str(src.get("id") or "").strip() != cogs_source_id:
                            continue
                        mp = src.get("mapping") if isinstance(src.get("mapping"), dict) else {}
                        if not cogs_sku_column:
                            cogs_sku_column = str(mp.get("sku_primary") or "").strip()
                        if not cogs_value_column:
                            cogs_value_column = str(mp.get("cogs") or "").strip()
                        break
                cogs_map = _load_cogs_map_from_source(
                    source_id=cogs_source_id,
                    sku_column=cogs_sku_column,
                    value_column=cogs_value_column,
                )
                cogs_map_by_store_uid[str(s["store_uid"])] = cogs_map
                cogs_rub_to_usd_by_store_uid[str(s["store_uid"])] = (
                    str(s.get("currency_code") or "RUB").strip().upper() == "USD"
                    and _is_probably_rub_column(cogs_value_column)
                )
            else:
                cogs_map_by_store_uid[str(s["store_uid"])] = {}
                cogs_rub_to_usd_by_store_uid[str(s["store_uid"])] = False
        except Exception:
            cogs_map_by_store_uid[str(s["store_uid"])] = {}
            cogs_rub_to_usd_by_store_uid[str(s["store_uid"])] = False

        try:
            tree = get_pricing_category_tree(store_uid=str(s["store_uid"]))
            rows_settings = tree.get("rows") if isinstance(tree, dict) else None
            m: dict[str, dict] = {}
            if isinstance(rows_settings, list):
                for rs in rows_settings:
                    if not isinstance(rs, dict):
                        continue
                    leaf = str(rs.get("leaf_path") or "").strip()
                    if not leaf:
                        continue
                    m[leaf] = rs
            category_settings_by_store_uid[str(s["store_uid"])] = m
        except Exception:
            category_settings_by_store_uid[str(s["store_uid"])] = {}

        try:
            store_settings_by_store_uid[str(s["store_uid"])] = get_pricing_store_settings(store_uid=str(s["store_uid"])) or {}
        except Exception:
            store_settings_by_store_uid[str(s["store_uid"])] = {}

        try:
            logistics_store_by_store_uid[str(s["store_uid"])] = get_pricing_logistics_store_settings(store_uid=str(s["store_uid"])) or {}
        except Exception:
            logistics_store_by_store_uid[str(s["store_uid"])] = {}

        try:
            skus = list(source_row_by_store_sku.get(str(s["store_uid"]), {}).keys())
            logistics_product_by_store_uid[str(s["store_uid"])] = (
                get_pricing_logistics_product_settings_map(store_uid=str(s["store_uid"]), skus=skus) if skus else {}
            )
        except Exception:
            logistics_product_by_store_uid[str(s["store_uid"])] = {}

    tree_assignment_by_sku: dict[str, list[str]] = {}
    if tree_mode_norm == "marketplaces" and tree_source and tree_source.get("table_name"):
        try:
            for row in _read_source_rows(str(tree_source["table_name"])):
                sku = str(row.get("sku") or "").strip()
                if not sku:
                    continue
                path = _catalog_path_from_row(row)
                if path:
                    tree_assignment_by_sku[sku] = path
        except Exception:
            tree_assignment_by_sku = {}

    merged: dict[str, dict] = {}
    for s in target_stores:
        suid = str(s["store_uid"])
        rows = source_rows_map.get(suid) or []
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue
            item = merged.get(sku)
            if item is None:
                item = {
                    "sku": sku,
                    "name": str(row.get("name") or "").strip(),
                    "placements": {},
                    "updated_at": "",
                }
                merged[sku] = item
            if not item.get("name") and str(row.get("name") or "").strip():
                item["name"] = str(row.get("name") or "").strip()
            item["placements"][suid] = True
            ru = str(row.get("updated_at") or "").strip()
            if ru and (not item["updated_at"] or ru > item["updated_at"]):
                item["updated_at"] = ru

    selected_prefix = [p.strip() for p in str(category_path or "").split("/") if p.strip()]
    q = str(search or "").strip().lower()
    usd_rub_rate: float | None = None
    if any(cogs_rub_to_usd_by_store_uid.get(str(s["store_uid"]), False) for s in target_stores):
        usd_rub_rate = await _get_cbr_usd_rub_rate_for_date(datetime.date.today())
    rows_out: list[dict] = []
    for sku, item in merged.items():
        path = tree_assignment_by_sku.get(sku) or ["Не определено"]
        if selected_prefix:
            if path[: len(selected_prefix)] != selected_prefix:
                continue
        if q:
            hay = f"{sku} {str(item.get('name') or '')}".lower()
            if q not in hay:
                continue
        cogs_by_store: dict[str, float | None] = {}
        rrc_by_store: dict[str, float | None] = {}
        price_metrics_by_store: dict[str, dict] = {}
        for s in target_stores:
            suid = str(s["store_uid"])
            is_placed = bool((item.get("placements") or {}).get(suid))
            if not is_placed:
                cogs_by_store[suid] = None
                rrc_by_store[suid] = None
                price_metrics_by_store[suid] = {
                    "rrc": None,
                    "profit_abs": None,
                    "profit_pct": None,
                    "target_price": None,
                    "target_profit_abs": None,
                    "target_profit_pct": None,
                    "handling_text": None,
                    "logistics_cost": None,
                }
                continue

            raw_val = cogs_map_by_store_uid.get(suid, {}).get(sku)
            if raw_val is None:
                cogs_by_store[suid] = None
            elif cogs_rub_to_usd_by_store_uid.get(suid, False) and usd_rub_rate and usd_rub_rate > 0:
                cogs_by_store[suid] = float(int(round(float(raw_val) / float(usd_rub_rate))))
            else:
                cogs_by_store[suid] = float(int(round(float(raw_val))))

            raw_rrc = rrc_map_by_store_uid.get(suid, {}).get(sku)
            if raw_rrc is None:
                rrc_by_store[suid] = None
            elif str(s.get("currency_code") or "RUB").strip().upper() == "USD" and usd_rub_rate and usd_rub_rate > 0:
                rrc_by_store[suid] = float(int(round(float(raw_rrc) / float(usd_rub_rate))))
            else:
                rrc_by_store[suid] = float(int(round(float(raw_rrc))))

            # Real price metrics
            src_row = source_row_by_store_sku.get(suid, {}).get(sku) or {}
            leaf = _leaf_path_from_row(src_row)
            st = category_settings_by_store_uid.get(suid, {}).get(leaf) or {}
            store_st = store_settings_by_store_uid.get(suid, {}) or {}
            lg_store = logistics_store_by_store_uid.get(suid, {}) or {}
            lg_prod = logistics_product_by_store_uid.get(suid, {}).get(sku) or {}
            platform_id = str(s.get("platform") or "").strip().lower()
            currency_code = str(s.get("currency_code") or "RUB").strip().upper()

            cogs_val = cogs_by_store.get(suid)
            rrc_val = rrc_by_store.get(suid)
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
            # Для маржи себестоимость не участвует в расчете целевого показателя.
            fixed_cost = fixed_cost_core + (cogs_cost if earning_mode == "profit" else 0.0)

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

            # Выбираем целевой показатель по режиму магазина.
            target_pct_raw = None
            target_abs_raw = None
            if earning_unit == "percent":
                if earning_mode == "margin":
                    target_pct_raw = st.get("target_margin_percent")
                    if target_pct_raw in (None, ""):
                        target_pct_raw = store_st.get("target_margin_percent")
                else:
                    target_pct_raw = st.get("target_profit_percent")
                    if target_pct_raw in (None, ""):
                        target_pct_raw = store_st.get("target_profit_percent")
                # fallback между полями, если основной не заполнен
                if target_pct_raw in (None, ""):
                    target_pct_raw = (
                        st.get("target_profit_percent")
                        or store_st.get("target_profit_percent")
                        or st.get("target_margin_percent")
                        or store_st.get("target_margin_percent")
                    )
            else:
                if earning_mode == "margin":
                    target_abs_raw = st.get("target_margin_rub")
                    if target_abs_raw in (None, ""):
                        target_abs_raw = store_st.get("target_margin_rub")
                else:
                    target_abs_raw = st.get("target_profit_rub")
                    if target_abs_raw in (None, ""):
                        target_abs_raw = store_st.get("target_profit_rub")
                if target_abs_raw in (None, ""):
                    target_abs_raw = (
                        st.get("target_profit_rub")
                        or store_st.get("target_profit_rub")
                        or st.get("target_margin_rub")
                        or store_st.get("target_margin_rub")
                    )

            target_pct = _num0(target_pct_raw)
            target_abs = _num0(target_abs_raw)
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
                    target_price = float(int(round(hi)))
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

            price_metrics_by_store[suid] = {
                "rrc": rrc_val,
                "profit_abs": rrc_profit_abs,
                "profit_pct": rrc_profit_pct,
                "target_price": target_price,
                "target_profit_abs": target_profit_abs,
                "target_profit_pct": target_profit_pct,
                "handling_text": handling_text,
                "logistics_cost": float(int(round(logistics_cost))) if logistics_cost else 0.0,
            }
        rows_out.append(
            {
                "sku": sku,
                "name": str(item.get("name") or ""),
                "tree_path": path,
                "placements": item.get("placements") or {},
                "cogs_by_store": cogs_by_store,
                "rrc_by_store": rrc_by_store,
                "price_metrics_by_store": price_metrics_by_store,
                "updated_at": str(item.get("updated_at") or ""),
            }
        )

    rows_out.sort(key=lambda r: (str(r.get("sku") or "")))
    total_count = len(rows_out)
    page_size_n = max(1, min(int(page_size or 50), 500))
    page_n = max(1, int(page or 1))
    start = (page_n - 1) * page_size_n
    paged = rows_out[start:start + page_size_n]

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
    _cache_set("overview", cache_payload, resp)
    return resp


@router.post("/api/catalog/products/refresh")
async def catalog_products_refresh():
    from backend.routers.pricing import pricing_settings_market_items

    market_updates: list[dict] = []
    gsheets_updates: list[dict] = []
    errors: list[str] = []

    stores = _catalog_marketplace_stores_context()
    for s in stores:
        platform = str(s.get("platform") or "").strip().lower()
        sid = str(s.get("store_id") or "").strip()
        if platform not in {"yandex_market", "ozon"} or not sid:
            continue
        try:
            result = await pricing_settings_market_items(platform=platform, store_id=sid)
            if isinstance(result, JSONResponse):
                market_updates.append({"platform": platform, "store_id": sid, "ok": False})
                errors.append(f"{platform}:{sid}: refresh failed")
            else:
                market_updates.append({"platform": platform, "store_id": sid, "ok": bool(result.get("ok", True))})
        except Exception as e:
            market_updates.append({"platform": platform, "store_id": sid, "ok": False})
            errors.append(f"{platform}:{sid}: {e}")

    for src in (load_sources() or []):
        if str(src.get("type") or "").strip().lower() != "gsheets":
            continue
        sid = str(src.get("id") or "").strip()
        if not sid:
            continue
        spreadsheet_id = str(src.get("spreadsheet_id") or "").strip()
        worksheet = str(src.get("worksheet") or "").strip() or None
        title = str(src.get("title") or sid)
        mapping = src.get("mapping") if isinstance(src.get("mapping"), dict) else {}
        sku_key = str(mapping.get("sku_primary") or "").strip()
        name_key = str(mapping.get("name") or "").strip()
        c1 = str(mapping.get("category_l1") or mapping.get("category") or "").strip()
        c2 = str(mapping.get("category_l2") or "").strip()
        c3 = str(mapping.get("category_l3") or "").strip()
        c4 = str(mapping.get("category_l4") or "").strip()
        c5 = str(mapping.get("category_l5") or "").strip()
        price_key = str(mapping.get("price_site") or mapping.get("price") or "").strip()
        ccy_key = str(mapping.get("currency") or "").strip()
        try:
            data = read_sheet_all(spreadsheet_id, worksheet=worksheet)
            all_rows = data.get("rows") if isinstance(data, dict) else None
            if not isinstance(all_rows, list) or not all_rows:
                gsheets_updates.append({"source_id": sid, "ok": True, "saved_count": 0})
                continue
            header = [str(x or "").strip() for x in (all_rows[0] if isinstance(all_rows[0], list) else [])]
            if not header:
                gsheets_updates.append({"source_id": sid, "ok": True, "saved_count": 0})
                continue
            h_norm = [_norm_col_name(h) for h in header]

            def idx_of(key: str) -> int:
                k = _norm_col_name(key)
                if not k:
                    return -1
                for i, hv in enumerate(h_norm):
                    if hv == k:
                        return i
                return -1

            sku_idx = idx_of(sku_key) if sku_key else -1
            if sku_idx < 0:
                # fallback common names
                for fallback in ("sku", "артикул", "артикул gt"):
                    sku_idx = idx_of(fallback)
                    if sku_idx >= 0:
                        break
            name_idx = idx_of(name_key) if name_key else idx_of("наименование товара")
            c1_idx = idx_of(c1) if c1 else -1
            c2_idx = idx_of(c2) if c2 else -1
            c3_idx = idx_of(c3) if c3 else -1
            c4_idx = idx_of(c4) if c4 else -1
            c5_idx = idx_of(c5) if c5 else -1
            price_idx = idx_of(price_key) if price_key else -1
            ccy_idx = idx_of(ccy_key) if ccy_key else -1

            if sku_idx < 0:
                gsheets_updates.append({"source_id": sid, "ok": False, "saved_count": 0})
                errors.append(f"gsheets:{sid}: sku column not found")
                continue

            prepared: list[dict] = []
            for row in all_rows[1:]:
                if not isinstance(row, list):
                    continue
                sku = str(row[sku_idx] if sku_idx < len(row) else "").strip()
                if not sku:
                    continue
                payload = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
                c1v = str(row[c1_idx] if c1_idx >= 0 and c1_idx < len(row) else "").strip()
                subparts = [
                    str(row[i] if i >= 0 and i < len(row) else "").strip()
                    for i in (c2_idx, c3_idx, c4_idx, c5_idx)
                    if i >= 0
                ]
                sub = " / ".join([x for x in subparts if x])
                prepared.append(
                    {
                        "sku": sku,
                        "name": str(row[name_idx] if name_idx >= 0 and name_idx < len(row) else "").strip(),
                        "category": c1v,
                        "subcategory": sub,
                        "price": row[price_idx] if price_idx >= 0 and price_idx < len(row) else None,
                        "currency": str(row[ccy_idx] if ccy_idx >= 0 and ccy_idx < len(row) else "").strip(),
                        "payload": payload,
                    }
                )
            saved = replace_source_rows(f"gsheets:{sid}", prepared, source_type="gsheets", title=title)
            gsheets_updates.append({"source_id": sid, "ok": True, "saved_count": int(saved)})
        except Exception as e:
            gsheets_updates.append({"source_id": sid, "ok": False, "saved_count": 0})
            errors.append(f"gsheets:{sid}: {e}")

    _cache_invalidate_all()
    return {
        "ok": True,
        "marketplaces": market_updates,
        "gsheets": gsheets_updates,
        "errors": errors,
        "cache_invalidated": True,
    }


@router.get("/api/imports/catalog/config")
async def get_catalog_import_config():
    data = load_integrations()
    options = _build_catalog_import_options(data)
    item = _get_catalog_import_config(data)
    valid_ids = {
        str(o.get("id") or "")
        for o in (options.get("source_options") or [])
        if isinstance(o, dict) and str(o.get("id") or "").strip()
    }
    selected = [sid for sid in _normalize_catalog_source_ids(item.get("selected_sources") or []) if sid in valid_ids]
    master = str(item.get("master_source") or "").strip()
    if master not in selected:
        master = ""
    raw_mapping = item.get("mapping_by_source") if isinstance(item.get("mapping_by_source"), dict) else {}
    mapping_by_source: dict[str, dict[str, str]] = {}
    for sid in selected:
        if not sid.startswith("gsheets:"):
            continue
        mp = raw_mapping.get(sid) if isinstance(raw_mapping.get(sid), dict) else {}
        mapping_by_source[sid] = _sanitize_catalog_mapping(mp)
    item["selected_sources"] = selected
    item["master_source"] = master
    item["mapping_by_source"] = mapping_by_source
    ready, blockers = _evaluate_catalog_import_config(item, options)

    return {
        "ok": True,
        "item": item,
        "status": {
            "ready": ready,
            "blockers": blockers,
        },
        "options": options,
    }


@router.post("/api/imports/catalog/config")
async def save_catalog_import_config(payload: dict):
    raw_selected = payload.get("selected_sources")
    selected_sources = (
        _normalize_catalog_source_ids(raw_selected)
        if isinstance(raw_selected, list)
        else []
    )
    master_source = str(payload.get("master_source") or "").strip()
    if not selected_sources:
        return JSONResponse({"ok": False, "message": "Выберите минимум один источник каталога"}, status_code=400)
    if not master_source:
        return JSONResponse({"ok": False, "message": "Выберите источник дерева категорий и наименований"}, status_code=400)
    if master_source not in selected_sources:
        return JSONResponse({"ok": False, "message": "Источник дерева должен быть выбран в источниках каталога"}, status_code=400)
    raw_mapping_by_source = payload.get("mapping_by_source")
    mapping_by_source: dict[str, dict[str, str]] = {}
    if isinstance(raw_mapping_by_source, dict):
        for sid, mp in raw_mapping_by_source.items():
            sid_norm = str(sid or "").strip()
            if not sid_norm or not isinstance(mp, dict):
                continue
            mapping_by_source[sid_norm] = _sanitize_catalog_mapping(mp)

    data = load_integrations()
    options = _build_catalog_import_options(data)
    valid_ids = {
        str(o.get("id") or "")
        for o in (options.get("source_options") or [])
        if isinstance(o, dict)
    }
    selected_sources = [x for x in selected_sources if x in valid_ids]
    if not selected_sources:
        return JSONResponse({"ok": False, "message": "Выберите минимум один доступный источник каталога"}, status_code=400)
    if master_source not in selected_sources:
        return JSONResponse({"ok": False, "message": "Источник дерева должен быть выбран в источниках каталога"}, status_code=400)

    filtered_mapping: dict[str, dict[str, str]] = {}
    for sid in selected_sources:
        if not sid.startswith("gsheets:"):
            continue
        mp = mapping_by_source.get(sid) if isinstance(mapping_by_source.get(sid), dict) else {}
        filtered_mapping[sid] = _sanitize_catalog_mapping(mp)

    now = _now_iso()
    cfg = _default_catalog_import_config()
    cfg["selected_sources"] = selected_sources
    cfg["master_source"] = master_source
    cfg["mapping_by_source"] = filtered_mapping
    cfg["updated_at"] = now

    imports_obj = data.get("imports") if isinstance(data.get("imports"), dict) else {}
    imports_obj["catalog"] = cfg
    data["imports"] = imports_obj
    save_integrations(data)
    ready, blockers = _evaluate_catalog_import_config(cfg, options)
    return {
        "ok": True,
        "item": cfg,
        "status": {
            "ready": ready,
            "blockers": blockers,
        },
    }
