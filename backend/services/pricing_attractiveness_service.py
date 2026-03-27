from __future__ import annotations

import asyncio

import copy
import datetime
import hashlib
import json
import logging
import re
from typing import Any

import httpx

from backend.routers._shared import (
    YANDEX_BASE_URL,
    _find_ozon_account_credentials,
    _find_yandex_shop_credentials,
    _fetch_ozon_product_info_map,
    _ym_headers,
)
from backend.services.pricing_prices_service import (
    _profit_for_price,
    get_prices_context,
    get_prices_overview,
    get_prices_tree,
    refresh_prices_data,
)
from backend.services.store_data_model import (
    get_fx_rates_cache,
    get_pricing_attractiveness_results_map,
    get_pricing_promo_offer_results_map,
    get_pricing_promo_results_map,
    get_pricing_price_results_map,
    get_pricing_strategy_iteration_latest_map,
    get_pricing_strategy_results_map,
    upsert_pricing_attractiveness_results_bulk,
)

_ATTR_CACHE: dict[str, dict] = {}
_ATTR_CACHE_GEN = 1
_ATTR_CACHE_MAX = 400
logger = logging.getLogger("uvicorn.error")
_FX_OZON_USD_RUB_MEM: dict[str, float] = {}


def _iteration_attr_payload(iteration_row: dict[str, Any] | None) -> dict[str, Any]:
    row = iteration_row if isinstance(iteration_row, dict) else {}
    status_code = str(row.get("attractiveness_status") or "").strip().lower()
    status_label = (
        "Выгодная" if status_code == "profitable" else
        "Умеренная" if status_code == "moderate" else
        "—" if status_code in {"", "unknown"} else
        "Завышенная"
    )
    return {
        "price": _to_num(row.get("tested_price")),
        "boost_pct": _to_num(row.get("tested_boost_pct")),
        "status_code": status_code,
        "status_label": status_label,
        "coinvest_pct": _to_num(row.get("coinvest_pct")),
        "on_display_price": _to_num(row.get("on_display_price")),
    }


def _cache_key(name: str, payload: dict) -> str:
    raw = json.dumps({"name": name, "gen": _ATTR_CACHE_GEN, "payload": payload}, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_get(name: str, payload: dict):
    key = _cache_key(name, payload)
    got = _ATTR_CACHE.get(key)
    if isinstance(got, dict):
        logger.warning("[pricing_attractiveness] cache hit name=%s key=%s", name, key[:12])
    else:
        logger.warning("[pricing_attractiveness] cache miss name=%s key=%s", name, key[:12])
    return copy.deepcopy(got) if isinstance(got, dict) else None


def _cache_set(name: str, payload: dict, value: dict):
    key = _cache_key(name, payload)
    if len(_ATTR_CACHE) >= _ATTR_CACHE_MAX:
        _ATTR_CACHE.clear()
    _ATTR_CACHE[key] = copy.deepcopy(value)


def invalidate_attractiveness_cache():
    global _ATTR_CACHE_GEN
    _ATTR_CACHE.clear()
    _ATTR_CACHE_GEN += 1


def _to_num(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except Exception:
        pass
    s = str(v).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


async def _get_ozon_sales_usd_rub_rate_for_date(calc_date: datetime.date) -> float | None:
    key = calc_date.isoformat()
    if key in _FX_OZON_USD_RUB_MEM:
        return _FX_OZON_USD_RUB_MEM[key]
    try:
        cached = get_fx_rates_cache(source="ozon_sales", pair="USD_RUB")
        rows = cached.get("rows") if isinstance(cached, dict) else None
        by_date: dict[str, float] = {}
        if isinstance(rows, list):
            for r in rows:
                d = str(r.get("date") or "").strip()
                try:
                    v = float(r.get("rate"))
                except Exception:
                    continue
                if d and v > 0:
                    by_date[d] = v
        if by_date:
            best_date = max(by_date.keys())
            rate = float(by_date[best_date])
            if rate > 0:
                _FX_OZON_USD_RUB_MEM[key] = rate
                return rate
    except Exception:
        pass
    # Fallback: официальный курс ЦБ, если Ozon-курс недоступен.
    try:
        cached = get_fx_rates_cache(source="cbr", pair="USD_RUB")
        rows = cached.get("rows") if isinstance(cached, dict) else None
        by_date: dict[str, float] = {}
        if isinstance(rows, list):
            for r in rows:
                d = str(r.get("date") or "").strip()
                try:
                    v = float(r.get("rate"))
                except Exception:
                    continue
                if d and v > 0:
                    by_date[d] = v
        if by_date:
            best_date = max(by_date.keys())
            rate = float(by_date[best_date])
            if rate > 0:
                _FX_OZON_USD_RUB_MEM[key] = rate
                return rate
    except Exception:
        pass
    return None


def _scan_for_price(node: Any, include_keys: tuple[str, ...], exclude_keys: tuple[str, ...] = ()) -> float | None:
    best: float | None = None

    def walk(x: Any, path: str = ""):
        nonlocal best
        if isinstance(x, dict):
            for k, v in x.items():
                nk = str(k).lower()
                np = f"{path}.{nk}" if path else nk
                walk(v, np)
            return
        if isinstance(x, list):
            for i, v in enumerate(x):
                walk(v, f"{path}[{i}]")
            return
        p = path.lower()
        if include_keys and not any(k in p for k in include_keys):
            return
        if exclude_keys and any(k in p for k in exclude_keys):
            return
        n = _to_num(x)
        if n is None:
            return
        if best is None:
            best = n
        else:
            best = max(best, n)

    walk(node)
    return best


def _scan_for_price_all(node: Any, include_all_keys: tuple[str, ...], exclude_keys: tuple[str, ...] = ()) -> float | None:
    best: float | None = None

    def walk(x: Any, path: str = ""):
        nonlocal best
        if isinstance(x, dict):
            for k, v in x.items():
                nk = str(k).lower()
                np = f"{path}.{nk}" if path else nk
                walk(v, np)
            return
        if isinstance(x, list):
            for i, v in enumerate(x):
                walk(v, f"{path}[{i}]")
            return
        p = path.lower()
        if include_all_keys and not all(k in p for k in include_all_keys):
            return
        if exclude_keys and any(k in p for k in exclude_keys):
            return
        n = _to_num(x)
        if n is None:
            return
        if best is None:
            best = n
        else:
            best = max(best, n)

    walk(node)
    return best


def _extract_yandex_level_prices(node: Any) -> dict[str, float | None]:
    out: dict[str, float | None] = {"low": None, "average": None, "optimal": None}

    def setv(level: str, value: Any):
        n = _to_num(value)
        if n is None:
            return
        cur = out.get(level)
        if cur is None:
            out[level] = n
        else:
            out[level] = max(cur, n)

    def walk(x: Any, path: str = ""):
        if isinstance(x, dict):
            # Common direct keyed shape: {LOW: {...}, AVERAGE: {...}, OPTIMAL: {...}}
            for k, v in x.items():
                lk = str(k).strip().lower()
                if lk in {"low", "average", "optimal"}:
                    if isinstance(v, dict):
                        # explicit price fields in level object
                        for pk in ("price", "value", "recommendedPrice", "priceValue"):
                            if pk in v:
                                setv(lk, v.get(pk))
                        # fallback to any numeric leaf
                        if out.get(lk) is None:
                            setv(lk, _scan_for_price(v, ("price", "value", "amount", "recommended")))
                    else:
                        setv(lk, v)
                walk(v, f"{path}.{lk}" if path else lk)
            return
        if isinstance(x, list):
            for i, it in enumerate(x):
                # Common list shape: [{type:LOW, price:...}, ...]
                if isinstance(it, dict):
                    t = str(
                        it.get("type")
                        or it.get("level")
                        or it.get("recommendationType")
                        or it.get("kind")
                        or ""
                    ).strip().lower()
                    if t in {"low", "average", "optimal"}:
                        for pk in ("price", "value", "recommendedPrice", "priceValue"):
                            if pk in it:
                                setv(t, it.get(pk))
                        if out.get(t) is None:
                            setv(t, _scan_for_price(it, ("price", "value", "amount", "recommended")))
                walk(it, f"{path}[{i}]")
            return
        # Path-driven fallback if token and price coexist in path
        p = path.lower()
        if "low" in p and "price" in p:
            setv("low", x)
        if "average" in p and "price" in p:
            setv("average", x)
        if "optimal" in p and "price" in p:
            setv("optimal", x)

    walk(node)
    return out


def _extract_yandex_offer_recommendations(payload: dict) -> dict[str, dict]:
    root = payload if isinstance(payload, dict) else {}
    result = root.get("result") if isinstance(root.get("result"), dict) else root

    candidates: list[dict] = []
    for key in (
        "offerRecommendations",
        "offers",
        "items",
        "recommendations",
        "offerRecommendation",
    ):
        arr = result.get(key) if isinstance(result, dict) else None
        if isinstance(arr, list):
            candidates.extend([x for x in arr if isinstance(x, dict)])

    if not candidates and isinstance(result, dict):
        nested = result.get("offerRecommendations")
        if isinstance(nested, dict):
            for _, v in nested.items():
                if isinstance(v, dict):
                    candidates.append(v)

    out: dict[str, dict] = {}
    for item in candidates:
        offer_obj = item.get("offer") if isinstance(item.get("offer"), dict) else {}
        rec_obj = item.get("recommendation") if isinstance(item.get("recommendation"), dict) else {}
        sku = str(
            item.get("offerId")
            or item.get("offer_id")
            or item.get("shopSku")
            or offer_obj.get("shopSku")
            or offer_obj.get("offerId")
            or offer_obj.get("offer_id")
            or offer_obj.get("shop_sku")
            or offer_obj.get("vendorCode")
            or ""
        ).strip()
        if not sku:
            continue

        # Primary parse from explicit recommendation levels LOW/AVERAGE/OPTIMAL.
        lvl = _extract_yandex_level_prices(rec_obj if rec_obj else item)
        non_profitable = lvl.get("low")
        moderate = lvl.get("average")
        profitable = lvl.get("optimal")

        # Secondary: explicit thresholds map (e.g. competitivenessThresholds.*Price)
        thresholds = rec_obj.get("competitivenessThresholds") if isinstance(rec_obj.get("competitivenessThresholds"), dict) else {}
        if isinstance(thresholds, dict) and thresholds:
            for k, v in thresholds.items():
                lk = str(k).strip().lower()
                pv = _to_num(v.get("value") if isinstance(v, dict) else v)
                if pv is None:
                    continue
                if any(t in lk for t in ("optimal", "profitable", "recommended", "best")):
                    if profitable is None:
                        profitable = pv
                elif any(t in lk for t in ("average", "moderate", "middle")):
                    if moderate is None:
                        moderate = pv
                elif any(t in lk for t in ("low", "notprofitable", "nonprofitable", "unprofitable", "overpriced", "bad")):
                    if non_profitable is None:
                        non_profitable = pv

        # Fallbacks for legacy/alternative shapes.
        # IMPORTANT: do not auto-fill LOW/AVERAGE from OPTIMAL; keep missing values empty.
        if non_profitable is None:
            non_profitable = _scan_for_price_all(rec_obj, ("low", "price"))
        if non_profitable is None:
            non_profitable = _scan_for_price_all(rec_obj, ("notprofitable", "price"))
        if non_profitable is None:
            non_profitable = _scan_for_price_all(rec_obj, ("nonprofitable", "price"))
        if non_profitable is None:
            non_profitable = _scan_for_price_all(rec_obj, ("unprofitable", "price"))

        if moderate is None:
            moderate = _scan_for_price_all(rec_obj, ("average", "price"))
        if moderate is None:
            moderate = _scan_for_price_all(rec_obj, ("moderate", "price"))
        if moderate is None:
            moderate = _scan_for_price_all(rec_obj, ("middle", "price"))

        if profitable is None:
            profitable = _scan_for_price_all(rec_obj, ("optimal", "price"))
        if profitable is None:
            profitable = _scan_for_price_all(rec_obj, ("recommended", "price"))
        if profitable is None:
            profitable = _scan_for_price_all(rec_obj, ("profitable", "price"), ("not",))

        out[sku] = {
            "attractiveness_overpriced_price": non_profitable,
            "attractiveness_moderate_price": moderate,
            "attractiveness_profitable_price": profitable,
            "payload": item,
        }
    return out


async def _fetch_yandex_offer_recommendations_map(
    *,
    business_id: str,
    campaign_id: str,
    api_key: str,
    offer_ids: list[str],
) -> tuple[dict[str, dict], list[dict[str, Any]]]:
    bid = str(business_id or "").strip()
    if not bid or not offer_ids:
        return {}, [{"ok": False, "reason": "empty_business_or_offer_ids", "business_id": bid, "offers": len(offer_ids or [])}]

    unique_offer_ids: list[str] = []
    seen: set[str] = set()
    for sku in offer_ids:
        val = str(sku or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        unique_offer_ids.append(val)

    out: dict[str, dict] = {}
    debug_calls: list[dict[str, Any]] = []
    # По ТЗ: используем только актуальный endpoint рекомендаций офферов.
    url = f"{YANDEX_BASE_URL}/businesses/{bid}/offers/recommendations"

    async with httpx.AsyncClient(timeout=45) as client:
        for i in range(0, len(unique_offer_ids), 100):
            chunk = unique_offer_ids[i:i + 100]
            try:
                logger.warning(
                    "[pricing_attractiveness] Yandex recommendations request business_id=%s campaign_id=%s chunk=%s url=%s",
                    bid,
                    campaign_id,
                    len(chunk),
                    url,
                )
                resp = await client.post(
                    url,
                    headers=_ym_headers(api_key),
                    json={"offerIds": chunk},
                )
                logger.warning(
                    "[pricing_attractiveness] Yandex recommendations response business_id=%s campaign_id=%s status=%s chunk=%s",
                    bid,
                    campaign_id,
                    resp.status_code,
                    len(chunk),
                )
                resp.raise_for_status()
                payload = resp.json()
                parsed = _extract_yandex_offer_recommendations(payload)
                result_obj = payload.get("result") if isinstance(payload, dict) and isinstance(payload.get("result"), dict) else {}
                result_keys = list(result_obj.keys())[:15] if isinstance(result_obj, dict) else []
                first_item_keys: list[str] = []
                first_item_recommendation_preview: dict[str, Any] | None = None
                first_item_threshold_keys: list[str] = []
                for _k in ("offerRecommendations", "offers", "items", "recommendations", "offerRecommendation"):
                    arr = result_obj.get(_k) if isinstance(result_obj, dict) else None
                    if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                        first_item_keys = list(arr[0].keys())[:20]
                        rec0 = arr[0].get("recommendation") if isinstance(arr[0].get("recommendation"), dict) else None
                        if isinstance(rec0, dict):
                            first_item_recommendation_preview = {k: rec0.get(k) for k in list(rec0.keys())[:8]}
                            th0 = rec0.get("competitivenessThresholds") if isinstance(rec0.get("competitivenessThresholds"), dict) else {}
                            if isinstance(th0, dict):
                                first_item_threshold_keys = list(th0.keys())[:20]
                        break
                logger.warning(
                    "[pricing_attractiveness] Yandex recommendations parsed business_id=%s campaign_id=%s parsed=%s result_keys=%s first_item_keys=%s threshold_keys=%s rec_preview=%s",
                    bid,
                    campaign_id,
                    len(parsed),
                    result_keys,
                    first_item_keys,
                    first_item_threshold_keys,
                    first_item_recommendation_preview,
                )
                if parsed:
                    out.update(parsed)
                debug_calls.append(
                    {
                        "ok": True,
                        "business_id": bid,
                        "campaign_id": campaign_id,
                        "url": url,
                        "status_code": resp.status_code,
                        "chunk_size": len(chunk),
                        "parsed_count": len(parsed),
                        "result_keys": result_keys,
                        "first_item_keys": first_item_keys,
                        "threshold_keys": first_item_threshold_keys,
                        "recommendation_preview": first_item_recommendation_preview,
                    }
                )
            except Exception:
                status_code = None
                body_preview = ""
                err = "request_failed"
                if "resp" in locals():
                    try:
                        status_code = resp.status_code
                        body_preview = (resp.text or "")[:500]
                        err = f"http_{status_code}"
                    except Exception:
                        pass
                logger.exception(
                    "[pricing_attractiveness] Yandex recommendations failed business_id=%s campaign_id=%s chunk=%s url=%s status=%s",
                    bid,
                    campaign_id,
                    len(chunk),
                    url,
                    status_code,
                )
                debug_calls.append(
                    {
                        "ok": False,
                        "business_id": bid,
                        "campaign_id": campaign_id,
                        "url": url,
                        "status_code": status_code,
                        "chunk_size": len(chunk),
                        "error": err,
                        "response_preview": body_preview,
                    }
                )
                # 403 по магазину означает, что endpoint недоступен для этого кабинета.
                # Продолжать остальные чанки бессмысленно и только шумит логами.
                if status_code == 403:
                    break
                # Не валим весь overview из-за одного недоступного источника.
                continue
    return out, debug_calls


def _has_terminal_yandex_403(debug_calls: list[dict[str, Any]] | None) -> bool:
    calls = debug_calls if isinstance(debug_calls, list) else []
    if not calls:
        return False
    for item in calls:
        if not isinstance(item, dict):
            continue
        if int(item.get("status_code") or 0) == 403:
            return True
        err = str(item.get("error") or "").strip().lower()
        if err in {"http_403", "403", "forbidden"}:
            return True
    return False


def _extract_ozon_competitor_prices_from_info(info_item: dict) -> tuple[float | None, float | None]:
    payload = info_item.get("payload") if isinstance(info_item.get("payload"), dict) else {}
    pi = payload.get("price_indexes") if isinstance(payload, dict) else None
    if not isinstance(pi, dict):
        return None, None

    ozon_data = pi.get("ozon_index_data")
    external_data = pi.get("external_index_data")

    ozon_price = _scan_for_price(ozon_data, ("price", "value", "index", "min", "base"))
    external_price = _scan_for_price(external_data, ("price", "value", "index", "min", "base"))
    return ozon_price, external_price


async def get_attractiveness_context():
    return await get_prices_context()


async def get_attractiveness_tree(
    *,
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
):
    return await get_prices_tree(
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
    )


async def get_attractiveness_overview(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    status_filter: str = "all",
    stock_filter: str = "all",
    page: int = 1,
    page_size: int = 50,
    fetch_live: bool = False,
):
    status_filter_norm = str(status_filter or "all").strip().lower()
    if status_filter_norm not in {"all", "profitable", "moderate", "overpriced"}:
        status_filter_norm = "all"
    stock_filter_norm = str(stock_filter or "all").strip().lower()
    if stock_filter_norm not in {"all", "in_stock", "out_of_stock"}:
        stock_filter_norm = "all"
    cache_payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "status_filter": status_filter_norm,
        "stock_filter": stock_filter_norm,
        "page": page,
        "page_size": page_size,
        "fetch_live": bool(fetch_live),
    }
    if not fetch_live:
        cached = _cache_get("overview", cache_payload)
        if cached:
            return cached
    logger.warning(
        "[pricing_attractiveness] overview build scope=%s platform=%s store_id=%s page=%s size=%s",
        scope,
        platform,
        store_id,
        page,
        page_size,
    )

    # Для status_filter != all или stock_filter != all фильтрация должна идти по всем товарам,
    # а не по текущей странице. Тогда сначала собираем все строки из prices-layer,
    # затем фильтруем и пагинируем здесь.
    if status_filter_norm == "all" and stock_filter_norm == "all":
        base = await get_prices_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_mode=tree_mode,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter_norm,
            page=page,
            page_size=page_size,
            force_refresh=bool(fetch_live),
        )
        rows = base.get("rows") if isinstance(base, dict) else None
        stores = base.get("stores") if isinstance(base, dict) else None
    else:
        base = await get_prices_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_mode=tree_mode,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter_norm,
            page=1,
            page_size=200,
            force_refresh=bool(fetch_live),
        )
        stores = base.get("stores") if isinstance(base, dict) else None
        rows_all = base.get("rows") if isinstance(base, dict) and isinstance(base.get("rows"), list) else []
        total_base = int(base.get("total_count") or len(rows_all)) if isinstance(base, dict) else len(rows_all)
        loaded = len(rows_all)
        page_i = 1
        while loaded < total_base:
            page_i += 1
            nxt = await get_prices_overview(
                scope=scope,
                platform=platform,
                store_id=store_id,
                tree_mode=tree_mode,
                tree_source_store_id=tree_source_store_id,
                category_path=category_path,
                search=search,
                stock_filter=stock_filter_norm,
                page=page_i,
                page_size=200,
                force_refresh=bool(fetch_live),
            )
            chunk = nxt.get("rows") if isinstance(nxt, dict) and isinstance(nxt.get("rows"), list) else []
            if not chunk:
                break
            rows_all.extend(chunk)
            loaded += len(chunk)
            if page_i > 200:
                break
        rows = rows_all

    if not isinstance(rows, list) or not isinstance(stores, list) or not rows:
        resp = {
            **(base if isinstance(base, dict) else {}),
            "rows": rows if isinstance(rows, list) else [],
        }
        _cache_set("overview", cache_payload, resp)
        return resp

    skus = [str(r.get("sku") or "").strip() for r in rows if str(r.get("sku") or "").strip()]

    store_uids = [str(s.get("store_uid") or "").strip() for s in stores if str(s.get("store_uid") or "").strip()]
    db_price_map = get_pricing_price_results_map(store_uids=store_uids, skus=skus)
    strategy_map = get_pricing_strategy_results_map(store_uids=store_uids, skus=skus)
    iteration_map = get_pricing_strategy_iteration_latest_map(store_uids=store_uids, skus=skus)

    # Primary source для базовой цены и базовых метрик — materialized таблица pricing_price_results.
    # Здесь базовый слой = наша РРЦ/целевая цена из страницы "Цены".
    for row in rows:
        sku = str(row.get("sku") or "").strip()
        if not isinstance(row.get("base_price_by_store"), dict):
            row["base_price_by_store"] = {}
        if not isinstance(row.get("mrc_price_by_store"), dict):
            row["mrc_price_by_store"] = {}
        if not isinstance(row.get("installed_price_by_store"), dict):
            row["installed_price_by_store"] = {}
        if not isinstance(row.get("installed_profit_abs_by_store"), dict):
            row["installed_profit_abs_by_store"] = {}
        if not isinstance(row.get("installed_profit_pct_by_store"), dict):
            row["installed_profit_pct_by_store"] = {}
        if not isinstance(row.get("chosen_boost_bid_by_store"), dict):
            row["chosen_boost_bid_by_store"] = {}
        if not isinstance(row.get("price_metrics_by_store"), dict):
            row["price_metrics_by_store"] = {}

        base_price_map = row.get("base_price_by_store") if isinstance(row.get("base_price_by_store"), dict) else {}
        mrc_price_map = row.get("mrc_price_by_store") if isinstance(row.get("mrc_price_by_store"), dict) else {}
        installed_price_map = row.get("installed_price_by_store") if isinstance(row.get("installed_price_by_store"), dict) else {}
        installed_profit_abs_map = row.get("installed_profit_abs_by_store") if isinstance(row.get("installed_profit_abs_by_store"), dict) else {}
        installed_profit_pct_map = row.get("installed_profit_pct_by_store") if isinstance(row.get("installed_profit_pct_by_store"), dict) else {}
        chosen_boost_bid_map = row.get("chosen_boost_bid_by_store") if isinstance(row.get("chosen_boost_bid_by_store"), dict) else {}
        pm_map = row.get("price_metrics_by_store") if isinstance(row.get("price_metrics_by_store"), dict) else {}
        for suid in store_uids:
            db_rec = (db_price_map.get(suid) or {}).get(sku) or {}
            strategy_rec = (strategy_map.get(suid) or {}).get(sku) or {}
            live_metric = pm_map.get(suid) if isinstance(pm_map.get(suid), dict) else {}
            live_target_price = live_metric.get("target_price") if isinstance(live_metric, dict) else None
            if live_target_price not in (None, ""):
                base_price_map[suid] = live_target_price
            if db_rec:
                rrc_display = live_target_price
                if rrc_display in (None, ""):
                    rrc_display = db_rec.get("target_price")
                if rrc_display not in (None, ""):
                    base_price_map[suid] = rrc_display
                pm = pm_map.get(suid) if isinstance(pm_map.get(suid), dict) else {}
                pm["base_price"] = rrc_display
                pm["mrc_price"] = db_rec.get("mrc_price")
                pm["mrc_profit_abs"] = db_rec.get("mrc_profit_abs")
                pm["mrc_profit_pct"] = db_rec.get("mrc_profit_pct")
                pm["target_price"] = db_rec.get("target_price")
                pm["target_profit_abs"] = db_rec.get("target_profit_abs")
                pm["target_profit_pct"] = db_rec.get("target_profit_pct")
                pm["base_profit_abs"] = db_rec.get("target_profit_abs")
                pm["base_profit_pct"] = db_rec.get("target_profit_pct")
                pm_map[suid] = pm
                if db_rec.get("mrc_price") not in (None, ""):
                    mrc_price_map[suid] = db_rec.get("mrc_price")
            else:
                pm = pm_map.get(suid)
                if isinstance(pm, dict) and (suid not in base_price_map or base_price_map.get(suid) in (None, "")):
                    base_price_map[suid] = pm.get("target_price")
            if strategy_rec:
                installed_price_map[suid] = strategy_rec.get("installed_price")
                installed_profit_abs_map[suid] = strategy_rec.get("installed_profit_abs")
                installed_profit_pct_map[suid] = strategy_rec.get("installed_profit_pct")

        row["base_price_by_store"] = base_price_map
        row["mrc_price_by_store"] = mrc_price_map
        row["installed_price_by_store"] = installed_price_map
        row["installed_profit_abs_by_store"] = installed_profit_abs_map
        row["installed_profit_pct_by_store"] = installed_profit_pct_map
        row["chosen_boost_bid_by_store"] = chosen_boost_bid_map
        row["price_metrics_by_store"] = pm_map

        iteration_scenarios_by_store: dict[str, dict[str, Any]] = {}
        for suid in store_uids:
            latest_iterations = ((iteration_map.get(suid) or {}).get(sku) or {}) if isinstance(iteration_map.get(suid), dict) else {}
            price_metric = ((db_price_map.get(suid) or {}).get(sku) or {}) if isinstance(db_price_map.get(suid), dict) else {}
            strategy_item = (strategy_map.get(suid) or {}).get(sku) or {}
            iteration_scenarios_by_store[suid] = {
                "selected_price": _to_num(strategy_item.get("installed_price")),
                "selected_boost_pct": _to_num(strategy_item.get("boost_bid_percent")),
                "selected_coinvest_pct": _to_num(strategy_item.get("coinvest_pct")),
                "selected_decision_label": str(strategy_item.get("decision_label") or "").strip(),
                "selected_iteration_code": str(strategy_item.get("selected_iteration_code") or "").strip(),
                "rrc_with_boost": _iteration_attr_payload(latest_iterations.get("rrc_with_boost")),
                "rrc_no_ads": _iteration_attr_payload(latest_iterations.get("rrc_no_ads")),
                "mrc_with_boost": _iteration_attr_payload(latest_iterations.get("mrc_with_boost")),
                "mrc": _iteration_attr_payload(latest_iterations.get("mrc")),
                "rrc_price": _to_num(price_metric.get("target_price")),
                "rrc_no_ads_price": _to_num(price_metric.get("rrc_no_ads_price")),
                "mrc_with_boost_price": _to_num(price_metric.get("mrc_with_boost_price")),
                "mrc_price": _to_num(price_metric.get("mrc_price")),
            }
        row["iteration_scenarios_by_store"] = iteration_scenarios_by_store

    ym_map_by_store_uid: dict[str, dict[str, dict]] = {}
    ym_debug_by_store_uid: dict[str, list[dict[str, Any]]] = {}
    oz_map_by_store_uid: dict[str, dict[str, dict]] = {}
    # Existing materialized values: used in DB-only mode and as fallback in live refresh
    # to avoid wiping previously known competitor prices with NULL.
    attr_db_map = get_pricing_attractiveness_results_map(store_uids=store_uids, skus=skus)
    promo_summary_map = get_pricing_promo_results_map(store_uids=store_uids, skus=skus)
    promo_offer_map = get_pricing_promo_offer_results_map(store_uids=store_uids, skus=skus)

    for s in stores:
        suid = str(s.get("store_uid") or "").strip()
        sid = str(s.get("store_id") or "").strip()
        platform_id = str(s.get("platform") or "").strip().lower()
        if not suid or not sid:
            continue

        if not fetch_live:
            ym_map_by_store_uid[suid] = {}
            ym_debug_by_store_uid[suid] = [{"ok": True, "source": "db_only_mode"}]
            oz_map_by_store_uid[suid] = {}
            continue

        if platform_id == "yandex_market":
            creds = _find_yandex_shop_credentials(sid)
            if creds:
                bid, cid, api_key = creds
                try:
                    ym_map, ym_debug = await _fetch_yandex_offer_recommendations_map(
                        business_id=bid,
                        campaign_id=cid,
                        api_key=api_key,
                        offer_ids=skus,
                    )
                    ym_map_by_store_uid[suid] = ym_map
                    ym_debug_by_store_uid[suid] = ym_debug
                except Exception:
                    ym_map_by_store_uid[suid] = {}
                    ym_debug_by_store_uid[suid] = [
                        {
                            "ok": False,
                            "business_id": bid,
                            "campaign_id": cid,
                            "error": "fetch_wrapper_failed",
                        }
                    ]
            else:
                ym_map_by_store_uid[suid] = {}
                ym_debug_by_store_uid[suid] = [{"ok": False, "error": "credentials_not_found"}]

        if platform_id == "ozon":
            creds = _find_ozon_account_credentials(sid)
            if creds:
                client_id, api_key, _seller_id, _seller_name = creds
                try:
                    info_map = await _fetch_ozon_product_info_map(
                        client_id=client_id,
                        api_key=api_key,
                        product_ids=[],
                        offer_ids=skus,
                    )
                    local: dict[str, dict] = {}
                    for sku in skus:
                        item = info_map.get(sku)
                        if not item:
                            # fallback by embedded sku field
                            for v in info_map.values():
                                if str(v.get("sku") or "").strip() == sku:
                                    item = v
                                    break
                        if not item:
                            continue
                        oz_price, ext_price = _extract_ozon_competitor_prices_from_info(item)
                        local[sku] = {
                            "ozon_competitor_price": oz_price,
                            "external_competitor_price": ext_price,
                        }
                    oz_map_by_store_uid[suid] = local
                except Exception:
                    oz_map_by_store_uid[suid] = {}
            else:
                oz_map_by_store_uid[suid] = {}

    def _calc_profit_for_price_from_ctx(row_obj: dict, suid: str, price_val: float | None) -> tuple[float | None, float | None]:
        if price_val is None:
            return None, None
        pm = (row_obj.get("price_metrics_by_store") or {}).get(suid) or {}
        calc_ctx = pm.get("calc_ctx") if isinstance(pm.get("calc_ctx"), dict) else {}
        if not calc_ctx:
            return None, None
        try:
            dep_rate = float(calc_ctx.get("dep_rate") or 0.0)
            ads_rate = float(calc_ctx.get("ads_rate") or 0.0)
            pa, pp = _profit_for_price(
                price=float(price_val),
                dep_rate=max(0.0, dep_rate - ads_rate),
                tax_rate=float(calc_ctx.get("tax_rate") or 0.0),
                fixed_cost=float(calc_ctx.get("fixed_cost") or 0.0),
                apply_tax=bool(calc_ctx.get("apply_tax", True)),
                handling_mode=str(calc_ctx.get("handling_mode") or "fixed"),
                handling_fixed=float(calc_ctx.get("handling_fixed") or 0.0),
                handling_percent=float(calc_ctx.get("handling_percent") or 0.0),
                handling_min=float(calc_ctx.get("handling_min") or 0.0),
                handling_max=float(calc_ctx.get("handling_max") or 0.0),
            )
            return float(int(round(pa))), round(pp * 100.0, 2)
        except Exception:
            return None, None

    materialized_rows: list[dict] = []
    rows_with_status: list[dict] = []
    for row in rows:
        sku = str(row.get("sku") or "").strip()
        attr_by_store: dict[str, dict] = {}
        for s in stores:
            suid = str(s.get("store_uid") or "").strip()
            platform_id = str(s.get("platform") or "").strip().lower()
            if not suid:
                continue
            db_prev = (attr_db_map.get(suid) or {}).get(sku) or {}
            metric = {
                "attractiveness_set_price": None,
                "attractiveness_set_profit_abs": None,
                "attractiveness_set_profit_pct": None,
                "attractiveness_overpriced_price": None,
                "attractiveness_overpriced_profit_abs": None,
                "attractiveness_overpriced_profit_pct": None,
                "attractiveness_moderate_price": None,
                "attractiveness_moderate_profit_abs": None,
                "attractiveness_moderate_profit_pct": None,
                "attractiveness_profitable_price": None,
                "attractiveness_profitable_profit_abs": None,
                "attractiveness_profitable_profit_pct": None,
                "ozon_competitor_price": None,
                "ozon_competitor_profit_abs": None,
                "ozon_competitor_profit_pct": None,
                "external_competitor_price": None,
                "external_competitor_profit_abs": None,
                "external_competitor_profit_pct": None,
                "attractiveness_chosen_price": None,
                "attractiveness_chosen_profit_abs": None,
                "attractiveness_chosen_profit_pct": None,
                "attractiveness_chosen_boost_bid_percent": None,
            }
            if not fetch_live:
                db_attr = (attr_db_map.get(suid) or {}).get(sku) or {}
                if db_attr:
                    metric["attractiveness_overpriced_price"] = db_attr.get("attractiveness_overpriced_price")
                    metric["attractiveness_moderate_price"] = db_attr.get("attractiveness_moderate_price")
                    metric["attractiveness_profitable_price"] = db_attr.get("attractiveness_profitable_price")
                    metric["ozon_competitor_price"] = db_attr.get("ozon_competitor_price")
                    metric["external_competitor_price"] = db_attr.get("external_competitor_price")
                    metric["attractiveness_chosen_price"] = db_attr.get("attractiveness_chosen_price")
                    metric["attractiveness_chosen_boost_bid_percent"] = db_attr.get("attractiveness_chosen_boost_bid_percent")
                    chosen_boost_bid_map[suid] = db_attr.get("attractiveness_chosen_boost_bid_percent")
            elif platform_id == "yandex_market":
                ym = (ym_map_by_store_uid.get(suid) or {}).get(sku) or {}
                metric["attractiveness_overpriced_price"] = ym.get("attractiveness_overpriced_price")
                metric["attractiveness_moderate_price"] = ym.get("attractiveness_moderate_price")
                metric["attractiveness_profitable_price"] = ym.get("attractiveness_profitable_price")
                if metric["attractiveness_overpriced_price"] in (None, "") and db_prev.get("attractiveness_overpriced_price") not in (None, ""):
                    metric["attractiveness_overpriced_price"] = db_prev.get("attractiveness_overpriced_price")
                if metric["attractiveness_moderate_price"] in (None, "") and db_prev.get("attractiveness_moderate_price") not in (None, ""):
                    metric["attractiveness_moderate_price"] = db_prev.get("attractiveness_moderate_price")
                if metric["attractiveness_profitable_price"] in (None, "") and db_prev.get("attractiveness_profitable_price") not in (None, ""):
                    metric["attractiveness_profitable_price"] = db_prev.get("attractiveness_profitable_price")
                if metric["attractiveness_chosen_price"] in (None, "") and db_prev.get("attractiveness_chosen_price") not in (None, ""):
                    metric["attractiveness_chosen_price"] = db_prev.get("attractiveness_chosen_price")
                if metric["attractiveness_chosen_boost_bid_percent"] in (None, "") and db_prev.get("attractiveness_chosen_boost_bid_percent") not in (None, ""):
                    metric["attractiveness_chosen_boost_bid_percent"] = db_prev.get("attractiveness_chosen_boost_bid_percent")
            elif platform_id == "ozon":
                oz = (oz_map_by_store_uid.get(suid) or {}).get(sku) or {}
                oz_price = oz.get("ozon_competitor_price")
                ext_price = oz.get("external_competitor_price")
                # Для USD-кабинетов Ozon часть API-цен приходит в RUB.
                # Переводим конкурентные цены в USD по курсу Ozon (для продаж).
                if str(s.get("currency_code") or "RUB").strip().upper() == "USD":
                    fx_ozon = await _get_ozon_sales_usd_rub_rate_for_date(datetime.date.today())
                    if fx_ozon and fx_ozon > 0:
                        try:
                            if oz_price is not None:
                                oz_price = float(oz_price) / float(fx_ozon)
                        except Exception:
                            pass
                        try:
                            if ext_price is not None:
                                ext_price = float(ext_price) / float(fx_ozon)
                        except Exception:
                            pass
                # Не затираем исторические значения в БД, если текущий API-ответ пустой.
                if oz_price is None and db_prev.get("ozon_competitor_price") not in (None, ""):
                    oz_price = db_prev.get("ozon_competitor_price")
                if ext_price is None and db_prev.get("external_competitor_price") not in (None, ""):
                    ext_price = db_prev.get("external_competitor_price")
                metric["ozon_competitor_price"] = oz_price
                metric["external_competitor_price"] = ext_price
                oz_abs, oz_pct = _calc_profit_for_price_from_ctx(row, suid, metric["ozon_competitor_price"])
                ex_abs, ex_pct = _calc_profit_for_price_from_ctx(row, suid, metric["external_competitor_price"])
                metric["ozon_competitor_profit_abs"] = oz_abs
                metric["ozon_competitor_profit_pct"] = oz_pct
                metric["external_competitor_profit_abs"] = ex_abs
                metric["external_competitor_profit_pct"] = ex_pct

            if platform_id == "yandex_market":
                np_abs, np_pct = _calc_profit_for_price_from_ctx(row, suid, metric.get("attractiveness_overpriced_price"))
                md_abs, md_pct = _calc_profit_for_price_from_ctx(row, suid, metric.get("attractiveness_moderate_price"))
                pf_abs, pf_pct = _calc_profit_for_price_from_ctx(row, suid, metric.get("attractiveness_profitable_price"))

                # Если Маркет не вернул LOW, достраиваем "завышенную" цену по бизнес-правилу.
                if metric.get("attractiveness_overpriced_price") in (None, ""):
                    rrc_val = (row.get("base_price_by_store") or {}).get(suid) if isinstance(row.get("base_price_by_store"), dict) else None
                    if (
                        md_abs is not None
                        and pf_abs is not None
                        and md_abs < 0
                        and pf_abs < 0
                        and rrc_val is not None
                    ):
                        metric["attractiveness_overpriced_price"] = rrc_val
                    elif md_abs is not None and md_abs > 0 and metric.get("attractiveness_moderate_price") is not None:
                        try:
                            metric["attractiveness_overpriced_price"] = float(metric["attractiveness_moderate_price"]) + 1.0
                        except Exception:
                            metric["attractiveness_overpriced_price"] = None
                    np_abs, np_pct = _calc_profit_for_price_from_ctx(row, suid, metric.get("attractiveness_overpriced_price"))

                metric["attractiveness_overpriced_profit_abs"] = np_abs
                metric["attractiveness_overpriced_profit_pct"] = np_pct
                metric["attractiveness_moderate_profit_abs"] = md_abs
                metric["attractiveness_moderate_profit_pct"] = md_pct
                metric["attractiveness_profitable_profit_abs"] = pf_abs
                metric["attractiveness_profitable_profit_pct"] = pf_pct

                rrc_val = (row.get("base_price_by_store") or {}).get(suid) if isinstance(row.get("base_price_by_store"), dict) else None
                mrc_val = (row.get("mrc_price_by_store") or {}).get(suid) if isinstance(row.get("mrc_price_by_store"), dict) else None
                chosen: float | None = None
                pm_for_store = (row.get("price_metrics_by_store") or {}).get(suid) if isinstance(row.get("price_metrics_by_store"), dict) else {}
                promo_summary = ((promo_summary_map.get(suid) or {}).get(sku) or {}) if suid else {}
                promo_offers = list(((promo_offer_map.get(suid) or {}).get(sku) or [])) if suid else []
                valid_promo_thresholds = [
                    _to_num(offer.get("promo_price"))
                    for offer in promo_offers
                    if isinstance(offer, dict) and str(offer.get("promo_fit_mode") or "").strip().lower() in {"with_ads", "without_ads"}
                ]
                promo_threshold = min(float(value) for value in valid_promo_thresholds if value is not None) if valid_promo_thresholds else _to_num(promo_summary.get("promo_selected_price"))
                prof_val = _to_num(metric.get("attractiveness_profitable_price"))
                mod_val = _to_num(metric.get("attractiveness_moderate_price"))
                rrc_num = _to_num(rrc_val)
                mrc_num = _to_num(mrc_val)
                total_abs: float | None = None
                total_pct: float | None = None
                calc_ctx = (pm_for_store or {}).get("calc_ctx") if isinstance(pm_for_store, dict) and isinstance((pm_for_store or {}).get("calc_ctx"), dict) else None
                metric["attractiveness_chosen_boost_bid_percent"] = 0.0

                if promo_threshold is not None and mrc_num is not None and float(promo_threshold) >= float(mrc_num):
                    promo_candidates: list[float] = [float(promo_threshold)]
                    if mod_val is not None and float(mrc_num) <= float(mod_val) <= float(promo_threshold):
                        promo_candidates.append(float(mod_val))
                    if prof_val is not None and float(mrc_num) <= float(prof_val) <= float(promo_threshold):
                        promo_candidates.append(float(prof_val))
                    chosen = min(promo_candidates)
                elif mrc_num is not None:
                    chosen = mrc_num
                else:
                    chosen = rrc_num

                metric["attractiveness_chosen_price"] = chosen
                if chosen is not None:
                    selected_promo_price = _to_num(promo_summary.get("promo_selected_price")) if isinstance(promo_summary, dict) else None
                    if (
                        total_abs is None
                        and total_pct is None
                        and selected_promo_price is not None
                        and abs(float(chosen) - float(selected_promo_price)) < 0.5
                    ):
                        total_abs = _to_num(promo_summary.get("promo_selected_profit_abs")) if isinstance(promo_summary, dict) else None
                        total_pct = _to_num(promo_summary.get("promo_selected_profit_pct")) if isinstance(promo_summary, dict) else None
                    if total_abs is None and total_pct is None:
                        matching_offer = next(
                            (
                                offer
                                for offer in promo_offers
                                if _to_num(offer.get("promo_price")) is not None
                                and abs(float(_to_num(offer.get("promo_price")) or 0.0) - float(chosen)) < 0.5
                            ),
                            None,
                        )
                        if isinstance(matching_offer, dict):
                            total_abs = _to_num(matching_offer.get("promo_profit_abs"))
                            total_pct = _to_num(matching_offer.get("promo_profit_pct"))
                    if total_abs is None and total_pct is None and mrc_num is not None and abs(float(chosen) - float(mrc_num)) < 0.5:
                        total_abs = _to_num((pm_for_store or {}).get("mrc_profit_abs")) if isinstance(pm_for_store, dict) else None
                        total_pct = _to_num((pm_for_store or {}).get("mrc_profit_pct")) if isinstance(pm_for_store, dict) else None
                    elif total_abs is None and total_pct is None:
                        total_abs, total_pct = _calc_profit_for_price_from_ctx(row, suid, chosen)
                metric["attractiveness_chosen_profit_abs"] = total_abs
                metric["attractiveness_chosen_profit_pct"] = total_pct
                chosen_boost_bid_map[suid] = metric.get("attractiveness_chosen_boost_bid_percent")
            attr_by_store[suid] = metric
            pm = (row.get("price_metrics_by_store") or {}).get(suid) if isinstance(row.get("price_metrics_by_store"), dict) else {}
            if fetch_live:
                materialized_rows.append(
                    {
                        "store_uid": suid,
                        "sku": sku,
                        "attractiveness_overpriced_price": metric.get("attractiveness_overpriced_price"),
                        "attractiveness_moderate_price": metric.get("attractiveness_moderate_price"),
                        "attractiveness_profitable_price": metric.get("attractiveness_profitable_price"),
                        "ozon_competitor_price": metric.get("ozon_competitor_price"),
                        "external_competitor_price": metric.get("external_competitor_price"),
                        "attractiveness_chosen_price": metric.get("attractiveness_chosen_price"),
                        "attractiveness_chosen_boost_bid_percent": metric.get("attractiveness_chosen_boost_bid_percent"),
                        "source_updated_at": str(row.get("updated_at") or "").strip() or None,
                    }
                )
        row["attractiveness_by_store"] = attr_by_store
        row["chosen_boost_bid_by_store"] = chosen_boost_bid_map

        def _status_from_metric(m: dict, platform_code: str) -> str:
            try:
                price_fields = (
                    "attractiveness_chosen_price",
                    "attractiveness_overpriced_price",
                    "attractiveness_moderate_price",
                    "attractiveness_profitable_price",
                    "ozon_competitor_price",
                    "external_competitor_price",
                )
                chosen = _to_num(m.get("attractiveness_chosen_price"))
                prof = _to_num(m.get("attractiveness_profitable_price"))
                mod = _to_num(m.get("attractiveness_moderate_price"))
                has_any_price_data = any(_to_num(m.get(field)) is not None for field in price_fields)
                if not has_any_price_data:
                    return "profitable"
                if chosen is None:
                    return "profitable"
                if platform_code == "yandex_market":
                    if prof is not None and chosen <= prof:
                        return "profitable"
                    if mod is not None:
                        return "moderate" if chosen <= mod else "overpriced"
                    return "moderate"
                if prof is not None and chosen <= prof:
                    return "profitable"
                if mod is not None and chosen <= mod:
                    return "moderate"
                return "overpriced"
            except Exception:
                return "profitable"

        status_by_store: dict[str, str] = {}
        for suid, m in attr_by_store.items():
            platform_code = str(suid).split(":", 1)[0].strip().lower()
            status_by_store[suid] = _status_from_metric(m if isinstance(m, dict) else {}, platform_code)
        row["status_by_store"] = status_by_store
        rows_with_status.append(row)

    if fetch_live:
        try:
            if materialized_rows:
                upsert_pricing_attractiveness_results_bulk(rows=materialized_rows)
        except Exception:
            pass

    filtered_rows = rows_with_status
    if status_filter_norm != "all":
        scope_norm = str(scope or "all").strip().lower()
        platform_norm = str(platform or "").strip().lower()
        store_norm = str(store_id or "").strip()
        if scope_norm == "store":
            target_uid = f"{platform_norm}:{store_norm}"
            filtered_rows = [
                r for r in rows_with_status
                if isinstance(r.get("status_by_store"), dict) and r["status_by_store"].get(target_uid) == status_filter_norm
            ]
        else:
            filtered_rows = [
                r for r in rows_with_status
                if isinstance(r.get("status_by_store"), dict) and any(v == status_filter_norm for v in r["status_by_store"].values())
            ]

    if stock_filter_norm in {"in_stock", "out_of_stock"}:
        scope_norm = str(scope or "all").strip().lower()
        platform_norm = str(platform or "").strip().lower()
        store_norm = str(store_id or "").strip()
        current_store_uid = f"{platform_norm}:{store_norm}" if scope_norm == "store" and platform_norm and store_norm else ""

        def _has_stock(row_obj: dict) -> bool:
            stock_map = row_obj.get("stock_by_store") if isinstance(row_obj.get("stock_by_store"), dict) else {}
            values = [stock_map.get(current_store_uid)] if current_store_uid else list(stock_map.values())
            return any((_to_num(v) or 0) > 0 for v in values)

        filtered_rows = [
            r for r in filtered_rows
            if (_has_stock(r) if stock_filter_norm == "in_stock" else not _has_stock(r))
        ]

    page_size_n = max(1, min(int(page_size or 50), 200))
    page_n = max(1, int(page or 1))
    if status_filter_norm == "all" and stock_filter_norm == "all" and isinstance(base, dict):
        total_filtered = int(base.get("total_count") or len(filtered_rows))
        paged = filtered_rows
    else:
        total_filtered = len(filtered_rows)
        start = (page_n - 1) * page_size_n
        paged = filtered_rows[start:start + page_size_n]

    resp = {
        **(base if isinstance(base, dict) else {}),
        "rows": paged,
        "total_count": total_filtered,
        "page": page_n,
        "page_size": page_size_n,
        "status_filter": status_filter_norm,
        "stock_filter": stock_filter_norm,
        "debug_yandex_recommendations": ym_debug_by_store_uid,
    }
    if not fetch_live:
        _cache_set("overview", cache_payload, resp)
    return resp


async def refresh_attractiveness_data(*, refresh_base: bool = True, store_uids: list[str] | None = None):
    out: dict[str, Any] = {}
    if refresh_base:
        out = await refresh_prices_data()
    invalidate_attractiveness_cache()
    ctx = await get_prices_context()
    stores = list(ctx.get("marketplace_stores") or [])
    stores = [s for s in stores if str(s.get("platform") or "").strip().lower() == "yandex_market" and str(s.get("store_id") or "").strip()]
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [s for s in stores if str(s.get("store_uid") or "").strip() in selected]
    async def _run_store(store: dict[str, Any]) -> dict[str, Any]:
        store_id = str(store.get("store_id") or "").strip()
        store_uid = str(store.get("store_uid") or "").strip()
        page = 1
        page_size = 200
        try:
            local_total = 0
            local_processed = 0
            while True:
                resp = await get_attractiveness_overview(
                    scope="store",
                    platform="yandex_market",
                    store_id=store_id,
                    tree_mode="marketplaces",
                    tree_source_store_id=store_id,
                    page=page,
                    page_size=page_size,
                    fetch_live=True,
                )
                local_total = max(local_total, int(resp.get("total_count") or 0))
                rows = resp.get("rows") if isinstance(resp.get("rows"), list) else []
                debug_live = resp.get("debug_yandex_recommendations") if isinstance(resp, dict) else {}
                store_debug = debug_live.get(store_uid) if isinstance(debug_live, dict) else []
                if _has_terminal_yandex_403(store_debug):
                    return {
                        "ok": False,
                        "store_uid": store_uid,
                        "store_id": store_id,
                        "reason": "yandex_recommendations_403_forbidden",
                        "total": local_total,
                        "processed": local_processed,
                    }
                local_processed += len(rows)
                if not rows or len(rows) < page_size:
                    break
                page += 1
                if page > 500:
                    break
            return {"ok": True, "store_uid": store_uid, "store_id": store_id, "total": local_total, "processed": local_processed}
        except Exception as exc:
            return {"ok": False, "store_uid": store_uid, "store_id": store_id, "reason": str(exc)}

    settled = await asyncio.gather(*[_run_store(store) for store in stores])
    stores_success: list[dict[str, str]] = [
        {"store_uid": str(item.get("store_uid") or "").strip(), "store_id": str(item.get("store_id") or "").strip()}
        for item in settled
        if item.get("ok")
    ]
    skipped_rows: list[dict[str, str]] = [
        {"store_uid": str(item.get("store_uid") or "").strip(), "store_id": str(item.get("store_id") or "").strip(), "reason": str(item.get("reason") or "").strip()}
        for item in settled
        if not item.get("ok")
    ]
    total = sum(int(item.get("total") or 0) for item in settled if item.get("ok"))
    processed = sum(int(item.get("processed") or 0) for item in settled if item.get("ok"))
    invalidate_attractiveness_cache()
    return {
        **(out if isinstance(out, dict) else {}),
        "attractiveness_materialized": processed,
        "attractiveness_total": total,
        "stores": stores_success,
        "stores_skipped": skipped_rows,
        "stores_total": len(stores),
        "stores_updated": len(stores_success),
    }
