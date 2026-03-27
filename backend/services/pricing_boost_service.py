from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from backend.routers._shared import YANDEX_BASE_URL, _find_yandex_shop_credentials, _ym_headers
from backend.services.pricing_prices_service import (
    _profit_for_price,
    get_prices_context,
    get_prices_overview,
    get_prices_tree,
    refresh_prices_data,
)
from backend.services.store_data_model import (
    clear_pricing_boost_results_for_store,
    get_pricing_attractiveness_results_map,
    get_pricing_boost_results_map,
    get_pricing_promo_offer_results_map,
    get_pricing_strategy_results_map,
    get_sales_overview_order_rows,
    upsert_pricing_boost_results_bulk,
)

logger = logging.getLogger("uvicorn.error")
MSK = ZoneInfo("Europe/Moscow")

_BOOST_CACHE: dict[str, dict] = {}
_BOOST_CACHE_GEN = 1
_BOOST_CACHE_MAX = 400


def _cache_key(name: str, payload: dict) -> str:
    raw = json.dumps({"name": name, "gen": _BOOST_CACHE_GEN, "payload": payload}, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_get(name: str, payload: dict):
    key = _cache_key(name, payload)
    got = _BOOST_CACHE.get(key)
    return copy.deepcopy(got) if isinstance(got, dict) else None


def _cache_set(name: str, payload: dict, value: dict):
    key = _cache_key(name, payload)
    if len(_BOOST_CACHE) >= _BOOST_CACHE_MAX:
        _BOOST_CACHE.clear()
    _BOOST_CACHE[key] = copy.deepcopy(value)


def invalidate_boost_cache():
    global _BOOST_CACHE_GEN
    _BOOST_CACHE.clear()
    _BOOST_CACHE_GEN += 1


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


def _extract_recommended_bid(item: dict) -> float | None:
    offer_obj = item.get("offer") if isinstance(item.get("offer"), dict) else {}
    rec_obj = item.get("recommendation") if isinstance(item.get("recommendation"), dict) else {}

    candidates = [
        item.get("bid"),
        item.get("recommendedBid"),
        item.get("recommended_bid"),
        item.get("value"),
        rec_obj.get("bid") if isinstance(rec_obj, dict) else None,
        rec_obj.get("recommendedBid") if isinstance(rec_obj, dict) else None,
        rec_obj.get("recommended_bid") if isinstance(rec_obj, dict) else None,
        rec_obj.get("value") if isinstance(rec_obj, dict) else None,
        offer_obj.get("bid") if isinstance(offer_obj, dict) else None,
    ]
    for candidate in candidates:
        num = _to_num(candidate)
        if num is not None:
            return num / 100.0
    return None


def _extract_bid_by_show_percent(item: dict) -> dict[str, float | None]:
    bid_recommendations = item.get("bidRecommendations")
    if not isinstance(bid_recommendations, list):
        return {"30": None, "60": None, "80": None, "95": None}
    out: dict[str, float | None] = {"30": None, "60": None, "80": None, "95": None}
    for rec in bid_recommendations:
        if not isinstance(rec, dict):
            continue
        show_percent = _to_num(rec.get("showPercent"))
        bid = _to_num(rec.get("bid"))
        if show_percent is None or bid is None:
            continue
        key = str(int(show_percent))
        if key in out:
            out[key] = bid / 100.0
    return out


def _extract_yandex_bid_recommendations(payload: dict) -> dict[str, dict[str, Any]]:
    root = payload if isinstance(payload, dict) else {}
    result = root.get("result") if isinstance(root.get("result"), dict) else root
    candidates: list[dict] = []
    for key in ("recommendations", "offers", "items", "offerRecommendations", "bidRecommendations"):
        arr = result.get(key) if isinstance(result, dict) else None
        if isinstance(arr, list):
            candidates.extend([x for x in arr if isinstance(x, dict)])
    out: dict[str, dict[str, Any]] = {}
    for item in candidates:
        offer_obj = item.get("offer") if isinstance(item.get("offer"), dict) else {}
        sku = str(
            item.get("sku")
            or item.get("offerId")
            or item.get("shopSku")
            or item.get("offer_id")
            or item.get("shop_sku")
            or offer_obj.get("shopSku")
            or offer_obj.get("offerId")
            or offer_obj.get("offer_id")
            or offer_obj.get("shop_sku")
            or ""
        ).strip()
        if not sku:
            continue
        bid_by_show = _extract_bid_by_show_percent(item)
        out[sku] = {
            "recommended_bid": _extract_recommended_bid(item),
            "bid_30": bid_by_show["30"],
            "bid_60": bid_by_show["60"],
            "bid_80": bid_by_show["80"],
            "bid_95": bid_by_show["95"],
            "payload": item,
        }
    return out


def _clamp_rate(value: float) -> float:
    return max(0.0, min(1.0, float(value or 0.0)))


def _today_msk_str() -> str:
    return datetime.now(MSK).date().isoformat()


def _status_kind(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"cancelled", "canceled", "отменен", "отменён"}:
        return "ignore"
    if normalized in {"returned", "return", "возвращен", "возвращён"}:
        return "return"
    if normalized in {"delivered", "delivered_to_customer", "доставлен", "получен"}:
        return "delivered"
    if normalized:
        return "open"
    return "open"


async def _load_order_rows_for_store_day(*, store_uid: str, report_date: str) -> list[dict[str, Any]]:
    page = 1
    page_size = 1000
    rows: list[dict[str, Any]] = []
    while True:
        snapshot = get_sales_overview_order_rows(
            store_uid=store_uid,
            date_from=report_date,
            date_to=report_date,
            page=page,
            page_size=page_size,
        )
        chunk = snapshot.get("rows") if isinstance(snapshot, dict) and isinstance(snapshot.get("rows"), list) else []
        if not chunk:
            break
        rows.extend(chunk)
        total_count = int(snapshot.get("total_count") or len(rows)) if isinstance(snapshot, dict) else len(rows)
        if len(rows) >= total_count:
            break
        page += 1
        if page > 100:
            break
    return rows


def _profit_for_price_with_ads_rate(
    *,
    price: float | None,
    calc_ctx: dict[str, Any] | None,
    ads_rate_override: float,
) -> tuple[float | None, float | None]:
    if price is None or not isinstance(calc_ctx, dict):
        return None, None
    dep_rate = float(calc_ctx.get("dep_rate") or 0.0)
    ads_rate = float(calc_ctx.get("ads_rate") or 0.0)
    # For boost scenarios we replace the configured ads burden with the recommended boost rate.
    effective_dep_rate = _clamp_rate(max(0.0, dep_rate - ads_rate) + float(ads_rate_override or 0.0))
    pa, pp = _profit_for_price(
        price=float(price),
        dep_rate=effective_dep_rate,
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


def _calc_target_price_with_ads_rate(
    *,
    calc_ctx: dict[str, Any] | None,
    target_profit_pct: float | None,
    target_profit_abs: float | None,
    market_price: float | None,
    ads_rate_override: float,
) -> tuple[float | None, float | None, float | None]:
    if not isinstance(calc_ctx, dict):
        return None, None, None
    target_pct = _to_num(target_profit_pct)
    target_abs = _to_num(target_profit_abs)
    if (target_pct is None or target_pct <= 0) and (target_abs is None or target_abs <= 0):
        return None, None, None

    effective_target_pct = (float(target_pct) / 100.0) if target_pct is not None and target_pct > 0 else None

    def _ok(price_val: float) -> bool:
        pa, pp = _profit_for_price_with_ads_rate(price=price_val, calc_ctx=calc_ctx, ads_rate_override=ads_rate_override)
        if pa is None or pp is None:
            return False
        if effective_target_pct is not None:
            return (pp / 100.0) >= effective_target_pct
        return float(pa) >= float(target_abs or 0.0)

    lo = 1.0
    hi = max(float(market_price or 0.0), 1000.0)
    for _ in range(40):
        if _ok(hi):
            break
        hi *= 2.0
        if hi > 1e9:
            break
    if not _ok(hi):
        return None, None, None
    for _ in range(60):
        mid = (lo + hi) / 2.0
        if _ok(mid):
            hi = mid
        else:
            lo = mid
    target_price = float(int(round(hi)))
    pa, pp = _profit_for_price_with_ads_rate(price=target_price, calc_ctx=calc_ctx, ads_rate_override=ads_rate_override)
    return target_price, pa, pp


def _target_met(store_metric: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if not isinstance(store_metric, dict):
        return None, None
    return _to_num(store_metric.get("target_profit_pct")), _to_num(store_metric.get("target_profit_abs"))


async def _fetch_yandex_bids_recommendations_map(
    *,
    business_id: str,
    campaign_id: str,
    api_key: str,
    offer_ids: list[str],
) -> dict[str, dict[str, Any]]:
    bid = str(business_id or "").strip()
    if not bid or not offer_ids:
        return {}

    unique_offer_ids: list[str] = []
    seen: set[str] = set()
    for sku in offer_ids:
        val = str(sku or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        unique_offer_ids.append(val)

    url = f"{YANDEX_BASE_URL}/businesses/{bid}/bids/recommendations"
    out: dict[str, dict[str, Any]] = {}
    async with httpx.AsyncClient(timeout=45) as client:
        for i in range(0, len(unique_offer_ids), 1500):
            chunk = unique_offer_ids[i:i + 1500]
            logger.warning(
                "[pricing_boost] Yandex bids request business_id=%s campaign_id=%s chunk=%s url=%s",
                bid,
                campaign_id,
                len(chunk),
                url,
            )
            resp = await client.post(
                url,
                headers=_ym_headers(api_key),
                json={"skus": chunk},
            )
            logger.warning(
                "[pricing_boost] Yandex bids response business_id=%s campaign_id=%s status=%s chunk=%s",
                bid,
                campaign_id,
                resp.status_code,
                len(chunk),
            )
            if resp.status_code >= 400:
                logger.warning(
                    "[pricing_boost] Yandex bids error body business_id=%s campaign_id=%s body=%s",
                    bid,
                    campaign_id,
                    resp.text[:2000],
                )
            resp.raise_for_status()
            payload = resp.json()
            parsed = _extract_yandex_bid_recommendations(payload)
            root = payload if isinstance(payload, dict) else {}
            result = root.get("result") if isinstance(root.get("result"), dict) else root
            first_item = None
            if isinstance(result, dict):
                for key in ("recommendations", "offers", "items", "offerRecommendations", "bidRecommendations"):
                    arr = result.get(key)
                    if isinstance(arr, list) and arr:
                        first_item = arr[0]
                        break
            logger.warning(
                "[pricing_boost] Yandex bids parsed business_id=%s campaign_id=%s parsed=%s result_keys=%s first_item_keys=%s first_item_preview=%s",
                bid,
                campaign_id,
                len(parsed),
                list(result.keys())[:20] if isinstance(result, dict) else [],
                list(first_item.keys())[:20] if isinstance(first_item, dict) else [],
                first_item if isinstance(first_item, dict) else None,
            )
            out.update(parsed)
    return out


async def get_boost_context():
    return await get_prices_context()


async def get_boost_tree(
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


async def get_boost_overview(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    stock_filter: str = "all",
    report_date: str = "",
    page: int = 1,
    page_size: int = 50,
):
    report_date_value = str(report_date or "").strip() or _today_msk_str()
    cache_payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "stock_filter": stock_filter,
        "report_date": report_date_value,
        "page": page,
        "page_size": page_size,
    }
    cached = _cache_get("overview", cache_payload)
    if cached:
        return cached

    stock_filter_norm = str(stock_filter or "all").strip().lower()
    if stock_filter_norm == "all":
        base = await get_prices_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_mode=tree_mode,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter,
            page=page,
            page_size=page_size,
        )
        rows = base.get("rows") if isinstance(base, dict) and isinstance(base.get("rows"), list) else []
    else:
        base = await get_prices_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_mode=tree_mode,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter,
            page=1,
            page_size=200,
        )
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
                stock_filter=stock_filter,
                page=page_i,
                page_size=200,
            )
            chunk = nxt.get("rows") if isinstance(nxt, dict) and isinstance(nxt.get("rows"), list) else []
            if not chunk:
                break
            rows_all.extend(chunk)
            loaded += len(chunk)
            if page_i > 200:
                break
        rows = rows_all
    stores = base.get("stores") if isinstance(base, dict) and isinstance(base.get("stores"), list) else []
    if not rows or not stores:
        resp = {**(base if isinstance(base, dict) else {}), "rows": rows, "report_date": report_date_value}
        _cache_set("overview", cache_payload, resp)
        return resp

    store_uids = [str(s.get("store_uid") or "").strip() for s in stores if str(s.get("store_uid") or "").strip()]
    skus = [str(r.get("sku") or "").strip() for r in rows if str(r.get("sku") or "").strip()]
    strategy_map = get_pricing_strategy_results_map(store_uids=store_uids, skus=skus)
    orders_by_store: dict[str, dict[str, dict[str, Any]]] = {}
    for suid in store_uids:
        order_rows = await _load_order_rows_for_store_day(store_uid=suid, report_date=report_date_value)
        sku_aggs: dict[str, dict[str, Any]] = {}
        for order_row in order_rows:
            sku = str(order_row.get("sku") or "").strip()
            if not sku:
                continue
            if sku not in skus:
                continue
            status_kind = _status_kind(str(order_row.get("item_status") or ""))
            if status_kind == "ignore":
                continue
            revenue = float(_to_num(order_row.get("sale_price")) or 0.0)
            profit = float(_to_num(order_row.get("profit")) or 0.0)
            ads = float(_to_num(order_row.get("ads")) or 0.0)
            boosted = float(_to_num(order_row.get("strategy_market_boost_bid_percent")) or 0.0) > 0.01
            bucket = sku_aggs.setdefault(
                sku,
                {
                    "orders_count": 0,
                    "revenue": 0.0,
                    "profit": 0.0,
                    "ads": 0.0,
                    "boosted_orders_count": 0,
                    "boosted_revenue": 0.0,
                    "boosted_ads": 0.0,
                    "last_order_at": "",
                },
            )
            bucket["orders_count"] += 1
            bucket["revenue"] = round(float(bucket["revenue"]) + revenue, 2)
            bucket["profit"] = round(float(bucket["profit"]) + profit, 2)
            bucket["ads"] = round(float(bucket["ads"]) + ads, 2)
            order_created_at = str(order_row.get("order_created_at") or "").strip()
            if order_created_at and order_created_at > str(bucket["last_order_at"] or ""):
                bucket["last_order_at"] = order_created_at
            if boosted:
                bucket["boosted_orders_count"] += 1
                bucket["boosted_revenue"] = round(float(bucket["boosted_revenue"]) + revenue, 2)
                bucket["boosted_ads"] = round(float(bucket["boosted_ads"]) + ads, 2)
        orders_by_store[suid] = sku_aggs

    for row in rows:
        sku = str(row.get("sku") or "").strip()
        selected_decision_by_store: dict[str, str] = {}
        coinvest_pct_by_store: dict[str, float | None] = {}
        mrc_price_by_store: dict[str, float | None] = dict(row.get("mrc_price_by_store") or {})
        mrc_with_boost_price_by_store: dict[str, float | None] = dict(row.get("mrc_with_boost_price_by_store") or {})
        rrc_price_by_store: dict[str, float | None] = dict(row.get("target_price_by_store") or {})
        on_display_price_by_store: dict[str, float | None] = {}
        selected_price_by_store: dict[str, float | None] = {}
        internal_boost_by_store: dict[str, float | None] = {}
        market_boost_by_store: dict[str, float | None] = {}
        expected_boost_share_by_store: dict[str, float | None] = {}
        orders_count_by_store: dict[str, int] = {}
        revenue_by_store: dict[str, float | None] = {}
        profit_by_store: dict[str, float | None] = {}
        ads_by_store: dict[str, float | None] = {}
        boosted_orders_count_by_store: dict[str, int] = {}
        boosted_revenue_by_store: dict[str, float | None] = {}
        boosted_ads_by_store: dict[str, float | None] = {}
        boost_revenue_share_by_store: dict[str, float | None] = {}
        boost_orders_share_by_store: dict[str, float | None] = {}
        last_order_at_by_store: dict[str, str] = {}
        for suid in store_uids:
            strategy = (strategy_map.get(suid) or {}).get(sku) or {}
            agg = (orders_by_store.get(suid) or {}).get(sku) or {}
            revenue = _to_num(agg.get("revenue"))
            boosted_revenue = _to_num(agg.get("boosted_revenue"))
            orders_count = int(agg.get("orders_count") or 0)
            boosted_orders_count = int(agg.get("boosted_orders_count") or 0)
            selected_decision_by_store[suid] = str(strategy.get("decision_label") or "").strip()
            coinvest_pct_by_store[suid] = _to_num(strategy.get("coinvest_pct"))
            on_display_price_by_store[suid] = _to_num(strategy.get("on_display_price"))
            selected_price_by_store[suid] = _to_num(strategy.get("installed_price"))
            internal_boost_by_store[suid] = _to_num(strategy.get("boost_bid_percent"))
            market_boost_by_store[suid] = _to_num(strategy.get("market_boost_bid_percent"))
            expected_boost_share_by_store[suid] = _to_num(strategy.get("boost_share"))
            orders_count_by_store[suid] = orders_count
            revenue_by_store[suid] = revenue
            profit_by_store[suid] = _to_num(agg.get("profit"))
            ads_by_store[suid] = _to_num(agg.get("ads"))
            boosted_orders_count_by_store[suid] = boosted_orders_count
            boosted_revenue_by_store[suid] = boosted_revenue
            boosted_ads_by_store[suid] = _to_num(agg.get("boosted_ads"))
            boost_revenue_share_by_store[suid] = round((float(boosted_revenue or 0.0) / float(revenue)) * 100.0, 2) if revenue not in (None, 0) else None
            boost_orders_share_by_store[suid] = round((float(boosted_orders_count) / float(orders_count)) * 100.0, 2) if orders_count > 0 else None
            last_order_at_by_store[suid] = str(agg.get("last_order_at") or "").strip()
        row["selected_price_by_store"] = selected_price_by_store
        row["selected_decision_by_store"] = selected_decision_by_store
        row["coinvest_pct_by_store"] = coinvest_pct_by_store
        row["mrc_price_by_store"] = mrc_price_by_store
        row["mrc_with_boost_price_by_store"] = mrc_with_boost_price_by_store
        row["rrc_price_by_store"] = rrc_price_by_store
        row["on_display_price_by_store"] = on_display_price_by_store
        row["internal_boost_by_store"] = internal_boost_by_store
        row["market_boost_by_store"] = market_boost_by_store
        row["expected_boost_share_by_store"] = expected_boost_share_by_store
        row["orders_count_by_store"] = orders_count_by_store
        row["revenue_by_store"] = revenue_by_store
        row["profit_by_store"] = profit_by_store
        row["ads_by_store"] = ads_by_store
        row["boosted_orders_count_by_store"] = boosted_orders_count_by_store
        row["boosted_revenue_by_store"] = boosted_revenue_by_store
        row["boosted_ads_by_store"] = boosted_ads_by_store
        row["boost_revenue_share_by_store"] = boost_revenue_share_by_store
        row["boost_orders_share_by_store"] = boost_orders_share_by_store
        row["last_order_at_by_store"] = last_order_at_by_store

    page_size_n = max(1, min(int(page_size or 50), 200))
    page_n = max(1, int(page or 1))
    total_count = len(rows) if stock_filter_norm != "all" else int(base.get("total_count") or len(rows))
    paged_rows = rows if stock_filter_norm == "all" else rows[(page_n - 1) * page_size_n:(page_n - 1) * page_size_n + page_size_n]

    resp = {**base, "rows": paged_rows, "total_count": total_count, "page": page_n, "page_size": page_size_n, "report_date": report_date_value}
    _cache_set("overview", cache_payload, resp)
    return resp


async def refresh_boost_data(*, refresh_base: bool = True, store_uids: list[str] | None = None):
    logger.warning("[pricing_boost] refresh started")
    out: dict[str, Any] = {}
    if refresh_base:
        out = await refresh_prices_data()
    ctx = await get_prices_context()
    stores = ctx.get("marketplace_stores") if isinstance(ctx, dict) and isinstance(ctx.get("marketplace_stores"), list) else []
    yandex_stores = [s for s in stores if str(s.get("platform") or "").strip().lower() == "yandex_market"]
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        yandex_stores = [s for s in yandex_stores if str(s.get("store_uid") or "").strip() in selected]
    logger.warning(
        "[pricing_boost] refresh context stores_total=%s yandex_stores=%s",
        len(stores),
        len(yandex_stores),
    )

    async def _run_store(store: dict[str, Any]) -> dict[str, Any]:
        suid = str(store.get("store_uid") or "").strip()
        sid = str(store.get("store_id") or "").strip()
        if not suid or not sid:
            logger.warning("[pricing_boost] skip store with empty ids store_uid=%s store_id=%s", suid, sid)
            return {"ok": False, "store_uid": suid, "store_id": sid, "reason": "invalid_store"}
        creds = _find_yandex_shop_credentials(sid)
        if not creds:
            logger.warning("[pricing_boost] skip store_uid=%s store_id=%s reason=credentials_not_found", suid, sid)
            return {"ok": False, "store_uid": suid, "store_id": sid, "reason": "credentials_not_found"}
        business_id, campaign_id, api_key = creds
        try:
            all_rows: list[dict[str, Any]] = []
            page_n = 1
            while True:
                resp = await get_prices_overview(
                    scope="store",
                    platform="yandex_market",
                    store_id=sid,
                    tree_mode="marketplaces",
                    tree_source_store_id=sid,
                    page=page_n,
                    page_size=500,
                    force_refresh=False,
                )
                chunk = resp.get("rows") if isinstance(resp, dict) and isinstance(resp.get("rows"), list) else []
                if not chunk:
                    break
                all_rows.extend(chunk)
                total_count = int(resp.get("total_count") or len(all_rows))
                if len(all_rows) >= total_count:
                    break
                page_n += 1
                if page_n > 200:
                    break

            offer_ids = [str(r.get("sku") or "").strip() for r in all_rows if str(r.get("sku") or "").strip()]
            rec_map = await _fetch_yandex_bids_recommendations_map(
                business_id=business_id,
                campaign_id=campaign_id,
                api_key=api_key,
                offer_ids=offer_ids,
            )
            prepared: list[dict[str, Any]] = []
            for row in all_rows:
                sku = str(row.get("sku") or "").strip()
                if not sku:
                    continue
                rec = rec_map.get(sku) or {}
                prepared.append(
                    {
                        "store_uid": suid,
                        "sku": sku,
                        "recommended_bid": rec.get("recommended_bid"),
                        "bid_30": rec.get("bid_30"),
                        "bid_60": rec.get("bid_60"),
                        "bid_80": rec.get("bid_80"),
                        "bid_95": rec.get("bid_95"),
                        "source_updated_at": str(row.get("updated_at") or "").strip() or None,
                    }
                )
            clear_pricing_boost_results_for_store(store_uid=suid)
            if prepared:
                upsert_pricing_boost_results_bulk(rows=prepared)
            logger.warning(
                "[pricing_boost] store prepared store_uid=%s business_id=%s campaign_id=%s rows=%s",
                suid,
                business_id,
                campaign_id,
                len(prepared),
            )
            return {"ok": True, "store_uid": suid, "store_id": sid}
        except Exception as exc:
            logger.warning(
                "[pricing_boost] store skipped store_uid=%s business_id=%s campaign_id=%s error=%s",
                suid,
                business_id,
                campaign_id,
                exc,
            )
            return {"ok": False, "store_uid": suid, "store_id": sid, "business_id": business_id, "campaign_id": campaign_id, "reason": str(exc)}

    settled = await asyncio.gather(*[_run_store(store) for store in yandex_stores])
    stores_success: list[dict[str, str]] = [
        {"store_uid": str(item.get("store_uid") or "").strip(), "store_id": str(item.get("store_id") or "").strip()}
        for item in settled
        if item.get("ok")
    ]
    stores_skipped: list[dict[str, str]] = [
        {k: v for k, v in item.items() if k != "ok"}
        for item in settled
        if not item.get("ok")
    ]
    stores_updated = len(stores_success)

    invalidate_boost_cache()
    result = {
        "ok": True,
        "source_refresh": out,
        "stores_total": len(yandex_stores),
        "stores_updated": stores_updated,
        "stores_skipped": stores_skipped,
        "stores": stores_success,
    }
    logger.warning(
        "[pricing_boost] refresh finished stores_total=%s stores_updated=%s stores_skipped=%s",
        result["stores_total"],
        result["stores_updated"],
        len(stores_skipped),
    )
    return result
