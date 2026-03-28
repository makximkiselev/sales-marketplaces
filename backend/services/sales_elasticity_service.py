from __future__ import annotations

import copy
import datetime
import hashlib
import json
import logging
from typing import Any

import httpx

from backend.routers._shared import (
    _catalog_marketplace_stores_context,
    _catalog_path_from_row,
    _catalog_tree_from_paths,
    _find_yandex_shop_credentials,
    _read_source_rows,
    _ym_headers,
)
from backend.services.db import is_postgres_backend
from backend.services.store_data_model import (
    _connect,
    get_pricing_store_settings,
    get_fx_rates_cache,
    replace_sales_market_order_items_for_period,
)
from backend.services.db import load_json, save_json
from backend.services.pricing_prices_service import _load_stock_map_from_source

_ELASTICITY_CACHE: dict[str, dict] = {}
_ELASTICITY_CACHE_GEN = 1
_ELASTICITY_CACHE_MAX = 200
logger = logging.getLogger("uvicorn.error")

YANDEX_ELASTICITY_STATUSES = {"PROCESSING", "DELIVERY", "PICKUP", "DELIVERED"}
ELASTICITY_FULL_START = datetime.date(2025, 1, 1)
ELASTICITY_SYNC_STATE_KEY = "sales.elasticity.sync_state"
_FX_USD_RUB_MEM: dict[str, float] = {}


def _cache_key(name: str, payload: dict[str, Any]) -> str:
    raw = json.dumps({"name": name, "gen": _ELASTICITY_CACHE_GEN, "payload": payload}, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_get(name: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    key = _cache_key(name, payload)
    got = _ELASTICITY_CACHE.get(key)
    return copy.deepcopy(got) if isinstance(got, dict) else None


def _cache_set(name: str, payload: dict[str, Any], value: dict[str, Any]) -> None:
    key = _cache_key(name, payload)
    if len(_ELASTICITY_CACHE) >= _ELASTICITY_CACHE_MAX:
        _ELASTICITY_CACHE.clear()
    _ELASTICITY_CACHE[key] = copy.deepcopy(value)


def invalidate_sales_elasticity_cache() -> None:
    global _ELASTICITY_CACHE_GEN
    _ELASTICITY_CACHE.clear()
    _ELASTICITY_CACHE_GEN += 1


def _iter_month_ranges(date_from: datetime.date, date_to: datetime.date) -> list[tuple[datetime.date, datetime.date]]:
    current = date_from.replace(day=1)
    out: list[tuple[datetime.date, datetime.date]] = []
    while current <= date_to:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        chunk_from = max(date_from, current)
        chunk_to = min(date_to, next_month - datetime.timedelta(days=1))
        if chunk_from <= chunk_to:
            out.append((chunk_from, chunk_to))
        current = next_month
    return out


def _to_num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        pass
    raw = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    filtered = []
    dot_seen = False
    for ch in raw:
        if ch.isdigit() or (ch == "-" and not filtered):
            filtered.append(ch)
        elif ch == "." and not dot_seen:
            filtered.append(ch)
            dot_seen = True
    try:
        return float("".join(filtered))
    except Exception:
        return None


def _to_int(value: Any, default: int = 1) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except Exception:
        num = _to_num(value)
        if num is None:
            return default
        try:
            return int(round(num))
        except Exception:
            return default


def _store_currency_map(stores: list[dict[str, Any]]) -> dict[str, str]:
    return {
        str(store.get("store_uid") or "").strip(): str(store.get("currency_code") or "RUB").strip().upper() or "RUB"
        for store in stores
        if str(store.get("store_uid") or "").strip()
    }


def _get_cached_usd_rub_rate_for_date(calc_date: datetime.date) -> float | None:
    key = calc_date.isoformat()
    if key in _FX_USD_RUB_MEM:
        return _FX_USD_RUB_MEM[key]
    try:
        cached = get_fx_rates_cache(source="cbr", pair="USD_RUB")
        rows = cached.get("rows") if isinstance(cached, dict) else None
        if isinstance(rows, list) and rows:
            by_date: dict[str, float] = {}
            for row in rows:
                rate_date = str(row.get("date") or "").strip()
                try:
                    rate_value = float(row.get("rate"))
                except Exception:
                    continue
                if rate_date and rate_value > 0:
                    by_date[rate_date] = rate_value
            if by_date:
                best_date = max(by_date.keys())
                rate = float(by_date[best_date])
                if rate > 0:
                    _FX_USD_RUB_MEM[key] = rate
                    return rate
    except Exception:
        pass
    return None


def _normalize_money_to_rub(value: float | None, *, currency_code: str, order_date: str) -> float | None:
    if value is None:
        return None
    if str(currency_code or "RUB").strip().upper() != "USD":
        return float(value)
    try:
        calc_date = datetime.date.fromisoformat(str(order_date or "").strip())
    except Exception:
        return float(value)
    rate = _get_cached_usd_rub_rate_for_date(calc_date)
    if rate and rate > 0:
        return float(value) * float(rate)
    return float(value)


def _extract_response_orders(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    root = payload if isinstance(payload, dict) else {}
    result = root.get("result") if isinstance(root.get("result"), dict) else root
    orders = result.get("orders") if isinstance(result.get("orders"), list) else root.get("orders")
    if not isinstance(orders, list):
        orders = []
    paging = result.get("paging") if isinstance(result.get("paging"), dict) else {}
    next_page_token = str(
        paging.get("nextPageToken")
        or paging.get("next_page_token")
        or result.get("nextPageToken")
        or root.get("nextPageToken")
        or ""
    ).strip()
    return [x for x in orders if isinstance(x, dict)], next_page_token


def _parse_order_created_date(value: Any) -> tuple[str, str] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        # Для витрины и дневной аналитики нужен календарный день из ответа Маркета,
        # а не UTC-дата после пересчета, иначе ночные заказы "уезжают" во вчера.
        source_date = dt.date().isoformat()
        dt_utc = dt.astimezone(datetime.timezone.utc)
        return (
            dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            source_date,
        )
    except Exception:
        return None


def _extract_row_stock(row: dict[str, Any]) -> float | None:
    if not isinstance(row, dict):
        return None
    for key in ("stock", "qty", "quantity", "available"):
        value = _to_num(row.get(key))
        if value is not None:
            return float(value)
    return None


def _extract_payment_price(item: dict[str, Any]) -> float | None:
    prices = item.get("prices") if isinstance(item.get("prices"), dict) else {}
    payment = prices.get("payment") if isinstance(prices.get("payment"), dict) else {}
    for candidate in (
        payment.get("value"),
        item.get("buyerPrice"),
        item.get("price"),
    ):
        num = _to_num(candidate)
        if num is not None:
            return num
    return None


def _extract_subsidy_amount(item: dict[str, Any]) -> float | None:
    prices = item.get("prices") if isinstance(item.get("prices"), dict) else {}
    subsidy = prices.get("subsidy") if isinstance(prices.get("subsidy"), dict) else {}
    return _to_num(subsidy.get("value"))


def _extract_order_items(order: dict[str, Any]) -> list[dict[str, Any]]:
    order_id = str(order.get("id") or order.get("orderId") or "").strip()
    status = str(order.get("status") or "").strip().upper()
    created = _parse_order_created_date(order.get("creationDate") or order.get("createdAt") or order.get("created"))
    if not order_id or not created or status not in YANDEX_ELASTICITY_STATUSES:
        return []
    created_at, created_date = created
    items = order.get("items") if isinstance(order.get("items"), list) else []
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        sku = str(
            item.get("offerId")
            or item.get("shopSku")
            or item.get("sku")
            or item.get("offer_id")
            or ""
        ).strip()
        if not sku:
            continue
        raw_item_id = str(
            item.get("id")
            or item.get("itemId")
            or item.get("offerId")
            or f"{order_id}:{sku}:{idx + 1}"
        ).strip()
        item_id = f"{order_id}:{raw_item_id}" if raw_item_id else f"{order_id}:{sku}:{idx + 1}"
        count = _to_int(item.get("count") or item.get("quantity") or 1, default=1)
        payment_price = _extract_payment_price(item)
        subsidy_amount = _extract_subsidy_amount(item) or 0.0
        sale_price = (payment_price or 0.0) + subsidy_amount
        if payment_price is None and subsidy_amount <= 0:
            sale_price = None
        line_revenue = sale_price * count if sale_price is not None else None
        out.append(
            {
                "order_id": order_id,
                "order_item_id": item_id,
                "order_status": status,
                "order_created_at": created_at,
                "order_created_date": created_date,
                "sku": sku,
                "item_name": str(item.get("offerName") or item.get("name") or "").strip(),
                "sale_price": sale_price,
                "payment_price": payment_price,
                "subsidy_amount": subsidy_amount,
                "item_count": count,
                "line_revenue": line_revenue,
            }
        )
    return out


async def _fetch_yandex_business_orders_for_range(
    *,
    business_id: str,
    campaign_id: str,
    api_key: str,
    date_from: datetime.date,
    date_to: datetime.date,
) -> list[dict[str, Any]]:
    bid = str(business_id or "").strip()
    cid = str(campaign_id or "").strip()
    if not bid or not cid:
        return []
    url = f"https://api.partner.market.yandex.ru/v1/businesses/{bid}/orders"
    request_to = date_to + datetime.timedelta(days=1)
    out: list[dict[str, Any]] = []
    page_token = ""
    seen_page_tokens: set[str] = set()
    seen_page_signatures: set[str] = set()
    seen_order_item_ids: set[str] = set()
    async with httpx.AsyncClient(timeout=90) as client:
        for _ in range(500):
            body: dict[str, Any] = {
                "statuses": sorted(YANDEX_ELASTICITY_STATUSES),
                "campaignIds": [int(cid)] if cid.isdigit() else [cid],
                "dates": {
                    "creationDateFrom": date_from.isoformat(),
                    "creationDateTo": request_to.isoformat(),
                },
            }
            params: dict[str, str] = {}
            if page_token:
                params["pageToken"] = page_token
            logger.warning(
                "[sales_elasticity] Yandex orders request business_id=%s campaign_id=%s date_from=%s date_to=%s request_date_to=%s page_token=%s",
                bid,
                cid,
                date_from.isoformat(),
                date_to.isoformat(),
                request_to.isoformat(),
                page_token or "-",
            )
            resp = await client.post(url, headers=_ym_headers(api_key), params=params, json=body)
            logger.warning(
                "[sales_elasticity] Yandex orders response business_id=%s campaign_id=%s status=%s",
                bid,
                cid,
                resp.status_code,
            )
            if not resp.is_success:
                logger.warning(
                    "[sales_elasticity] Yandex orders error body business_id=%s campaign_id=%s body=%s",
                    bid,
                    cid,
                    resp.text,
                )
            resp.raise_for_status()
            orders, next_page_token = _extract_response_orders(resp.json())
            page_order_ids = [
                str(order.get("id") or order.get("orderId") or "").strip()
                for order in orders
                if isinstance(order, dict) and str(order.get("id") or order.get("orderId") or "").strip()
            ]
            page_signature = "|".join(page_order_ids)
            logger.warning(
                "[sales_elasticity] Yandex orders parsed business_id=%s campaign_id=%s page_orders=%s next_page_token=%s",
                bid,
                cid,
                len(orders),
                "set" if next_page_token else "-",
            )
            if page_signature and page_signature in seen_page_signatures:
                logger.warning(
                    "[sales_elasticity] Yandex orders pagination stopped by repeated page signature business_id=%s campaign_id=%s",
                    bid,
                    cid,
                )
                break
            if page_signature:
                seen_page_signatures.add(page_signature)
            for order in orders:
                for item in _extract_order_items(order):
                    item_key = str(item.get("order_item_id") or "").strip()
                    if not item_key or item_key in seen_order_item_ids:
                        continue
                    seen_order_item_ids.add(item_key)
                    out.append(item)
            if not next_page_token or next_page_token == page_token or next_page_token in seen_page_tokens:
                break
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
    return out


def _get_yandex_stores() -> list[dict[str, Any]]:
    stores = _catalog_marketplace_stores_context()
    return [store for store in stores if str(store.get("platform") or "").strip().lower() == "yandex_market" and store.get("table_name")]


async def get_sales_elasticity_context() -> dict[str, Any]:
    cached = _cache_get("context", {})
    if cached:
        return cached
    resp = {
        "ok": True,
        "marketplace_stores": _get_yandex_stores(),
        "sync_state": get_sales_elasticity_sync_state(),
    }
    _cache_set("context", {}, resp)
    return resp


async def get_sales_elasticity_tree(
    *,
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    stock_filter: str = "all",
) -> dict[str, Any]:
    cache_payload = {
        "tree_source_store_id": tree_source_store_id,
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "stock_filter": stock_filter,
    }
    cached = _cache_get("tree", cache_payload)
    if cached:
        return cached

    stores = _get_yandex_stores()
    scope_norm = str(scope or "all").strip().lower()
    platform_norm = str(platform or "").strip().lower()
    store_norm = str(store_id or "").strip()
    chosen = str(tree_source_store_id or "").strip()

    if scope_norm == "store":
        src_store = next((s for s in stores if s["platform"] == platform_norm and s["store_id"] == store_norm), None)
    else:
        src_store = next((s for s in stores if str(s.get("store_uid") or "").strip() == chosen), None)
        if not src_store:
            src_store = next((s for s in stores if str(s.get("store_id") or "").strip() == chosen), None)
    if not src_store:
        src_store = stores[0] if stores else None
    if not src_store:
        resp = {"ok": True, "tree_mode": "marketplaces", "roots": [{"name": "Не определено", "children": []}], "source": None}
        _cache_set("tree", cache_payload, resp)
        return resp

    rows = _read_source_rows(str(src_store.get("table_name") or ""))
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
    resp = {"ok": True, "tree_mode": "marketplaces", "roots": roots, "source": src_store}
    _cache_set("tree", cache_payload, resp)
    return resp


def _period_bounds(
    *,
    period: str,
    today: datetime.date,
    date_from: str = "",
    date_to: str = "",
) -> tuple[datetime.date, datetime.date, datetime.date, datetime.date]:
    period_norm = str(period or "today").strip().lower()
    if period_norm == "today":
        current_from = today
        current_to = today
    elif period_norm == "yesterday":
        current_from = today - datetime.timedelta(days=1)
        current_to = today - datetime.timedelta(days=1)
    elif period_norm == "week":
        current_to = today
        current_from = today - datetime.timedelta(days=6)
    elif period_norm == "month":
        current_to = today
        current_from = today - datetime.timedelta(days=29)
    elif period_norm == "quarter":
        current_to = today
        current_from = today - datetime.timedelta(days=89)
    else:
        try:
            current_from = datetime.date.fromisoformat(str(date_from or ""))
            current_to = datetime.date.fromisoformat(str(date_to or ""))
        except Exception as exc:
            raise ValueError("Для произвольного периода нужны корректные date_from/date_to") from exc
        if current_from > current_to:
            raise ValueError("date_from не может быть больше date_to")
    length = (current_to - current_from).days + 1
    prev_to = current_from - datetime.timedelta(days=1)
    prev_from = prev_to - datetime.timedelta(days=length - 1)
    return current_from, current_to, prev_from, prev_to


def _parse_iso_date(value: str) -> datetime.date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.date.fromisoformat(raw)
    except Exception:
        return None


def _resolve_comparison_bounds(
    *,
    current_from: datetime.date,
    current_to: datetime.date,
    prev_from: datetime.date,
    prev_to: datetime.date,
    data_min: datetime.date | None,
    data_max: datetime.date | None,
) -> tuple[datetime.date | None, datetime.date | None, bool]:
    if data_min is None or data_max is None:
        return None, None, False
    if current_from <= data_min and current_to >= data_max:
        return None, None, False
    if prev_to < data_min:
        return None, None, False
    compare_from = max(prev_from, data_min)
    compare_to = min(prev_to, data_max)
    if compare_from > compare_to:
        return None, None, False
    return compare_from, compare_to, True


def _refresh_bounds(mode: str, today: datetime.date) -> tuple[datetime.date, datetime.date, str]:
    mode_norm = str(mode or "today").strip().lower()
    if mode_norm == "full":
        return ELASTICITY_FULL_START, today, "full"
    if mode_norm == "year":
        return today.replace(month=1, day=1), today, "year"
    if mode_norm == "current_month":
        return today.replace(day=1), today, "current_month"
    if mode_norm == "week":
        return today - datetime.timedelta(days=6), today, "week"
    if mode_norm == "month":
        return today - datetime.timedelta(days=29), today, "month"
    return today, today, "today"


def _percent_delta(current: float, previous: float) -> float | None:
    if previous == 0:
        return None if current == 0 else 100.0
    return ((current - previous) / previous) * 100.0


def _arc_elasticity(*, current_qty: float, previous_qty: float, current_price: float | None, previous_price: float | None) -> float | None:
    if current_price is None or previous_price is None:
        return None
    price_avg = (current_price + previous_price) / 2.0
    qty_avg = (current_qty + previous_qty) / 2.0
    if price_avg == 0 or qty_avg == 0:
        return None
    price_change = (current_price - previous_price) / price_avg
    qty_change = (current_qty - previous_qty) / qty_avg
    if abs(price_change) < 1e-9:
        return None
    return qty_change / price_change


def _format_turnover(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _overview_status_kind(status: Any) -> str:
    norm = str(status or "").strip().lower()
    if not norm:
        return ""
    if "возврат" in norm or "отмен" in norm or "невыкуп" in norm:
        return "ignore"
    if norm == "доставлен покупателю":
        return "delivered"
    if norm in {"оформлен", "отгружен"}:
        return "open"
    return "other"


def _get_overview_order_rows(
    *,
    store_uids: list[str],
    skus: list[str] | None = None,
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in (skus or []) if str(x or "").strip()]
    from_date = str(date_from or "").strip()
    to_date = str(date_to or "").strip()
    if not suids or not from_date or not to_date:
        return []
    marker = "%s" if is_postgres_backend() else "?"
    placeholders_suid = ",".join([marker] * len(suids))
    params: list[Any] = [*suids, from_date, to_date]
    sql = f"""
        SELECT
            store_uid,
            order_id,
            order_created_date,
            order_created_at,
            item_status,
            sku,
            item_name,
            sale_price,
            sale_price_with_coinvest,
            profit
        FROM sales_overview_order_rows
        WHERE store_uid IN ({placeholders_suid})
          AND order_created_date >= {marker}
          AND order_created_date <= {marker}
    """
    if sku_list:
        placeholders_sku = ",".join([marker] * len(sku_list))
        sql += f" AND sku IN ({placeholders_sku})"
        params.extend(sku_list)
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def _get_overview_date_bounds(*, store_uids: list[str]) -> dict[str, str]:
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    if not suids:
        return {}
    marker = "%s" if is_postgres_backend() else "?"
    placeholders = ",".join([marker] * len(suids))
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT
                MIN(order_created_date) AS min_date,
                MAX(order_created_date) AS max_date
            FROM sales_overview_order_rows
            WHERE store_uid IN ({placeholders})
            """,
            suids,
        ).fetchone()
    if not row:
        return {}
    out: dict[str, str] = {}
    if str(row["min_date"] or "").strip():
        out["min_date"] = str(row["min_date"] or "").strip()
    if str(row["max_date"] or "").strip():
        out["max_date"] = str(row["max_date"] or "").strip()
    return out


def _get_overview_activity_items(
    *,
    store_uids: list[str],
    date_from: str,
    date_to: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    rows = _get_overview_order_rows(store_uids=store_uids, date_from=date_from, date_to=date_to)
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        if _overview_status_kind(row.get("item_status")) == "ignore":
            continue
        suid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not suid or not sku:
            continue
        local = out.setdefault(suid, {})
        item = local.setdefault(sku, {"sku": sku, "item_name": str(row.get("item_name") or "").strip()})
        if not item.get("item_name"):
            item["item_name"] = str(row.get("item_name") or "").strip()
    return out


def _build_sales_metrics_map(
    *,
    stores: list[dict[str, Any]],
    skus: list[str],
    date_from: datetime.date,
    date_to: datetime.date,
) -> dict[str, dict[str, dict[str, Any]]]:
    store_uids = [str(s.get("store_uid") or "").strip() for s in stores if str(s.get("store_uid") or "").strip()]
    raw_rows = _get_overview_order_rows(
        store_uids=store_uids,
        skus=skus,
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
    )
    currency_by_store = _store_currency_map(stores)
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for row in raw_rows:
        store_uid = str(row.get("store_uid") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not store_uid or not sku:
            continue
        if _overview_status_kind(row.get("item_status")) == "ignore":
            continue
        currency_code = currency_by_store.get(store_uid, "RUB")
        sale_price_raw = _to_num(row.get("sale_price"))
        payment_price_raw = _to_num(row.get("sale_price_with_coinvest"))
        order_date = str(row.get("order_created_date") or "").strip()
        sale_price = _normalize_money_to_rub(sale_price_raw, currency_code=currency_code, order_date=order_date)
        payment_price = _normalize_money_to_rub(payment_price_raw, currency_code=currency_code, order_date=order_date)
        if payment_price is None and sale_price is not None:
            payment_price = float(sale_price)
        subsidy_amount = (float(sale_price) - float(payment_price)) if sale_price is not None and payment_price is not None else 0.0
        line_revenue = sale_price
        local = out.setdefault(store_uid, {})
        stats = local.setdefault(
            sku,
            {
                "mentions_count": 0,
                "turnover": 0.0,
                "avg_sale_price": None,
                "avg_payment_price": None,
                "avg_coinvest_percent": None,
                "units_count": 0,
                "_price_sum": 0.0,
                "_payment_sum": 0.0,
                "_subsidy_sum": 0.0,
                "_price_count": 0,
            },
        )
        stats["mentions_count"] += 1
        stats["units_count"] += 1
        stats["turnover"] += float(line_revenue or sale_price or 0.0)
        if sale_price is not None:
            stats["_price_sum"] += float(sale_price)
            stats["_price_count"] += 1
        if payment_price is not None:
            stats["_payment_sum"] += float(payment_price)
        if subsidy_amount is not None:
            stats["_subsidy_sum"] += float(subsidy_amount)
    for local in out.values():
        for stats in local.values():
            price_count = int(stats.pop("_price_count", 0) or 0)
            price_sum = float(stats.pop("_price_sum", 0.0) or 0.0)
            payment_sum = float(stats.pop("_payment_sum", 0.0) or 0.0)
            subsidy_sum = float(stats.pop("_subsidy_sum", 0.0) or 0.0)
            stats["avg_sale_price"] = round(price_sum / price_count, 2) if price_count > 0 else None
            stats["avg_payment_price"] = round(payment_sum / price_count, 2) if price_count > 0 else None
            stats["avg_coinvest_percent"] = round((subsidy_sum / price_sum) * 100.0, 2) if price_sum > 0 else None
            stats["turnover"] = round(float(stats.get("turnover") or 0.0), 2)
            stats["total_sale_amount"] = round(price_sum, 2)
            stats["total_payment_amount"] = round(payment_sum, 2)
            stats["total_subsidy_amount"] = round(subsidy_sum, 2)
    return out


def _build_sales_timeseries(
    *,
    stores: list[dict[str, Any]],
    skus: list[str],
    date_from: datetime.date,
    date_to: datetime.date,
) -> list[dict[str, Any]]:
    store_uids = [str(s.get("store_uid") or "").strip() for s in stores if str(s.get("store_uid") or "").strip()]
    raw_rows = _get_overview_order_rows(
        store_uids=store_uids,
        skus=skus,
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
    )
    currency_by_store = _store_currency_map(stores)
    by_date: dict[str, dict[str, float]] = {}
    for row in raw_rows:
        order_date = str(row.get("order_created_date") or "").strip()
        store_uid = str(row.get("store_uid") or "").strip()
        if not order_date or not store_uid:
            continue
        if _overview_status_kind(row.get("item_status")) == "ignore":
            continue
        currency_code = currency_by_store.get(store_uid, "RUB")
        sale_price = _normalize_money_to_rub(_to_num(row.get("sale_price")), currency_code=currency_code, order_date=order_date) or 0.0
        payment_price = _normalize_money_to_rub(_to_num(row.get("sale_price_with_coinvest")), currency_code=currency_code, order_date=order_date)
        if payment_price is None:
            payment_price = sale_price
        subsidy_amount = max(0.0, float(sale_price) - float(payment_price))
        line_revenue = sale_price
        stats = by_date.setdefault(
            order_date,
            {
                "mentions_count": 0.0,
                "turnover": 0.0,
                "sale_sum": 0.0,
                "payment_sum": 0.0,
                "subsidy_sum": 0.0,
            },
        )
        stats["mentions_count"] += 1.0
        stats["turnover"] += float(line_revenue)
        stats["sale_sum"] += float(sale_price)
        stats["payment_sum"] += float(payment_price)
        stats["subsidy_sum"] += float(subsidy_amount)
    series: list[dict[str, Any]] = []
    for day in sorted(by_date.keys()):
        stats = by_date[day]
        mentions = int(stats["mentions_count"])
        sale_sum = float(stats["sale_sum"])
        payment_sum = float(stats["payment_sum"])
        subsidy_sum = float(stats["subsidy_sum"])
        series.append(
            {
                "date": day,
                "mentions_count": mentions,
                "turnover": round(float(stats["turnover"]), 2),
                "avg_sale_price": round(sale_sum / mentions, 2) if mentions > 0 else None,
                "avg_payment_price": round(payment_sum / mentions, 2) if mentions > 0 else None,
                "avg_coinvest_percent": round((subsidy_sum / sale_sum) * 100.0, 2) if sale_sum > 0 else None,
            }
        )
    return series


async def get_sales_elasticity_overview(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    stock_filter: str = "all",
    page: int = 1,
    page_size: int = 200,
    period: str = "today",
    date_from: str = "",
    date_to: str = "",
) -> dict[str, Any]:
    cache_payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "stock_filter": stock_filter,
        "page": page,
        "page_size": page_size,
        "period": period,
        "date_from": date_from,
        "date_to": date_to,
    }
    cached = _cache_get("overview", cache_payload)
    if cached:
        return cached

    stores = _get_yandex_stores()
    scope_norm = str(scope or "all").strip().lower()
    platform_norm = str(platform or "").strip().lower()
    store_norm = str(store_id or "").strip()
    query = str(search or "").strip().lower()
    selected_prefix = [p.strip() for p in str(category_path or "").split("/") if p.strip()]

    if scope_norm == "store":
        target_stores = [s for s in stores if s["platform"] == platform_norm and s["store_id"] == store_norm]
    else:
        target_stores = list(stores)

    stock_map_by_store_uid: dict[str, dict[str, float]] = {}
    for store in target_stores:
        suid = str(store.get("store_uid") or "").strip()
        if not suid:
            continue
        try:
            settings = get_pricing_store_settings(store_uid=suid) or {}
            stock_source_id = str(settings.get("stock_source_id") or "").strip()
            stock_sku_column = str(settings.get("stock_sku_column") or "").strip()
            stock_value_column = str(settings.get("stock_value_column") or "").strip()
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

    merged: dict[str, dict[str, Any]] = {}
    preferred_tree_store_uid = str(tree_source_store_id or "").strip() if scope_norm == "all" else ""
    for store in target_stores:
        store_uid = str(store.get("store_uid") or "").strip()
        table_name = str(store.get("table_name") or "").strip()
        if not store_uid or not table_name:
            continue
        for row in _read_source_rows(table_name):
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue
            item = merged.setdefault(
                sku,
                {
                    "sku": sku,
                    "name": str(row.get("name") or "").strip(),
                    "tree_path": _catalog_path_from_row(row),
                    "placements": {},
                    "stock_by_store": {},
                },
            )
            if not item.get("name") and str(row.get("name") or "").strip():
                item["name"] = str(row.get("name") or "").strip()
            path = _catalog_path_from_row(row)
            if path and preferred_tree_store_uid and store_uid == preferred_tree_store_uid:
                item["tree_path"] = path
            elif path and not item.get("tree_path"):
                item["tree_path"] = path
            item["placements"][store_uid] = True
            stock_value = stock_map_by_store_uid.get(store_uid, {}).get(sku)
            if stock_value is None:
                stock_value = _extract_row_stock(row)
            item.setdefault("stock_by_store", {})[store_uid] = None if stock_value is None else float(stock_value)

    rows_all = list(merged.values())
    today = datetime.date.today()
    current_from, current_to, prev_from, prev_to = _period_bounds(period=period, today=today, date_from=date_from, date_to=date_to)
    store_uids = [str(s.get("store_uid") or "").strip() for s in target_stores if str(s.get("store_uid") or "").strip()]
    data_bounds = _get_overview_date_bounds(store_uids=store_uids)
    data_min = _parse_iso_date(data_bounds.get("min_date") or "")
    data_max = _parse_iso_date(data_bounds.get("max_date") or "")
    comparison_from, comparison_to, comparison_enabled = _resolve_comparison_bounds(
        current_from=current_from,
        current_to=current_to,
        prev_from=prev_from,
        prev_to=prev_to,
        data_min=data_min,
        data_max=data_max,
    )
    current_activity = _get_overview_activity_items(
        store_uids=store_uids,
        date_from=current_from.isoformat(),
        date_to=current_to.isoformat(),
    )
    previous_activity = (
        _get_overview_activity_items(
            store_uids=store_uids,
            date_from=comparison_from.isoformat(),
            date_to=comparison_to.isoformat(),
        )
        if comparison_enabled and comparison_from and comparison_to
        else {}
    )
    for activity_map in (current_activity, previous_activity):
        for store_uid, sku_map in activity_map.items():
            for sku, activity in (sku_map or {}).items():
                item = merged.setdefault(
                    sku,
                {
                    "sku": sku,
                    "name": str(activity.get("item_name") or "").strip(),
                    "tree_path": [],
                    "placements": {},
                    "stock_by_store": {},
                },
            )
                if not item.get("name") and str(activity.get("item_name") or "").strip():
                    item["name"] = str(activity.get("item_name") or "").strip()
                item["placements"][store_uid] = True
    rows_all = list(merged.values())
    rows_all.sort(key=lambda row: str(row.get("sku") or ""))
    stock_filter_norm = str(stock_filter or "all").strip().lower()
    all_filtered_rows: list[dict[str, Any]] = []
    for row in rows_all:
        path = row.get("tree_path") if isinstance(row.get("tree_path"), list) else []
        if selected_prefix and path[: len(selected_prefix)] != selected_prefix:
            continue
        if query:
            hay = f"{row.get('sku') or ''} {row.get('name') or ''}".lower()
            if query not in hay:
                continue
        if stock_filter_norm in {"in_stock", "out_of_stock"}:
            if scope_norm == "store":
                stock_values = [((row.get("stock_by_store") or {}).get(target_stores[0]["store_uid"]) if target_stores else None)]
            else:
                stock_values = list((row.get("stock_by_store") or {}).values())
            has_stock = any(value is not None and float(value) > 0 for value in stock_values)
            if stock_filter_norm == "in_stock" and not has_stock:
                continue
            if stock_filter_norm == "out_of_stock" and has_stock:
                continue
        all_filtered_rows.append(row)
    skus = [str(row.get("sku") or "").strip() for row in all_filtered_rows if str(row.get("sku") or "").strip()]
    current_map = _build_sales_metrics_map(
        stores=target_stores,
        skus=skus,
        date_from=current_from,
        date_to=current_to,
    )
    previous_map = (
        _build_sales_metrics_map(
            stores=target_stores,
            skus=skus,
            date_from=comparison_from,
            date_to=comparison_to,
        )
        if comparison_enabled and comparison_from and comparison_to
        else {}
    )

    metric_rows: list[dict[str, Any]] = []
    for row in all_filtered_rows:
        sku = str(row.get("sku") or "").strip()
        current_mentions = 0
        current_turnover = 0.0
        current_sale_sum = 0.0
        current_payment_sum = 0.0
        current_subsidy_sum = 0.0
        previous_mentions = 0
        previous_turnover = 0.0
        previous_payment_sum = 0.0
        by_store: list[dict[str, Any]] = []
        for store_uid in store_uids:
            current_stats = (current_map.get(store_uid) or {}).get(sku) or {}
            previous_stats = (previous_map.get(store_uid) or {}).get(sku) or {}
            current_mentions += int(current_stats.get("mentions_count") or 0)
            previous_mentions += int(previous_stats.get("mentions_count") or 0)
            current_turnover += float(current_stats.get("turnover") or 0.0)
            previous_turnover += float(previous_stats.get("turnover") or 0.0)
            current_sale_sum += float(current_stats.get("total_sale_amount") or 0.0)
            current_payment_sum += float(current_stats.get("total_payment_amount") or 0.0)
            current_subsidy_sum += float(current_stats.get("total_subsidy_amount") or 0.0)
            previous_payment_sum += float(previous_stats.get("total_payment_amount") or 0.0)
            sale_total = float(current_stats.get("total_sale_amount") or 0.0)
            payment_total = float(current_stats.get("total_payment_amount") or 0.0)
            subsidy_total = float(current_stats.get("total_subsidy_amount") or 0.0)
            mentions = int(current_stats.get("mentions_count") or 0)
            store_meta = next((s for s in target_stores if str(s.get("store_uid") or "").strip() == store_uid), None)
            by_store.append(
                {
                    "store_uid": store_uid,
                    "store_id": str((store_meta or {}).get("store_id") or ""),
                    "label": str((store_meta or {}).get("label") or store_uid),
                    "platform_label": str((store_meta or {}).get("platform_label") or ""),
                    "mentions_count": mentions,
                    "avg_sale_price": round(sale_total / mentions, 2) if mentions > 0 else None,
                    "avg_payment_price": round(payment_total / mentions, 2) if mentions > 0 else None,
                    "avg_coinvest_percent": round((subsidy_total / sale_total) * 100.0, 2) if sale_total > 0 else None,
                }
            )
        avg_sale_price = round(current_sale_sum / current_mentions, 2) if current_mentions > 0 else None
        avg_payment_price = round(current_payment_sum / current_mentions, 2) if current_mentions > 0 else None
        avg_coinvest_percent = round((current_subsidy_sum / current_sale_sum) * 100.0, 2) if current_sale_sum > 0 else None
        previous_avg_payment_price = round(previous_payment_sum / previous_mentions, 2) if previous_mentions > 0 else None
        if current_mentions <= 0 and previous_mentions <= 0 and current_turnover <= 0 and previous_turnover <= 0:
            continue
        count_delta_percent = _percent_delta(float(current_mentions), float(previous_mentions)) if comparison_enabled else None
        turnover_delta_percent = _percent_delta(float(current_turnover), float(previous_turnover)) if comparison_enabled else None
        elasticity_value = (
            None
            if str(period or "").strip().lower() in {"today", "yesterday"} or not comparison_enabled
            else _arc_elasticity(
                current_qty=float(current_mentions),
                previous_qty=float(previous_mentions),
                current_price=avg_payment_price,
                previous_price=previous_avg_payment_price,
            )
        )
        metric_rows.append(
            {
                "sku": sku,
                "name": str(row.get("name") or ""),
                "tree_path": row.get("tree_path") if isinstance(row.get("tree_path"), list) else [],
                "placements": row.get("placements") or {},
                "stock_by_store": row.get("stock_by_store") or {},
                "mentions_count": current_mentions,
                "turnover": _format_turnover(current_turnover),
                "avg_sale_price": avg_sale_price,
                "avg_payment_price": avg_payment_price,
                "avg_coinvest_percent": avg_coinvest_percent,
                "by_store": by_store,
                "count_delta_percent": None if count_delta_percent is None else round(count_delta_percent, 2),
                "turnover_delta_percent": None if turnover_delta_percent is None else round(turnover_delta_percent, 2),
                "elasticity": None if elasticity_value is None else round(elasticity_value, 4),
                "comparison_from": comparison_from.isoformat() if comparison_enabled and comparison_from else None,
                "comparison_to": comparison_to.isoformat() if comparison_enabled and comparison_to else None,
            }
        )
    metric_rows.sort(
        key=lambda row: (
            -float(row.get("turnover") or 0.0),
            -int(row.get("mentions_count") or 0),
            str(row.get("sku") or ""),
        )
    )
    summary_mentions = sum(int(row.get("mentions_count") or 0) for row in metric_rows)
    summary_turnover = sum(float(row.get("turnover") or 0.0) for row in metric_rows)
    summary_sale_sum = sum(float(((current_map.get(store_uid) or {}).get(str(row.get("sku") or "").strip()) or {}).get("total_sale_amount") or 0.0) for row in metric_rows for store_uid in store_uids)
    summary_payment_sum = sum(float(((current_map.get(store_uid) or {}).get(str(row.get("sku") or "").strip()) or {}).get("total_payment_amount") or 0.0) for row in metric_rows for store_uid in store_uids)
    summary_subsidy_sum = sum(float(((current_map.get(store_uid) or {}).get(str(row.get("sku") or "").strip()) or {}).get("total_subsidy_amount") or 0.0) for row in metric_rows for store_uid in store_uids)
    summary_avg_sale_price = (summary_sale_sum / summary_mentions) if summary_mentions > 0 else None
    summary_avg_payment_price = (summary_payment_sum / summary_mentions) if summary_mentions > 0 else None
    summary_avg_coinvest_percent = ((summary_subsidy_sum / summary_sale_sum) * 100.0) if summary_sale_sum > 0 else None
    summary_previous_mentions = 0
    summary_previous_turnover = 0.0
    for row in metric_rows:
        sku = str(row.get("sku") or "").strip()
        for store_uid in store_uids:
            previous_stats = (previous_map.get(store_uid) or {}).get(sku) or {}
            summary_previous_mentions += int(previous_stats.get("mentions_count") or 0)
            summary_previous_turnover += float(previous_stats.get("turnover") or 0.0)
    summary_count_delta = _percent_delta(float(summary_mentions), float(summary_previous_mentions)) if comparison_enabled else None
    summary_turnover_delta = _percent_delta(float(summary_turnover), float(summary_previous_turnover)) if comparison_enabled else None
    summary = {
        "turnover": _format_turnover(summary_turnover),
        "mentions_count": summary_mentions,
        "avg_sale_price": None if summary_avg_sale_price is None else round(summary_avg_sale_price, 2),
        "avg_payment_price": None if summary_avg_payment_price is None else round(summary_avg_payment_price, 2),
        "avg_coinvest_percent": None if summary_avg_coinvest_percent is None else round(summary_avg_coinvest_percent, 2),
        "count_delta_percent": None if summary_count_delta is None else round(summary_count_delta, 2),
        "turnover_delta_percent": None if summary_turnover_delta is None else round(summary_turnover_delta, 2),
    }
    total_count = len(metric_rows)
    page_size_n = max(1, min(int(page_size or 200), 500))
    page_n = max(1, int(page or 1))
    start = (page_n - 1) * page_size_n
    out_rows = metric_rows[start:start + page_size_n]

    resp = {
        "ok": True,
        "rows": out_rows,
        "stores": target_stores,
        "total_count": total_count,
        "page": page_n,
        "page_size": page_size_n,
        "period": str(period or "today").strip().lower(),
        "current_from": current_from.isoformat(),
        "current_to": current_to.isoformat(),
        "comparison_from": comparison_from.isoformat() if comparison_enabled and comparison_from else None,
        "comparison_to": comparison_to.isoformat() if comparison_enabled and comparison_to else None,
        "comparison_enabled": comparison_enabled,
        "data_min": data_min.isoformat() if data_min else None,
        "data_max": data_max.isoformat() if data_max else None,
        "summary": summary,
        "series": _build_sales_timeseries(
            stores=target_stores,
            skus=skus,
            date_from=current_from,
            date_to=current_to,
        ),
    }
    _cache_set("overview", cache_payload, resp)
    return resp


def _load_sync_state() -> dict[str, Any]:
    state = load_json(ELASTICITY_SYNC_STATE_KEY, {"done": False})
    return state if isinstance(state, dict) else {"done": False}


def _save_sync_state(patch: dict[str, Any]) -> dict[str, Any]:
    state = _load_sync_state()
    state.update(patch)
    save_json(ELASTICITY_SYNC_STATE_KEY, state)
    return state


async def refresh_sales_elasticity_data(*, mode: str = "recent", manual: bool = False, store_uids: list[str] | None = None) -> dict[str, Any]:
    today = datetime.date.today()
    range_from, range_to, mode_norm = _refresh_bounds(mode, today)
    refresh_ranges = [(range_from, range_to)] if mode_norm in {"today", "week", "month", "current_month"} else _iter_month_ranges(range_from, range_to)

    stores = _get_yandex_stores()
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [store for store in stores if str(store.get("store_uid") or "").strip() in selected]
    processed_stores = 0
    loaded_rows = 0
    skipped: list[dict[str, Any]] = []
    processed: list[dict[str, Any]] = []
    for store in stores:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        creds = _find_yandex_shop_credentials(campaign_id)
        if not creds or not store_uid:
            skipped.append({"store_uid": store_uid, "store_id": campaign_id, "reason": "credentials_not_found"})
            continue
        business_id, _, api_key = creds
        month_rows: list[dict[str, Any]] = []
        try:
            for chunk_from, chunk_to in refresh_ranges:
                rows = await _fetch_yandex_business_orders_for_range(
                    business_id=business_id,
                    campaign_id=campaign_id,
                    api_key=api_key,
                    date_from=chunk_from,
                    date_to=chunk_to,
                )
                replace_sales_market_order_items_for_period(
                    store_uid=store_uid,
                    platform="yandex_market",
                    date_from=chunk_from.isoformat(),
                    date_to=chunk_to.isoformat(),
                    rows=rows,
                )
                month_rows.extend(rows)
            processed_stores += 1
            loaded_rows += len(month_rows)
            logger.warning(
                "[sales_elasticity] store refreshed store_uid=%s store_id=%s rows=%s mode=%s",
                store_uid,
                campaign_id,
                len(month_rows),
                mode_norm,
            )
            processed.append({"store_uid": store_uid, "store_id": campaign_id, "rows": len(month_rows)})
        except Exception as exc:
            logger.warning(
                "[sales_elasticity] store refresh failed store_uid=%s store_id=%s business_id=%s error=%s",
                store_uid,
                campaign_id,
                business_id,
                exc,
            )
            skipped.append(
                {
                    "store_uid": store_uid,
                    "store_id": campaign_id,
                    "business_id": business_id,
                    "reason": str(exc),
                }
            )
    invalidate_sales_elasticity_cache()
    if processed_stores == 0 and skipped:
        reasons = "; ".join(f"{item.get('store_id')}: {item.get('reason')}" for item in skipped[:3])
        raise RuntimeError(f"Не удалось обновить ни один магазин Я.Маркета: {reasons}")
    sync_state = _load_sync_state()
    if processed_stores > 0:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        patch: dict[str, Any] = {
            "last_mode": mode_norm,
            "last_updated_at": now_iso,
            "last_date_from": range_from.isoformat(),
            "last_date_to": range_to.isoformat(),
        }
        if mode_norm == "full":
            patch.update(
                {
                    "done": True,
                    "full_updated_at": now_iso,
                    "full_date_from": range_from.isoformat(),
                    "full_date_to": range_to.isoformat(),
                }
            )
        elif mode_norm == "year":
            patch["daily_updated_at"] = now_iso
        elif mode_norm in {"month", "week", "today", "current_month"}:
            patch["recent_updated_at"] = now_iso
        if manual:
            patch["manual_updated_at"] = now_iso
            patch["manual_mode"] = mode_norm
        sync_state = _save_sync_state(patch)
    return {
        "ok": True,
        "mode": mode_norm,
        "date_from": range_from.isoformat(),
        "date_to": range_to.isoformat(),
        "stores_total": len(stores),
        "stores_processed": processed_stores,
        "rows_loaded": loaded_rows,
        "stores": processed,
        "stores_skipped": skipped,
        "sync_state": sync_state,
    }


def get_sales_elasticity_sync_state() -> dict[str, Any]:
    return _load_sync_state()
