from __future__ import annotations

import asyncio
import io
import json
import logging
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from backend.routers._shared import (
    YANDEX_BASE_URL,
    _catalog_marketplace_stores_context,
    _fetch_cbr_usd_rates,
    _find_yandex_shop_credentials,
    _read_source_rows,
    _safe_source_table_name,
    _ym_headers,
    load_sources,
)
from backend.services.db import is_postgres_backend
from backend.services.pricing_prices_service import _clamp_rate, _compute_max_weight_kg, _num0, _resolve_category_settings_for_leaf
from backend.services.source_tables import get_registered_source_table
from backend.services.gsheets import read_sheet_all
from backend.services.storage import is_source_mode_enabled
from backend.services.store_data_model import (
    _connect,
    _connect_history,
    _placeholders,
    get_pricing_strategy_history_rows,
    get_fx_rates_cache,
    get_pricing_catalog_sku_path_map,
    get_pricing_category_settings_map,
    get_pricing_category_tree,
    get_pricing_logistics_product_settings_map,
    get_pricing_logistics_store_settings,
    get_pricing_store_settings,
    get_sales_overview_cogs_source_map,
    get_sales_overview_order_rows_map,
    get_sales_overview_order_rows,
    prune_pricing_cogs_snapshots_for_store,
    replace_fx_rates_cache,
    replace_sales_overview_cogs_source_rows,
    replace_sales_overview_order_rows,
    replace_sales_overview_order_rows_hot,
    replace_sales_market_order_items_for_period,
    replace_sales_united_order_transactions_for_period,
)
from backend.services.sales_elasticity_service import _fetch_yandex_business_orders_for_range

logger = logging.getLogger("uvicorn.error")
MSK = ZoneInfo("Europe/Moscow")

DEFAULT_HISTORY_DATE_FROM = "2025-07-01"
_FX_USD_RUB_MEM: dict[str, float] = {}
_REPORT_INFO_POLL_INTERVAL_SEC = 30
_REPORT_INFO_MAX_ATTEMPTS = 12
_DELIVERED_STATUS = "доставлен покупателю"


def _today_msk() -> date:
    return datetime.now(timezone(timedelta(hours=3))).date()


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    current_month = date(year, month, 1)
    return max(1, (next_month - current_month).days)


def _default_history_date_to() -> str:
    return (_today_msk() - timedelta(days=1)).isoformat()


def _history_month_cutoff_date() -> date:
    today = datetime.now(MSK).date()
    return today.replace(day=1) - timedelta(days=1)


def _resolve_store_campaign_id(store_uid: str) -> str:
    suid = str(store_uid or "").strip()
    if not suid:
        return ""
    active_store = next(
        (store for store in _catalog_marketplace_stores_context() if str(store.get("store_uid") or "").strip() == suid),
        None,
    )
    if isinstance(active_store, dict):
        campaign_id = str(active_store.get("store_id") or "").strip()
        if campaign_id:
            return campaign_id
    if ":" in suid:
        return str(suid.split(":", 1)[1] or "").strip()
    return ""


def _resolve_store_currency_code(store_uid: str) -> str:
    suid = str(store_uid or "").strip()
    if not suid:
        return "RUB"
    active_store = next(
        (store for store in _catalog_marketplace_stores_context() if str(store.get("store_uid") or "").strip() == suid),
        None,
    )
    return str((active_store or {}).get("currency_code") or "RUB").strip().upper() or "RUB"


def _parse_decimal(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ".").replace(" ", ""))
    except Exception:
        return None


def _normalize_order_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = raw.replace("\xa0", "").replace(" ", "").replace("'", "")
    if re.fullmatch(r"\d+\.0+", normalized):
        normalized = normalized.split(".", 1)[0]
    if re.fullmatch(r"\d+[.,]\d+", normalized):
        try:
            numeric = float(normalized.replace(",", "."))
            if numeric.is_integer():
                normalized = str(int(numeric))
        except Exception:
            pass
    if re.search(r"[eE][+-]?\d+", normalized):
        try:
            numeric = float(normalized.replace(",", "."))
            if numeric.is_integer():
                normalized = str(int(numeric))
        except Exception:
            pass
    return normalized


def _norm_source_col_name(v: str) -> str:
    return " ".join(str(v or "").strip().lower().split())


def _resolve_source_table_name(source_id: str) -> str:
    sid = str(source_id or "").strip()
    if not sid:
        return ""
    table_name = str(get_registered_source_table(sid) or "").strip()
    if table_name:
        _safe_source_table_name(table_name)
    return table_name


def _load_order_cogs_map_from_source(*, source_id: str, order_column: str, sku_column: str, value_column: str) -> dict[tuple[str, str], float]:
    sid = str(source_id or "").strip()
    if not sid:
        return {}
    if not is_source_mode_enabled(sid, "import", default=True):
        return {}
    src = next((item for item in (load_sources() or []) if str(item.get("id") or "").strip() == sid), None)
    if src and str(src.get("type") or "").strip().lower() == "gsheets":
        spreadsheet_id = str(src.get("spreadsheet_id") or "").strip()
        worksheet = str(src.get("worksheet") or "").strip() or None
        if spreadsheet_id:
            try:
                payload = read_sheet_all(spreadsheet_id, worksheet=worksheet)
                rows = payload.get("rows") if isinstance(payload, dict) else None
                if isinstance(rows, list) and len(rows) >= 2:
                    header = rows[0] if isinstance(rows[0], list) else []
                    if isinstance(header, list):
                        header_norm = [_norm_key(cell) for cell in header]
                        target_order = _norm_key(order_column)
                        target_sku = _norm_key(sku_column)
                        target_value = _norm_key(value_column)
                        try:
                            order_idx = header_norm.index(target_order)
                            sku_idx = header_norm.index(target_sku) if target_sku else -1
                            value_idx = header_norm.index(target_value)
                            out: dict[tuple[str, str], float] = {}
                            for row in rows[1:]:
                                if not isinstance(row, list):
                                    continue
                                order_id = _normalize_order_key(row[order_idx] if order_idx < len(row) else None)
                                sku = str(row[sku_idx] if sku_idx >= 0 and sku_idx < len(row) else "").strip()
                                value = _parse_decimal(row[value_idx] if value_idx < len(row) else None)
                                if order_id and value is not None:
                                    out[(order_id, sku)] = float(value)
                            if out:
                                return out
                        except ValueError:
                            pass
            except Exception:
                pass

    table_name = _resolve_source_table_name(sid)
    if not table_name:
        return {}
    rows = _read_source_rows(table_name)
    target_order = _norm_source_col_name(order_column)
    target_sku = _norm_source_col_name(sku_column)
    target_value = _norm_source_col_name(value_column)
    out: dict[tuple[str, str], float] = {}
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        order_raw = None
        sku_raw = None
        value_raw = None
        for key, value in payload.items():
            key_norm = _norm_source_col_name(str(key))
            if key_norm == target_order and order_raw in (None, ""):
                order_raw = value
            elif target_sku and key_norm == target_sku and sku_raw in (None, ""):
                sku_raw = value
            elif key_norm == target_value and value_raw in (None, ""):
                value_raw = value
        order_id = _normalize_order_key(order_raw)
        sku = str(sku_raw or "").strip()
        value = _parse_decimal(value_raw)
        if order_id and value is not None:
            out[(order_id, sku)] = float(value)
    return out


def refresh_sales_overview_cogs_source_for_store(*, store_uid: str) -> dict[str, Any]:
    settings = get_pricing_store_settings(store_uid=store_uid) or {}
    source_id = str(settings.get("overview_cogs_source_id") or "").strip()
    order_column = str(settings.get("overview_cogs_order_column") or "").strip()
    sku_column = str(settings.get("overview_cogs_sku_column") or "").strip()
    value_column = str(settings.get("overview_cogs_value_column") or "").strip()
    if not source_id or not order_column or not value_column:
        loaded = replace_sales_overview_cogs_source_rows(store_uid=store_uid, rows=[])
        return {"store_uid": store_uid, "rows": loaded, "source_id": source_id}
    cogs_map = _load_order_cogs_map_from_source(source_id=source_id, order_column=order_column, sku_column=sku_column, value_column=value_column)
    loaded = replace_sales_overview_cogs_source_rows(
        store_uid=store_uid,
        rows=[{"order_key": order_key, "sku_key": sku_key, "cogs_value": value} for (order_key, sku_key), value in cogs_map.items()],
    )
    logger.warning(
        "[sales_overview] cogs source refreshed store_uid=%s source_id=%s rows=%s",
        store_uid,
        source_id,
        loaded,
    )
    return {"store_uid": store_uid, "rows": loaded, "source_id": source_id}


def refresh_sales_overview_cogs_sources() -> dict[str, Any]:
    stores = [
        store for store in _catalog_marketplace_stores_context()
        if str(store.get("platform") or "").strip().lower() == "yandex_market"
    ]
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for store in stores:
        store_uid = str(store.get("store_uid") or "").strip()
        if not store_uid:
            continue
        try:
            results.append(refresh_sales_overview_cogs_source_for_store(store_uid=store_uid))
        except Exception as exc:
            logger.warning("[sales_overview] cogs source refresh failed store_uid=%s error=%s", store_uid, exc)
            errors.append({"store_uid": store_uid, "error": str(exc)})
    return {"ok": True, "stores": results, "errors": errors}


async def _get_cbr_usd_rub_rate_for_date(calc_date: date) -> float | None:
    key = calc_date.isoformat()
    if key in _FX_USD_RUB_MEM:
        return _FX_USD_RUB_MEM[key]

    def _prefer_latest_published(by_date: dict[str, float]) -> tuple[str | None, float | None]:
        if not by_date:
            return None, None
        latest_date = max(by_date.keys())
        latest_rate = float(by_date[latest_date])
        return latest_date, latest_rate
    try:
        cached = get_fx_rates_cache(source="cbr", pair="USD_RUB")
        rows = cached.get("rows") if isinstance(cached, dict) else None
        if isinstance(rows, list) and rows:
            by_date: dict[str, float] = {}
            for row in rows:
                d = str(row.get("date") or "").strip()
                rate = _parse_decimal(row.get("rate"))
                if d and rate and rate > 0:
                    by_date[d] = rate
            if by_date:
                best_date, rate = _prefer_latest_published(by_date)
                if best_date and best_date > key:
                    _FX_USD_RUB_MEM[key] = float(rate or 0.0)
                    return float(rate or 0.0)
                rate = float(by_date[best_date])
                _FX_USD_RUB_MEM[key] = rate
                return rate
    except Exception:
        pass
    try:
        start = calc_date - timedelta(days=60)
        fresh_rows = await _fetch_cbr_usd_rates(start, calc_date + timedelta(days=1))
        if fresh_rows:
            replace_fx_rates_cache(source="cbr", pair="USD_RUB", rows=fresh_rows, meta={"loaded_from": "sales_overview"})
            by_date: dict[str, float] = {}
            for row in fresh_rows:
                d = str(row.get("date") or "").strip()
                rate = _parse_decimal(row.get("rate"))
                if d and rate and rate > 0:
                    by_date[d] = rate
            if by_date:
                best_date, rate = _prefer_latest_published(by_date)
                if best_date and best_date > key:
                    _FX_USD_RUB_MEM[key] = float(rate or 0.0)
                    return float(rate or 0.0)
                rate = float(by_date[best_date])
                _FX_USD_RUB_MEM[key] = rate
                return rate
    except Exception:
        pass
    return None


def _parse_iso_datetime(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=MSK)
            return dt.isoformat(), dt.date().isoformat()
        except Exception:
            pass
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return raw, raw[:10]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    source_date_match = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    source_date = source_date_match.group(1) if source_date_match else dt.astimezone(MSK).date().isoformat()
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"), source_date


def _norm_key(value: Any) -> str:
    s = str(value or "").strip().lower()
    s = s.replace("\xa0", " ")
    s = re.sub(r"[\s_\-:/]+", "", s)
    return s


def _pick_value(node: dict[str, Any], *names: str) -> Any:
    if not isinstance(node, dict):
        return None
    normalized = {_norm_key(key): value for key, value in node.items()}
    for name in names:
        key = _norm_key(name)
        value = normalized.get(key)
        if value not in (None, ""):
            return value
    return None


def _is_transaction_row(node: dict[str, Any]) -> bool:
    if not isinstance(node, dict):
        return False
    order_id = _pick_value(node, "ORDER_ID", "orderId", "order_id", "order", "номер заказа")
    sku = _pick_value(node, "SHOP_SKU", "shopSku", "offerId", "sku", "shop_sku", "ваш sku", "вашsku")
    item_status = _pick_value(node, "OFFER_STATUS", "offerStatus", "itemStatus", "status", "статус товара", "статустовара")
    return bool(order_id and sku and item_status)


def _walk_json_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if _is_transaction_row(node):
                rows.append(node)
                return
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return rows


def _parse_report_bytes(content: bytes) -> list[dict[str, Any]]:
    parsed_rows: list[dict[str, Any]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            prioritized = sorted(
                zf.namelist(),
                key=lambda name: (
                    0 if "transaction" in name.lower() else 1,
                    0 if "order" in name.lower() else 1,
                    name.lower(),
                ),
            )
            for name in prioritized:
                if not name.lower().endswith(".json"):
                    continue
                try:
                    raw = zf.read(name)
                    payload = json.loads(raw.decode("utf-8"))
                except Exception:
                    continue
                rows = _walk_json_rows(payload)
                if rows:
                    parsed_rows.extend(rows)
    except zipfile.BadZipFile:
        try:
            payload = json.loads(content.decode("utf-8"))
            parsed_rows.extend(_walk_json_rows(payload))
        except Exception:
            pass
    return parsed_rows


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
    return [item for item in orders if isinstance(item, dict)], next_page_token


def _parse_business_order_created_date(value: Any) -> tuple[str, str] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        source_date_match = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
        source_date = source_date_match.group(1) if source_date_match else dt.astimezone(MSK).date().isoformat()
        dt_utc = dt.astimezone(timezone.utc)
        return (
            dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            source_date,
        )
    except Exception:
        return None


async def _fetch_yandex_business_order_created_map(
    *,
    business_id: str,
    campaign_id: str,
    api_key: str,
    date_from: str,
    date_to: str,
) -> dict[str, tuple[str, str]]:
    bid = str(business_id or "").strip()
    cid = str(campaign_id or "").strip()
    from_key = str(date_from or "").strip()
    to_key = str(date_to or "").strip()
    if not bid or not cid or not from_key or not to_key:
        return {}
    out: dict[str, tuple[str, str]] = {}
    try:
        range_from = date.fromisoformat(from_key)
        range_to = date.fromisoformat(to_key)
    except Exception:
        return {}
    url = f"https://api.partner.market.yandex.ru/v1/businesses/{bid}/orders"
    async with httpx.AsyncClient(timeout=90) as client:
        chunk_from = range_from
        while chunk_from <= range_to:
            chunk_to = min(range_to, chunk_from + timedelta(days=29))
            logger.warning(
                "[sales_overview] business orders chunk started business_id=%s campaign_id=%s date_from=%s date_to=%s",
                bid,
                cid,
                chunk_from.isoformat(),
                chunk_to.isoformat(),
            )
            page_token = ""
            seen_page_tokens: set[str] = set()
            seen_page_signatures: set[str] = set()
            pages_loaded = 0
            orders_loaded = 0
            for _ in range(500):
                body: dict[str, Any] = {
                    "campaignIds": [int(cid)] if cid.isdigit() else [cid],
                    "dates": {
                        "creationDateFrom": chunk_from.isoformat(),
                        "creationDateTo": chunk_to.isoformat(),
                    },
                }
                params: dict[str, str] = {}
                if page_token:
                    params["pageToken"] = page_token
                resp = await client.post(url, headers=_ym_headers(api_key), params=params, json=body)
                if not resp.is_success:
                    logger.warning(
                        "[sales_overview] business orders failed business_id=%s campaign_id=%s date_from=%s date_to=%s status=%s page=%s body=%s",
                        bid,
                        cid,
                        chunk_from.isoformat(),
                        chunk_to.isoformat(),
                        resp.status_code,
                        pages_loaded + 1,
                        resp.text[:800],
                    )
                resp.raise_for_status()
                orders, next_page_token = _extract_response_orders(resp.json())
                pages_loaded += 1
                orders_loaded += len(orders)
                page_order_ids = [
                    str(order.get("id") or order.get("orderId") or "").strip()
                    for order in orders
                    if isinstance(order, dict) and str(order.get("id") or order.get("orderId") or "").strip()
                ]
                page_signature = "|".join(page_order_ids)
                if page_signature and page_signature in seen_page_signatures:
                    break
                if page_signature:
                    seen_page_signatures.add(page_signature)
                for order in orders:
                    order_id = str(order.get("id") or order.get("orderId") or "").strip()
                    created = _parse_business_order_created_date(
                        order.get("creationDate") or order.get("createdAt") or order.get("created")
                    )
                    if order_id and created:
                        out[order_id] = created
                if not next_page_token or next_page_token == page_token or next_page_token in seen_page_tokens:
                    break
                seen_page_tokens.add(next_page_token)
                page_token = next_page_token
            logger.warning(
                "[sales_overview] business orders chunk finished business_id=%s campaign_id=%s date_from=%s date_to=%s pages=%s orders=%s",
                bid,
                cid,
                chunk_from.isoformat(),
                chunk_to.isoformat(),
                pages_loaded,
                orders_loaded,
            )
            chunk_from = chunk_to + timedelta(days=1)
    return out


def _normalize_rows(rows: list[dict[str, Any]], *, report_id: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        order_id = str(_pick_value(row, "ORDER_ID", "orderId", "order_id", "order", "номер заказа") or "").strip()
        sku = str(_pick_value(row, "SHOP_SKU", "shopSku", "offerId", "sku", "shop_sku", "ваш sku", "вашsku") or "").strip()
        item_name = str(_pick_value(row, "OFFER_NAME", "offerName", "itemName", "наименование товара", "название") or "").strip()
        created_raw = _pick_value(
            row,
            "CREATION_DATE",
            "creationDate",
            "orderCreationDate",
            "createdAt",
            "created_at",
            "дата оформления",
            "датаоформления",
        )
        created_at, created_date = _parse_iso_datetime(created_raw)
        shipment_raw = _pick_value(
            row,
            "SHIPMENT_DATE",
            "shipmentDate",
            "shipment_date",
            "shipDate",
            "дата отгрузки",
            "датаотгрузки",
        )
        _shipment_at, shipment_date = _parse_iso_datetime(shipment_raw)
        delivery_raw = _pick_value(
            row,
            "DELIVERY_DATE",
            "deliveryDate",
            "delivery_date",
            "orderDeliveryDate",
            "дата доставки",
            "датадоставки",
        )
        _delivery_at, delivery_date = _parse_iso_datetime(delivery_raw)
        item_status = str(
            _pick_value(
                row,
                "OFFER_STATUS",
                "offerStatus",
                "itemStatus",
                "status",
                "статус товара",
                "статустовара",
                "orderStatus",
            )
            or ""
        ).strip()
        if not order_id.strip() or not sku.strip() or not created_at or not created_date:
            continue
        out.append(
            {
                "order_id": order_id.strip(),
                "order_created_at": created_at,
                "order_created_date": created_date,
                "shipment_date": shipment_date,
                "delivery_date": delivery_date,
                "sku": sku.strip(),
                "item_name": item_name,
                "item_status": item_status,
                "source_updated_at": report_id,
                "payload": row,
            }
        )
    return out


async def _generate_report(*, business_id: str, campaign_id: str, api_key: str, date_from: str, date_to: str) -> str:
    url = f"{YANDEX_BASE_URL}/reports/united-orders/generate"
    params = {"format": "JSON", "language": "RU"}
    body = {
        "businessId": int(business_id),
        "dateFrom": date_from,
        "dateTo": date_to,
        "campaignIds": [int(campaign_id)],
    }
    async with httpx.AsyncClient(timeout=90) as client:
        logger.warning(
            "[sales_overview] united orders generate request business_id=%s campaign_id=%s date_from=%s date_to=%s",
            business_id,
            campaign_id,
            date_from,
            date_to,
        )
        resp = await client.post(url, headers=_ym_headers(api_key), params=params, json=body)
        logger.warning(
            "[sales_overview] united orders generate response business_id=%s campaign_id=%s status=%s",
            business_id,
            campaign_id,
            resp.status_code,
        )
        if resp.status_code >= 400:
            logger.warning(
                "[sales_overview] united orders generate error business_id=%s campaign_id=%s body=%s",
                business_id,
                campaign_id,
                resp.text[:800],
            )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
    report_id = str(
        ((data.get("result") or {}) if isinstance(data.get("result"), dict) else {}).get("reportId")
        or data.get("reportId")
        or ""
    ).strip()
    if not report_id:
        raise RuntimeError("Маркет не вернул reportId для united orders report")
    return report_id


async def _wait_report_download_url(*, report_id: str, api_key: str) -> tuple[str, str]:
    url = f"{YANDEX_BASE_URL}/reports/info/{report_id}"
    last_error = ""
    async with httpx.AsyncClient(timeout=60) as client:
        for attempt in range(_REPORT_INFO_MAX_ATTEMPTS):
            resp = await client.get(url, headers=_ym_headers(api_key))
            if resp.status_code >= 400:
                last_error = resp.text[:400]
                resp.raise_for_status()
            data = resp.json() if resp.content else {}
            result = data.get("result") if isinstance(data.get("result"), dict) else data
            status = str(result.get("status") or data.get("status") or "").strip().upper()
            file_url = str(
                result.get("file")
                or result.get("url")
                or result.get("fileUrl")
                or result.get("downloadUrl")
                or ""
            ).strip()
            file_name = str(result.get("fileName") or result.get("filename") or "").strip()
            logger.warning(
                "[sales_overview] united orders report info report_id=%s attempt=%s status=%s file=%s",
                report_id,
                attempt + 1,
                status or "-",
                "set" if file_url else "-",
            )
            if file_url:
                return file_url, file_name
            if status in {"FAILED", "ERROR"}:
                raise RuntimeError(f"Маркет не смог подготовить united orders report: {data}")
            await asyncio.sleep(_REPORT_INFO_POLL_INTERVAL_SEC)
    raise RuntimeError(f"Не удалось дождаться united orders report: {last_error or report_id}")


async def refresh_yandex_united_orders_history_for_store(
    *,
    store_uid: str,
    campaign_id: str,
    date_from: str,
    date_to: str,
) -> dict[str, Any]:
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        raise ValueError(f"credentials_not_found:{campaign_id}")
    business_id, _campaign_id, api_key = creds
    report_id = await _generate_report(
        business_id=business_id,
        campaign_id=campaign_id,
        api_key=api_key,
        date_from=date_from,
        date_to=date_to,
    )
    file_url, file_name = await _wait_report_download_url(report_id=report_id, api_key=api_key)
    async with httpx.AsyncClient(timeout=180) as client:
        file_resp = await client.get(file_url)
        file_resp.raise_for_status()
    raw_rows = _parse_report_bytes(file_resp.content)
    rows = _normalize_rows(raw_rows, report_id=report_id)
    try:
        created_map = await _fetch_yandex_business_order_created_map(
            business_id=business_id,
            campaign_id=campaign_id,
            api_key=api_key,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as exc:
        created_map = {}
        logger.warning(
            "[sales_overview] business orders created_at overlay skipped store_uid=%s campaign_id=%s error=%s",
            store_uid,
            campaign_id,
            exc,
        )
    if created_map:
        for row in rows:
            order_id = str(row.get("order_id") or "").strip()
            created = created_map.get(order_id)
            if not order_id or not created:
                continue
            row["order_created_at"] = created[0]
            row["order_created_date"] = created[1]
    try:
        from_date_obj = date.fromisoformat(str(date_from).strip())
        to_date_obj = date.fromisoformat(str(date_to).strip())
        rows = [
            row
            for row in rows
            if (
                str(row.get("order_created_date") or "").strip()
                and from_date_obj
                <= date.fromisoformat(str(row.get("order_created_date") or "").strip())
                <= to_date_obj
            )
        ]
    except Exception:
        pass
    loaded = replace_sales_united_order_transactions_for_period(
        store_uid=store_uid,
        platform="yandex_market",
        date_from=date_from,
        date_to=date_to,
        rows=rows,
    )
    logger.warning(
        "[sales_overview] united orders loaded store_uid=%s campaign_id=%s report_id=%s rows=%s file_name=%s",
        store_uid,
        campaign_id,
        report_id,
        loaded,
        file_name,
    )
    return {
        "store_uid": store_uid,
        "campaign_id": campaign_id,
        "report_id": report_id,
        "rows": loaded,
        "file_name": file_name,
    }


async def _refresh_yandex_live_orders_for_store(
    *,
    store_uid: str,
    campaign_id: str,
    date_from: str,
    date_to: str,
) -> dict[str, Any]:
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        raise ValueError(f"credentials_not_found:{campaign_id}")
    business_id, _campaign_id, api_key = creds
    from_day = date.fromisoformat(str(date_from).strip())
    to_day = date.fromisoformat(str(date_to).strip())
    rows = await _fetch_yandex_business_orders_for_range(
        business_id=business_id,
        campaign_id=campaign_id,
        api_key=api_key,
        date_from=from_day,
        date_to=to_day,
    )
    loaded = replace_sales_market_order_items_for_period(
        store_uid=store_uid,
        platform="yandex_market",
        date_from=date_from,
        date_to=date_to,
        rows=rows,
    )
    logger.warning(
        "[sales_overview] live business orders loaded store_uid=%s campaign_id=%s rows=%s",
        store_uid,
        campaign_id,
        loaded,
    )
    return {
        "store_uid": store_uid,
        "campaign_id": campaign_id,
        "rows": loaded,
        "mode": "live_orders",
    }


async def refresh_sales_overview_history(
    *,
    date_from: str = DEFAULT_HISTORY_DATE_FROM,
    date_to: str = "",
    store_uids: list[str] | None = None,
) -> dict[str, Any]:
    history_cutoff = _history_month_cutoff_date()
    raw_to_date = _parse_date_any(str(date_to or "").strip()) or _parse_date_any(_default_history_date_to()) or history_cutoff
    to_date_obj = min(raw_to_date, history_cutoff)
    from_date_obj = _parse_date_any(str(date_from or "").strip()) or _parse_date_any(DEFAULT_HISTORY_DATE_FROM) or history_cutoff
    if from_date_obj > to_date_obj:
        return {
            "ok": True,
            "date_from": from_date_obj.isoformat(),
            "date_to": to_date_obj.isoformat(),
            "stores": [],
            "errors": [],
            "cogs_refresh": {"ok": True, "rows": 0},
            "orders_rows_refresh": [],
        }
    date_from = from_date_obj.isoformat()
    to_date = to_date_obj.isoformat()
    stores = [
        store for store in _catalog_marketplace_stores_context()
        if str(store.get("platform") or "").strip().lower() == "yandex_market"
    ]
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [store for store in stores if str(store.get("store_uid") or "").strip() in selected]
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для united orders report")

    async def _run_store(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        if not store_uid or not campaign_id:
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": "store_uid_or_campaign_id_missing"}
        try:
            result = await refresh_yandex_united_orders_history_for_store(
                store_uid=store_uid,
                campaign_id=campaign_id,
                date_from=date_from,
                date_to=to_date,
            )
            return {"ok": True, "result": result}
        except Exception as exc:
            logger.warning(
                "[sales_overview] united orders refresh failed store_uid=%s campaign_id=%s error=%s",
                store_uid,
                campaign_id,
                exc,
            )
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    settled = await asyncio.gather(*[_run_store(store) for store in stores])
    results: list[dict[str, Any]] = [item["result"] for item in settled if item.get("ok")]
    errors: list[dict[str, str]] = [
        {
            "store_uid": str(item.get("store_uid") or "").strip(),
            "campaign_id": str(item.get("campaign_id") or "").strip(),
            "error": str(item.get("error") or "").strip(),
        }
        for item in settled
        if not item.get("ok")
    ]

    if not results:
        raise RuntimeError(
            "Не удалось обновить ни один магазин Маркета: "
            + "; ".join(f"{x['campaign_id']}: {x['error']}" for x in errors)
        )
    cogs_refresh = refresh_sales_overview_cogs_sources()
    rebuilt: list[dict[str, Any]] = []
    month_start = datetime.now(MSK).date().replace(day=1).isoformat()
    for item in results:
        store_uid = str(item.get("store_uid") or "").strip()
        if not store_uid:
            continue
        built = await _build_sales_overview_order_rows_for_store(store_uid=store_uid)
        built_rows = list(built.get("rows") or [])
        history_rows = [row for row in built_rows if str(row.get("order_created_date") or "").strip() < month_start]
        hot_rows = [row for row in built_rows if str(row.get("order_created_date") or "").strip() >= month_start]
        rebuilt_rows = replace_sales_overview_order_rows(store_uid=store_uid, rows=history_rows)
        replace_sales_overview_order_rows_hot(
            store_uid=store_uid,
            rows=hot_rows,
            replace_all=False,
            date_from=month_start,
            date_to=datetime.now(MSK).date().isoformat(),
        )
        pruned_cogs_rows = prune_pricing_cogs_snapshots_for_store(store_uid=store_uid, keep_recent_days=3)
        rebuilt.append({"store_uid": store_uid, "rows": rebuilt_rows, "pruned_cogs_rows": pruned_cogs_rows})
    return {
        "ok": True,
        "date_from": date_from,
        "date_to": to_date,
        "stores": results,
        "errors": errors,
        "cogs_refresh": cogs_refresh,
        "orders_rows_refresh": rebuilt,
    }


async def refresh_sales_overview_order_rows_for_store(*, store_uid: str) -> dict[str, Any]:
    built = await _build_sales_overview_order_rows_for_store(store_uid=store_uid)
    month_start = datetime.now(MSK).date().replace(day=1).isoformat()
    today_key = datetime.now(MSK).date().isoformat()
    built_rows = list(built.get("rows") or [])
    history_rows = [row for row in built_rows if str(row.get("order_created_date") or "").strip() < month_start]
    hot_rows = [row for row in built_rows if str(row.get("order_created_date") or "").strip() >= month_start]
    rows_count = replace_sales_overview_order_rows(store_uid=store_uid, rows=history_rows)
    replace_sales_overview_order_rows_hot(
        store_uid=store_uid,
        rows=hot_rows,
        replace_all=False,
        date_from=month_start,
        date_to=today_key,
    )
    pruned_cogs_rows = prune_pricing_cogs_snapshots_for_store(store_uid=store_uid, keep_recent_days=3)
    return {"ok": True, "store_uid": store_uid, "rows": rows_count, "pruned_cogs_rows": pruned_cogs_rows}


async def refresh_sales_overview_order_rows_current_month_for_store(*, store_uid: str) -> dict[str, Any]:
    month_start = datetime.now(MSK).date().replace(day=1).isoformat()
    today_key = datetime.now(MSK).date().isoformat()
    campaign_id = _resolve_store_campaign_id(store_uid)
    if campaign_id:
        await _refresh_yandex_live_orders_for_store(
            store_uid=store_uid,
            campaign_id=campaign_id,
            date_from=month_start,
            date_to=today_key,
        )
        try:
            refresh_sales_overview_cogs_source_for_store(store_uid=store_uid)
        except Exception:
            pass
    built = await _build_sales_overview_order_rows_for_store(store_uid=store_uid)
    rows = [
        row for row in list(built.get("rows") or [])
        if month_start <= str(row.get("order_created_date") or "").strip() <= today_key
    ]
    rows_count = replace_sales_overview_order_rows_hot(
        store_uid=store_uid,
        rows=rows,
        replace_all=False,
        date_from=month_start,
        date_to=today_key,
    )
    return {
        "ok": True,
        "store_uid": store_uid,
        "mode": "current_month",
        "date_from": month_start,
        "date_to": today_key,
        "rows": rows_count,
        "campaign_id": campaign_id,
    }


async def refresh_sales_overview_order_rows_today_for_store(*, store_uid: str) -> dict[str, Any]:
    today_key = datetime.now(MSK).date().isoformat()
    campaign_id = _resolve_store_campaign_id(store_uid)
    if campaign_id:
        await _refresh_yandex_live_orders_for_store(
            store_uid=store_uid,
            campaign_id=campaign_id,
            date_from=today_key,
            date_to=today_key,
        )
        try:
            refresh_sales_overview_cogs_source_for_store(store_uid=store_uid)
        except Exception:
            pass
    built = await _build_sales_overview_order_rows_for_store(store_uid=store_uid)
    rows = [
        row for row in list(built.get("rows") or [])
        if str(row.get("order_created_date") or "").strip() == today_key
    ]
    rows_count = replace_sales_overview_order_rows_hot(
        store_uid=store_uid,
        rows=rows,
        replace_all=False,
        date_from=today_key,
        date_to=today_key,
    )
    return {
        "ok": True,
        "store_uid": store_uid,
        "mode": "today",
        "date_from": today_key,
        "date_to": today_key,
        "rows": rows_count,
        "campaign_id": campaign_id,
    }


def get_sales_overview_data_flow_status(*, store_id: str = "") -> dict[str, Any]:
    sid = str(store_id or "").strip()
    store_uid = f"yandex_market:{sid}" if sid else ""
    if not store_uid:
        return {"ok": True, "flows": []}
    orders_snapshot = get_sales_overview_order_rows(store_uid=store_uid, page=1, page_size=1)
    ph = "%s" if is_postgres_backend() else "?"
    month_start = datetime.now(MSK).date().replace(day=1).isoformat()
    history_cutoff = _history_month_cutoff_date().isoformat()
    with _connect_history() as conn:
        try:
            united_bounds = conn.execute(
                f"""
                SELECT
                    MIN(order_created_date) AS min_date,
                    MAX(order_created_date) AS max_date,
                    MAX(loaded_at) AS loaded_at
                FROM sales_united_order_transactions
                WHERE store_uid = {ph}
                  AND order_created_date < {ph}
                """,
                (store_uid, month_start),
            ).fetchone()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            united_bounds = conn.execute(
                f"""
                SELECT
                    MIN(order_created_date) AS min_date,
                    MAX(order_created_date) AS max_date,
                    MAX(calculated_at) AS loaded_at
                FROM sales_overview_order_rows
                WHERE store_uid = {ph}
                  AND order_created_date < {ph}
                """,
                (store_uid, month_start),
            ).fetchone()
    today_key = datetime.now(MSK).date().isoformat()
    return {
        "ok": True,
        "store_uid": store_uid,
        "flows": [
            {
                "code": "history_once",
                "label": "Исторический слой",
                "description": "Один раз загружаем историю заказов и дальше не перетягиваем её целиком.",
                "date_from": str((united_bounds["min_date"] if united_bounds else "") or "").strip(),
                "date_to": str((united_bounds["max_date"] if united_bounds else "") or "").strip() or history_cutoff,
                "loaded_at": str((united_bounds["loaded_at"] if united_bounds else "") or "").strip(),
            },
            {
                "code": "current_month_incremental",
                "label": "Текущий месяц",
                "description": "Донасыщаем новые заказы текущего месяца и обновляем фактические данные по уже известным строкам.",
                "date_from": month_start,
                "date_to": today_key,
                "loaded_at": str(orders_snapshot.get("loaded_at") or "").strip(),
            },
            {
                "code": "today_rolling",
                "label": "В течение дня",
                "description": "Оперативно подтягиваем новые заказы дня и считаем предварительную экономику до получения факта.",
                "date_from": today_key,
                "date_to": today_key,
                "loaded_at": str(orders_snapshot.get("loaded_at") or "").strip(),
            },
        ],
    }


def get_sales_overview_context() -> dict[str, Any]:
    stores = [
        {
            "store_uid": str(store.get("store_uid") or "").strip(),
            "store_id": str(store.get("store_id") or "").strip(),
            "platform": str(store.get("platform") or "").strip(),
            "platform_label": str(store.get("platform_label") or "").strip(),
            "label": str(store.get("store_name") or store.get("label") or store.get("store_id") or "").strip(),
            "currency_code": str(store.get("currency_code") or "RUB").strip().upper() or "RUB",
        }
        for store in _catalog_marketplace_stores_context()
        if str(store.get("platform") or "").strip().lower() == "yandex_market"
    ]
    return {"ok": True, "marketplace_stores": stores}


def _parse_date_any(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _parse_datetime_any(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=MSK) if dt.tzinfo is None else dt
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.replace(tzinfo=MSK) if dt.tzinfo is None else dt
    except Exception:
        return None


def _period_range(*, period: str, custom_date_from: str = "", custom_date_to: str = "", min_date: str = "") -> tuple[str, str]:
    today = datetime.now(MSK).date()
    period_norm = str(period or "month").strip().lower()
    if period_norm == "today":
        start = end = today
    elif period_norm == "yesterday":
        start = end = today - timedelta(days=1)
    elif period_norm == "week":
        start, end = today - timedelta(days=6), today
    elif period_norm == "month":
        start, end = today - timedelta(days=29), today
    elif period_norm == "quarter":
        start, end = today - timedelta(days=89), today
    elif period_norm == "custom":
        start = _parse_date_any(custom_date_from) or _parse_date_any(min_date) or today
        end = _parse_date_any(custom_date_to) or today
    else:
        start = _parse_date_any(min_date) or today
        end = today
    if start > end:
        start, end = end, start
    return start.isoformat(), end.isoformat()


def _status_kind(status: str) -> str:
    norm = str(status or "").strip().lower()
    if "возврат" in norm:
        return "return"
    if "отмен" in norm or "невыкуп" in norm:
        return "ignore"
    if norm == _DELIVERED_STATUS:
        return "delivered"
    if norm in {"оформлен", "отгружен"}:
        return "open"
    return "other"


def _abs_amount(value: Any) -> float:
    num = _parse_decimal(value)
    return round(abs(float(num or 0.0)), 4)


async def _convert_rub_amount_for_store_currency(amount: float, *, currency_code: str, calc_date: date | None) -> float:
    value = float(amount or 0.0)
    code = str(currency_code or "RUB").strip().upper() or "RUB"
    if value <= 0 or code != "USD":
        return round(value, 4)
    rate_day = calc_date or _today_msk()
    rate = await _get_cbr_usd_rub_rate_for_date(rate_day)
    if not rate or rate <= 0:
        return round(value, 4)
    return round(value / float(rate), 4)


def _tracking_status_allowed(status: str, *, mode: str = "created") -> bool:
    status_kind = _status_kind(status)
    if str(mode or "").strip().lower() == "delivery":
        return status_kind == "delivered"
    return status_kind in {"delivered", "open"}


def _service_bucket(name: str) -> str:
    norm = str(name or "").strip().lower()
    if norm == "размещение товарных предложений":
        return "commission"
    if norm == "перевод платежа":
        return "acquiring_transfer"
    if norm == "приём платежа":
        return "acquiring_acceptance"
    if norm in {"начисления за доставку", "отгрузка или доставка не вовремя"}:
        return "delivery"
    if norm in {"буст продаж, оплата за продажи", "отзывы за баллы"}:
        return "ads"
    if norm in {"полка", "буст продаж, оплата за показы"}:
        return "extra_ads"
    if norm == "отмена заказа по вине продавца":
        return "operational_error"
    return ""


def _empty_actual_costs_bucket() -> dict[str, float]:
    return {
        "commission": 0.0,
        "acquiring_transfer": 0.0,
        "acquiring_acceptance": 0.0,
        "delivery": 0.0,
        "ads": 0.0,
    }


def _resolve_acquiring_amount(fact: dict[str, Any], planned: dict[str, Any]) -> tuple[float, bool]:
    transfer = float(fact.get("acquiring_transfer") or 0.0)
    acceptance = float(fact.get("acquiring_acceptance") or 0.0)
    if transfer > 0 and acceptance > 0:
        return round(transfer + acceptance, 4), False
    planned_acquiring = float(planned.get("acquiring") or 0.0)
    if planned_acquiring > 0:
        return planned_acquiring, True
    return round(transfer + acceptance, 4), False


def _load_orders_scope(*, store_uid: str, item_status: str, date_from: str, date_to: str) -> tuple[list[dict[str, Any]], list[str], str, str, str]:
    from_day = _parse_date_any(date_from)
    to_day = _parse_date_any(date_to)
    ph = "%s" if is_postgres_backend() else "?"
    with _connect_history() as conn:
        statuses = conn.execute(
            f"""
            SELECT item_status
            FROM sales_united_order_transactions
            WHERE store_uid = {ph}
              AND TRIM(COALESCE(item_status, '')) <> ''
            GROUP BY item_status
            ORDER BY item_status ASC
            """,
            (store_uid,),
        ).fetchall()
        bounds = conn.execute(
            f"""
            SELECT MAX(loaded_at) AS loaded_at
            FROM sales_united_order_transactions
            WHERE store_uid = {ph}
            """,
            (store_uid,),
        ).fetchone()
        params: list[Any] = [store_uid]
        where_parts = [f"t.store_uid = {ph}"]
        if item_status:
            where_parts.append(f"t.item_status = {ph}")
            params.append(item_status)
        where_sql = "WHERE " + " AND ".join(where_parts)
        rows = conn.execute(
            f"""
            SELECT
                t.store_uid,
                t.platform,
                '' AS store_id,
                '' AS store_name,
                '' AS currency_code,
                t.order_id,
                t.order_created_at,
                t.order_created_date,
                t.sku,
                t.item_name,
                t.item_status,
                t.payload_json,
                t.source_updated_at,
                t.loaded_at
            FROM sales_united_order_transactions t
            {where_sql}
            ORDER BY t.order_created_at DESC, t.order_id DESC, t.sku ASC
            """,
            params,
        ).fetchall()
    row_dicts = [dict(row) for row in rows]
    min_day: date | None = None
    max_day: date | None = None
    filtered: list[dict[str, Any]] = []
    for row in row_dicts:
        if not _tracking_status_allowed(str(row.get("item_status") or ""), mode=mode):
            continue
        row_day = _parse_date_any(row.get("order_created_date")) or (
            (_parse_datetime_any(row.get("order_created_at")) or None).date()
            if _parse_datetime_any(row.get("order_created_at")) is not None
            else None
        )
        if row_day is not None:
            min_day = row_day if min_day is None or row_day < min_day else min_day
            max_day = row_day if max_day is None or row_day > max_day else max_day
        if from_day and row_day and row_day < from_day:
            continue
        if to_day and row_day and row_day > to_day:
            continue
        filtered.append(row)
    return (
        filtered,
        [
            str(row["item_status"] or "").strip()
            for row in statuses
            if str(row["item_status"] or "").strip() and _status_kind(str(row["item_status"] or "")) != "ignore"
        ],
        min_day.isoformat() if min_day else "",
        max_day.isoformat() if max_day else "",
        str((bounds["loaded_at"] if bounds else "") or "").strip(),
    )


def _load_netting_scope(*, store_uid: str, date_from: str, date_to: str) -> list[dict[str, Any]]:
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT payload_json
            FROM sales_united_netting_report_rows
            WHERE store_uid = {ph}
              AND report_date_to >= {ph}
              AND report_date_from <= {ph}
            """,
            (store_uid, date_from, date_to),
        ).fetchall()
    out: list[dict[str, Any]] = []
    seen_payloads: set[str] = set()
    for row in rows:
        raw_payload = str(row["payload_json"] or "").strip()
        if not raw_payload or raw_payload in seen_payloads:
            continue
        seen_payloads.add(raw_payload)
        try:
            payload = json.loads(raw_payload or "{}")
        except Exception:
            continue
        if isinstance(payload, dict):
            out.append(payload)
    return out


def _map_live_order_status(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    mapping = {
        "DELIVERED": "Доставлен покупателю",
        "PROCESSING": "Оформлен",
        "PENDING": "Оформлен",
        "PICKUP": "Отгружен",
        "DELIVERY": "Отгружен",
        "CANCELLED": "Отменен",
        "CANCELED": "Отменен",
        "UNPAID": "Оформлен",
    }
    return mapping.get(raw, str(value or "").strip())


def _load_live_current_month_orders(*, store_uid: str) -> list[dict[str, Any]]:
    today = datetime.now(MSK).date()
    month_start = today.replace(day=1).isoformat()
    today_key = today.isoformat()
    ph = "%s" if is_postgres_backend() else "?"
    with _connect_history() as conn:
        rows = conn.execute(
            f"""
            SELECT
                store_uid,
                order_id,
                order_created_at,
                order_created_date,
                sku,
                item_name,
                order_status,
                sale_price,
                payment_price,
                subsidy_amount,
                loaded_at
            FROM sales_market_order_items
            WHERE store_uid = {ph}
              AND order_created_date >= {ph}
              AND order_created_date <= {ph}
            ORDER BY order_created_at DESC, order_id DESC, sku ASC
            """,
            (store_uid, month_start, today_key),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        out.append(
            {
                "store_uid": str(item.get("store_uid") or "").strip(),
                "platform": "yandex_market",
                "order_id": str(item.get("order_id") or "").strip(),
                "order_created_at": str(item.get("order_created_at") or "").strip(),
                "order_created_date": str(item.get("order_created_date") or "").strip(),
                "sku": str(item.get("sku") or "").strip(),
                "item_name": str(item.get("item_name") or "").strip(),
                "item_status": _map_live_order_status(item.get("order_status")),
                "sale_price": item.get("sale_price"),
                "payment_price": item.get("payment_price"),
                "subsidy_amount": item.get("subsidy_amount"),
                "payload_json": "",
                "source_updated_at": str(item.get("loaded_at") or "").strip(),
                "loaded_at": str(item.get("loaded_at") or "").strip(),
                "shipment_date": "",
                "delivery_date": "",
            }
        )
    return out


def _ads_report_real_cost(payload: dict[str, Any]) -> float:
    real_cost = _parse_decimal(payload.get("realCost"))
    cost = _parse_decimal(payload.get("cost"))
    if real_cost is not None and real_cost > 0:
        return round(float(real_cost), 4)
    if cost is not None and cost > 0:
        return round(float(cost), 4)
    return 0.0


def _load_extra_ads_scope(*, store_uid: str, date_from: str, date_to: str) -> dict[str, float]:
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        shelves = conn.execute(
            f"""
            SELECT report_date_from, report_date_to, payload_json
            FROM sales_shelfs_statistics_report_rows
            WHERE store_uid = {ph}
              AND report_date_to >= {ph}
              AND report_date_from <= {ph}
            """,
            (store_uid, date_from, date_to),
        ).fetchall()
        shows = conn.execute(
            f"""
            SELECT report_date_from, report_date_to, payload_json
            FROM sales_shows_boost_report_rows
            WHERE store_uid = {ph}
              AND report_date_to >= {ph}
              AND report_date_from <= {ph}
            """,
            (store_uid, date_from, date_to),
        ).fetchall()
    by_day: dict[str, float] = {}
    by_day_rank: dict[str, tuple[str, str, str]] = {}
    for row in [*shelves, *shows]:
        try:
            payload = json.loads(str(row["payload_json"] or "").strip() or "{}")
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        day = _parse_date_any(payload.get("date"))
        amount = _ads_report_real_cost(payload)
        if day is None or amount <= 0:
            continue
        day_key = day.isoformat()
        # Reports are cumulative and overlap by date range; for a given day we
        # keep only the latest snapshot instead of summing the same day across
        # report windows like 01-16, 01-17, 01-18.
        report_to = str(row["report_date_to"] or "").strip()
        report_from = str(row["report_date_from"] or "").strip()
        payload_key = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        rank = (report_to, report_from, payload_key)
        current_rank = by_day_rank.get(day_key)
        if current_rank is None or rank > current_rank:
            by_day_rank[day_key] = rank
            by_day[day_key] = round(float(amount), 4)
    return by_day


def _load_snapshot_cogs_by_order(*, store_uid: str, orders: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    groups: dict[str, list[datetime]] = {}
    for row in orders:
        sku = str(row.get("sku") or "").strip()
        created_at = _parse_datetime_any(row.get("order_created_at")) or _parse_datetime_any(row.get("order_created_date"))
        if sku and created_at is not None:
            groups.setdefault(sku, []).append(created_at.astimezone(MSK))
    if not groups:
        return {}
    skus = list(groups.keys())
    placeholders = _placeholders(len(skus))
    max_cutoff = max(max(values) for values in groups.values()).replace(minute=59, second=59, microsecond=999999).astimezone(timezone.utc).isoformat()
    ph = "%s" if is_postgres_backend() else "?"
    with _connect_history() as conn:
        rows = conn.execute(
            f"""
            SELECT sku, snapshot_at, cogs_value
            FROM pricing_cogs_snapshots
            WHERE store_uid = {ph} AND sku IN ({placeholders}) AND snapshot_at <= {ph}
            ORDER BY sku ASC, snapshot_at ASC
            """,
            [store_uid, *skus, max_cutoff],
        ).fetchall()
    by_sku: dict[str, list[tuple[datetime, float]]] = {}
    for row in rows:
        try:
            snap_dt = datetime.fromisoformat(str(row["snapshot_at"]).replace("Z", "+00:00")).astimezone(MSK)
            cogs_value = float(row["cogs_value"])
        except Exception:
            continue
        by_sku.setdefault(str(row["sku"] or "").strip(), []).append((snap_dt, cogs_value))
    out: dict[tuple[str, str], float] = {}
    for row in orders:
        order_id = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        created_at = _parse_datetime_any(row.get("order_created_at")) or _parse_datetime_any(row.get("order_created_date"))
        if not order_id or not sku or created_at is None:
            continue
        cutoff = created_at.astimezone(MSK).replace(minute=59, second=59, microsecond=999999)
        chosen: float | None = None
        for snap_dt, value in by_sku.get(sku, []):
            if snap_dt <= cutoff:
                chosen = value
            else:
                break
        if chosen is not None:
            out[(order_id, sku)] = chosen
    return out


def _snapshot_fallback_metrics(*, store_uid: str, orders: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], float], dict[str, float], dict[str, float]]:
    per_order = _load_snapshot_cogs_by_order(store_uid=store_uid, orders=orders)
    day_values: dict[str, list[float]] = {}
    latest_by_sku: dict[str, tuple[datetime, float]] = {}
    for row in orders:
        order_id = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        order_day = _parse_date_any(row.get("order_created_date")) or (
            (_parse_datetime_any(row.get("order_created_at")) or datetime.now(MSK)).date()
        )
        value = per_order.get((order_id, sku))
        if value is not None:
            day_values.setdefault(order_day.isoformat(), []).append(float(value))
    if not day_values and not per_order:
        skus = sorted({str(row.get("sku") or "").strip() for row in orders if str(row.get("sku") or "").strip()})
        if skus:
            placeholders = _placeholders(len(skus))
            ph = "%s" if is_postgres_backend() else "?"
            with _connect_history() as conn:
                snap_rows = conn.execute(
                    f"""
                    SELECT sku, snapshot_at, cogs_value
                    FROM pricing_cogs_snapshots
                    WHERE store_uid = {ph} AND sku IN ({placeholders})
                    ORDER BY snapshot_at DESC
                    """,
                    [store_uid, *skus],
                ).fetchall()
            for snap in snap_rows:
                sku = str(snap["sku"] or "").strip()
                if sku in latest_by_sku:
                    continue
                try:
                    snap_dt = datetime.fromisoformat(str(snap["snapshot_at"]).replace("Z", "+00:00"))
                    cogs_value = float(snap["cogs_value"])
                except Exception:
                    continue
                latest_by_sku[sku] = (snap_dt, cogs_value)
        return per_order, {}, {sku: value for sku, (_dt, value) in latest_by_sku.items()}
    day_avg = {day: (sum(values) / len(values)) for day, values in day_values.items() if values}
    skus = sorted({str(row.get("sku") or "").strip() for row in orders if str(row.get("sku") or "").strip()})
    if skus:
        placeholders = _placeholders(len(skus))
        ph = "%s" if is_postgres_backend() else "?"
        with _connect_history() as conn:
            snap_rows = conn.execute(
                f"""
                SELECT sku, snapshot_at, cogs_value
                FROM pricing_cogs_snapshots
                WHERE store_uid = {ph} AND sku IN ({placeholders})
                ORDER BY snapshot_at DESC
                """,
                [store_uid, *skus],
            ).fetchall()
        for snap in snap_rows:
            sku = str(snap["sku"] or "").strip()
            if sku in latest_by_sku:
                continue
            try:
                snap_dt = datetime.fromisoformat(str(snap["snapshot_at"]).replace("Z", "+00:00"))
                cogs_value = float(snap["cogs_value"])
            except Exception:
                continue
            latest_by_sku[sku] = (snap_dt, cogs_value)
    return per_order, day_avg, {sku: value for sku, (_dt, value) in latest_by_sku.items()}


def _load_sales_overview_order_fact_rows(*, store_uid: str) -> list[dict[str, Any]]:
    ph = "%s" if is_postgres_backend() else "?"
    month_start = datetime.now(MSK).date().replace(day=1).isoformat()
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    with _connect_history() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM sales_overview_order_rows
            WHERE store_uid = {ph}
              AND order_created_date < {ph}
            ORDER BY COALESCE(order_created_at, order_created_date) ASC, order_id ASC
            """,
            (store_uid, month_start),
        ).fetchall()
    for row in rows:
        item = dict(row)
        key = (str(item.get("order_id") or "").strip(), str(item.get("sku") or "").strip())
        if all(key):
            merged[key] = item
    try:
        with _connect() as conn:
            hot_rows = conn.execute(
                f"""
                SELECT *
                FROM sales_overview_order_rows_hot
                WHERE store_uid = {ph}
                ORDER BY COALESCE(order_created_at, order_created_date) ASC, order_id ASC
                """,
                (store_uid,),
            ).fetchall()
        for row in hot_rows:
            item = dict(row)
            key = (str(item.get("order_id") or "").strip(), str(item.get("sku") or "").strip())
            if all(key):
                merged[key] = item
    except Exception:
        pass
    result = list(merged.values())
    result.sort(key=lambda item: (str(item.get("order_created_at") or item.get("order_created_date") or ""), str(item.get("order_id") or ""), str(item.get("sku") or "")))
    return result


def _tracking_anchor_day(row: dict[str, Any], *, mode: str) -> date | None:
    created_day = _parse_date_any(row.get("order_created_date")) or (
        (_parse_datetime_any(row.get("order_created_at")) or datetime.now(MSK)).date()
    )
    if mode == "delivery":
        delivery_day = _parse_date_any(row.get("delivery_date"))
        if delivery_day:
            return delivery_day
        return None
    return created_day


def _tracking_include_financials(
    row: dict[str, Any],
    *,
    mode: str = "created",
    month_key: tuple[int, int],
    active_pair: tuple[int, int],
    anchor: date | None = None,
) -> bool:
    status_kind = _status_kind(str(row.get("item_status") or ""))
    if status_kind == "delivered":
        return True
    if str(mode or "").strip().lower() == "delivery":
        return False
    if status_kind != "open":
        return False
    if month_key == active_pair:
        return True
    anchor_day = anchor or _tracking_anchor_day(row, mode="created")
    if not anchor_day:
        return False
    # Keep recent cross-month days visible in operational trends.
    return anchor_day >= (datetime.now(MSK).date() - timedelta(days=31))


def _tracking_period_key(anchor: date, *, grain: str) -> tuple[str, str]:
    if str(grain or "").strip().lower() == "day":
        key = anchor.isoformat()
        return key, key
    key = f"{anchor.year}-{anchor.month:02d}"
    label = f"{anchor.month:02d}.{anchor.year}"
    return key, label


def _resolve_category_path_for_sku(sku: str, path_map: dict[str, dict[str, Any]]) -> str:
    leaf = str(((path_map.get(sku) or {}) if isinstance(path_map.get(sku), dict) else {}).get("leaf_path") or "").strip()
    return leaf or "Не определено"


def _is_problem_order_row(row: dict[str, Any]) -> bool:
    status_kind = _status_kind(str(row.get("item_status") or ""))
    cogs_value = row.get("cogs_price")
    return status_kind == "delivered" and cogs_value in (None, "")


def _planned_cost_context(store_uid: str, skus: list[str]) -> dict[str, Any]:
    tree = get_pricing_category_tree(store_uid=store_uid)
    dataset_key = str(tree.get("dataset_key") or "").strip() if isinstance(tree, dict) else ""
    return {
        "category_settings": get_pricing_category_settings_map(dataset_key=dataset_key, store_uid=store_uid) if dataset_key else {},
        "store_settings": get_pricing_store_settings(store_uid=store_uid) or {},
        "logistics_store": get_pricing_logistics_store_settings(store_uid=store_uid) or {},
        "logistics_product": get_pricing_logistics_product_settings_map(store_uid=store_uid, skus=skus) if skus else {},
        "path_map": get_pricing_catalog_sku_path_map(priority_platform="yandex_market"),
    }


def _load_strategy_snapshot_map(*, store_uid: str, orders: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    order_points: dict[tuple[str, str], datetime] = {}
    sku_set: set[str] = set()
    for row in orders:
        order_id = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        created_at = _parse_datetime_any(row.get("order_created_at")) or _parse_datetime_any(row.get("order_created_date"))
        if not order_id or not sku or created_at is None:
            continue
        created_at = created_at.astimezone(MSK)
        key = (order_id, sku)
        order_points[key] = created_at
        sku_set.add(sku)
    if not order_points or not sku_set:
        return {}
    history_rows = get_pricing_strategy_history_rows(
        store_uid=store_uid,
        skus=sorted(sku_set),
    )
    by_sku: dict[str, list[tuple[datetime, dict[str, Any]]]] = {}
    for row in history_rows:
        sku = str(row.get("sku") or "").strip()
        raw_dt = str(row.get("captured_at") or "").strip()
        if not sku or not raw_dt:
            continue
        try:
            snap_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
        except Exception:
            continue
        if snap_dt.tzinfo is None:
            snap_dt = snap_dt.replace(tzinfo=timezone.utc)
        by_sku.setdefault(sku, []).append((snap_dt.astimezone(MSK), row))
    resolved: dict[tuple[str, str], dict[str, Any]] = {}
    for key, created_at in order_points.items():
        _order_id, sku = key
        chosen: dict[str, Any] | None = None
        for snap_dt, snap_row in by_sku.get(sku, []):
            if snap_dt <= created_at:
                chosen = snap_row
                continue
            break
        if chosen is not None:
            resolved[key] = chosen
    return resolved


def _strategy_snapshot_from_existing_order_row(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    snapshot_at = str(row.get("strategy_snapshot_at") or "").strip()
    if not snapshot_at:
        return {}
    return {
        "cycle_started_at": str(row.get("strategy_cycle_started_at") or "").strip(),
        "captured_at": snapshot_at,
        "installed_price": row.get("strategy_installed_price"),
        "boost_bid_percent": row.get("strategy_boost_bid_percent"),
        "market_boost_bid_percent": row.get("strategy_market_boost_bid_percent"),
        "boost_share": row.get("strategy_boost_share"),
        "decision_code": str(row.get("strategy_decision_code") or "").strip(),
        "decision_label": str(row.get("strategy_decision_label") or "").strip(),
        "control_state": str(row.get("strategy_control_state") or "").strip(),
        "attractiveness_status": str(row.get("strategy_attractiveness_status") or "").strip(),
        "promo_count": int(row.get("strategy_promo_count") or 0),
        "coinvest_pct": row.get("strategy_coinvest_pct"),
        "selected_iteration_code": str(row.get("strategy_selected_iteration_code") or "").strip(),
        "uses_promo": bool(row.get("strategy_uses_promo")),
        "market_promo_status": str(row.get("strategy_market_promo_status") or "").strip(),
    }


def _strategy_snapshot_is_complete(snapshot: dict[str, Any]) -> bool:
    if not isinstance(snapshot, dict):
        return False
    snapshot_at = str(snapshot.get("captured_at") or snapshot.get("strategy_snapshot_at") or "").strip()
    if not snapshot_at:
        return False
    decision_label = str(snapshot.get("decision_label") or snapshot.get("strategy_decision_label") or "").strip().lower()
    promo_count = int(snapshot.get("promo_count") or snapshot.get("strategy_promo_count") or 0)
    if "2 промо" in decision_label and promo_count < 2:
        return False
    if "1 промо" in decision_label and promo_count < 1:
        return False
    return True


def _iteration_rank(iteration_code: str) -> int:
    normalized = str(iteration_code or "").strip().lower()
    if normalized == "rrc_no_ads":
        return 0
    if normalized == "mrc":
        return 1
    if normalized == "rrc_with_boost":
        return 2
    return 9


def _iteration_attr_rank(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized in {"profitable", "выгодная"}:
        return 2
    if normalized in {"moderate", "умеренная"}:
        return 1
    return 0


def _decision_label_from_iteration_snapshot(snapshot: dict[str, Any]) -> str:
    promo_count = int(snapshot.get("promo_count") or 0)
    attr_rank = _iteration_attr_rank(snapshot.get("attractiveness_status"))
    boost_pct = float(_parse_decimal(snapshot.get("tested_boost_pct")) or 0.0)
    if promo_count >= 2 and attr_rank >= 2:
        return "2 промо + выгодно"
    if promo_count == 1 and attr_rank >= 2:
        return "1 промо + выгодно"
    if promo_count >= 2 and attr_rank >= 1:
        return "2 промо + умеренно"
    if promo_count == 1 and attr_rank >= 1:
        return "1 промо + умеренно"
    if boost_pct > 0.01 and attr_rank >= 2:
        return "Буст + выгодно"
    if boost_pct > 0.01 and attr_rank >= 1:
        return "Буст + умеренно"
    if attr_rank >= 2:
        return "Выгодно"
    if attr_rank >= 1:
        return "Умеренно"
    if boost_pct > 0.01:
        return "Буст"
    return "Умеренно"


def _build_strategy_snapshot_from_iterations(*, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    ordered = sorted(
        [row for row in rows if isinstance(row, dict)],
        key=lambda snapshot: (
            -min(2, max(0, int(snapshot.get("promo_count") or 0))),
            -_iteration_attr_rank(str(snapshot.get("attractiveness_status") or "")),
            _iteration_rank(str(snapshot.get("iteration_code") or "")),
        ),
    )
    selected = ordered[0] if ordered else {}
    if not isinstance(selected, dict) or not selected:
        return {}
    attr_rank = _iteration_attr_rank(selected.get("attractiveness_status"))
    captured_at_values = [
        str(item.get("captured_at") or "").strip()
        for item in ordered
        if isinstance(item, dict) and str(item.get("captured_at") or "").strip()
    ]
    captured_at = captured_at_values[-1] if captured_at_values else str(selected.get("cycle_started_at") or "").strip()
    return {
        "cycle_started_at": str(selected.get("cycle_started_at") or "").strip(),
        "captured_at": captured_at,
        "installed_price": _parse_decimal(selected.get("tested_price")),
        "boost_bid_percent": _parse_decimal(selected.get("tested_boost_pct")),
        "market_boost_bid_percent": _parse_decimal(selected.get("market_boost_bid_percent")),
        "boost_share": _parse_decimal(selected.get("boost_share")),
        "decision_code": "",
        "decision_label": _decision_label_from_iteration_snapshot(selected),
        "control_state": "",
        "attractiveness_status": "Выгодная" if attr_rank >= 2 else "Умеренная" if attr_rank >= 1 else "",
        "promo_count": int(selected.get("promo_count") or 0),
        "coinvest_pct": _parse_decimal(selected.get("coinvest_pct")),
        "selected_iteration_code": str(selected.get("iteration_code") or "").strip(),
        "uses_promo": int(selected.get("promo_count") or 0) > 0,
        "market_promo_status": "verified" if int(selected.get("promo_count") or 0) > 0 else "",
    }


def _load_strategy_iteration_snapshot_map(*, store_uid: str, orders: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    order_points: dict[tuple[str, str], datetime] = {}
    sku_set: set[str] = set()
    for row in orders:
        order_id = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        created_at = _parse_datetime_any(row.get("order_created_at")) or _parse_datetime_any(row.get("order_created_date"))
        if not order_id or not sku or created_at is None:
            continue
        order_points[(order_id, sku)] = created_at.astimezone(MSK)
        sku_set.add(sku)
    if not order_points or not sku_set:
        return {}
    placeholders = _placeholders(len(sku_set))
    with _connect_history() as conn:
        rows = conn.execute(
            f"""
            SELECT store_uid, sku, cycle_started_at, captured_at, iteration_code, tested_price, tested_boost_pct,
                   market_boost_bid_percent, boost_share, promo_count, attractiveness_status, coinvest_pct
            FROM pricing_strategy_iteration_history
            WHERE store_uid = {'%s' if is_postgres_backend() else '?'}
              AND sku IN ({placeholders})
            ORDER BY sku ASC, cycle_started_at ASC, iteration_code ASC
            """,
            [store_uid, *sorted(sku_set)],
        ).fetchall()
    by_sku_cycle: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        item = dict(row)
        sku = str(item.get("sku") or "").strip()
        cycle_started_at = str(item.get("cycle_started_at") or "").strip()
        if not sku or not cycle_started_at:
            continue
        by_sku_cycle.setdefault(sku, {}).setdefault(cycle_started_at, []).append(item)
    resolved: dict[tuple[str, str], dict[str, Any]] = {}
    for key, created_at in order_points.items():
        _order_id, sku = key
        cycle_candidates: list[tuple[datetime, str]] = []
        for cycle_started_at in (by_sku_cycle.get(sku) or {}).keys():
            try:
                cycle_dt = datetime.fromisoformat(cycle_started_at.replace("Z", "+00:00"))
            except Exception:
                continue
            if cycle_dt.tzinfo is None:
                cycle_dt = cycle_dt.replace(tzinfo=timezone.utc)
            cycle_candidates.append((cycle_dt.astimezone(MSK), cycle_started_at))
        cycle_candidates.sort(key=lambda item: item[0])
        selected_cycle_key = ""
        for cycle_dt, cycle_key in cycle_candidates:
            if cycle_dt <= created_at:
                selected_cycle_key = cycle_key
                continue
            break
        chosen_cycle = selected_cycle_key
        if not chosen_cycle:
            continue
        snapshot = _build_strategy_snapshot_from_iterations(rows=(by_sku_cycle.get(sku) or {}).get(chosen_cycle) or [])
        if snapshot:
            resolved[key] = snapshot
    return resolved


def _planned_costs_for_row(row: dict[str, Any], ctx: dict[str, Any]) -> dict[str, float | None]:
    sku = str(row.get("sku") or "").strip()
    sale_price = float(_parse_decimal(row.get("sale_price")) or 0.0)
    path_map = ctx.get("path_map") or {}
    leaf = str(((path_map.get(sku) or {}) if isinstance(path_map.get(sku), dict) else {}).get("leaf_path") or "").strip()
    st = _resolve_category_settings_for_leaf(ctx.get("category_settings"), leaf) if leaf else {}
    store_st = ctx.get("store_settings") or {}
    commission_raw = st.get("commission_percent")
    if commission_raw in (None, ""):
        commission_raw = store_st.get("commission_percent")
    acquiring_raw = st.get("acquiring_percent")
    if acquiring_raw in (None, ""):
        acquiring_raw = store_st.get("acquiring_percent")
    commission_rate = _clamp_rate(_num0(commission_raw) / 100.0)
    acquiring_rate = _clamp_rate(_num0(acquiring_raw) / 100.0)
    ads_raw = row.get("strategy_boost_bid_percent")
    ads_from_strategy = ads_raw not in (None, "")
    if ads_raw in (None, ""):
        ads_raw = st.get("ads_percent")
    if ads_raw in (None, ""):
        ads_raw = store_st.get("target_drr_percent")
    ads_rate = _clamp_rate(_num0(ads_raw) / 100.0)
    tax_raw = st.get("tax_percent")
    if tax_raw in (None, ""):
        tax_raw = store_st.get("tax_percent")
    tax_rate = _clamp_rate(_num0(tax_raw) / 100.0)
    logistics_rub = _parse_decimal(st.get("logistics_rub"))
    if logistics_rub in (None, 0):
        logistics_rub = _parse_decimal(store_st.get("logistics_rub"))
    delivery = float(logistics_rub or 0.0)
    if delivery <= 0:
        logistics_store = ctx.get("logistics_store") or {}
        product = (ctx.get("logistics_product") or {}).get(sku) or {}
        max_weight = _compute_max_weight_kg("yandex_market", product)
        if max_weight is not None:
            delivery = max(0.0, _num0(logistics_store.get("delivery_cost_per_kg")) * float(max_weight))
    return {
        "commission": round(sale_price * commission_rate, 4) if sale_price > 0 and commission_rate > 0 else None,
        "acquiring": round(sale_price * acquiring_rate, 4) if sale_price > 0 and acquiring_rate > 0 else None,
        "delivery": round(delivery, 4) if delivery > 0 else None,
        "ads": round(sale_price * ads_rate, 4) if sale_price > 0 and ads_rate > 0 else None,
        "tax": round(sale_price * tax_rate, 4) if sale_price > 0 and tax_rate > 0 else None,
        "ads_from_strategy": ads_from_strategy,
    }


def _build_netting_delivery_map(netting_rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    by_order_sku: dict[tuple[str, str], str] = {}
    by_order: dict[str, str] = {}
    for payload in netting_rows:
        order_id = str(payload.get("orderId") or payload.get("shopOrderId") or "").strip()
        if not order_id:
            continue
        sku = str(payload.get("shopSku") or "").strip()
        _delivery_at, delivery_date = _parse_iso_datetime(payload.get("orderDeliveryDate"))
        if not delivery_date:
            continue
        if sku:
            by_order_sku[(order_id, sku)] = delivery_date
        by_order[order_id] = delivery_date
    return by_order_sku, by_order


async def _build_sales_overview_order_rows_for_store(*, store_uid: str) -> dict[str, Any]:
    rows, available_statuses, min_date, max_date, loaded_at = _load_orders_scope(
        store_uid=store_uid,
        item_status="",
        date_from="1900-01-01",
        date_to="2999-12-31",
    )
    month_start = datetime.now(MSK).date().replace(day=1).isoformat()
    rows = [row for row in rows if str(row.get("order_created_date") or "").strip() < month_start]
    live_rows = _load_live_current_month_orders(store_uid=store_uid)
    if live_rows:
        merged: dict[tuple[str, str], dict[str, Any]] = {
            (str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip()): row
            for row in rows
            if str(row.get("order_id") or "").strip() and str(row.get("sku") or "").strip()
        }
        for row in live_rows:
            key = (str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip())
            if not all(key):
                continue
            prev = merged.get(key) or {}
            merged[key] = {
                **prev,
                **row,
                "shipment_date": str(prev.get("shipment_date") or row.get("shipment_date") or "").strip(),
                "delivery_date": str(prev.get("delivery_date") or row.get("delivery_date") or "").strip(),
            }
        rows = list(merged.values())
    settings = get_pricing_store_settings(store_uid=store_uid) or {}
    source_id = str(settings.get("overview_cogs_source_id") or "").strip()
    cogs_cache = get_sales_overview_cogs_source_map(store_uid=store_uid)
    cogs_rows = cogs_cache.get("rows") if isinstance(cogs_cache, dict) else None
    cogs_map = {
        (_normalize_order_key(row.get("order_key")), str(row.get("sku_key") or "").strip()): float(row.get("cogs_value"))
        for row in (cogs_rows or [])
        if str(row.get("order_key") or "").strip() and row.get("cogs_value") not in (None, "")
    }
    logger.warning(
        "[sales_overview] cogs source resolve store_uid=%s source_id=%s source_rows=%s",
        store_uid,
        source_id or "-",
        len(cogs_map),
    )
    active_store = next((store for store in _catalog_marketplace_stores_context() if str(store.get("store_uid") or "").strip() == store_uid), None)
    currency_code = str((active_store or {}).get("currency_code") or "RUB").strip().upper() or "RUB"
    snapshot_cogs, snapshot_day_avg, snapshot_latest_by_sku = _snapshot_fallback_metrics(store_uid=store_uid, orders=rows)
    order_keys = [(str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip()) for row in rows]
    existing_order_rows_map = get_sales_overview_order_rows_map(store_uid=store_uid, order_skus=order_keys)
    strategy_iteration_snapshot_map = _load_strategy_iteration_snapshot_map(
        store_uid=store_uid,
        orders=[
            row
            for row in rows
            if not _strategy_snapshot_is_complete(
                _strategy_snapshot_from_existing_order_row(
                    existing_order_rows_map.get((str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip())) or {}
                )
            )
        ],
    )
    strategy_snapshot_map = _load_strategy_snapshot_map(
        store_uid=store_uid,
        orders=[
            row
            for row in rows
            if not _strategy_snapshot_is_complete(
                _strategy_snapshot_from_existing_order_row(
                    existing_order_rows_map.get((str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip())) or {}
                )
            ) and not strategy_iteration_snapshot_map.get(
                (str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip())
            )
        ],
    )
    plan_ctx = _planned_cost_context(store_uid, [str(row.get("sku") or "").strip() for row in rows])
    netting_rows = _load_netting_scope(store_uid=store_uid, date_from=min_date or "1900-01-01", date_to=max_date or "2999-12-31")
    netting_delivery_by_order_sku, netting_delivery_by_order = _build_netting_delivery_map(netting_rows)
    order_keys = {(str(row.get("order_id") or "").strip(), str(row.get("sku") or "").strip()) for row in rows}
    order_line_counts: dict[str, int] = {}
    for row in rows:
        order_id = str(row.get("order_id") or "").strip()
        if order_id:
            order_line_counts[order_id] = int(order_line_counts.get(order_id, 0)) + 1
    actual_costs: dict[tuple[str, str], dict[str, float]] = {}
    order_level_costs: dict[str, dict[str, float]] = {}
    for payload in netting_rows:
        transaction_type = str(payload.get("transactionType") or "").strip().lower()
        if transaction_type not in {"удержание", "списание"}:
            continue
        bucket = _service_bucket(str(payload.get("offerOrServiceName") or ""))
        trans_dt = _parse_datetime_any(payload.get("transactionDate"))
        amount = await _convert_rub_amount_for_store_currency(
            _abs_amount(payload.get("transactionSum")),
            currency_code=currency_code,
            calc_date=trans_dt.astimezone(MSK).date() if trans_dt else None,
        )
        if not bucket or amount <= 0 or bucket in {"extra_ads", "operational_error"}:
            continue
        order_id = str(payload.get("orderId") or payload.get("shopOrderId") or "").strip()
        sku = str(payload.get("shopSku") or "").strip()
        if order_id and not sku and order_id in order_line_counts:
            order_bucket = order_level_costs.setdefault(order_id, _empty_actual_costs_bucket())
            order_bucket[bucket] = round(float(order_bucket[bucket]) + amount, 4)
            continue
        key = (order_id, sku)
        if key not in order_keys and (order_id, "") not in order_keys:
            continue
        bucket_obj = actual_costs.setdefault(key, _empty_actual_costs_bucket())
        bucket_obj[bucket] = round(float(bucket_obj[bucket]) + amount, 4)

    matched = 0
    materialized_rows: list[dict[str, Any]] = []
    for row in rows:
        payload_raw = str((row or {}).get("payload_json") or "").strip()
        payload: dict[str, Any] = {}
        if payload_raw:
            try:
                parsed = json.loads(payload_raw)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = {}
        billing_price = _parse_decimal(payload.get("billingPrice"))
        if billing_price is None:
            billing_price = _parse_decimal((row or {}).get("sale_price"))
        marketplace_subsidy = _parse_decimal(payload.get("marketplaceSubsidy"))
        if marketplace_subsidy is None:
            marketplace_subsidy = _parse_decimal((row or {}).get("subsidy_amount"))
        payment_price = _parse_decimal((row or {}).get("payment_price"))
        sale_price = float(billing_price or 0.0)
        sale_price_with_coinvest = (
            payment_price
            if payment_price is not None
            else (billing_price - marketplace_subsidy)
            if billing_price is not None and marketplace_subsidy is not None
            else billing_price
        )
        order_id = str((row or {}).get("order_id") or "").strip()
        sku_key = str((row or {}).get("sku") or "").strip()
        status_kind = _status_kind(str((row or {}).get("item_status") or ""))
        strategy_snapshot = (
            (
                _strategy_snapshot_from_existing_order_row(existing_order_rows_map.get((order_id, sku_key)) or {})
                if _strategy_snapshot_is_complete(_strategy_snapshot_from_existing_order_row(existing_order_rows_map.get((order_id, sku_key)) or {}))
                else {}
            )
            or strategy_iteration_snapshot_map.get((order_id, sku_key))
            or strategy_snapshot_map.get((order_id, sku_key))
            or {}
        )
        strategy_cycle_started_at = str(strategy_snapshot.get("cycle_started_at") or "").strip()
        strategy_boost_bid_percent = _parse_decimal(strategy_snapshot.get("boost_bid_percent"))
        strategy_market_boost_bid_percent = _parse_decimal(strategy_snapshot.get("market_boost_bid_percent"))
        strategy_boost_share = _parse_decimal(strategy_snapshot.get("boost_share"))
        strategy_snapshot_at = str(strategy_snapshot.get("captured_at") or "").strip()
        strategy_installed_price = _parse_decimal(strategy_snapshot.get("installed_price"))
        strategy_decision_code = str(strategy_snapshot.get("decision_code") or "").strip()
        strategy_decision_label = str(strategy_snapshot.get("decision_label") or "").strip()
        strategy_control_state = str(strategy_snapshot.get("control_state") or "").strip()
        strategy_attractiveness_status = str(strategy_snapshot.get("attractiveness_status") or "").strip()
        strategy_promo_count = int(strategy_snapshot.get("promo_count") or 0)
        strategy_coinvest_pct = _parse_decimal(strategy_snapshot.get("coinvest_pct"))
        strategy_selected_iteration_code = str(strategy_snapshot.get("selected_iteration_code") or "").strip()
        strategy_uses_promo = bool(strategy_snapshot.get("uses_promo"))
        strategy_market_promo_status = str(strategy_snapshot.get("market_promo_status") or "").strip()
        raw_cogs = cogs_map.get((_normalize_order_key(order_id), sku_key))
        if raw_cogs is None:
            raw_cogs = cogs_map.get((_normalize_order_key(order_id), ""))
        used_snapshot_cogs = False
        cogs_value: float | None = float(raw_cogs) if raw_cogs is not None else None
        strict_order_cogs_for_delivered = bool(source_id) and status_kind == "delivered"
        # For HOT/open orders we want the latest known SKU cogs, not a stale
        # hourly snapshot taken before the order was created.
        if cogs_value is None and status_kind == "open":
            cogs_value = snapshot_latest_by_sku.get(sku_key)
            used_snapshot_cogs = cogs_value is not None
        if cogs_value is None and not strict_order_cogs_for_delivered:
            cogs_value = snapshot_cogs.get((order_id, sku_key))
            used_snapshot_cogs = cogs_value is not None
        if cogs_value is None and status_kind == "delivered" and not strict_order_cogs_for_delivered:
            calc_day = str((row or {}).get("order_created_date") or "").strip()
            cogs_value = snapshot_day_avg.get(calc_day)
            used_snapshot_cogs = cogs_value is not None
        if cogs_value is None and not strict_order_cogs_for_delivered:
            cogs_value = snapshot_latest_by_sku.get(sku_key)
            used_snapshot_cogs = cogs_value is not None
        calc_date = _parse_date_any((row or {}).get("order_created_date")) or (
            (_parse_datetime_any((row or {}).get("order_created_at")) or datetime.now(MSK)).date()
        )
        if cogs_value is not None:
            cogs_value = await _convert_rub_amount_for_store_currency(
                float(cogs_value),
                currency_code=currency_code,
                calc_date=calc_date,
            )
        if cogs_value is not None:
            matched += 1
        key = (order_id, sku_key)
        fact = dict(actual_costs.get(key) or actual_costs.get((order_id, "")) or {})
        if order_id in order_level_costs and order_line_counts.get(order_id):
            share_divisor = float(order_line_counts[order_id])
            for bucket_name, bucket_value in (order_level_costs.get(order_id) or {}).items():
                if bucket_value:
                    fact[bucket_name] = round(float(fact.get(bucket_name) or 0.0) + (float(bucket_value) / share_divisor), 4)
        planned = _planned_costs_for_row(
            {
                **row,
                "sale_price": billing_price,
                "strategy_boost_bid_percent": strategy_boost_bid_percent,
            },
            plan_ctx,
        )
        planned_ads_from_strategy = bool(planned.get("ads_from_strategy"))
        if status_kind == "delivered":
            used_planned_commission = fact.get("commission") in (None, 0, 0.0) and planned.get("commission") not in (None, 0, 0.0)
            acquiring_amount, used_planned_acquiring = _resolve_acquiring_amount(fact, planned)
            used_planned_delivery = fact.get("delivery") in (None, 0, 0.0) and planned.get("delivery") not in (None, 0, 0.0)
            commission = fact.get("commission") if fact.get("commission") not in (None, 0, 0.0) else planned.get("commission")
            acquiring = acquiring_amount if acquiring_amount > 0 else None
            delivery = fact.get("delivery") if fact.get("delivery") not in (None, 0, 0.0) else planned.get("delivery")
            used_planned_ads = fact.get("ads") in (None, 0, 0.0) and planned_ads_from_strategy and planned.get("ads") not in (None, 0, 0.0)
            ads = fact.get("ads") if fact.get("ads") not in (None, 0, 0.0) else (planned.get("ads") if used_planned_ads else 0.0)
            tax = planned.get("tax")
            uses_planned_costs = bool(cogs_value is None) or used_snapshot_cogs or used_planned_commission or used_planned_acquiring or used_planned_delivery or used_planned_ads
        elif status_kind == "return":
            acquiring_amount, used_planned_acquiring = _resolve_acquiring_amount(fact, planned)
            used_planned_delivery = fact.get("delivery") in (None, 0, 0.0) and planned.get("delivery") not in (None, 0, 0.0)
            commission = None
            acquiring = acquiring_amount if acquiring_amount > 0 else None
            delivery = fact.get("delivery") if fact.get("delivery") not in (None, 0, 0.0) else planned.get("delivery")
            ads = None
            tax = planned.get("tax")
            uses_planned_costs = bool(cogs_value is None) or used_snapshot_cogs or used_planned_acquiring or used_planned_delivery
        elif status_kind == "open":
            commission = planned.get("commission")
            acquiring = planned.get("acquiring")
            delivery = planned.get("delivery")
            ads = planned.get("ads")
            tax = planned.get("tax")
            uses_planned_costs = True
        else:
            used_planned_commission = fact.get("commission") in (None, 0, 0.0) and planned.get("commission") not in (None, 0, 0.0)
            acquiring_amount, used_planned_acquiring = _resolve_acquiring_amount(fact, planned)
            used_planned_delivery = fact.get("delivery") in (None, 0, 0.0) and planned.get("delivery") not in (None, 0, 0.0)
            commission = fact.get("commission") if fact.get("commission") not in (None, 0, 0.0) else planned.get("commission")
            acquiring = acquiring_amount if acquiring_amount > 0 else None
            delivery = fact.get("delivery") if fact.get("delivery") not in (None, 0, 0.0) else planned.get("delivery")
            used_planned_ads = fact.get("ads") in (None, 0, 0.0) and planned_ads_from_strategy and planned.get("ads") not in (None, 0, 0.0)
            ads = fact.get("ads") if fact.get("ads") not in (None, 0, 0.0) else (planned.get("ads") if used_planned_ads else 0.0)
            tax = planned.get("tax")
            uses_planned_costs = bool(cogs_value is None) or used_snapshot_cogs or used_planned_commission or used_planned_acquiring or used_planned_delivery or used_planned_ads
        gross_profit = round(sale_price - float(cogs_value or 0.0), 2)
        profit = round(
            sale_price
            - float(cogs_value or 0.0)
            - float(commission or 0.0)
            - float(acquiring or 0.0)
            - float(delivery or 0.0)
            - float(ads or 0.0)
            - float(tax or 0.0),
            2,
        )
        materialized_rows.append(
            {
                "platform": "yandex_market",
                "order_created_date": str((row or {}).get("order_created_date") or "").strip(),
                "order_created_at": str((row or {}).get("order_created_at") or "").strip(),
                "shipment_date": str((row or {}).get("shipment_date") or "").strip(),
                "delivery_date": (
                    str((row or {}).get("delivery_date") or "").strip()
                    or netting_delivery_by_order_sku.get((order_id, sku_key), "")
                    or netting_delivery_by_order.get(order_id, "")
                ),
                "order_id": order_id,
                "item_status": str((row or {}).get("item_status") or "").strip(),
                "sku": sku_key,
                "item_name": str((row or {}).get("item_name") or "").strip(),
                "sale_price": billing_price,
                "gross_profit": gross_profit,
                "cogs_price": cogs_value,
                "commission": commission,
                "acquiring": acquiring,
                "delivery": delivery,
                "ads": ads,
                "tax": tax,
                "profit": profit,
                "sale_price_with_coinvest": sale_price_with_coinvest,
                "strategy_cycle_started_at": strategy_cycle_started_at,
                "strategy_market_boost_bid_percent": strategy_market_boost_bid_percent,
                "strategy_boost_share": strategy_boost_share,
                "strategy_boost_bid_percent": strategy_boost_bid_percent,
                "strategy_snapshot_at": strategy_snapshot_at,
                "strategy_installed_price": strategy_installed_price,
                "strategy_decision_code": strategy_decision_code,
                "strategy_decision_label": strategy_decision_label,
                "strategy_control_state": strategy_control_state,
                "strategy_attractiveness_status": strategy_attractiveness_status,
                "strategy_promo_count": strategy_promo_count,
                "strategy_coinvest_pct": strategy_coinvest_pct,
                "strategy_selected_iteration_code": strategy_selected_iteration_code,
                "strategy_uses_promo": strategy_uses_promo,
                "strategy_market_promo_status": strategy_market_promo_status,
                "uses_planned_costs": uses_planned_costs,
                "source_updated_at": str((row or {}).get("source_updated_at") or "").strip(),
            }
        )
    logger.warning(
        "[sales_overview] cogs source matched store_uid=%s matched_rows=%s all_rows=%s currency=%s",
        store_uid,
        matched,
        len(rows),
        currency_code,
    )
    return {
        "rows": materialized_rows,
        "available_statuses": available_statuses,
        "min_date": min_date,
        "max_date": max_date,
        "loaded_at": loaded_at,
    }


async def get_sales_overview_history(
    *,
    page: int = 1,
    page_size: int = 200,
    store_id: str = "",
    item_status: str = "",
    period: str = "month",
    date_from: str = "",
    date_to: str = "",
) -> dict[str, Any]:
    sid = str(store_id or "").strip()
    store_uid = f"yandex_market:{sid}" if sid else ""
    if not store_uid:
        return {"ok": True, "rows": [], "total_count": 0, "page": 1, "page_size": page_size, "available_statuses": [], "min_date": "", "max_date": "", "loaded_at": "", "kpis": {}}
    snapshot = get_sales_overview_order_rows(store_uid=store_uid, page=1, page_size=1)
    available_statuses = list(snapshot.get("available_statuses") or [])
    min_date = str(snapshot.get("min_date") or "").strip()
    max_date = str(snapshot.get("max_date") or "").strip()
    loaded_at = str(snapshot.get("loaded_at") or "").strip()
    range_from, range_to = _period_range(period=period, custom_date_from=date_from, custom_date_to=date_to, min_date=min_date)
    page_num = max(1, int(page or 1))
    size = max(1, min(int(page_size or 200), 1000))
    clean_rows: list[dict[str, Any]] = []
    raw_total_count = 0
    fetch_page = 1
    while True:
        chunk = get_sales_overview_order_rows(
            store_uid=store_uid,
            item_status=item_status,
            date_from=range_from,
            date_to=range_to,
            page=fetch_page,
            page_size=1000,
        )
        if fetch_page == 1:
            raw_total_count = int(chunk.get("total_count") or 0)
        chunk_rows = list(chunk.get("rows") or [])
        if not chunk_rows:
            break
        clean_rows.extend([row for row in chunk_rows if not _is_problem_order_row(row)])
        if len(chunk_rows) < 1000:
            break
        fetch_page += 1
    total_count = len(clean_rows)
    offset = (page_num - 1) * size
    rows = clean_rows[offset: offset + size]
    additional_ads = 0.0
    operational_errors = 0.0
    total_revenue = 0.0
    total_coinvest_amount = 0.0
    for row in rows:
        revenue = float(_num0(row.get("sale_price")))
        buyer_price = float(_num0(row.get("sale_price_with_coinvest")))
        if revenue > 0:
            total_revenue += revenue
            total_coinvest_amount += max(0.0, revenue - buyer_price)
    avg_coinvest_pct = round((total_coinvest_amount / total_revenue * 100.0), 2) if total_revenue > 0 else 0.0
    return {
        "ok": True,
        "rows": rows,
        "total_count": total_count,
        "page": page_num,
        "page_size": size,
        "available_statuses": available_statuses,
        "min_date": min_date,
        "max_date": max_date,
        "date_from": range_from,
        "date_to": range_to,
        "loaded_at": loaded_at,
        "raw_total_count": raw_total_count,
        "kpis": {
            "additional_ads": round(additional_ads, 2),
            "operational_errors": round(operational_errors, 2),
            "orders_count": total_count,
            "avg_coinvest_pct": avg_coinvest_pct,
        },
    }


async def get_sales_overview_problem_orders(
    *,
    page: int = 1,
    page_size: int = 200,
    store_id: str = "",
    period: str = "month",
    date_from: str = "",
    date_to: str = "",
) -> dict[str, Any]:
    sid = str(store_id or "").strip()
    store_uid = f"yandex_market:{sid}" if sid else ""
    if not store_uid:
        return {"ok": True, "rows": [], "total_count": 0, "page": 1, "page_size": page_size, "loaded_at": ""}
    snapshot = get_sales_overview_order_rows(store_uid=store_uid, page=1, page_size=1)
    min_date = str(snapshot.get("min_date") or "").strip()
    max_date = str(snapshot.get("max_date") or "").strip()
    loaded_at = str(snapshot.get("loaded_at") or "").strip()
    range_from, range_to = _period_range(period=period, custom_date_from=date_from, custom_date_to=date_to, min_date=min_date)
    result = get_sales_overview_order_rows(
        store_uid=store_uid,
        date_from=range_from,
        date_to=range_to,
        page=1,
        page_size=5000,
    )
    problem_rows = [row for row in list(result.get("rows") or []) if _is_problem_order_row(row)]
    page_num = max(1, int(page or 1))
    size = max(1, min(int(page_size or 200), 1000))
    offset = (page_num - 1) * size
    paged_rows = problem_rows[offset: offset + size]
    return {
        "ok": True,
        "rows": paged_rows,
        "total_count": len(problem_rows),
        "page": page_num,
        "page_size": size,
        "date_from": range_from,
        "date_to": range_to,
        "loaded_at": loaded_at,
        "kpis": {
            "problem_orders_count": len(problem_rows),
        },
    }


async def get_sales_overview_tracking(
    *,
    store_id: str = "",
    date_mode: str = "created",
) -> dict[str, Any]:
    sid = str(store_id or "").strip()
    all_market_stores = [
        store for store in _catalog_marketplace_stores_context()
        if str(store.get("platform") or "").strip().lower() == "yandex_market"
    ]
    if sid.lower() == "all":
        store_uids = [str(store.get("store_uid") or "").strip() for store in all_market_stores if str(store.get("store_uid") or "").strip()]
    else:
        store_uid = f"yandex_market:{sid}" if sid else ""
        store_uids = [store_uid] if store_uid else []
    if not store_uids:
        return {"ok": True, "date_mode": str(date_mode or "created"), "years": [], "active_month_key": "", "kpis": {}, "loaded_at": ""}

    mode = "delivery" if str(date_mode or "").strip().lower() == "delivery" else "created"
    planned_revenue = 0.0
    target_profit_rub = 0.0
    order_rows: list[dict[str, Any]] = []
    extra_ads_by_day: dict[str, float] = {}
    operational_errors_by_day: dict[str, float] = {}
    loaded_at = ""
    for store_uid in store_uids:
        settings = get_pricing_store_settings(store_uid=store_uid) or {}
        store_currency_code = _resolve_store_currency_code(store_uid)
        store_planned_revenue = float(_num0(settings.get("planned_revenue")))
        store_target_profit_rub = float(_num0(settings.get("target_profit_rub")))
        store_target_profit_pct = float(_num0(settings.get("target_profit_percent")))
        if store_target_profit_rub <= 0 and store_planned_revenue > 0 and store_target_profit_pct > 0:
            store_target_profit_rub = store_planned_revenue * (store_target_profit_pct / 100.0)
        planned_revenue += store_planned_revenue
        target_profit_rub += store_target_profit_rub

        store_order_rows = _load_sales_overview_order_fact_rows(store_uid=store_uid)
        order_rows.extend(store_order_rows)
        if not loaded_at:
            for row in store_order_rows:
                if _is_problem_order_row(row):
                    continue
                loaded_at = str(row.get("calculated_at") or row.get("source_updated_at") or "").strip()
                if loaded_at:
                    break

        store_extra_ads_by_day = _load_extra_ads_scope(store_uid=store_uid, date_from="1900-01-01", date_to="2999-12-31")
        for day_key, extra_amount in (store_extra_ads_by_day or {}).items():
            amount = await _convert_rub_amount_for_store_currency(
                float(extra_amount or 0.0),
                currency_code=store_currency_code,
                calc_date=_parse_date_any(day_key),
            )
            extra_ads_by_day[day_key] = round(float(extra_ads_by_day.get(day_key, 0.0)) + amount, 4)

        netting_rows = _load_netting_scope(store_uid=store_uid, date_from="1900-01-01", date_to="2999-12-31")
        for payload in netting_rows:
            transaction_type = str(payload.get("transactionType") or "").strip().lower()
            if transaction_type not in {"удержание", "списание"}:
                continue
            if _service_bucket(str(payload.get("offerOrServiceName") or "")) != "operational_error":
                continue
            trans_dt = _parse_datetime_any(payload.get("transactionDate"))
            if not trans_dt:
                continue
            day_key = trans_dt.astimezone(MSK).date().isoformat()
            amount = await _convert_rub_amount_for_store_currency(
                _abs_amount(payload.get("transactionSum")),
                currency_code=store_currency_code,
                calc_date=trans_dt.astimezone(MSK).date(),
            )
            operational_errors_by_day[day_key] = round(float(operational_errors_by_day.get(day_key, 0.0)) + amount, 4)

    if not order_rows:
        return {"ok": True, "date_mode": mode, "years": [], "active_month_key": "", "kpis": {}, "loaded_at": ""}

    available_months: set[tuple[int, int]] = set()
    for row in order_rows:
        if _is_problem_order_row(row):
            continue
        if not _tracking_status_allowed(str(row.get("item_status") or ""), mode=mode):
            continue
        anchor = _tracking_anchor_day(row, mode=mode)
        if anchor:
            available_months.add((anchor.year, anchor.month))
    if not available_months:
        return {"ok": True, "date_mode": mode, "years": [], "active_month_key": "", "kpis": {}, "loaded_at": loaded_at}

    today_msk = datetime.now(MSK).date()
    active_pair = (today_msk.year, today_msk.month) if (today_msk.year, today_msk.month) in available_months else max(available_months)
    active_month_key = f"{active_pair[0]}-{active_pair[1]:02d}"
    month_names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
        7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }

    monthly: dict[tuple[int, int], dict[str, Any]] = {}
    for row in order_rows:
        if _is_problem_order_row(row):
            continue
        status_raw = str(row.get("item_status") or "")
        if not _tracking_status_allowed(status_raw, mode=mode):
            continue
        status_kind = _status_kind(status_raw)
        anchor = _tracking_anchor_day(row, mode=mode)
        if anchor is None:
            continue
        month_key = (anchor.year, anchor.month)
        day_key = anchor.isoformat()
        bucket = monthly.setdefault(month_key, {
            "year": anchor.year,
            "month": anchor.month,
            "month_key": f"{anchor.year}-{anchor.month:02d}",
            "month_label": month_names.get(anchor.month, str(anchor.month)),
            "is_active": month_key == active_pair,
            "order_count_total": 0,
            "return_count": 0,
            "revenue": 0.0,
            "profit_amount": 0.0,
            "coinvest_amount": 0.0,
            "ads_amount": 0.0,
            "operational_errors": 0.0,
            "delivery_days_total": 0.0,
            "delivery_count": 0,
            "uses_planned_costs": False,
            "days": {},
        })
        day_bucket = bucket["days"].setdefault(day_key, {
            "date": day_key,
            "order_count_total": 0,
            "return_count": 0,
            "revenue": 0.0,
            "profit_amount": 0.0,
            "coinvest_amount": 0.0,
            "ads_amount": 0.0,
            "operational_errors": 0.0,
            "delivery_days_total": 0.0,
            "delivery_count": 0,
            "uses_planned_costs": False,
        })

        bucket["order_count_total"] += 1
        day_bucket["order_count_total"] += 1

        include_financials = _tracking_include_financials(row, mode=mode, month_key=month_key, active_pair=active_pair, anchor=anchor)
        if include_financials:
            revenue = float(_num0(row.get("sale_price")))
            buyer_price = float(_num0(row.get("sale_price_with_coinvest")))
            profit = float(_num0(row.get("profit")))
            ads = float(_num0(row.get("ads")))
            coinvest = max(0.0, revenue - buyer_price)
            bucket["revenue"] = round(float(bucket["revenue"]) + revenue, 4)
            bucket["profit_amount"] = round(float(bucket["profit_amount"]) + profit, 4)
            bucket["coinvest_amount"] = round(float(bucket["coinvest_amount"]) + coinvest, 4)
            bucket["ads_amount"] = round(float(bucket["ads_amount"]) + ads, 4)
            day_bucket["revenue"] = round(float(day_bucket["revenue"]) + revenue, 4)
            day_bucket["profit_amount"] = round(float(day_bucket["profit_amount"]) + profit, 4)
            day_bucket["coinvest_amount"] = round(float(day_bucket["coinvest_amount"]) + coinvest, 4)
            day_bucket["ads_amount"] = round(float(day_bucket["ads_amount"]) + ads, 4)
            if row.get("uses_planned_costs"):
                bucket["uses_planned_costs"] = True
                day_bucket["uses_planned_costs"] = True

        delivery_day = _parse_date_any(row.get("delivery_date"))
        created_dt = _parse_datetime_any(row.get("order_created_at"))
        created_day = _parse_date_any(row.get("order_created_date"))
        if status_kind == "delivered" and delivery_day and (created_dt or created_day):
            created_ref = created_dt.date() if created_dt else created_day
            if created_ref:
                days_delta = max(0, (delivery_day - created_ref).days)
                bucket["delivery_days_total"] = float(bucket["delivery_days_total"]) + days_delta
                bucket["delivery_count"] = int(bucket["delivery_count"]) + 1
                day_bucket["delivery_days_total"] = float(day_bucket["delivery_days_total"]) + days_delta
                day_bucket["delivery_count"] = int(day_bucket["delivery_count"]) + 1

    all_extra_days = {**extra_ads_by_day}
    for day_key, extra_amount in all_extra_days.items():
        parsed = _parse_date_any(day_key)
        if not parsed:
            continue
        month_key = (parsed.year, parsed.month)
        bucket = monthly.get(month_key)
        if not bucket:
            continue
        day_bucket = bucket["days"].setdefault(day_key, {
            "date": day_key,
            "order_count_total": 0,
            "return_count": 0,
            "revenue": 0.0,
            "profit_amount": 0.0,
            "coinvest_amount": 0.0,
            "ads_amount": 0.0,
            "operational_errors": 0.0,
            "delivery_days_total": 0.0,
            "delivery_count": 0,
            "uses_planned_costs": False,
        })
        bucket["ads_amount"] = round(float(bucket["ads_amount"]) + float(extra_amount or 0.0), 4)
        day_bucket["ads_amount"] = round(float(day_bucket["ads_amount"]) + float(extra_amount or 0.0), 4)
        bucket["profit_amount"] = round(float(bucket["profit_amount"]) - float(extra_amount or 0.0), 4)
        day_bucket["profit_amount"] = round(float(day_bucket["profit_amount"]) - float(extra_amount or 0.0), 4)

    for day_key, op_amount in operational_errors_by_day.items():
        parsed = _parse_date_any(day_key)
        if not parsed:
            continue
        month_key = (parsed.year, parsed.month)
        bucket = monthly.get(month_key)
        if not bucket:
            continue
        day_bucket = bucket["days"].setdefault(day_key, {
            "date": day_key,
            "order_count_total": 0,
            "return_count": 0,
            "revenue": 0.0,
            "profit_amount": 0.0,
            "coinvest_amount": 0.0,
            "ads_amount": 0.0,
            "operational_errors": 0.0,
            "delivery_days_total": 0.0,
            "delivery_count": 0,
            "uses_planned_costs": False,
        })
        bucket["operational_errors"] = round(float(bucket["operational_errors"]) + float(op_amount or 0.0), 4)
        day_bucket["operational_errors"] = round(float(day_bucket["operational_errors"]) + float(op_amount or 0.0), 4)
        bucket["profit_amount"] = round(float(bucket["profit_amount"]) - float(op_amount or 0.0), 4)
        day_bucket["profit_amount"] = round(float(day_bucket["profit_amount"]) - float(op_amount or 0.0), 4)

    years_map: dict[int, list[dict[str, Any]]] = {}
    active_month_summary: dict[str, Any] | None = None
    for month_key in sorted(monthly.keys(), reverse=True):
        bucket = monthly[month_key]
        revenue = float(bucket["revenue"])
        profit_amount = float(bucket["profit_amount"])
        order_count_total = int(bucket["order_count_total"])
        return_count = int(bucket["return_count"])
        delivery_count = int(bucket["delivery_count"])
        bucket["profit_pct"] = round((profit_amount / revenue * 100.0), 2) if revenue > 0 else 0.0
        bucket["returns_pct"] = round((return_count / order_count_total * 100.0), 2) if order_count_total > 0 else 0.0
        bucket["delivery_time_days"] = round((float(bucket["delivery_days_total"]) / delivery_count), 2) if delivery_count > 0 else 0.0
        bucket["revenue_plan_pct"] = round((revenue / planned_revenue * 100.0), 2) if bucket["is_active"] and planned_revenue > 0 else None
        bucket["profit_plan_pct"] = round((profit_amount / target_profit_rub * 100.0), 2) if bucket["is_active"] and target_profit_rub > 0 else None
        bucket["revenue_plan_amount"] = round(planned_revenue, 2) if bucket["is_active"] and planned_revenue > 0 else None
        bucket["profit_plan_amount"] = round(target_profit_rub, 2) if bucket["is_active"] and target_profit_rub > 0 else None
        days_in_month = _days_in_month(int(bucket["year"]), int(bucket["month"]))
        daily_revenue_plan = (planned_revenue / days_in_month) if bucket["is_active"] and planned_revenue > 0 else None
        daily_profit_plan = (target_profit_rub / days_in_month) if bucket["is_active"] and target_profit_rub > 0 else None
        day_rows: list[dict[str, Any]] = []
        for day_key in sorted(bucket["days"].keys()):
            day_item = bucket["days"][day_key]
            day_revenue = float(day_item["revenue"])
            day_profit = float(day_item["profit_amount"])
            day_orders = int(day_item["order_count_total"])
            day_returns = int(day_item["return_count"])
            day_delivery_count = int(day_item["delivery_count"])
            day_item["profit_pct"] = round((day_profit / day_revenue * 100.0), 2) if day_revenue > 0 else 0.0
            day_item["returns_pct"] = round((day_returns / day_orders * 100.0), 2) if day_orders > 0 else 0.0
            day_item["delivery_time_days"] = round((float(day_item["delivery_days_total"]) / day_delivery_count), 2) if day_delivery_count > 0 else 0.0
            day_rows.append({
                "date": day_key,
                "revenue": round(day_revenue, 2),
                "revenue_plan_amount": round(float(daily_revenue_plan), 2) if daily_revenue_plan is not None else None,
                "profit_amount": round(day_profit, 2),
                "profit_plan_amount": round(float(daily_profit_plan), 2) if daily_profit_plan is not None else None,
                "profit_pct": day_item["profit_pct"],
                "coinvest_amount": round(float(day_item["coinvest_amount"]), 2),
                "coinvest_pct": round((float(day_item["coinvest_amount"]) / day_revenue * 100.0), 2) if day_revenue > 0 else 0.0,
                "returns_pct": day_item["returns_pct"],
                "ads_amount": round(float(day_item["ads_amount"]), 2),
                "operational_errors": round(float(day_item["operational_errors"]), 2),
                "delivery_time_days": day_item["delivery_time_days"],
                "uses_planned_costs": bool(day_item["uses_planned_costs"]),
            })
        month_payload = {
            "month_key": bucket["month_key"],
            "month": bucket["month"],
            "month_label": bucket["month_label"],
            "is_active": bool(bucket["is_active"]),
            "revenue": round(revenue, 2),
            "revenue_plan_amount": bucket["revenue_plan_amount"],
            "revenue_plan_pct": bucket["revenue_plan_pct"],
            "profit_amount": round(profit_amount, 2),
            "profit_plan_amount": bucket["profit_plan_amount"],
            "profit_plan_pct": bucket["profit_plan_pct"],
            "profit_pct": bucket["profit_pct"],
            "coinvest_amount": round(float(bucket["coinvest_amount"]), 2),
            "returns_pct": bucket["returns_pct"],
            "ads_amount": round(float(bucket["ads_amount"]), 2),
            "operational_errors": round(float(bucket["operational_errors"]), 2),
            "delivery_time_days": bucket["delivery_time_days"],
            "uses_planned_costs": bool(bucket["uses_planned_costs"]),
            "days": day_rows,
        }
        years_map.setdefault(bucket["year"], []).append(month_payload)
        if bucket["is_active"]:
            active_month_summary = month_payload

    years_payload = [
        {
            "year": year_key,
            "months": sorted(years_map.get(year_key, []), key=lambda item: int(item.get("month") or 0)),
        }
        for year_key in sorted(years_map.keys(), reverse=True)
    ]
    return {
        "ok": True,
        "date_mode": mode,
        "years": years_payload,
        "active_month_key": active_month_key,
        "loaded_at": loaded_at,
        "kpis": {
            "revenue": round(float((active_month_summary or {}).get("revenue") or 0.0), 2),
            "profit": round(float((active_month_summary or {}).get("profit_amount") or 0.0), 2),
            "profit_pct": round(float((active_month_summary or {}).get("profit_pct") or 0.0), 2),
            "days": len(list((active_month_summary or {}).get("days") or [])),
            "avg_coinvest_pct": round(
                (
                    float((active_month_summary or {}).get("coinvest_amount") or 0.0)
                    / float((active_month_summary or {}).get("revenue") or 0.0)
                    * 100.0
                ),
                2,
            ) if float((active_month_summary or {}).get("revenue") or 0.0) > 0 else 0.0,
        },
    }


async def get_sales_overview_retrospective(
    *,
    store_id: str = "",
    date_mode: str = "created",
    group_by: str = "sku",
    grain: str = "month",
    date_from: str = "",
    date_to: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    sid = str(store_id or "").strip()
    store_uid = f"yandex_market:{sid}" if sid else ""
    if not store_uid:
        return {"ok": True, "rows": [], "group_by": group_by, "grain": grain, "date_mode": date_mode}

    mode = "delivery" if str(date_mode or "").strip().lower() == "delivery" else "created"
    dimension = "category" if str(group_by or "").strip().lower() == "category" else "sku"
    period_grain = "day" if str(grain or "").strip().lower() == "day" else "month"
    from_day = _parse_date_any(date_from)
    to_day = _parse_date_any(date_to)
    order_rows = _load_sales_overview_order_fact_rows(store_uid=store_uid)
    if not order_rows:
        return {"ok": True, "rows": [], "group_by": dimension, "grain": period_grain, "date_mode": mode}

    available_months: set[tuple[int, int]] = set()
    for row in order_rows:
        if _is_problem_order_row(row):
            continue
        if _status_kind(str(row.get("item_status") or "")) == "ignore":
            continue
        anchor = _tracking_anchor_day(row, mode=mode)
        if anchor:
            available_months.add((anchor.year, anchor.month))
    if not available_months:
        return {"ok": True, "rows": [], "group_by": dimension, "grain": period_grain, "date_mode": mode}

    today_msk = datetime.now(MSK).date()
    active_pair = (today_msk.year, today_msk.month) if (today_msk.year, today_msk.month) in available_months else max(available_months)
    path_map = get_pricing_catalog_sku_path_map(priority_platform="yandex_market")

    grouped: dict[str, dict[str, Any]] = {}
    for row in order_rows:
        if _is_problem_order_row(row):
            continue
        status_kind = _status_kind(str(row.get("item_status") or ""))
        if not _tracking_status_allowed(str(row.get("item_status") or ""), mode=mode):
            continue
        anchor = _tracking_anchor_day(row, mode=mode)
        if anchor is None:
            continue
        if from_day and anchor < from_day:
            continue
        if to_day and anchor > to_day:
            continue
        month_key = (anchor.year, anchor.month)
        sku = str(row.get("sku") or "").strip()
        item_name = str(row.get("item_name") or "").strip()
        category_path = _resolve_category_path_for_sku(sku, path_map)
        if dimension == "category":
            group_key = category_path
            group_label = category_path
        else:
            group_key = sku
            group_label = item_name or sku

        bucket = grouped.setdefault(group_key, {
            "key": group_key,
            "label": group_label,
            "sku": sku if dimension == "sku" else "",
            "item_name": item_name if dimension == "sku" else "",
            "category_path": category_path,
            "order_count_total": 0,
            "return_count": 0,
            "revenue": 0.0,
            "profit_amount": 0.0,
            "coinvest_amount": 0.0,
            "ads_amount": 0.0,
            "periods": {},
        })
        bucket["order_count_total"] += 1
        if "возврат" in str(row.get("item_status") or "").lower():
            bucket["return_count"] += 1

        period_key, period_label = _tracking_period_key(anchor, grain=period_grain)
        period_bucket = bucket["periods"].setdefault(period_key, {
            "period_key": period_key,
            "period_label": period_label,
            "order_count_total": 0,
            "return_count": 0,
            "revenue": 0.0,
            "profit_amount": 0.0,
            "coinvest_amount": 0.0,
            "ads_amount": 0.0,
        })
        period_bucket["order_count_total"] += 1
        if "возврат" in str(row.get("item_status") or "").lower():
            period_bucket["return_count"] += 1

        if _tracking_include_financials(row, mode=mode, month_key=month_key, active_pair=active_pair, anchor=anchor):
            revenue = float(_num0(row.get("sale_price")))
            buyer_price = float(_num0(row.get("sale_price_with_coinvest")))
            profit = float(_num0(row.get("profit")))
            ads = float(_num0(row.get("ads")))
            coinvest = max(0.0, revenue - buyer_price)
            bucket["revenue"] = round(float(bucket["revenue"]) + revenue, 4)
            bucket["profit_amount"] = round(float(bucket["profit_amount"]) + profit, 4)
            bucket["coinvest_amount"] = round(float(bucket["coinvest_amount"]) + coinvest, 4)
            bucket["ads_amount"] = round(float(bucket["ads_amount"]) + ads, 4)
            period_bucket["revenue"] = round(float(period_bucket["revenue"]) + revenue, 4)
            period_bucket["profit_amount"] = round(float(period_bucket["profit_amount"]) + profit, 4)
            period_bucket["coinvest_amount"] = round(float(period_bucket["coinvest_amount"]) + coinvest, 4)
            period_bucket["ads_amount"] = round(float(period_bucket["ads_amount"]) + ads, 4)

    rows_payload: list[dict[str, Any]] = []
    for bucket in grouped.values():
        revenue = float(bucket["revenue"])
        profit_amount = float(bucket["profit_amount"])
        order_count_total = int(bucket["order_count_total"])
        return_count = int(bucket["return_count"])
        periods = []
        for period_key in sorted(bucket["periods"].keys()):
            period = bucket["periods"][period_key]
            period_revenue = float(period["revenue"])
            period_profit = float(period["profit_amount"])
            period_orders = int(period["order_count_total"])
            period_returns = int(period["return_count"])
            periods.append({
                "period_key": period["period_key"],
                "period_label": period["period_label"],
                "revenue": round(period_revenue, 2),
                "profit_amount": round(period_profit, 2),
                "profit_pct": round((period_profit / period_revenue * 100.0), 2) if period_revenue > 0 else 0.0,
                "coinvest_amount": round(float(period["coinvest_amount"]), 2),
                "ads_amount": round(float(period["ads_amount"]), 2),
                "returns_pct": round((period_returns / period_orders * 100.0), 2) if period_orders > 0 else 0.0,
                "order_count_total": period_orders,
            })
        rows_payload.append({
            "key": bucket["key"],
            "label": bucket["label"],
            "sku": bucket["sku"],
            "item_name": bucket["item_name"],
            "category_path": bucket["category_path"],
            "revenue": round(revenue, 2),
            "profit_amount": round(profit_amount, 2),
            "profit_pct": round((profit_amount / revenue * 100.0), 2) if revenue > 0 else 0.0,
            "coinvest_amount": round(float(bucket["coinvest_amount"]), 2),
            "ads_amount": round(float(bucket["ads_amount"]), 2),
            "returns_pct": round((return_count / order_count_total * 100.0), 2) if order_count_total > 0 else 0.0,
            "order_count_total": order_count_total,
            "periods": periods,
        })

    rows_payload.sort(key=lambda item: (float(item.get("revenue") or 0.0), float(item.get("profit_amount") or 0.0)), reverse=True)
    limit_num = max(1, min(int(limit or 200), 1000))
    return {
        "ok": True,
        "store_uid": store_uid,
        "date_mode": mode,
        "group_by": dimension,
        "grain": period_grain,
        "rows": rows_payload[:limit_num],
        "total_count": len(rows_payload),
    }
