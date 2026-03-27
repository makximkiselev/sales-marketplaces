from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import datetime
from typing import Any

import httpx

from backend.routers._shared import (
    YANDEX_BASE_URL,
    _catalog_marketplace_stores_context,
    _find_yandex_shop_credentials,
    _read_source_rows,
    _ym_headers,
)
from backend.services.gsheets import update_sheet_column_by_sku, update_sheet_columns_by_sku
from backend.services.db import is_postgres_backend
from backend.services.integrations import get_data_flow_settings, get_google_credentials
from backend.services.pricing_prices_service import _load_stock_map_from_source
from backend.services.storage import get_source_by_id, is_source_mode_enabled, load_integrations
from backend.services.store_data_model import (
    _connect,
    append_pricing_market_price_export_history_bulk,
    get_pricing_store_settings,
    update_pricing_strategy_market_promo_feedback,
)

logger = logging.getLogger("uvicorn.error")


def _to_num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _round_for_sheet(value: float | None, *, digits: int = 2) -> int | float | None:
    if value is None:
        return None
    rounded = round(float(value), int(digits))
    if abs(rounded - round(rounded)) < 1e-9:
        return int(round(rounded))
    return rounded


def _has_embedded_google_credentials() -> bool:
    raw_json, raw_b64 = get_google_credentials()
    return bool(str(raw_json or "").strip() or str(raw_b64 or "").strip())


def _load_store_row(store_uid: str) -> dict[str, Any] | None:
    suid = str(store_uid or "").strip()
    if not suid:
        return None
    item = next(
        (
            {
                "store_uid": str(store.get("store_uid") or "").strip(),
                "platform": str(store.get("platform") or "").strip(),
                "store_id": str(store.get("store_id") or "").strip(),
                "store_name": str(store.get("store_name") or store.get("label") or "").strip(),
                "currency_code": str(store.get("currency_code") or "RUB").strip().upper() or "RUB",
            }
            for store in (_catalog_marketplace_stores_context() or [])
            if str(store.get("store_uid") or "").strip() == suid
        ),
        None,
    )
    if not isinstance(item, dict):
        return None
    if str(item.get("platform") or "").strip().lower() == "yandex_market":
        data = load_integrations()
        ym = data.get("yandex_market") if isinstance(data.get("yandex_market"), dict) else {}
        accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
        store_id = str(item.get("store_id") or "").strip()
        for account in accounts:
            if not isinstance(account, dict):
                continue
            for shop in account.get("shops") or []:
                if not isinstance(shop, dict):
                    continue
                if str(shop.get("campaign_id") or "").strip() != store_id:
                    continue
                shop_currency = str(shop.get("currency_code") or "").strip()
                if shop_currency:
                    item["currency_code"] = shop_currency
                if not str(item.get("store_name") or "").strip():
                    item["store_name"] = str(shop.get("campaign_name") or shop.get("campaign_id") or "").strip()
                break
    return item


def _currency_id_for_market(currency_code: str | None) -> str:
    code = str(currency_code or "").strip().upper()
    if code == "RUB":
        return "RUR"
    return code or "RUR"


def _chunked(items: list[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        return [items]
    return [items[index : index + size] for index in range(0, len(items), size)]


def _market_response_error_message(resp: httpx.Response) -> str | None:
    try:
        payload = resp.json() if resp.content else {}
    except Exception:
        payload = {}
    status = str(payload.get("status") or "").strip().upper() if isinstance(payload, dict) else ""
    if resp.status_code >= 400:
        return resp.text[:500] or f"HTTP {resp.status_code}"
    if status == "ERROR":
        errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
        if errors:
            preview: list[str] = []
            for item in errors[:10]:
                if not isinstance(item, dict):
                    continue
                code = str(item.get("code") or "").strip()
                message = str(item.get("message") or "").strip()
                joined = ": ".join(part for part in [code, message] if part)
                if joined:
                    preview.append(joined)
            if preview:
                return "; ".join(preview)
        return json.dumps(payload, ensure_ascii=False)[:500]
    return None


def _promo_offer_status_message(*, status: str, offer: dict[str, Any]) -> str:
    normalized = str(status or "").strip().upper()
    params = offer.get("params") if isinstance(offer.get("params"), dict) else {}
    discount = params.get("discountParams") if isinstance(params.get("discountParams"), dict) else {}
    max_promo_price = _to_num(discount.get("maxPromoPrice"))
    promo_price = _to_num(discount.get("promoPrice"))
    detail_parts: list[str] = []
    if max_promo_price not in (None, 0):
        detail_parts.append(f"maxPromoPrice={int(round(float(max_promo_price)))}")
    if promo_price not in (None, 0):
        detail_parts.append(f"promoPrice={int(round(float(promo_price)))}")
    base = {
        "NOT_PARTICIPATING": "status=NOT_PARTICIPATING",
        "RENEW_FAILED": "status=RENEW_FAILED",
        "MANUAL": "status=MANUAL",
        "AUTO": "status=AUTO",
        "PARTIALLY_AUTO": "status=PARTIALLY_AUTO",
        "RENEWED": "status=RENEWED",
        "MINIMUM_FOR_PROMOS": "status=MINIMUM_FOR_PROMOS",
    }.get(normalized, f"status={normalized or 'UNKNOWN'}")
    return " | ".join([base, *detail_parts]) if detail_parts else base


async def _fetch_market_promo_offer_statuses(
    *,
    business_id: str,
    api_key: str,
    promo_id: str,
) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    page_token: str | None = None
    async with httpx.AsyncClient(timeout=90) as client:
        while True:
            params: dict[str, Any] = {"limit": 500}
            if page_token:
                params["page_token"] = page_token
            body = {"promoId": promo_id}
            url = f"{YANDEX_BASE_URL}/businesses/{business_id}/promos/offers"
            resp = await client.post(url, headers=_ym_headers(api_key), params=params, json=body)
            if resp.status_code >= 400:
                raise RuntimeError(resp.text[:500] or f"HTTP {resp.status_code}")
            payload = resp.json() if resp.content else {}
            result = payload.get("result") if isinstance(payload, dict) else {}
            offers = result.get("offers") if isinstance(result, dict) else []
            for offer in offers or []:
                if not isinstance(offer, dict):
                    continue
                offer_id = str(offer.get("offerId") or "").strip()
                if offer_id:
                    found[offer_id] = offer
            paging = result.get("paging") if isinstance(result, dict) else {}
            next_page_token = str((paging or {}).get("nextPageToken") or "").strip()
            if not next_page_token:
                break
            page_token = next_page_token
    return found


def _is_market_export_enabled(*, store_uid: str, platform: str, store_id: str) -> bool:
    platform_code = str(platform or "").strip().lower()
    if platform_code != "yandex_market":
        return False
    flow = get_data_flow_settings()
    if not bool(flow.get("export_enabled")):
        return False
    if not bool(((flow.get("platforms") or {}).get("yandex_market") or {}).get("export_enabled")):
        return False
    campaign_id = str(store_id or "").strip()
    data = load_integrations()
    ym = data.get("yandex_market") if isinstance(data.get("yandex_market"), dict) else {}
    accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
    for account in accounts:
        if not isinstance(account, dict):
            continue
        shops = account.get("shops") if isinstance(account.get("shops"), list) else []
        for shop in shops:
            if not isinstance(shop, dict):
                continue
            if str(shop.get("campaign_id") or "").strip() != campaign_id:
                continue
            return bool(shop.get("export_enabled", account.get("export_enabled", True)))
    return False


def _build_export_values(*, store_uid: str, export_kind: str) -> dict[str, int | float]:
    kind = str(export_kind or "").strip().lower()
    if kind not in {"prices", "ads"}:
        return {}
    select_expr = "installed_price" if kind == "prices" else "boost_bid_percent"
    values: dict[str, int | float] = {}
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT sku, {select_expr} AS export_value
            FROM pricing_strategy_results
            WHERE store_uid = {ph}
            """,
            (str(store_uid or "").strip(),),
        ).fetchall()
    for row in rows:
        item = dict(row)
        sku = str(item.get("sku") or "").strip()
        raw_value = _to_num(item.get("export_value"))
        if kind == "ads" and raw_value is None:
            raw_value = 0.0
        if kind == "ads" and raw_value is not None:
            raw_value = float(raw_value) / 100.0
            value = _round_for_sheet(raw_value, digits=4)
        else:
            value = _round_for_sheet(raw_value)
        if not sku or value is None:
            continue
        values[sku] = value
    return values


def _strategy_layer_is_fresh(*, store_uid: str) -> tuple[bool, str | None]:
    suid = str(store_uid or "").strip()
    if not suid:
        return False, "store_uid обязателен"
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        price_meta = conn.execute(
            f"""
            SELECT COUNT(*) AS rows_count, MAX(calculated_at) AS max_calculated_at
            FROM pricing_price_results
            WHERE store_uid = {ph}
            """,
            (suid,),
        ).fetchone()
        strategy_meta = conn.execute(
            f"""
            SELECT COUNT(*) AS rows_count, MAX(calculated_at) AS max_calculated_at
            FROM pricing_strategy_results
            WHERE store_uid = {ph}
            """,
            (suid,),
        ).fetchone()
    price_meta = dict(price_meta) if price_meta else {}
    strategy_meta = dict(strategy_meta) if strategy_meta else {}
    price_count = int((price_meta.get("rows_count") if price_meta else 0) or 0)
    strategy_count = int((strategy_meta.get("rows_count") if strategy_meta else 0) or 0)
    price_ts = str((price_meta.get("max_calculated_at") if price_meta else "") or "").strip()
    strategy_ts = str((strategy_meta.get("max_calculated_at") if strategy_meta else "") or "").strip()
    if price_count <= 0:
        return False, "Нет materialized слоя цен"
    if strategy_count <= 0:
        return False, "Стратегия устарела: после обновления цен нужен новый прогон стратегии"
    if price_ts and strategy_ts and strategy_ts < price_ts:
        return False, "Стратегия устарела: слой стратегии старше слоя цен"
    return True, None


def _load_present_store_skus(*, store_uid: str) -> set[str] | None:
    store = _load_store_row(store_uid)
    if not isinstance(store, dict):
        return None
    table_name = str(store.get("table_name") or "").strip()
    if not table_name:
        return None
    rows = _read_source_rows(table_name)
    return {
        str(row.get("sku") or "").strip()
        for row in rows
        if isinstance(row, dict) and str(row.get("sku") or "").strip()
    }


def _build_google_export_descriptors(*, store_uid: str, export_kinds: list[str] | None = None) -> list[dict[str, Any]]:
    suid = str(store_uid or "").strip()
    settings = get_pricing_store_settings(store_uid=suid) or {}
    kinds = [str(kind or "").strip().lower() for kind in (export_kinds or ["prices", "ads"]) if str(kind or "").strip()]
    if not kinds:
        kinds = ["prices", "ads"]
    descriptors: list[dict[str, Any]] = []
    for kind in kinds:
        prefix = f"export_{kind}"
        source_id = str(settings.get(f"{prefix}_source_id") or "").strip()
        sku_column = str(settings.get(f"{prefix}_sku_column") or "").strip()
        value_column = str(settings.get(f"{prefix}_value_column") or "").strip()
        if not source_id or not sku_column or not value_column:
            descriptors.append({"kind": kind, "status": "skipped", "message": "Не настроен источник экспорта"})
            continue
        if not is_source_mode_enabled(source_id, "export", default=False):
            descriptors.append({"kind": kind, "status": "skipped", "message": "У источника выключен экспорт", "source_id": source_id})
            continue
        src = get_source_by_id(source_id)
        if not isinstance(src, dict):
            descriptors.append({"kind": kind, "status": "error", "message": "Источник не найден", "source_id": source_id})
            continue
        if str(src.get("type") or "").strip().lower() != "gsheets":
            descriptors.append({"kind": kind, "status": "skipped", "message": "Поддерживаются только Google Sheets", "source_id": source_id})
            continue
        spreadsheet_id = str(src.get("spreadsheet_id") or "").strip()
        worksheet = str(src.get("worksheet") or "").strip() or None
        if not spreadsheet_id:
            descriptors.append({"kind": kind, "status": "error", "message": "У источника не задан spreadsheet_id", "source_id": source_id})
            continue
        descriptors.append(
            {
                "kind": kind,
                "source_id": source_id,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": worksheet,
                "sku_column": sku_column,
                "value_column": value_column,
                "values_by_sku": _build_export_values(store_uid=suid, export_kind=kind),
            }
        )
    return descriptors


def _load_market_prices_payload(*, store_uid: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        fetched = conn.execute(
            f"""
            SELECT sku, installed_price
            FROM pricing_strategy_results
            WHERE store_uid = {ph}
              AND installed_price IS NOT NULL
            """,
            (store_uid,),
        ).fetchall()
    for row in fetched:
        item = dict(row)
        sku = str(item.get("sku") or "").strip()
        price = _to_num(item.get("installed_price"))
        if not sku or price in (None, 0):
            continue
        rows.append({"sku": sku, "price": round(float(price), 2)})
    return rows


async def push_market_prices_payload_for_store(
    *,
    store_uid: str,
    price_by_sku: dict[str, float | int],
) -> dict[str, Any]:
    suid = str(store_uid or "").strip()
    if not suid:
        return {"kind": "market_prices", "status": "error", "message": "store_uid обязателен"}
    store = _load_store_row(suid) or {}
    campaign_id = str(store.get("store_id") or "").strip()
    if not campaign_id:
        return {"kind": "market_prices", "status": "error", "message": "Не найден campaign_id магазина"}
    values = [
        {"sku": sku, "price": round(float(price), 2)}
        for sku, price in (price_by_sku or {}).items()
        if str(sku or "").strip() and _to_num(price) not in (None, 0)
    ]
    if not values:
        return {"kind": "market_prices", "status": "skipped", "message": "Нет цен для выгрузки"}
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        return {"kind": "market_prices", "status": "error", "message": "Не найдены credentials Маркета"}
    business_id, _campaign_id, api_key = creds
    currency_id = _currency_id_for_market(str(store.get("currency_code") or "RUB"))
    url = f"{YANDEX_BASE_URL}/businesses/{business_id}/offer-prices/updates"
    requested_at = datetime.now().astimezone().isoformat()
    preview = values[:3]
    logger.warning(
        "[pricing_export] market prices request store_uid=%s business_id=%s campaign_id=%s rows=%s preview=%s",
        suid,
        business_id,
        campaign_id,
        len(values),
        preview,
    )
    append_pricing_market_price_export_history_bulk(
        store_uid=suid,
        campaign_id=campaign_id,
        rows=values,
        requested_at=requested_at,
        source="strategy_cycle",
    )
    async with httpx.AsyncClient(timeout=90) as client:
        for batch in _chunked(values, 500):
            body = {
                "offers": [
                    {
                        "offerId": item["sku"],
                        "price": {"value": item["price"], "currencyId": currency_id},
                    }
                    for item in batch
                ]
            }
            resp = await client.post(url, headers=_ym_headers(api_key), json=body)
            error_message = _market_response_error_message(resp)
            if error_message:
                logger.warning(
                    "[pricing_export] market prices response store_uid=%s business_id=%s campaign_id=%s status=error message=%s",
                    suid,
                    business_id,
                    campaign_id,
                    error_message,
                )
                return {"kind": "market_prices", "status": "error", "message": error_message}
    logger.warning(
        "[pricing_export] market prices response store_uid=%s business_id=%s campaign_id=%s status=success rows=%s",
        suid,
        business_id,
        campaign_id,
        len(values),
    )
    return {
        "kind": "market_prices",
        "status": "success",
        "updated_cells": len(values),
        "matched_rows": len(values),
        "values_total": len(values),
    }


def _load_market_boosts_payload(*, store_uid: str) -> list[dict[str, Any]]:
    in_stock_skus = _load_in_stock_skus(store_uid=store_uid)
    rows: list[dict[str, Any]] = []
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        fetched = conn.execute(
            f"""
            SELECT s.sku, s.boost_bid_percent
            FROM pricing_strategy_results AS s
            WHERE s.store_uid = {ph}
            """,
            (store_uid,),
        ).fetchall()
    for row in fetched:
        item = dict(row)
        sku = str(item.get("sku") or "").strip()
        bid = _to_num(item.get("boost_bid_percent"))
        if bid is None:
            bid = 0.0
        if not sku:
            continue
        if in_stock_skus is not None and sku not in in_stock_skus:
            rows.append({"sku": sku, "bid": 0.0})
            continue
        if float(bid) < 0.01:
            rows.append({"sku": sku, "bid": 0.0})
            continue
        rows.append({"sku": sku, "bid": round(float(bid), 2)})
    return rows


def _load_market_promos_plan(*, store_uid: str) -> tuple[dict[str, list[dict[str, Any]]], set[str], set[str]]:
    in_stock_skus = _load_in_stock_skus(store_uid=store_uid)
    grouped: dict[str, list[dict[str, Any]]] = {}
    active_promo_ids: set[str] = set()
    managed_skus: set[str] = set()
    ph = "%s" if is_postgres_backend() else "?"
    with _connect() as conn:
        strategy_rows = conn.execute(
            f"""
            SELECT sku
            FROM pricing_strategy_results
            WHERE store_uid = {ph}
            """,
            (store_uid,),
        ).fetchall()
        managed_skus = {
            str(dict(row).get("sku") or "").strip()
            for row in strategy_rows
            if str(dict(row).get("sku") or "").strip()
        }
        fetched = conn.execute(
            f"""
            SELECT
                s.sku,
                s.uses_promo,
                s.rrc_price,
                s.installed_price,
                s.promo_price,
                p.promo_id,
                p.promo_price AS offer_promo_price,
                p.promo_fit_mode
            FROM pricing_strategy_results s
            JOIN pricing_promo_offer_results p
              ON p.store_uid = s.store_uid
             AND p.sku = s.sku
            WHERE s.store_uid = {ph}
            ORDER BY s.sku, p.promo_price, p.promo_id
            """,
            (store_uid,),
        ).fetchall()
    rows_by_sku: dict[str, list[dict[str, Any]]] = {}
    for row in fetched:
        item = dict(row)
        sku = str(item.get("sku") or "").strip()
        promo_id = str(item.get("promo_id") or "").strip()
        if not sku or not promo_id:
            continue
        active_promo_ids.add(promo_id)
        rows_by_sku.setdefault(sku, []).append(item)

    for sku, sku_rows in rows_by_sku.items():
        if in_stock_skus is not None and sku not in in_stock_skus:
            continue
        uses_promo = any(bool(row.get("uses_promo")) for row in sku_rows)
        if not uses_promo:
            continue
        installed_price = next((_to_num(row.get("installed_price")) for row in sku_rows if _to_num(row.get("installed_price")) is not None), None)
        strategy_price = next((_to_num(row.get("promo_price")) for row in sku_rows if _to_num(row.get("promo_price")) is not None), None)
        offer_prices = [_to_num(row.get("offer_promo_price")) for row in sku_rows if _to_num(row.get("offer_promo_price")) is not None]
        chosen_price = installed_price if installed_price is not None else strategy_price
        if chosen_price is None and offer_prices:
            chosen_price = min(float(price) for price in offer_prices if price is not None)
        if chosen_price is None:
            continue
        old_price_candidates = [_to_num(row.get("rrc_price")) for row in sku_rows if _to_num(row.get("rrc_price")) is not None]
        old_price = max(old_price_candidates) if old_price_candidates else None
        minimum_old_price = math.ceil(float(chosen_price) / 0.95)
        old_price = max(float(old_price or 0.0), float(minimum_old_price))
        for row in sku_rows:
            promo_id = str(row.get("promo_id") or "").strip()
            if not promo_id:
                continue
            promo_threshold = _to_num(row.get("offer_promo_price"))
            promo_fit_mode = str(row.get("promo_fit_mode") or "").strip().lower()
            if promo_fit_mode not in {"with_ads", "without_ads"}:
                continue
            if promo_threshold in (None, 0) or float(chosen_price) > float(promo_threshold) + 0.01:
                continue
            grouped.setdefault(promo_id, []).append(
                {
                    "sku": sku,
                    "old_price": round(float(old_price), 2),
                    "promo_price": round(float(chosen_price), 2),
                }
            )
    return grouped, active_promo_ids, managed_skus


def _load_in_stock_skus(*, store_uid: str) -> set[str] | None:
    suid = str(store_uid or "").strip()
    if not suid:
        return None
    settings = get_pricing_store_settings(store_uid=suid) or {}
    stock_source_id = str(settings.get("stock_source_id") or "").strip()
    stock_sku_column = str(settings.get("stock_sku_column") or "").strip()
    stock_value_column = str(settings.get("stock_value_column") or "").strip()
    if not stock_source_id or not stock_value_column:
        return None
    try:
        stock_map = _load_stock_map_from_source(
            source_id=stock_source_id,
            sku_column=stock_sku_column,
            value_column=stock_value_column,
        )
    except Exception:
        return None
    return {
        str(sku or "").strip()
        for sku, qty in (stock_map or {}).items()
        if str(sku or "").strip() and qty is not None and float(qty) > 0
    }


async def _push_market_prices_for_store(*, store_uid: str, campaign_id: str, currency_code: str | None) -> dict[str, Any]:
    values = _load_market_prices_payload(store_uid=store_uid)
    if not values:
        return {"kind": "market_prices", "status": "skipped", "message": "Нет цен для выгрузки"}
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        return {"kind": "market_prices", "status": "error", "message": "Не найдены credentials Маркета"}
    business_id, _campaign_id, api_key = creds
    currency_id = _currency_id_for_market(currency_code)
    url = f"{YANDEX_BASE_URL}/businesses/{business_id}/offer-prices/updates"
    requested_at = datetime.now().astimezone().isoformat()
    logger.warning(
        "[pricing_export] final market prices request store_uid=%s business_id=%s campaign_id=%s rows=%s preview=%s",
        store_uid,
        business_id,
        campaign_id,
        len(values),
        values[:3],
    )
    append_pricing_market_price_export_history_bulk(
        store_uid=store_uid,
        campaign_id=campaign_id,
        rows=values,
        requested_at=requested_at,
        source="final_export",
    )
    async with httpx.AsyncClient(timeout=90) as client:
        for batch in _chunked(values, 500):
            body = {
                "offers": [
                    {
                        "offerId": item["sku"],
                        "price": {"value": item["price"], "currencyId": currency_id},
                    }
                    for item in batch
                ]
            }
            resp = await client.post(url, headers=_ym_headers(api_key), json=body)
            error_message = _market_response_error_message(resp)
            if error_message:
                logger.warning(
                    "[pricing_export] final market prices response store_uid=%s business_id=%s campaign_id=%s status=error message=%s",
                    store_uid,
                    business_id,
                    campaign_id,
                    error_message,
                )
                return {"kind": "market_prices", "status": "error", "message": error_message}
    logger.warning(
        "[pricing_export] final market prices response store_uid=%s business_id=%s campaign_id=%s status=success rows=%s",
        store_uid,
        business_id,
        campaign_id,
        len(values),
    )
    return {"kind": "market_prices", "status": "success", "updated_cells": len(values), "matched_rows": len(values), "values_total": len(values)}


async def _push_market_boosts_for_store(*, store_uid: str, business_id: str, campaign_id: str) -> dict[str, Any]:
    values = _load_market_boosts_payload(store_uid=store_uid)
    if not values:
        return {"kind": "market_boosts", "status": "skipped", "message": "Нет ставок буста для выгрузки"}
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        return {"kind": "market_boosts", "status": "error", "message": "Не найдены credentials Маркета"}
    _business_id, _campaign_id, api_key = creds
    body = {
        "bids": [
            {
                "sku": item["sku"],
                "bid": int(round(float(item["bid"]) * 100.0)),
            }
            for item in values
        ]
    }
    url = f"{YANDEX_BASE_URL}/businesses/{business_id}/bids"
    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.put(url, headers=_ym_headers(api_key), json=body)
        if resp.status_code >= 400:
            return {"kind": "market_boosts", "status": "error", "message": resp.text[:500] or f"HTTP {resp.status_code}"}
    return {"kind": "market_boosts", "status": "success", "updated_cells": len(values), "matched_rows": len(values), "values_total": len(values)}


async def _push_market_promos_for_store(*, store_uid: str, business_id: str, campaign_id: str, currency_code: str | None) -> dict[str, Any]:
    grouped, active_promo_ids, managed_skus = _load_market_promos_plan(store_uid=store_uid)
    if not grouped and not active_promo_ids:
        return {"kind": "market_promos", "status": "skipped", "message": "Нет товаров для участия в промо"}
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        return {"kind": "market_promos", "status": "error", "message": "Не найдены credentials Маркета"}
    _business_id, _campaign_id, api_key = creds
    updated = 0
    deleted = 0
    rejected: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    requested_by_promo: dict[str, set[str]] = {}
    async with httpx.AsyncClient(timeout=90) as client:
        for promo_id, items in grouped.items():
            requested_by_promo[promo_id] = {str(item.get("sku") or "").strip() for item in items if str(item.get("sku") or "").strip()}
            for batch in _chunked(items, 500):
                body = {
                    "promoId": promo_id,
                    "offers": [
                        {
                            "offerId": item["sku"],
                            "params": {
                                "discountParams": {
                                    "price": int(round(float(item["old_price"]))),
                                    "promoPrice": int(round(float(item["promo_price"]))),
                                }
                            },
                        }
                        for item in batch
                    ],
                }
                url = f"{YANDEX_BASE_URL}/businesses/{business_id}/promos/offers/update"
                resp = await client.post(url, headers=_ym_headers(api_key), json=body)
                if resp.status_code >= 400:
                    return {"kind": "market_promos", "status": "error", "message": resp.text[:500] or f"HTTP {resp.status_code}"}
                payload = resp.json() if resp.content else {}
                result = payload.get("result") if isinstance(payload, dict) else {}
                batch_rejected = result.get("rejectedOffers") if isinstance(result, dict) else []
                batch_warnings = result.get("warningOffers") if isinstance(result, dict) else []
                if isinstance(batch_rejected, list):
                    rejected.extend(x for x in batch_rejected if isinstance(x, dict))
                if isinstance(batch_warnings, list):
                    warnings.extend(x for x in batch_warnings if isinstance(x, dict))
                updated += len(batch)
    participating_statuses = {"MANUAL", "AUTO", "PARTIALLY_AUTO", "RENEWED", "MINIMUM_FOR_PROMOS"}
    rejected_statuses = {"NOT_PARTICIPATING", "RENEW_FAILED"}
    verified_by_promo: dict[str, list[str]] = {}
    pending_by_promo: dict[str, list[str]] = {}
    verification_errors: list[str] = []
    status_snapshot_by_promo: dict[str, dict[str, str]] = {}
    promo_ids_to_check = sorted({*active_promo_ids, *requested_by_promo.keys()})
    async with httpx.AsyncClient(timeout=90) as client:
        for promo_id in promo_ids_to_check:
            requested_skus = requested_by_promo.get(promo_id, set())
            try:
                offer_statuses = await _fetch_market_promo_offer_statuses(
                    business_id=business_id,
                    api_key=api_key,
                    promo_id=promo_id,
                )
                status_snapshot_by_promo[promo_id] = {
                    sku: str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper()
                    for sku in requested_skus
                    if sku in offer_statuses
                }
                verified = sorted(
                    sku for sku in requested_skus
                    if str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper() in participating_statuses
                )
                pending = sorted(
                    sku for sku in requested_skus
                    if str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper() not in participating_statuses | rejected_statuses
                )
                final_rejected = sorted(
                    sku for sku in requested_skus
                    if str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper() in rejected_statuses
                )
                if verified:
                    verified_by_promo[promo_id] = verified
                if pending:
                    pending_by_promo[promo_id] = pending
                for sku in final_rejected:
                    offer = offer_statuses.get(sku) or {}
                    rejected.append(
                        {
                            "offerId": sku,
                            "reason": _promo_offer_status_message(
                                status=str(offer.get("status") or "").strip(),
                                offer=offer,
                            ),
                        }
                    )
                removable = sorted(
                    sku
                    for sku, offer in offer_statuses.items()
                    if sku in managed_skus
                    and sku not in requested_skus
                    and str((offer or {}).get("status") or "").strip().upper() in participating_statuses
                )
                if removable:
                    for batch in _chunked(removable, 500):
                        delete_body = {
                            "promoId": promo_id,
                            "offerIds": batch,
                        }
                        delete_url = f"{YANDEX_BASE_URL}/businesses/{business_id}/promos/offers/delete"
                        delete_resp = await client.post(delete_url, headers=_ym_headers(api_key), json=delete_body)
                        if delete_resp.status_code >= 400:
                            verification_errors.append(f"{promo_id}: delete {delete_resp.text[:300] or delete_resp.status_code}")
                            continue
                        delete_payload = delete_resp.json() if delete_resp.content else {}
                        delete_result = delete_payload.get("result") if isinstance(delete_payload, dict) else {}
                        delete_rejected = delete_result.get("rejectedOffers") if isinstance(delete_result, dict) else []
                        if isinstance(delete_rejected, list):
                            rejected.extend(x for x in delete_rejected if isinstance(x, dict))
                        deleted += len(batch)
            except Exception as exc:
                verification_errors.append(f"{promo_id}: {exc}")

    if pending_by_promo:
        # Keep polling shortly after the update request: Market usually applies promo
        # participation quickly, and we want a final verified/rejected state whenever possible.
        for delay_seconds in (1, 2, 2, 3, 5, 5):
            await asyncio.sleep(delay_seconds)
            still_pending: dict[str, list[str]] = {}
            for promo_id, skus in list(pending_by_promo.items()):
                pending_set = {str(sku).strip() for sku in skus if str(sku).strip()}
                if not pending_set:
                    continue
                try:
                    offer_statuses = await _fetch_market_promo_offer_statuses(
                        business_id=business_id,
                        api_key=api_key,
                        promo_id=promo_id,
                    )
                except Exception as exc:
                    verification_errors.append(f"{promo_id}: {exc}")
                    still_pending[promo_id] = sorted(pending_set)
                    continue
                newly_verified = sorted(
                    sku for sku in pending_set
                    if str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper() in participating_statuses
                )
                newly_rejected = sorted(
                    sku for sku in pending_set
                    if str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper() in rejected_statuses
                )
                remaining = sorted(
                    sku for sku in pending_set
                    if str((offer_statuses.get(sku) or {}).get("status") or "").strip().upper() not in participating_statuses | rejected_statuses
                )
                if newly_verified:
                    verified_by_promo.setdefault(promo_id, [])
                    merged_verified = sorted({*verified_by_promo[promo_id], *newly_verified})
                    verified_by_promo[promo_id] = merged_verified
                for sku in newly_rejected:
                    offer = offer_statuses.get(sku) or {}
                    rejected.append(
                        {
                            "offerId": sku,
                            "reason": _promo_offer_status_message(
                                status=str(offer.get("status") or "").strip(),
                                offer=offer,
                            ),
                        }
                    )
                if remaining:
                    still_pending[promo_id] = remaining
            pending_by_promo = still_pending
            if not pending_by_promo:
                break

    message_parts: list[str] = []
    if rejected:
        preview = ", ".join(f"{str(item.get('offerId') or '').strip()}:{str(item.get('reason') or '').strip()}" for item in rejected[:10])
        if preview:
            message_parts.append(f"rejected={preview}")
    if warnings:
        preview = ", ".join(str(item.get("offerId") or "").strip() or "-" for item in warnings[:10])
        if preview:
            message_parts.append(f"warnings={preview}")
    if pending_by_promo:
        pending_total = sum(len(v) for v in pending_by_promo.values())
        message_parts.append(f"waiting_processing={pending_total}")
    if verification_errors:
        message_parts.append("verify_errors=" + "; ".join(verification_errors[:5]))

    feedback_by_sku: dict[str, dict[str, Any]] = {}

    def _merge_feedback(sku: str, status: str, message: str) -> None:
        current = feedback_by_sku.get(sku) or {}
        current_status = str(current.get("market_promo_status") or "").strip().lower()
        next_status = str(status or "").strip().lower()
        rank = {"verified": 4, "pending": 3, "rejected": 2, "warning": 1, "": 0}
        if rank.get(next_status, 0) >= rank.get(current_status, 0):
            feedback_by_sku[sku] = {
                "market_promo_status": next_status,
                "market_promo_message": str(message or "").strip(),
            }

    for promo_id, skus in verified_by_promo.items():
        for sku in skus:
            _merge_feedback(str(sku), "verified", f"promo_id={promo_id}")
    for promo_id, skus in pending_by_promo.items():
        for sku in skus:
            _merge_feedback(str(sku), "pending", f"promo_id={promo_id}")
    for item in rejected:
        sku = str(item.get("offerId") or "").strip()
        if not sku:
            continue
        _merge_feedback(sku, "rejected", str(item.get("reason") or "").strip())
    for item in warnings:
        sku = str(item.get("offerId") or "").strip()
        if not sku:
            continue
        warning_codes = ", ".join(
            str(entry.get("code") or "").strip()
            for entry in (item.get("warnings") or [])
            if isinstance(entry, dict) and str(entry.get("code") or "").strip()
        )
        _merge_feedback(sku, "warning", warning_codes)
    if feedback_by_sku:
        update_pricing_strategy_market_promo_feedback(
            store_uid=store_uid,
            feedback_by_sku=feedback_by_sku,
        )

    status = "success"
    if rejected:
        status = "warning"
    elif pending_by_promo:
        status = "warning"

    result_payload: dict[str, Any] = {
        "kind": "market_promos",
        "status": status,
        "updated_cells": updated,
        "matched_rows": updated,
        "values_total": updated,
        "deleted_offers": deleted,
        "verified_by_promo": verified_by_promo,
        "pending_by_promo": pending_by_promo,
        "rejected_offers": rejected,
        "warning_offers": warnings,
    }
    if message_parts:
        result_payload["message"] = " | ".join(message_parts)
    return result_payload


def _export_google_for_store(*, store_uid: str, export_kinds: list[str] | None = None) -> dict[str, Any]:
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    store = _load_store_row(suid) or {}
    result_rows: list[dict[str, Any]] = []
    ok = True
    if not _has_embedded_google_credentials():
        descriptors = _build_google_export_descriptors(store_uid=suid, export_kinds=export_kinds)
        for item in descriptors:
            result_rows.append(
                {
                    "kind": item.get("kind") or "google_export",
                    "status": "skipped",
                    "source_id": item.get("source_id"),
                    "message": "Google credentials недоступны в integrations.json",
                }
            )
        return {
            "ok": True,
            "store_uid": suid,
            "store_id": str(store.get("store_id") or "").strip(),
            "store_name": str(store.get("store_name") or store.get("store_id") or suid).strip(),
            "results": result_rows,
        }
    present_skus = _load_present_store_skus(store_uid=suid)
    descriptors = _build_google_export_descriptors(store_uid=suid, export_kinds=export_kinds)
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for item in descriptors:
        if item.get("status"):
            if str(item.get("status")).strip().lower() == "error":
                ok = False
            result_rows.append(item)
            continue
        key = (
            str(item.get("spreadsheet_id") or "").strip(),
            str(item.get("worksheet") or "").strip(),
            str(item.get("sku_column") or "").strip(),
        )
        grouped.setdefault(key, []).append(item)

    for (spreadsheet_id, worksheet, sku_column), items in grouped.items():
        try:
            if len(items) > 1:
                values_by_column = {
                    str(item.get("value_column") or "").strip(): dict(item.get("values_by_sku") or {})
                    for item in items
                }
                update_info = update_sheet_columns_by_sku(
                    spreadsheet_id=spreadsheet_id,
                    worksheet=worksheet or None,
                    sku_column=sku_column,
                    values_by_column=values_by_column,
                    present_skus=present_skus,
                )
                updated_by_column = update_info.get("updated_by_column") if isinstance(update_info, dict) else {}
                for item in items:
                    value_column = str(item.get("value_column") or "").strip()
                    result_rows.append(
                        {
                            "kind": item["kind"],
                            "status": "success",
                            "source_id": item.get("source_id"),
                            "updated_cells": int((updated_by_column or {}).get(value_column) or 0),
                            "matched_rows": int(update_info.get("matched_rows") or 0),
                            "values_total": len(item.get("values_by_sku") or {}),
                        }
                    )
            else:
                item = items[0]
                update_info = update_sheet_column_by_sku(
                    spreadsheet_id=spreadsheet_id,
                    worksheet=worksheet or None,
                    sku_column=sku_column,
                    value_column=str(item.get("value_column") or "").strip(),
                    values_by_sku=dict(item.get("values_by_sku") or {}),
                    present_skus=present_skus,
                )
                result_rows.append(
                    {
                        "kind": item["kind"],
                        "status": "success",
                        "source_id": item.get("source_id"),
                        "updated_cells": int(update_info.get("updated_cells") or 0),
                        "matched_rows": int(update_info.get("matched_rows") or 0),
                        "values_total": len(item.get("values_by_sku") or {}),
                    }
                )
        except Exception as exc:
            ok = False
            for item in items:
                result_rows.append(
                    {
                        "kind": item["kind"],
                        "status": "error",
                        "source_id": item.get("source_id"),
                        "message": str(exc),
                    }
                )

    return {
        "ok": ok,
        "store_uid": suid,
        "store_id": str(store.get("store_id") or "").strip(),
        "store_name": str(store.get("store_name") or store.get("store_id") or suid).strip(),
        "results": result_rows,
    }


async def export_strategy_outputs_for_store(*, store_uid: str, export_kinds: list[str] | None = None) -> dict[str, Any]:
    suid = str(store_uid or "").strip()
    if not suid:
        raise ValueError("store_uid обязателен")
    store = _load_store_row(suid) or {}
    strategy_fresh, stale_message = _strategy_layer_is_fresh(store_uid=suid)
    if not strategy_fresh:
        return {
            "ok": False,
            "store_uid": suid,
            "store_id": str(store.get("store_id") or "").strip(),
            "store_name": str(store.get("store_name") or store.get("store_id") or suid).strip(),
            "results": [
                {
                    "kind": "strategy_guard",
                    "status": "error",
                    "message": stale_message or "Стратегия устарела",
                }
            ],
        }
    google_result = _export_google_for_store(store_uid=suid, export_kinds=export_kinds)
    result_rows = list(google_result.get("results") or [])
    ok = bool(google_result.get("ok"))
    logger.warning(
        "[pricing_export] google export store_uid=%s ok=%s results=%s",
        suid,
        ok,
        [
            {
                "kind": str(item.get("kind") or "").strip(),
                "status": str(item.get("status") or "").strip(),
                "source_id": str(item.get("source_id") or "").strip(),
                "updated_cells": int(item.get("updated_cells") or 0),
                "matched_rows": int(item.get("matched_rows") or 0),
                "values_total": int(item.get("values_total") or 0),
                "message": str(item.get("message") or "").strip(),
            }
            for item in result_rows
            if isinstance(item, dict)
        ],
    )

    platform = str(store.get("platform") or "").strip().lower()
    store_id = str(store.get("store_id") or "").strip()
    currency_code = str(store.get("currency_code") or "").strip().upper() or None
    if platform == "yandex_market":
        if not _is_market_export_enabled(store_uid=suid, platform=platform, store_id=store_id):
            result_rows.extend(
                [
                    {"kind": "market_prices", "status": "skipped", "message": "У магазина выключен экспорт на площадку"},
                    {"kind": "market_promos", "status": "skipped", "message": "У магазина выключен экспорт на площадку"},
                    {"kind": "market_boosts", "status": "skipped", "message": "У магазина выключен экспорт на площадку"},
                ]
            )
        else:
            creds = _find_yandex_shop_credentials(store_id)
            if not creds:
                result_rows.extend(
                    [
                        {"kind": "market_prices", "status": "error", "message": "Не найдены credentials Маркета"},
                        {"kind": "market_promos", "status": "error", "message": "Не найдены credentials Маркета"},
                        {"kind": "market_boosts", "status": "error", "message": "Не найдены credentials Маркета"},
                    ]
                )
                ok = False
            else:
                business_id, campaign_id, _api_key = creds
                market_prices = await _push_market_prices_for_store(
                    store_uid=suid,
                    campaign_id=campaign_id,
                    currency_code=currency_code,
                )
                market_promos = await _push_market_promos_for_store(
                    store_uid=suid,
                    business_id=business_id,
                    campaign_id=campaign_id,
                    currency_code=currency_code,
                )
                market_boosts = await _push_market_boosts_for_store(
                    store_uid=suid,
                    business_id=business_id,
                    campaign_id=campaign_id,
                )
                result_rows.extend([market_prices, market_promos, market_boosts])
                ok = ok and all(str(item.get("status") or "").strip().lower() != "error" for item in [market_prices, market_promos, market_boosts])

    return {
        "ok": ok,
        "store_uid": suid,
        "store_id": store_id,
        "store_name": str(store.get("store_name") or store.get("store_id") or suid).strip(),
        "results": result_rows,
    }


async def export_strategy_outputs_for_all_stores(*, store_uids: list[str] | None = None) -> dict[str, Any]:
    selected = [str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()]
    if not selected:
        selected = [
            str(store.get("store_uid") or "").strip()
            for store in (_catalog_marketplace_stores_context() or [])
            if str(store.get("store_uid") or "").strip()
        ]

    results = []
    for suid in selected:
        results.append(await export_strategy_outputs_for_store(store_uid=suid))
    return {
        "ok": all(bool(item.get("ok")) for item in results),
        "stores": results,
    }
