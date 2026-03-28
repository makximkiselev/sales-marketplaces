from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Any

import httpx

from backend.routers._shared import YANDEX_BASE_URL, _find_yandex_shop_credentials, _ym_headers
from backend.services.numeric_helpers import to_num_loose
from backend.services.pricing_prices_service import (
    _profit_for_price,
    get_prices_context,
    get_prices_overview,
    get_prices_tree,
    invalidate_prices_cache,
    refresh_prices_data,
)
from backend.services.store_data_model import (
    get_pricing_attractiveness_results_map,
    get_pricing_price_results_map,
    get_pricing_store_settings,
    clear_pricing_promo_results_for_store,
    get_pricing_promo_columns,
    get_pricing_promo_offer_raw_map,
    get_pricing_promo_offer_results_map,
    get_pricing_promo_results_map,
    get_pricing_strategy_iteration_latest_map,
    get_pricing_strategy_results_map,
    update_pricing_strategy_market_promo_feedback,
    upsert_pricing_promo_campaign_raw_bulk,
    upsert_pricing_promo_offer_results_bulk,
    upsert_pricing_promo_offer_raw_bulk,
    upsert_pricing_promo_results_bulk,
)
from backend.services.service_cache_helpers import cache_get_copy, cache_set_copy, make_cache_key

logger = logging.getLogger("uvicorn.error")

_PROMOS_CACHE: dict[str, dict] = {}
_PROMOS_CACHE_GEN = 1
_PROMOS_CACHE_MAX = 400


def _cache_key(name: str, payload: dict) -> str:
    return make_cache_key(name, payload, _PROMOS_CACHE_GEN)


def _cache_get(name: str, payload: dict):
    key = _cache_key(name, payload)
    return cache_get_copy(_PROMOS_CACHE, key)


def _cache_set(name: str, payload: dict, value: dict):
    key = _cache_key(name, payload)
    cache_set_copy(_PROMOS_CACHE, key, value, _PROMOS_CACHE_MAX)


def invalidate_promos_cache():
    global _PROMOS_CACHE_GEN
    _PROMOS_CACHE.clear()
    _PROMOS_CACHE_GEN += 1


def _normalize_selected_promo_summary(
    summary: dict[str, Any] | None,
    offers: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    item = dict(summary or {})
    offer_rows = [x for x in (offers or []) if isinstance(x, dict)]
    selected_price = _to_num(item.get("promo_selected_price"))
    if not offer_rows or selected_price is None:
        return item

    matching = [
        x
        for x in offer_rows
        if _to_num(x.get("promo_price")) is not None and abs(float(_to_num(x.get("promo_price")) or 0.0) - float(selected_price)) < 0.01
    ]
    if not matching:
        matching = [
            x
            for x in offer_rows
            if str(x.get("promo_fit_mode") or "").strip().lower() in {"with_ads", "without_ads"}
        ]
    if not matching:
        return item

    chosen = sorted(
        matching,
        key=lambda x: (
            (_to_num(x.get("promo_price")) is None),
            _to_num(x.get("promo_price")) or 0.0,
            str(x.get("promo_name") or x.get("promo_id") or ""),
        ),
    )[0]
    status_text = {
        "with_ads": "Проходит",
        "without_ads": "Проходит без ДРР",
        "rejected": "Не проходит",
    }.get(str(chosen.get("promo_fit_mode") or "").strip().lower(), "Не проходит")
    chosen_name = str(chosen.get("promo_name") or chosen.get("promo_id") or "").strip()
    if chosen_name:
        item["promo_selected_items"] = [{"name": chosen_name, "status": status_text}]
    return item


def _to_num(v: Any) -> float | None:
    return to_num_loose(v)


def _next_page_token(payload: dict) -> str:
    root = payload if isinstance(payload, dict) else {}
    result = root.get("result") if isinstance(root.get("result"), dict) else root
    paging = result.get("paging") if isinstance(result.get("paging"), dict) else {}
    return str(
        paging.get("nextPageToken")
        or paging.get("next_page_token")
        or result.get("nextPageToken")
        or root.get("nextPageToken")
        or ""
    ).strip()


def _extract_yandex_promos(payload: dict) -> list[dict[str, Any]]:
    root = payload if isinstance(payload, dict) else {}
    result = root.get("result") if isinstance(root.get("result"), dict) else root
    raw_items = []
    for key in ("promos", "items", "promoOffers", "promo"):
        arr = result.get(key) if isinstance(result.get(key), list) else None
        if arr:
            raw_items.extend([x for x in arr if isinstance(x, dict)])
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw_items:
        promo_id = str(item.get("promoId") or item.get("id") or item.get("promo_id") or "").strip()
        if not promo_id or promo_id in seen:
            continue
        seen.add(promo_id)
        out.append(
            {
                "promo_id": promo_id,
                "promo_name": str(item.get("name") or item.get("promoName") or item.get("title") or promo_id).strip() or promo_id,
                "dateTimeFrom": (
                    item.get("dateTimeFrom")
                    or ((item.get("period") or {}).get("dateTimeFrom") if isinstance(item.get("period"), dict) else None)
                ),
                "dateTimeTo": (
                    item.get("dateTimeTo")
                    or ((item.get("period") or {}).get("dateTimeTo") if isinstance(item.get("period"), dict) else None)
                ),
                "startDate": item.get("startDate") or item.get("startTime") or item.get("startsAt") or item.get("starts_at") or item.get("fromDate") or item.get("from"),
                "endDate": item.get("endDate") or item.get("endTime") or item.get("endsAt") or item.get("ends_at") or item.get("toDate") or item.get("to"),
                "payload": item,
            }
        )
    return out


def _parse_promo_datetime(value: Any, *, is_end: bool = False) -> dt.datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        pass
    for chunk in (raw[:10], raw.split("T", 1)[0], raw.split(" ", 1)[0]):
        try:
            date_val = dt.date.fromisoformat(chunk)
            time_val = dt.time.max if is_end else dt.time.min
            return dt.datetime.combine(date_val, time_val, tzinfo=dt.timezone.utc)
        except Exception:
            continue
    return None


def _promo_is_active_today(item: dict[str, Any], *, now: dt.datetime | None = None) -> bool:
    obj = item if isinstance(item, dict) else {}
    now_val = now or dt.datetime.now(dt.timezone.utc)
    start_candidates = (
        obj.get("dateTimeFrom"),
        obj.get("date_from"),
        obj.get("startDate"),
        obj.get("startTime"),
        obj.get("startsAt"),
        obj.get("starts_at"),
        obj.get("fromDate"),
        obj.get("from"),
    )
    end_candidates = (
        obj.get("dateTimeTo"),
        obj.get("date_to"),
        obj.get("endDate"),
        obj.get("endTime"),
        obj.get("endsAt"),
        obj.get("ends_at"),
        obj.get("toDate"),
        obj.get("to"),
    )
    start_at = next((parsed for parsed in (_parse_promo_datetime(v, is_end=False) for v in start_candidates) if parsed is not None), None)
    end_at = next((parsed for parsed in (_parse_promo_datetime(v, is_end=True) for v in end_candidates) if parsed is not None), None)
    if start_at and now_val < start_at:
        return False
    if end_at and now_val > end_at:
        return False
    return True


def _extract_promo_price(item: dict) -> float | None:
    def pick(*path: str) -> float | None:
        cur: Any = item
        for part in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        if isinstance(cur, dict):
            return _to_num(cur.get("value"))
        return _to_num(cur)

    threshold_candidates = [
        pick("params", "discountParams", "maxPromoPrice"),
        pick("params", "discountParams", "price"),
        pick("discountParams", "maxPromoPrice"),
        pick("discountParams", "price"),
        pick("maxPromoPrice"),
        pick("price"),
    ]
    numeric_thresholds = [v for v in threshold_candidates if v is not None and v > 0]
    if numeric_thresholds:
        return min(numeric_thresholds)

    candidates = [
        pick("params", "discountParams", "promoPrice"),
        pick("discountParams", "promoPrice"),
        pick("promoPrice"),
    ]
    numeric = [v for v in candidates if v is not None and v > 0]
    if numeric:
        return min(numeric)
    return None


def _promo_floor_target_pct(*, target_pct: float | None, minimum_profit_pct: float | None) -> float:
    min_floor = _to_num(minimum_profit_pct)
    if min_floor is not None and min_floor > 0:
        return round(float(min_floor), 2)
    if target_pct is None:
        return 4.0
    return max(3.0, min(float(target_pct), 4.0))


def build_market_promo_details(
    *,
    promo_offers: list[dict[str, Any]],
    promo_offers_raw: list[dict[str, Any]],
    installed_price_view: float | None,
    effective_market_promo_status: str,
    effective_market_promo_message: str,
    promo_candidate_ready: bool,
) -> tuple[bool, list[dict[str, Any]]]:
    promo_details: list[dict[str, Any]] = []
    participating_statuses = {"AUTO", "PARTIALLY_AUTO", "MANUAL", "RENEWED", "MINIMUM_FOR_PROMOS"}
    promo_offer_by_id = {
        str(offer.get("promo_id") or "").strip(): offer
        for offer in promo_offers
        if str(offer.get("promo_id") or "").strip()
    }
    promo_raw_by_id = {
        str(offer.get("promo_id") or "").strip(): offer
        for offer in promo_offers_raw
        if str(offer.get("promo_id") or "").strip()
    }
    targeted_promo_id = ""
    promo_message_raw = str(effective_market_promo_message or "").strip()
    if promo_message_raw.lower().startswith("promo_id="):
        targeted_promo_id = promo_message_raw.split("=", 1)[1].strip()
    market_status_norm = str(effective_market_promo_status or "").strip().lower()
    promo_participates = False
    for promo_id in sorted(set([*promo_offer_by_id.keys(), *promo_raw_by_id.keys()])):
        raw_offer = promo_raw_by_id.get(promo_id) or {}
        result_offer = promo_offer_by_id.get(promo_id) or {}
        promo_name = str(raw_offer.get("promo_name") or result_offer.get("promo_name") or promo_id).strip() or promo_id
        payload = raw_offer.get("payload") if isinstance(raw_offer.get("payload"), dict) else {}
        raw_status = str(payload.get("status") or "").strip().upper()
        discount_params = payload.get("params", {}).get("discountParams", {}) if isinstance(payload.get("params"), dict) else {}
        promo_threshold = _to_num(discount_params.get("maxPromoPrice")) or _to_num(raw_offer.get("promo_price")) or _to_num(result_offer.get("promo_price"))
        installed_fits_threshold = (
            installed_price_view not in (None, 0)
            and promo_threshold not in (None, 0)
            and float(installed_price_view) <= float(promo_threshold) + 0.01
        )
        status_label = "Нет статуса"
        status_tone = "warning"

        if market_status_norm == "verified" and targeted_promo_id and promo_id == targeted_promo_id and installed_fits_threshold:
            status_label = "Участвует"
            status_tone = "positive"
        elif market_status_norm == "verified" and not targeted_promo_id and installed_fits_threshold:
            status_label = "Участвует"
            status_tone = "positive"
        elif market_status_norm == "verified" and not installed_fits_threshold:
            status_label = "Не прошли по цене"
            status_tone = "negative"
        elif raw_status in participating_statuses and installed_fits_threshold:
            status_label = "Участвует"
            status_tone = "positive"
        elif market_status_norm == "pending" and targeted_promo_id and promo_id == targeted_promo_id:
            status_label = "Ждёт подтверждения"
            status_tone = "warning"
        elif market_status_norm == "pending" and not targeted_promo_id:
            status_label = "Ждёт подтверждения"
            status_tone = "warning"
        elif market_status_norm == "rejected":
            message_upper = promo_message_raw.upper()
            status_label = "Отклонено"
            status_tone = "negative"
            if "MAX_PROMO_PRICE_EXCEEDED" in message_upper:
                status_label = "Не прошли по цене"
        elif market_status_norm == "warning":
            status_label = "Предупреждение"
            status_tone = "warning"
        elif market_status_norm == "error":
            status_label = "Ошибка"
            status_tone = "negative"
        elif targeted_promo_id and promo_id != targeted_promo_id:
            if installed_fits_threshold:
                status_label = "Не участвует"
                status_tone = "warning"
            else:
                status_label = "Не прошли по цене"
                status_tone = "negative"
        elif raw_status == "NOT_PARTICIPATING":
            status_label = "Не участвует"
            status_tone = "warning"
        elif raw_status == "RENEW_FAILED":
            status_label = "Ошибка продления"
            status_tone = "negative"
        elif raw_status:
            status_label = "Ждёт подтверждения"
            status_tone = "warning"
        detail_parts = []
        if installed_price_view not in (None, 0):
            detail_parts.append(f"наша {int(round(float(installed_price_view)))}")
        if promo_threshold not in (None, 0):
            detail_parts.append(f"порог {int(round(float(promo_threshold)))}")
        promo_details.append(
            {
                "promo_id": promo_id,
                "promo_name": promo_name,
                "status_label": status_label,
                "status_tone": status_tone,
                "threshold_price": promo_threshold,
                "detail": " / ".join(detail_parts),
            }
        )
        if status_tone == "positive":
            promo_participates = True
    return promo_participates, promo_details


def _status_feedback_from_promo_entries(*, promo_entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    participating_statuses = {"AUTO", "PARTIALLY_AUTO", "MANUAL", "RENEWED", "MINIMUM_FOR_PROMOS"}
    rejected_statuses = {"NOT_PARTICIPATING", "RENEW_FAILED"}
    out: dict[str, dict[str, Any]] = {}
    by_sku: dict[str, list[dict[str, Any]]] = {}
    for entry in promo_entries:
        sku = str(entry.get("sku") or "").strip()
        if not sku:
            continue
        by_sku.setdefault(sku, []).append(entry)
    for sku, entries in by_sku.items():
        normalized = []
        for entry in entries:
            payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
            status = str(payload.get("status") or "").strip().upper()
            promo_id = str(entry.get("promo_id") or "").strip()
            if not status or not promo_id:
                continue
            normalized.append({"promo_id": promo_id, "status": status})
        if not normalized:
            continue
        verified = next((item for item in normalized if item["status"] in participating_statuses), None)
        if verified:
            out[sku] = {
                "market_promo_status": "verified",
                "market_promo_message": f"promo_id={verified['promo_id']}",
            }
            continue
        pending = next((item for item in normalized if item["status"] not in rejected_statuses), None)
        if pending:
            out[sku] = {
                "market_promo_status": "pending",
                "market_promo_message": f"promo_id={pending['promo_id']}",
            }
            continue
        rejected = normalized[0]
        out[sku] = {
            "market_promo_status": "rejected",
            "market_promo_message": rejected["status"],
        }
    return out


def _scenario_promo_payload_from_current_data(
    *,
    iteration_row: dict[str, Any] | None,
    promo_offers: list[dict[str, Any]],
    promo_offers_raw: list[dict[str, Any]],
) -> dict[str, Any]:
    row = iteration_row if isinstance(iteration_row, dict) else {}
    tested_price = _to_num(row.get("tested_price"))
    normalized_details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_offer in promo_offers_raw or []:
        if not isinstance(raw_offer, dict):
            continue
        promo_id = str(raw_offer.get("promo_id") or "").strip()
        if not promo_id or promo_id in seen:
            continue
        seen.add(promo_id)
        promo_name = str(raw_offer.get("promo_name") or promo_id).strip() or promo_id
        payload = raw_offer.get("payload") if isinstance(raw_offer.get("payload"), dict) else {}
        discount = payload.get("params", {}).get("discountParams", {}) if isinstance(payload.get("params"), dict) else {}
        threshold_price = (
            _to_num(discount.get("maxPromoPrice"))
            or _to_num(raw_offer.get("promo_price"))
            or next(
                (
                    _to_num(item.get("promo_price"))
                    for item in (promo_offers or [])
                    if isinstance(item, dict) and str(item.get("promo_id") or "").strip() == promo_id
                ),
                None,
            )
        )
        fits = tested_price not in (None, 0) and threshold_price not in (None, 0) and float(tested_price) <= float(threshold_price) + 0.01
        normalized_details.append(
            {
                "promo_id": promo_id,
                "promo_name": promo_name,
                "status_label": "Участвует" if fits else "Не прошли по цене",
                "status_tone": "positive" if fits else "negative",
                "threshold_price": threshold_price,
                "detail": (
                    f"наша {int(round(float(tested_price)))} / порог {int(round(float(threshold_price)))}"
                    if tested_price not in (None, 0) and threshold_price not in (None, 0)
                    else ""
                ),
            }
        )
    normalized_details.sort(key=lambda item: str(item.get("promo_name") or ""))
    return {
        "price": tested_price,
        "boost_pct": _to_num(row.get("tested_boost_pct")),
        "promo_count": sum(1 for item in normalized_details if str(item.get("status_tone") or "") == "positive"),
        "market_promo_status": str(row.get("market_promo_status") or "").strip(),
        "market_promo_message": str(row.get("market_promo_message") or "").strip(),
        "promo_details": normalized_details,
    }


def _extract_yandex_promo_offers(payload: dict, *, promo_id: str, promo_name: str) -> dict[str, dict[str, Any]]:
    root = payload if isinstance(payload, dict) else {}
    result = root.get("result") if isinstance(root.get("result"), dict) else root
    raw_items = []
    for key in ("offers", "items", "promoOffers"):
        arr = result.get(key) if isinstance(result.get(key), list) else None
        if arr:
            raw_items.extend([x for x in arr if isinstance(x, dict)])
    out: dict[str, dict[str, Any]] = {}
    for item in raw_items:
        if not _promo_is_active_today(item):
            continue
        offer_obj = item.get("offer") if isinstance(item.get("offer"), dict) else {}
        sku = str(
            item.get("offerId")
            or item.get("shopSku")
            or item.get("offer_id")
            or offer_obj.get("offerId")
            or offer_obj.get("shopSku")
            or offer_obj.get("shop_sku")
            or ""
        ).strip()
        if not sku:
            continue
        promo_price = _extract_promo_price(item)
        current = out.get(sku)
        if current is None or (promo_price is not None and (current.get("promo_price") is None or promo_price < current.get("promo_price"))):
            out[sku] = {
                "promo_id": promo_id,
                "promo_name": promo_name,
                "promo_price": promo_price,
                "date_time_from": item.get("dateTimeFrom") or item.get("startDate") or item.get("startTime") or item.get("startsAt") or item.get("starts_at") or item.get("fromDate") or item.get("from"),
                "date_time_to": item.get("dateTimeTo") or item.get("endDate") or item.get("endTime") or item.get("endsAt") or item.get("ends_at") or item.get("toDate") or item.get("to"),
                "payload": item,
            }
    return out


async def _fetch_yandex_promos_for_business(*, business_id: str, api_key: str) -> list[dict[str, str]]:
    bid = str(business_id or "").strip()
    if not bid:
        return []
    url = f"{YANDEX_BASE_URL}/businesses/{bid}/promos"
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    page_token = ""
    seen_page_tokens: set[str] = set()
    async with httpx.AsyncClient(timeout=45) as client:
        while True:
            body: dict[str, Any] = {}
            if page_token:
                body["pageToken"] = page_token
            logger.warning("[pricing_promos] Yandex promos request business_id=%s page_token=%s url=%s", bid, page_token or "-", url)
            resp = await client.post(url, headers=_ym_headers(api_key), json=body)
            logger.warning("[pricing_promos] Yandex promos response business_id=%s status=%s", bid, resp.status_code)
            resp.raise_for_status()
            payload = resp.json()
            for promo in _extract_yandex_promos(payload):
                if not _promo_is_active_today(promo):
                    continue
                promo_id = promo["promo_id"]
                if promo_id in seen:
                    continue
                seen.add(promo_id)
                out.append(promo)
            next_page_token = _next_page_token(payload)
            if not next_page_token or next_page_token == page_token or next_page_token in seen_page_tokens:
                break
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
    return out


async def _fetch_yandex_promo_offer_map(*, business_id: str, api_key: str, promos: list[dict[str, str]]) -> dict[str, list[dict[str, Any]]]:
    bid = str(business_id or "").strip()
    if not bid or not promos:
        return {}
    url = f"{YANDEX_BASE_URL}/businesses/{bid}/promos/offers"
    out: dict[str, list[dict[str, Any]]] = {}
    async with httpx.AsyncClient(timeout=60) as client:
        for promo in promos:
            promo_id = str(promo.get("promo_id") or "").strip()
            promo_name = str(promo.get("promo_name") or promo_id).strip() or promo_id
            if not promo_id:
                continue
            page_token = ""
            seen_page_tokens: set[str] = set()
            while True:
                body: dict[str, Any] = {"promoId": promo_id}
                params: dict[str, Any] = {"limit": 500}
                if page_token:
                    params["page_token"] = page_token
                logger.warning(
                    "[pricing_promos] Yandex promo offers request business_id=%s promo_id=%s page_token=%s url=%s",
                    bid,
                    promo_id,
                    page_token or "-",
                    url,
                )
                resp = await client.post(url, headers=_ym_headers(api_key), params=params, json=body)
                logger.warning("[pricing_promos] Yandex promo offers response business_id=%s promo_id=%s status=%s", bid, promo_id, resp.status_code)
                resp.raise_for_status()
                payload = resp.json()
                parsed = _extract_yandex_promo_offers(payload, promo_id=promo_id, promo_name=promo_name)
                for sku, item in parsed.items():
                    out.setdefault(sku, []).append(item)
                next_page_token = _next_page_token(payload)
                if not next_page_token or next_page_token == page_token or next_page_token in seen_page_tokens:
                    break
                seen_page_tokens.add(next_page_token)
                page_token = next_page_token
    for sku, items in out.items():
        items.sort(key=lambda x: ((_to_num(x.get("promo_price")) is None), _to_num(x.get("promo_price")) or 0, str(x.get("promo_name") or "")))
    return out


def _calc_profit_from_ctx_with_ads_percent(price: float | None, calc_ctx: dict[str, Any] | None, ads_percent: float | None) -> tuple[float | None, float | None]:
    if price is None or not isinstance(calc_ctx, dict):
        return None, None
    dep_rate = float(calc_ctx.get("dep_rate") or 0.0)
    ads_rate_max = float(calc_ctx.get("ads_rate") or 0.0)
    dep_without_ads = max(0.0, dep_rate - ads_rate_max)
    ads_rate = max(0.0, float(ads_percent or 0.0) / 100.0)
    pa, pp = _profit_for_price(
        price=float(price),
        dep_rate=dep_without_ads + ads_rate,
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


async def get_promos_context():
    return await get_prices_context()


async def get_promos_tree(
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


async def get_promos_overview(
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
        base_rows = base.get("rows") if isinstance(base, dict) else []
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
        base_rows = rows_all
    stores = base.get("stores") if isinstance(base, dict) else []
    store_uids = [str(s.get("store_uid") or "").strip() for s in stores if str(s.get("store_uid") or "").strip()]
    skus = [str(r.get("sku") or "").strip() for r in base_rows if str(r.get("sku") or "").strip()]

    promo_summary_map = get_pricing_promo_results_map(store_uids=store_uids, skus=skus)
    promo_offer_map = get_pricing_promo_offer_results_map(store_uids=store_uids, skus=skus)
    promo_offer_raw_map = get_pricing_promo_offer_raw_map(store_uids=store_uids, skus=skus)
    attr_map = get_pricing_attractiveness_results_map(store_uids=store_uids, skus=skus)
    strategy_map = get_pricing_strategy_results_map(store_uids=store_uids, skus=skus)
    price_map = get_pricing_price_results_map(store_uids=store_uids, skus=skus)
    iteration_map = get_pricing_strategy_iteration_latest_map(store_uids=store_uids, skus=skus)

    promo_columns: list[dict[str, str]] = []
    if str(scope or "all").strip().lower() == "store" and str(platform or "").strip().lower() == "yandex_market" and str(store_id or "").strip():
        suid = f"{str(platform).strip().lower()}:{str(store_id).strip()}"
        promo_columns = get_pricing_promo_columns(store_uid=suid)

    rows_out: list[dict[str, Any]] = []
    for row in base_rows:
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        summary_by_store: dict[str, dict[str, Any]] = {}
        offers_by_store: dict[str, list[dict[str, Any]]] = {}
        details_by_store: dict[str, list[dict[str, Any]]] = {}
        market_status_by_store: dict[str, str] = {}
        market_message_by_store: dict[str, str] = {}
        installed_price_by_store: dict[str, float | None] = {}
        iteration_scenarios_by_store: dict[str, dict[str, Any]] = {}
        for suid in store_uids:
            offers = (promo_offer_map.get(suid) or {}).get(sku) or []
            offers_raw = (promo_offer_raw_map.get(suid) or {}).get(sku) or []
            summary = _normalize_selected_promo_summary((promo_summary_map.get(suid) or {}).get(sku), offers)
            attr = (attr_map.get(suid) or {}).get(sku) or {}
            strategy_item = (strategy_map.get(suid) or {}).get(sku) or {}
            metric = ((row.get("price_metrics_by_store") or {}).get(suid) or {}) if isinstance(row.get("price_metrics_by_store"), dict) else {}
            chosen_price = _to_num(attr.get("attractiveness_chosen_price"))
            chosen_boost_pct = _to_num(attr.get("attractiveness_chosen_boost_bid_percent"))
            chosen_profit_abs, chosen_profit_pct = _calc_profit_from_ctx_with_ads_percent(
                chosen_price,
                metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else None,
                chosen_boost_pct,
            )
            selected_price = _to_num(summary.get("promo_selected_price")) if isinstance(summary, dict) else None
            selected_boost_pct = _to_num(summary.get("promo_selected_boost_bid_percent")) if isinstance(summary, dict) else None
            selected_profit_abs = _to_num(summary.get("promo_selected_profit_abs")) if isinstance(summary, dict) else None
            selected_profit_pct = _to_num(summary.get("promo_selected_profit_pct")) if isinstance(summary, dict) else None
            calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else None
            store_settings = get_pricing_store_settings(store_uid=suid) or {}
            minimum_profit_pct_num = _to_num(store_settings.get("minimum_profit_percent"))
            if selected_price is not None:
                if chosen_price is not None and abs(float(selected_price) - float(chosen_price)) < 0.01:
                    selected_boost_pct = chosen_boost_pct
                    selected_profit_abs = chosen_profit_abs
                    selected_profit_pct = chosen_profit_pct
                elif selected_boost_pct is None or selected_profit_abs is None or selected_profit_pct is None:
                    pa, pp = _calc_profit_from_ctx_with_ads_percent(
                        selected_price,
                        calc_ctx,
                        0.0,
                    )
                    if minimum_profit_pct_num is None or (pp is not None and float(pp) >= float(minimum_profit_pct_num) - 0.05):
                        selected_boost_pct = 0.0
                        selected_profit_abs = None if pa is None else float(int(round(pa)))
                        selected_profit_pct = None if pp is None else round(float(pp), 2)
            if summary:
                summary_by_store[suid] = {
                    "promo_selected_items": summary.get("promo_selected_items") or [],
                    "promo_selected_price": selected_price,
                    "promo_selected_boost_bid_percent": selected_boost_pct,
                    "promo_selected_profit_abs": selected_profit_abs,
                    "promo_selected_profit_pct": selected_profit_pct,
                    "attractiveness_chosen_price": chosen_price,
                    "attractiveness_chosen_boost_bid_percent": chosen_boost_pct,
                    "attractiveness_chosen_profit_abs": chosen_profit_abs,
                    "attractiveness_chosen_profit_pct": chosen_profit_pct,
                }
            elif chosen_price is not None:
                summary_by_store[suid] = {
                    "promo_selected_items": [],
                    "promo_selected_price": chosen_price,
                    "promo_selected_boost_bid_percent": chosen_boost_pct,
                    "promo_selected_profit_abs": chosen_profit_abs,
                    "promo_selected_profit_pct": chosen_profit_pct,
                    "attractiveness_chosen_price": chosen_price,
                    "attractiveness_chosen_boost_bid_percent": chosen_boost_pct,
                    "attractiveness_chosen_profit_abs": chosen_profit_abs,
                    "attractiveness_chosen_profit_pct": chosen_profit_pct,
                }
            if offers:
                offers_by_store[suid] = offers
            installed_price = _to_num(strategy_item.get("installed_price"))
            market_status = str(strategy_item.get("market_promo_status") or "").strip()
            market_message = str(strategy_item.get("market_promo_message") or "").strip()
            promo_candidate_ready = bool(
                _to_num(summary.get("promo_selected_price")) not in (None, 0)
                or _to_num(chosen_price) not in (None, 0)
            )
            promo_participates, promo_details = build_market_promo_details(
                promo_offers=offers,
                promo_offers_raw=offers_raw,
                installed_price_view=installed_price,
                effective_market_promo_status=market_status,
                effective_market_promo_message=market_message,
                promo_candidate_ready=promo_candidate_ready,
            )
            details_by_store[suid] = promo_details
            market_status_by_store[suid] = market_status
            market_message_by_store[suid] = market_message
            installed_price_by_store[suid] = installed_price
            latest_iterations = ((iteration_map.get(suid) or {}).get(sku) or {}) if isinstance(iteration_map.get(suid), dict) else {}
            price_metric = ((price_map.get(suid) or {}).get(sku) or {}) if isinstance(price_map.get(suid), dict) else {}
            strategy_selected_iteration = str(strategy_item.get("selected_iteration_code") or "").strip()
            iteration_scenarios_by_store[suid] = {
                "selected_price": installed_price,
                "selected_boost_pct": _to_num(strategy_item.get("boost_bid_percent")),
                "selected_coinvest_pct": _to_num(strategy_item.get("coinvest_pct")),
                "selected_decision_label": str(strategy_item.get("decision_label") or "").strip(),
                "selected_iteration_code": strategy_selected_iteration,
                "rrc_with_boost": _scenario_promo_payload_from_current_data(
                    iteration_row=latest_iterations.get("rrc_with_boost"),
                    promo_offers=offers,
                    promo_offers_raw=offers_raw,
                ),
                "rrc_no_ads": _scenario_promo_payload_from_current_data(
                    iteration_row=latest_iterations.get("rrc_no_ads"),
                    promo_offers=offers,
                    promo_offers_raw=offers_raw,
                ),
                "mrc_with_boost": _scenario_promo_payload_from_current_data(
                    iteration_row=latest_iterations.get("mrc_with_boost"),
                    promo_offers=offers,
                    promo_offers_raw=offers_raw,
                ),
                "mrc": _scenario_promo_payload_from_current_data(
                    iteration_row=latest_iterations.get("mrc"),
                    promo_offers=offers,
                    promo_offers_raw=offers_raw,
                ),
                "rrc_price": _to_num(price_metric.get("target_price")),
                "rrc_no_ads_price": _to_num(price_metric.get("rrc_no_ads_price")),
                "mrc_with_boost_price": _to_num(price_metric.get("mrc_with_boost_price")),
                "mrc_price": _to_num(price_metric.get("mrc_price")),
            }
            if promo_participates and suid in summary_by_store:
                summary_by_store[suid]["market_promo_status"] = market_status
        rows_out.append(
            {
                "sku": sku,
                "name": str(row.get("name") or ""),
                "tree_path": row.get("tree_path") or [],
                "placements": row.get("placements") or {},
                "cogs_price_by_store": row.get("cogs_price_by_store") or {},
                "stock_by_store": row.get("stock_by_store") or {},
                "price_metrics_by_store": row.get("price_metrics_by_store") or {},
                "promo_summary_by_store": summary_by_store,
                "promo_offers_by_store": offers_by_store,
                "promo_details_by_store": details_by_store,
                "market_promo_status_by_store": market_status_by_store,
                "market_promo_message_by_store": market_message_by_store,
                "installed_price_by_store": installed_price_by_store,
                "iteration_scenarios_by_store": iteration_scenarios_by_store,
                "updated_at": str(row.get("updated_at") or ""),
            }
        )

    page_size_n = max(1, min(int(page_size or 50), 200))
    page_n = max(1, int(page or 1))
    total_count = len(rows_out) if stock_filter_norm != "all" else int(base.get("total_count") or len(rows_out))
    paged_rows = rows_out if stock_filter_norm == "all" else rows_out[(page_n - 1) * page_size_n:(page_n - 1) * page_size_n + page_size_n]

    resp = {
        "ok": True,
        "scope": base.get("scope"),
        "platform": base.get("platform"),
        "store_id": base.get("store_id"),
        "tree_mode": base.get("tree_mode"),
        "tree_source": base.get("tree_source"),
        "stores": stores,
        "rows": paged_rows,
        "promo_columns": promo_columns,
        "total_count": total_count,
        "page": page_n,
        "page_size": page_size_n,
    }
    _cache_set("overview", cache_payload, resp)
    return resp


async def refresh_promos_data(*, refresh_base: bool = True, store_uids: list[str] | None = None):
    out: dict[str, Any] = {}
    if refresh_base:
        out = await refresh_prices_data()
        invalidate_prices_cache()

    ctx = await get_prices_context()
    stores = ctx.get("marketplace_stores") if isinstance(ctx, dict) else []
    target_stores = [s for s in stores if str(s.get("platform") or "").strip().lower() == "yandex_market" and str(s.get("store_id") or "").strip()]
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        target_stores = [s for s in target_stores if str(s.get("store_uid") or "").strip() in selected]

    async def _run_store(store: dict[str, Any]) -> dict[str, Any]:
        store_id = str(store.get("store_id") or "").strip()
        store_uid = str(store.get("store_uid") or "").strip()
        creds = _find_yandex_shop_credentials(store_id)
        if not creds or not store_uid:
            logger.warning("[pricing_promos] skip store_uid=%s store_id=%s reason=credentials_not_found", store_uid, store_id)
            return {"ok": False, "store_uid": store_uid, "store_id": store_id, "reason": "credentials_not_found"}
        business_id, campaign_id, api_key = creds
        try:
            promos = await _fetch_yandex_promos_for_business(business_id=business_id, api_key=api_key)
            promo_offer_map = await _fetch_yandex_promo_offer_map(business_id=business_id, api_key=api_key, promos=promos)
            logger.warning(
                "[pricing_promos] store scan store_uid=%s business_id=%s campaign_id=%s promos=%s offer_skus=%s",
                store_uid,
                business_id,
                campaign_id,
                len(promos),
                len(promo_offer_map),
            )

            all_rows: list[dict[str, Any]] = []
            page_n = 1
            page_size_n = 500
            while True:
                resp = await get_prices_overview(
                    scope="store",
                    platform="yandex_market",
                    store_id=store_id,
                    page=page_n,
                    page_size=page_size_n,
                    force_refresh=True,
                )
                rows = resp.get("rows") if isinstance(resp, dict) else []
                if not isinstance(rows, list) or not rows:
                    break
                all_rows.extend(rows)
                total_count = int(resp.get("total_count") or 0)
                if page_n * page_size_n >= total_count:
                    break
                page_n += 1
            logger.warning(
                "[pricing_promos] store prices materialized store_uid=%s total_rows=%s",
                store_uid,
                len(all_rows),
            )
            skus = [str(r.get("sku") or "").strip() for r in all_rows if str(r.get("sku") or "").strip()]
            store_settings = get_pricing_store_settings(store_uid=store_uid) or {}
            minimum_profit_pct_num = _to_num(store_settings.get("minimum_profit_percent"))

            summary_rows: list[dict[str, Any]] = []
            detail_rows: list[dict[str, Any]] = []
            raw_campaign_rows: list[dict[str, Any]] = [
                {
                    "store_uid": store_uid,
                    "promo_id": str(promo.get("promo_id") or "").strip(),
                    "promo_name": str(promo.get("promo_name") or "").strip(),
                    "date_time_from": promo.get("dateTimeFrom") or promo.get("startDate"),
                    "date_time_to": promo.get("dateTimeTo") or promo.get("endDate"),
                    "source_updated_at": None,
                    "payload": promo.get("payload") if isinstance(promo.get("payload"), dict) else {},
                }
                for promo in promos
                if str(promo.get("promo_id") or "").strip()
            ]
            raw_offer_rows: list[dict[str, Any]] = []
            for row in all_rows:
                sku = str(row.get("sku") or "").strip()
                if not sku:
                    continue
                price_metrics = row.get("price_metrics_by_store") if isinstance(row.get("price_metrics_by_store"), dict) else {}
                metric = price_metrics.get(store_uid) if isinstance(price_metrics.get(store_uid), dict) else {}
                calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else {}
                mrc_price = _to_num(metric.get("mrc_price"))
                src_updated = str(row.get("updated_at") or "").strip() or None

                promo_entries_raw = promo_offer_map.get(sku) or []
                promo_entries_by_id: dict[str, dict[str, Any]] = {}
                for entry in promo_entries_raw:
                    promo_id = str(entry.get("promo_id") or "").strip()
                    if not promo_id:
                        continue
                    current = promo_entries_by_id.get(promo_id)
                    promo_price = _to_num(entry.get("promo_price"))
                    current_price = _to_num(current.get("promo_price")) if isinstance(current, dict) else None
                    if current is None or (
                        promo_price is not None and (current_price is None or promo_price < current_price)
                    ):
                        promo_entries_by_id[promo_id] = entry
                promo_entries = list(promo_entries_by_id.values())
                promo_entries.sort(
                    key=lambda x: (
                        (_to_num(x.get("promo_price")) is None),
                        _to_num(x.get("promo_price")) or 0,
                        str(x.get("promo_name") or ""),
                    )
                )
                selected_items: list[dict[str, str]] = []
                target_pct_num = _to_num(metric.get("target_profit_pct"))
                promo_target_pct_num = _promo_floor_target_pct(
                    target_pct=target_pct_num,
                    minimum_profit_pct=minimum_profit_pct_num,
                )

                selected_price: float | None = None
                selected_boost_pct: float | None = 0.0
                selected_profit_abs: float | None = None
                selected_profit_pct: float | None = None

                candidate_prices: list[dict[str, Any]] = []
                for entry in promo_entries:
                    raw_offer_rows.append(
                        {
                            "store_uid": store_uid,
                            "sku": sku,
                            "promo_id": str(entry.get("promo_id") or "").strip(),
                            "promo_name": str(entry.get("promo_name") or "").strip(),
                            "date_time_from": entry.get("date_time_from"),
                            "date_time_to": entry.get("date_time_to"),
                            "promo_price": _to_num(entry.get("promo_price")),
                            "source_updated_at": src_updated,
                            "payload": entry.get("payload") if isinstance(entry.get("payload"), dict) else {},
                        }
                    )
                    promo_price = _to_num(entry.get("promo_price"))
                    candidate: dict[str, Any] | None = None
                    if promo_price is not None:
                        pa, pp = _calc_profit_from_ctx_with_ads_percent(
                            promo_price,
                            calc_ctx,
                            0.0,
                        )
                        if (
                            mrc_price is not None
                            and float(promo_price) >= float(mrc_price)
                            and _to_num(pp) is not None
                            and float(_to_num(pp) or 0.0) >= float(promo_target_pct_num or 0.0) - 0.05
                        ):
                            candidate = {
                                "price": promo_price,
                                "boost_pct": 0.0,
                                "profit_abs": None if pa is None else float(int(round(pa))),
                                "profit_pct": None if pp is None else round(float(pp), 2),
                            }
                    if candidate is not None:
                        candidate_prices.append(candidate)

                if candidate_prices:
                    chosen_candidate = min(
                        candidate_prices,
                        key=lambda item: (
                            _to_num(item.get("price")) is None,
                            _to_num(item.get("price")) or 0.0,
                        ),
                    )
                    selected_price = _to_num(chosen_candidate.get("price"))
                    selected_boost_pct = _to_num(chosen_candidate.get("boost_pct"))
                    selected_profit_abs = _to_num(chosen_candidate.get("profit_abs"))
                    selected_profit_pct = _to_num(chosen_candidate.get("profit_pct"))

                for entry in promo_entries:
                    promo_price = _to_num(entry.get("promo_price"))
                    fit_mode = "rejected"
                    effective_profit_abs = None
                    effective_profit_pct = None
                    if promo_price is not None:
                        pa, pp = _calc_profit_from_ctx_with_ads_percent(
                            promo_price,
                            calc_ctx,
                            0.0,
                        )
                        effective_profit_abs = None if pa is None else float(int(round(pa)))
                        effective_profit_pct = None if pp is None else round(float(pp), 2)
                        if (
                            mrc_price is not None
                            and float(promo_price) >= float(mrc_price)
                            and effective_profit_pct is not None
                            and float(effective_profit_pct) >= float(promo_target_pct_num or 0.0) - 0.05
                        ):
                            fit_mode = "without_ads"
                    promo_name = str(entry.get("promo_name") or entry.get("promo_id") or "").strip()
                    status_text = {
                        "without_ads": "Проходит без ДРР",
                        "rejected": "Не проходит",
                    }.get(fit_mode, "Не проходит")
                    detail_rows.append(
                        {
                            "store_uid": store_uid,
                            "sku": sku,
                            "promo_id": entry.get("promo_id"),
                            "promo_name": promo_name,
                            "promo_price": promo_price,
                            "promo_profit_abs": effective_profit_abs,
                            "promo_profit_pct": effective_profit_pct,
                            "promo_fit_mode": fit_mode,
                            "source_updated_at": src_updated,
                        }
                    )
                    selected_items.append(
                        {
                            "promo_id": str(entry.get("promo_id") or "").strip(),
                            "name": promo_name,
                            "status": status_text,
                            "threshold_price": "" if promo_price is None else str(int(round(float(promo_price)))),
                        }
                    )
                summary_rows.append(
                    {
                        "store_uid": store_uid,
                        "sku": sku,
                        "promo_selected_items": selected_items,
                        "promo_selected_price": selected_price,
                        "promo_selected_boost_bid_percent": selected_boost_pct,
                        "promo_selected_profit_abs": selected_profit_abs,
                        "promo_selected_profit_pct": selected_profit_pct,
                        "source_updated_at": src_updated,
                    }
                )

            feedback_by_sku = _status_feedback_from_promo_entries(promo_entries=raw_offer_rows)

            logger.warning(
                "[pricing_promos] store prepared store_uid=%s summary_rows=%s detail_rows=%s",
                store_uid,
                len(summary_rows),
                len(detail_rows),
            )

            if summary_rows or detail_rows:
                clear_pricing_promo_results_for_store(store_uid=store_uid)
                upsert_pricing_promo_campaign_raw_bulk(rows=raw_campaign_rows)
                upsert_pricing_promo_offer_raw_bulk(rows=raw_offer_rows)
                summary_count = upsert_pricing_promo_results_bulk(rows=summary_rows)
                detail_count = upsert_pricing_promo_offer_results_bulk(rows=detail_rows)
                if feedback_by_sku:
                    update_pricing_strategy_market_promo_feedback(
                        store_uid=store_uid,
                        feedback_by_sku=feedback_by_sku,
                    )
                return {
                    "ok": True,
                    "store_uid": store_uid,
                    "store_id": store_id,
                    "promos": len(promos),
                    "offer_rows": sum(len(v) for v in promo_offer_map.values()),
                    "summary_rows": summary_count,
                    "detail_rows": detail_count,
                }
            else:
                logger.warning(
                    "[pricing_promos] store skipped materialize store_uid=%s reason=no_rows_prepared_keep_existing",
                    store_uid,
                )
                return {
                    "ok": True,
                    "store_uid": store_uid,
                    "store_id": store_id,
                    "promos": len(promos),
                    "offer_rows": sum(len(v) for v in promo_offer_map.values()),
                    "summary_rows": 0,
                    "detail_rows": 0,
                }
        except Exception as exc:
            logger.warning(
                "[pricing_promos] store skipped store_uid=%s business_id=%s campaign_id=%s error=%s",
                store_uid,
                business_id,
                campaign_id,
                exc,
            )
            return {
                "ok": False,
                "store_uid": store_uid,
                "store_id": store_id,
                "reason": str(exc),
            }

    settled = await asyncio.gather(*[_run_store(store) for store in target_stores])
    stores_success: list[dict[str, str]] = [
        {"store_uid": str(item.get("store_uid") or "").strip(), "store_id": str(item.get("store_id") or "").strip()}
        for item in settled
        if item.get("ok")
    ]
    skipped_stores: list[dict[str, str]] = [
        {"store_uid": str(item.get("store_uid") or "").strip(), "store_id": str(item.get("store_id") or "").strip(), "reason": str(item.get("reason") or "").strip()}
        for item in settled
        if not item.get("ok")
    ]
    refreshed_stores = len(stores_success)
    total_promos = sum(int(item.get("promos") or 0) for item in settled if item.get("ok"))
    total_offer_rows = sum(int(item.get("offer_rows") or 0) for item in settled if item.get("ok"))
    total_summary_rows = sum(int(item.get("summary_rows") or 0) for item in settled if item.get("ok"))
    total_detail_rows = sum(int(item.get("detail_rows") or 0) for item in settled if item.get("ok"))

    invalidate_promos_cache()
    actual_errors = [
        item
        for item in skipped_stores
        if "403 forbidden" not in str(item.get("reason") or "").strip().lower()
        or "/promos" not in str(item.get("reason") or "").strip().lower()
    ]
    has_errors = bool(actual_errors)
    return {
        "ok": not has_errors,
        "message": "Промо-данные обновлены" if not has_errors else "Промо-данные обновлены частично",
        "stores_refreshed": refreshed_stores,
        "stores": stores_success,
        "stores_skipped": skipped_stores,
        "promos_loaded": total_promos,
        "offer_rows_loaded": total_offer_rows,
        "summary_rows_materialized": total_summary_rows,
        "detail_rows_materialized": total_detail_rows,
        "prices_refresh": out,
    }
