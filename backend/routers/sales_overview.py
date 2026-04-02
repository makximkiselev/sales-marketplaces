from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.services.yandex_united_orders_report_service import (
    get_sales_overview_data_flow_status,
    get_sales_overview_context,
    get_sales_overview_history,
    get_sales_overview_problem_orders,
    get_sales_overview_retrospective,
    get_sales_overview_tracking,
    refresh_sales_overview_cogs_source_for_store,
    refresh_sales_overview_history,
    refresh_sales_overview_order_rows_for_store,
    refresh_sales_overview_order_rows_current_month_for_store,
    refresh_sales_overview_order_rows_today_for_store,
)
from backend.services.yandex_united_netting_report_service import refresh_sales_united_netting_history
from backend.services.service_cache_helpers import cache_get_copy, cache_set_copy, make_cache_key
from backend.services.store_data_model import (
    delete_dashboard_snapshots,
    get_dashboard_snapshot,
    get_pricing_store_settings,
    upsert_dashboard_snapshot,
    upsert_pricing_store_settings,
)

router = APIRouter()
logger = logging.getLogger("uvicorn.error")

_DASHBOARD_CACHE: dict[str, dict] = {}
_DASHBOARD_CACHE_GEN = 1
_DASHBOARD_SNAPSHOT_GEN = 2
_DASHBOARD_CACHE_MAX = 128
_DASHBOARD_CACHE_TTL_SECONDS = 180
_DASHBOARD_RECENT_PAYLOADS: dict[str, dict] = {}
_DASHBOARD_RECENT_MAX = 24
_DASHBOARD_DEFAULT_PERIODS = ("today", "yesterday", "week", "month", "quarter")
_DASHBOARD_WARM_TASK: asyncio.Task | None = None
_DASHBOARD_IN_FLIGHT: dict[str, asyncio.Task] = {}
_DASHBOARD_SNAPSHOT_NAME = "sales_overview_dashboard_summary"
_TODAY_AUTO_REFRESH_STALE_MINUTES = 20
MSK = ZoneInfo("Europe/Moscow")


def _local_date_only() -> date:
    return datetime.now(MSK).date()


def _to_iso_date(value: date) -> str:
    return value.isoformat()


def _shift_date(value: date, days: int) -> date:
    return value + timedelta(days=days)


def _is_loaded_at_stale(raw: str, *, minutes: int = _TODAY_AUTO_REFRESH_STALE_MINUTES) -> bool:
    value = str(raw or "").strip()
    if not value:
        return True
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return datetime.now(dt.tzinfo) - dt > timedelta(minutes=minutes)
    except Exception:
        return True


def _loaded_at_is_before_today(raw: str) -> bool:
    value = str(raw or "").strip()
    if not value:
        return True
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        return dt.astimezone(MSK).date() < _local_date_only()
    except Exception:
        return True


def _today_response_needs_refresh(response: dict | None) -> bool:
    payload = response if isinstance(response, dict) else {}
    rows = list(payload.get("rows") or [])
    loaded_at = str(payload.get("loaded_at") or "").strip()
    if rows:
        return False
    return _loaded_at_is_before_today(loaded_at) or _is_loaded_at_stale(loaded_at)


def _build_layer_freshness(*, period: str, response: dict | None) -> dict:
    payload = response if isinstance(response, dict) else {}
    loaded_at = str(payload.get("loaded_at") or "").strip()
    if str(period or "").strip().lower() == "today":
        is_stale = _today_response_needs_refresh(payload)
    else:
        is_stale = _is_loaded_at_stale(loaded_at)
    return {
        "period": str(period or "").strip().lower(),
        "loaded_at": loaded_at,
        "is_stale": is_stale,
        "status": "stale" if is_stale else "fresh",
        "message": "Данные обновляются" if is_stale else "",
    }


def _period_span_days(period: str) -> int:
    value = str(period or "").strip().lower()
    if value in {"today", "yesterday"}:
        return 1
    if value == "week":
        return 7
    if value == "quarter":
        return 90
    return 30


def _current_period_range(period: str) -> tuple[str, str, int]:
    raw = str(period or "").strip().lower()
    if raw == "yesterday":
        day = _shift_date(_local_date_only(), -1)
        return _to_iso_date(day), _to_iso_date(day), 1
    if raw == "today":
        day = _local_date_only()
        return _to_iso_date(day), _to_iso_date(day), 1
    end = _local_date_only()
    span = _period_span_days(raw)
    start = _shift_date(end, -(span - 1))
    return _to_iso_date(start), _to_iso_date(end), span


def _previous_period_range(period: str) -> tuple[str, str, int]:
    current_start, _, span = _current_period_range(period)
    start_day = datetime.fromisoformat(current_start).date()
    previous_end = _shift_date(start_day, -1)
    previous_start = _shift_date(previous_end, -(span - 1))
    return _to_iso_date(previous_start), _to_iso_date(previous_end), span


def _overview_date_range(period: str) -> tuple[str, str, str]:
    start, end, span = _current_period_range(period)
    grain = "day" if span <= 31 else "month"
    return start, end, grain


def _sum_by(rows: list[dict], key: str) -> float:
    return sum(float(item.get(key) or 0.0) for item in rows)


def _dashboard_status_kind(status: str) -> str:
    norm = str(status or "").strip().lower()
    if "возврат" in norm:
        return "return"
    if "отмен" in norm or "невыкуп" in norm:
        return "ignore"
    if norm == "доставлен покупателю":
        return "delivered"
    if norm in {"оформлен", "отгружен"}:
        return "open"
    return "other"


def _is_dashboard_commercial_row(row: dict) -> bool:
    return _dashboard_status_kind(str((row or {}).get("item_status") or "")) in {"delivered", "open"}


def _filter_dashboard_orders_response(response: dict | None) -> dict:
    payload = response if isinstance(response, dict) else {}
    rows = [row for row in list(payload.get("rows") or []) if _is_dashboard_commercial_row(row)]
    next_payload = dict(payload)
    next_payload["rows"] = rows
    next_payload["total_count"] = len(rows)
    next_payload["kpis"] = _build_orders_kpis(rows, fallback=payload.get("kpis") if isinstance(payload.get("kpis"), dict) else None)
    return next_payload


def _compare_delta(current: float, previous: float) -> float | None:
    if not previous:
        return None
    return ((current - previous) / previous) * 100.0


def _build_orders_kpis(rows: list[dict], fallback: dict | None = None) -> dict:
    total_revenue = _sum_by(rows, "sale_price")
    total_coinvest_amount = 0.0
    for row in rows:
        revenue = float(row.get("sale_price") or 0.0)
        buyer_price = float(row.get("sale_price_with_coinvest") or row.get("sale_price") or 0.0)
        if revenue <= 0:
            continue
        total_coinvest_amount += max(0.0, revenue - buyer_price)
    return {
        "orders_count": len(rows),
        "avg_coinvest_pct": round((total_coinvest_amount / total_revenue * 100.0), 2) if total_revenue > 0 else float((fallback or {}).get("avg_coinvest_pct") or 0.0),
        "additional_ads": float((fallback or {}).get("additional_ads") or 0.0),
        "operational_errors": float((fallback or {}).get("operational_errors") or 0.0),
    }


def _merge_orders_responses(responses: list[dict]) -> dict:
    rows = [row for response in responses for row in list(response.get("rows") or [])]
    date_from_values = sorted([str(response.get("date_from") or "").strip() for response in responses if str(response.get("date_from") or "").strip()])
    date_to_values = sorted([str(response.get("date_to") or "").strip() for response in responses if str(response.get("date_to") or "").strip()])
    loaded_at_values = sorted([str(response.get("loaded_at") or "").strip() for response in responses if str(response.get("loaded_at") or "").strip()])
    return {
        "ok": True,
        "rows": rows,
        "total_count": len(rows),
        "date_from": date_from_values[0] if date_from_values else "",
        "date_to": date_to_values[-1] if date_to_values else "",
        "loaded_at": loaded_at_values[-1] if loaded_at_values else "",
        "kpis": _build_orders_kpis(rows),
    }


def _merge_problem_responses(responses: list[dict]) -> dict:
    rows = [row for response in responses for row in list(response.get("rows") or [])]
    date_from_values = sorted([str(response.get("date_from") or "").strip() for response in responses if str(response.get("date_from") or "").strip()])
    date_to_values = sorted([str(response.get("date_to") or "").strip() for response in responses if str(response.get("date_to") or "").strip()])
    loaded_at_values = sorted([str(response.get("loaded_at") or "").strip() for response in responses if str(response.get("loaded_at") or "").strip()])
    return {
        "ok": True,
        "rows": rows,
        "total_count": len(rows),
        "date_from": date_from_values[0] if date_from_values else "",
        "date_to": date_to_values[-1] if date_to_values else "",
        "loaded_at": loaded_at_values[-1] if loaded_at_values else "",
    }


def _merge_retrospective_responses(responses: list[dict]) -> dict:
    grouped: dict[str, dict] = {}
    for response in responses:
        for row in list(response.get("rows") or []):
            key = str(row.get("key") or row.get("label") or row.get("sku") or row.get("category_path") or "").strip()
            if not key:
                continue
            existing = grouped.get(key)
            if not existing:
                grouped[key] = {**row, "revenue": float(row.get("revenue") or 0.0), "profit_amount": float(row.get("profit_amount") or 0.0)}
                continue
            next_revenue = float(existing.get("revenue") or 0.0) + float(row.get("revenue") or 0.0)
            next_profit = float(existing.get("profit_amount") or 0.0) + float(row.get("profit_amount") or 0.0)
            existing["revenue"] = next_revenue
            existing["profit_amount"] = next_profit
            existing["profit_pct"] = round((next_profit / next_revenue * 100.0), 2) if next_revenue > 0 else None
    rows = sorted(grouped.values(), key=lambda item: float(item.get("revenue") or 0.0), reverse=True)
    return {"ok": True, "rows": rows, "total_count": len(rows)}


def _split_category_path(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split("/") if part.strip()]


def _build_category_groups(rows: list[dict]) -> list[dict]:
    categories: dict[str, dict] = {}
    for row in rows:
        parts = _split_category_path(str(row.get("category_path") or row.get("label") or ""))
        category = parts[0] if parts else "Не определено"
        brand = (parts[-2] if len(parts) >= 3 else (parts[1] if len(parts) >= 2 else "Без бренда")) or "Без бренда"
        revenue = float(row.get("revenue") or 0.0)
        profit = float(row.get("profit_amount") or 0.0)
        bucket = categories.setdefault(category, {"label": category, "value": 0.0, "profit": 0.0, "brands": {}})
        bucket["value"] = float(bucket["value"]) + revenue
        bucket["profit"] = float(bucket["profit"]) + profit
        brand_bucket = bucket["brands"].setdefault(brand, {"label": brand, "value": 0.0, "profit": 0.0})
        brand_bucket["value"] = float(brand_bucket["value"]) + revenue
        brand_bucket["profit"] = float(brand_bucket["profit"]) + profit
    out: list[dict] = []
    for bucket in categories.values():
        value = float(bucket["value"] or 0.0)
        profit = float(bucket["profit"] or 0.0)
        brands = []
        for brand_bucket in bucket["brands"].values():
            brand_value = float(brand_bucket["value"] or 0.0)
            brand_profit = float(brand_bucket["profit"] or 0.0)
            brands.append(
                {
                    "label": str(brand_bucket["label"] or "Без бренда").strip() or "Без бренда",
                    "value": round(brand_value, 2),
                    "profit": round(brand_profit, 2),
                    "marginPct": round((brand_profit / brand_value) * 100.0, 2) if brand_value > 0 else None,
                }
            )
        brands.sort(key=lambda item: float(item.get("value") or 0.0), reverse=True)
        out.append(
            {
                "label": str(bucket["label"] or "Не определено").strip() or "Не определено",
                "value": round(value, 2),
                "profit": round(profit, 2),
                "marginPct": round((profit / value) * 100.0, 2) if value > 0 else None,
                "brandCount": len(brands),
                "brands": brands,
            }
        )
    out.sort(key=lambda item: float(item.get("value") or 0.0), reverse=True)
    return out


def _attach_category_groups(response: dict) -> dict:
    payload = dict(response or {})
    rows = list(payload.get("rows") or [])
    payload["category_groups"] = _build_category_groups(rows)
    return payload


def _merge_data_flow_responses(responses: list[dict]) -> dict:
    flows = [flow for response in responses for flow in list(response.get("flows") or [])]
    return {"ok": True, "flows": flows}


def _build_store_comparison(
    stores: list[dict],
    scoped: list[dict],
) -> list[dict]:
    by_store_id = {str(store.get("store_id") or "").strip(): store for store in stores}
    rows: list[dict] = []
    for item in scoped:
        store_id = str(item.get("storeId") or "").strip()
        store = by_store_id.get(store_id) or {}
        current = item.get("current") or {}
        previous = item.get("previous") or {}
        current_rows = list(current.get("rows") or [])
        previous_rows = list(previous.get("rows") or [])
        revenue = _sum_by(current_rows, "sale_price")
        profit = _sum_by(current_rows, "profit")
        orders_count = int((current.get("kpis") or {}).get("orders_count") or len(current_rows))
        previous_revenue = _sum_by(previous_rows, "sale_price")
        previous_profit = _sum_by(previous_rows, "profit")
        previous_orders_count = int((previous.get("kpis") or {}).get("orders_count") or len(previous_rows))
        rows.append(
            {
                "storeId": store_id,
                "label": str(store.get("label") or store_id).strip(),
                "platformLabel": str(store.get("platform_label") or "").strip(),
                "revenue": revenue,
                "profit": profit,
                "orders": orders_count,
                "marginPct": (profit / revenue * 100.0) if revenue > 0 else None,
                "revenueDeltaPct": _compare_delta(revenue, previous_revenue),
                "profitDeltaPct": _compare_delta(profit, previous_profit),
                "ordersDeltaPct": _compare_delta(float(orders_count), float(previous_orders_count)),
            }
        )
    return sorted(rows, key=lambda item: float(item.get("revenue") or 0.0), reverse=True)


def _dashboard_cache_payload(*, store_id: str, period: str) -> dict[str, str]:
    return {
        "store_id": str(store_id or "all").strip() or "all",
        "period": str(period or "today").strip().lower() or "today",
        "anchor_date": _to_iso_date(_local_date_only()),
    }


def _dashboard_cache_key(payload: dict[str, str]) -> str:
    return make_cache_key(_DASHBOARD_SNAPSHOT_NAME, payload, _DASHBOARD_CACHE_GEN)


def _dashboard_snapshot_key(payload: dict[str, str]) -> str:
    return make_cache_key(_DASHBOARD_SNAPSHOT_NAME, payload, _DASHBOARD_SNAPSHOT_GEN)


def _dashboard_cache_get(payload: dict[str, str]) -> dict | None:
    key = _dashboard_cache_key(payload)
    wrapped = cache_get_copy(_DASHBOARD_CACHE, key)
    if not isinstance(wrapped, dict):
        return None
    expires_at = float(wrapped.get("expires_at") or 0.0)
    if expires_at and expires_at < time.time():
        _DASHBOARD_CACHE.pop(key, None)
        return None
    data = wrapped.get("data")
    return data if isinstance(data, dict) else None


def _dashboard_cache_set(payload: dict[str, str], value: dict) -> None:
    key = _dashboard_cache_key(payload)
    snapshot_key = _dashboard_snapshot_key(payload)
    cache_set_copy(
        _DASHBOARD_CACHE,
        key,
        {"expires_at": time.time() + _DASHBOARD_CACHE_TTL_SECONDS, "data": value},
        _DASHBOARD_CACHE_MAX,
    )
    recent_key = f"{payload['store_id']}|{payload['period']}|{payload['anchor_date']}"
    _DASHBOARD_RECENT_PAYLOADS[recent_key] = dict(payload)
    if len(_DASHBOARD_RECENT_PAYLOADS) > _DASHBOARD_RECENT_MAX:
        oldest_key = next(iter(_DASHBOARD_RECENT_PAYLOADS.keys()))
        _DASHBOARD_RECENT_PAYLOADS.pop(oldest_key, None)
    upsert_dashboard_snapshot(
        snapshot_name=_DASHBOARD_SNAPSHOT_NAME,
        cache_key=snapshot_key,
        scope_id=payload["store_id"],
        period=payload["period"],
        payload=payload,
        response=value,
    )


def _dashboard_snapshot_get(payload: dict[str, str]) -> dict | None:
    snapshot = get_dashboard_snapshot(
        snapshot_name=_DASHBOARD_SNAPSHOT_NAME,
        cache_key=_dashboard_snapshot_key(payload),
    )
    if not isinstance(snapshot, dict):
        return None
    response = snapshot.get("response")
    return response if isinstance(response, dict) else None


def _dashboard_default_payloads() -> list[dict[str, str]]:
    payloads = [_dashboard_cache_payload(store_id="all", period=period) for period in _DASHBOARD_DEFAULT_PERIODS]
    try:
        context = get_sales_overview_context()
    except Exception:
        return payloads
    stores = list(context.get("marketplace_stores") or [])
    for store in stores:
        store_id = str(store.get("store_id") or "").strip()
        if not store_id:
            continue
        for period in _DASHBOARD_DEFAULT_PERIODS:
            payloads.append(_dashboard_cache_payload(store_id=store_id, period=period))
    return payloads


def invalidate_sales_overview_dashboard_cache() -> None:
    global _DASHBOARD_CACHE_GEN
    _DASHBOARD_CACHE.clear()
    _DASHBOARD_CACHE_GEN += 1
    delete_dashboard_snapshots(snapshot_name=_DASHBOARD_SNAPSHOT_NAME)


async def _warm_sales_overview_dashboard_cache(payloads: list[dict[str, str]]) -> None:
    payload_map: dict[tuple[str, str], dict[str, str]] = {}
    for raw_payload in payloads:
        store_id = str(raw_payload.get("store_id") or "all").strip() or "all"
        period = str(raw_payload.get("period") or "today").strip().lower() or "today"
        payload_map[(store_id, period)] = {"store_id": store_id, "period": period}
    for payload in payload_map.values():
        key = _dashboard_cache_key(payload)
        if _dashboard_cache_get(payload):
            continue
        existing_task = _DASHBOARD_IN_FLIGHT.get(key)
        if existing_task and not existing_task.done():
            continue
        task = asyncio.create_task(_build_dashboard_summary_response(store_id=payload["store_id"], period=payload["period"]))
        _DASHBOARD_IN_FLIGHT[key] = task
        try:
            built = await task
            _dashboard_cache_set(payload, built)
        except Exception as exc:
            logger.warning(
                "[sales_overview] dashboard cache warm failed store_id=%s period=%s error=%s",
                payload["store_id"],
                payload["period"],
                exc,
            )
        finally:
            _DASHBOARD_IN_FLIGHT.pop(key, None)


def schedule_sales_overview_dashboard_cache_warm(payloads: list[dict[str, str]] | None = None) -> None:
    global _DASHBOARD_WARM_TASK
    next_payloads = list(_dashboard_default_payloads())
    next_payloads.extend(list(_DASHBOARD_RECENT_PAYLOADS.values()))
    if payloads:
        next_payloads.extend(payloads)
    if not next_payloads:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if _DASHBOARD_WARM_TASK and not _DASHBOARD_WARM_TASK.done():
        return
    _DASHBOARD_WARM_TASK = loop.create_task(_warm_sales_overview_dashboard_cache(next_payloads))


async def warm_sales_overview_dashboard_cache_defaults() -> None:
    await _warm_sales_overview_dashboard_cache(_dashboard_default_payloads())


def run_sales_overview_dashboard_cache_warm_sync() -> None:
    try:
        asyncio.run(warm_sales_overview_dashboard_cache_defaults())
    except Exception as exc:
        logger.warning("[sales_overview] scheduled dashboard warm failed error=%s", exc)


async def _fetch_primary_store_dashboard(*, store_id: str, period: str, previous_start: str, previous_end: str) -> dict:
    store_query = str(store_id or "").strip()
    today_orders_task = get_sales_overview_history(page=1, page_size=1000, store_id=store_query, period="today")
    yesterday_orders_task = get_sales_overview_history(page=1, page_size=1000, store_id=store_query, period="yesterday")
    today_problems_task = get_sales_overview_problem_orders(page=1, page_size=500, store_id=store_query, period="today")
    yesterday_problems_task = get_sales_overview_problem_orders(page=1, page_size=500, store_id=store_query, period="yesterday")

    current_period = str(period or "").strip().lower()
    if current_period == "today":
        today, yesterday, today_problems, yesterday_problems = await asyncio.gather(
            today_orders_task,
            yesterday_orders_task,
            today_problems_task,
            yesterday_problems_task,
        )
        today = _filter_dashboard_orders_response(today)
        yesterday = _filter_dashboard_orders_response(yesterday)
        return {
            "orders": today,
            "problems": today_problems,
            "today": today,
            "yesterday": yesterday,
            "todayProblems": today_problems,
            "yesterdayProblems": yesterday_problems,
            "previousOrders": yesterday,
            "previousProblems": yesterday_problems,
        }

    current_orders_task = get_sales_overview_history(page=1, page_size=1000, store_id=store_query, period=current_period)
    current_problems_task = get_sales_overview_problem_orders(page=1, page_size=500, store_id=store_query, period=current_period)
    previous_orders_task = get_sales_overview_history(
        page=1,
        page_size=1000,
        store_id=store_query,
        period="custom",
        date_from=previous_start,
        date_to=previous_end,
    )
    previous_problems_task = get_sales_overview_problem_orders(
        page=1,
        page_size=500,
        store_id=store_query,
        period="custom",
        date_from=previous_start,
        date_to=previous_end,
    )
    current, current_problems, today, yesterday, today_problems, yesterday_problems, previous_orders, previous_problems = await asyncio.gather(
        current_orders_task,
        current_problems_task,
        today_orders_task,
        yesterday_orders_task,
        today_problems_task,
        yesterday_problems_task,
        previous_orders_task,
        previous_problems_task,
    )
    current = _filter_dashboard_orders_response(current)
    today = _filter_dashboard_orders_response(today)
    yesterday = _filter_dashboard_orders_response(yesterday)
    previous_orders = _filter_dashboard_orders_response(previous_orders)
    if current_period == "yesterday":
        current = yesterday
        current_problems = yesterday_problems
    return {
        "orders": current,
        "problems": current_problems,
        "today": today,
        "yesterday": yesterday,
        "todayProblems": today_problems,
        "yesterdayProblems": yesterday_problems,
        "previousOrders": previous_orders,
        "previousProblems": previous_problems,
    }


async def _fetch_secondary_store_dashboard(*, store_id: str, period: str) -> dict:
    date_from, date_to, grain = _overview_date_range(period)
    store_query = str(store_id or "").strip()
    data_flow, sku, category = await asyncio.gather(
        asyncio.to_thread(get_sales_overview_data_flow_status, store_id=store_query),
        get_sales_overview_retrospective(
            store_id=store_query,
            group_by="sku",
            grain=grain,
            date_mode="created",
            date_from=date_from,
            date_to=date_to,
            limit=120,
        ),
        get_sales_overview_retrospective(
            store_id=store_query,
            group_by="category",
            grain=grain,
            date_mode="created",
            date_from=date_from,
            date_to=date_to,
            limit=120,
        ),
    )
    return {"dataFlow": data_flow, "sku": sku, "category": category}


async def _build_dashboard_summary_response(*, store_id: str, period: str) -> dict:
    context = get_sales_overview_context()
    stores = list(context.get("marketplace_stores") or [])
    selected_stores = [store for store in stores if str(store.get("store_id") or "").strip()]
    selected_store_id = str(store_id or "all").strip() or "all"
    previous_start, previous_end, _ = _previous_period_range(period)
    tracking = await get_sales_overview_tracking(store_id=selected_store_id, date_mode="created")

    if selected_store_id == "all":
        primary_scoped = await asyncio.gather(
            *[
                _fetch_primary_store_dashboard(
                    store_id=str(store.get("store_id") or "").strip(),
                    period=period,
                    previous_start=previous_start,
                    previous_end=previous_end,
                )
                for store in selected_stores
            ]
        )
        bundle = {
            "tracking": tracking,
            "orders": _merge_orders_responses([item["orders"] for item in primary_scoped]),
            "problems": _merge_problem_responses([item["problems"] for item in primary_scoped]),
            "dataFlow": {"ok": True, "flows": []},
            "sku": {"ok": True, "rows": [], "total_count": 0},
            "category": {"ok": True, "rows": [], "total_count": 0},
            "today": _merge_orders_responses([item["today"] for item in primary_scoped]),
            "yesterday": _merge_orders_responses([item["yesterday"] for item in primary_scoped]),
            "todayProblems": _merge_problem_responses([item["todayProblems"] for item in primary_scoped]),
            "yesterdayProblems": _merge_problem_responses([item["yesterdayProblems"] for item in primary_scoped]),
            "previousOrders": _merge_orders_responses([item["previousOrders"] for item in primary_scoped]),
            "previousProblems": _merge_problem_responses([item["previousProblems"] for item in primary_scoped]),
        }
        comparison = _build_store_comparison(
            selected_stores,
            [
                {
                    "storeId": str(store.get("store_id") or "").strip(),
                    "current": item["orders"],
                    "previous": item["previousOrders"],
                }
                for store, item in zip(selected_stores, primary_scoped)
            ],
        )
        secondary_scoped = await asyncio.gather(
            *[
                _fetch_secondary_store_dashboard(store_id=str(store.get("store_id") or "").strip(), period=period)
                for store in selected_stores
            ]
        )
        bundle["dataFlow"] = _merge_data_flow_responses([item["dataFlow"] for item in secondary_scoped])
        bundle["sku"] = _merge_retrospective_responses([item["sku"] for item in secondary_scoped])
        bundle["category"] = _attach_category_groups(_merge_retrospective_responses([item["category"] for item in secondary_scoped]))
        return {
            "ok": True,
            "context": context,
            "bundle": bundle,
            "storeComparison": comparison,
            "freshness": {
                "today": _build_layer_freshness(period="today", response=bundle.get("today")),
                "yesterday": _build_layer_freshness(period="yesterday", response=bundle.get("yesterday")),
            },
        }

    primary = await _fetch_primary_store_dashboard(
        store_id=selected_store_id,
        period=period,
        previous_start=previous_start,
        previous_end=previous_end,
    )
    bundle = {
        "tracking": tracking,
        "orders": primary["orders"],
        "problems": primary["problems"],
        "dataFlow": {"ok": True, "flows": []},
        "sku": {"ok": True, "rows": [], "total_count": 0},
        "category": {"ok": True, "rows": [], "total_count": 0},
        "today": primary["today"],
        "yesterday": primary["yesterday"],
        "todayProblems": primary["todayProblems"],
        "yesterdayProblems": primary["yesterdayProblems"],
        "previousOrders": primary["previousOrders"],
        "previousProblems": primary["previousProblems"],
    }
    secondary, comparison_scoped = await asyncio.gather(
        _fetch_secondary_store_dashboard(store_id=selected_store_id, period=period),
        asyncio.gather(
            *[
                _fetch_primary_store_dashboard(
                    store_id=str(store.get("store_id") or "").strip(),
                    period=period,
                    previous_start=previous_start,
                    previous_end=previous_end,
                )
                for store in selected_stores
            ]
        ),
    )
    bundle["dataFlow"] = secondary["dataFlow"]
    bundle["sku"] = secondary["sku"]
    bundle["category"] = _attach_category_groups(secondary["category"])
    comparison = _build_store_comparison(
        selected_stores,
        [
            {
                "storeId": str(store.get("store_id") or "").strip(),
                "current": item["orders"],
                "previous": item["previousOrders"],
            }
            for store, item in zip(selected_stores, comparison_scoped)
        ],
    )
    return {
        "ok": True,
        "context": context,
        "bundle": bundle,
        "storeComparison": comparison,
        "freshness": {
            "today": _build_layer_freshness(period="today", response=bundle.get("today")),
            "yesterday": _build_layer_freshness(period="yesterday", response=bundle.get("yesterday")),
        },
    }


@router.get("/api/sales/overview/context")
async def sales_overview_context():
    try:
        return get_sales_overview_context()
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить контекст обзора продаж: {exc}"}, status_code=500)


@router.get("/api/sales/overview/data-flow")
async def sales_overview_data_flow(store_id: str = ""):
    try:
        return get_sales_overview_data_flow_status(store_id=store_id)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить статус слоя продаж: {exc}"}, status_code=500)


@router.get("/api/sales/overview/united-orders")
async def sales_overview_united_orders(
    page: int = 1,
    page_size: int = 200,
    store_id: str = "",
    item_status: str = "",
    period: str = "month",
    date_from: str = "",
    date_to: str = "",
):
    try:
        result = await get_sales_overview_history(
            page=page,
            page_size=page_size,
            store_id=store_id,
            item_status=item_status,
            period=period,
            date_from=date_from,
            date_to=date_to,
        )
        return result
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить united orders history: {exc}"}, status_code=500)


@router.get("/api/sales/overview/problem-orders")
async def sales_overview_problem_orders(
    page: int = 1,
    page_size: int = 200,
    store_id: str = "",
    period: str = "month",
    date_from: str = "",
    date_to: str = "",
):
    try:
        result = await get_sales_overview_problem_orders(
            page=page,
            page_size=page_size,
            store_id=store_id,
            period=period,
            date_from=date_from,
            date_to=date_to,
        )
        return result
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить проблемные заказы: {exc}"}, status_code=500)


@router.get("/api/sales/overview/tracking")
async def sales_overview_tracking(
    store_id: str = "",
    date_mode: str = "created",
):
    try:
        return await get_sales_overview_tracking(store_id=store_id, date_mode=date_mode)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить трекинг продаж: {exc}"}, status_code=500)


@router.get("/api/sales/overview/retrospective")
async def sales_overview_retrospective(
    store_id: str = "",
    date_mode: str = "created",
    group_by: str = "sku",
    grain: str = "month",
    date_from: str = "",
    date_to: str = "",
    limit: int = 200,
):
    try:
        return await get_sales_overview_retrospective(
            store_id=store_id,
            date_mode=date_mode,
            group_by=group_by,
            grain=grain,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить ретроспективу продаж: {exc}"}, status_code=500)


@router.get("/api/sales/overview/dashboard-summary")
async def sales_overview_dashboard_summary(
    store_id: str = "all",
    period: str = "today",
):
    try:
        payload = _dashboard_cache_payload(store_id=store_id, period=period)
        cached = _dashboard_cache_get(payload)
        if cached:
            logger.warning("[sales_overview] dashboard cache hit store_id=%s period=%s", payload["store_id"], payload["period"])
            return cached
        snapshot_cached = _dashboard_snapshot_get(payload)
        if snapshot_cached:
            logger.warning("[sales_overview] dashboard snapshot hit store_id=%s period=%s", payload["store_id"], payload["period"])
            _dashboard_cache_set(payload, snapshot_cached)
            return snapshot_cached
        logger.warning("[sales_overview] dashboard cache miss store_id=%s period=%s", payload["store_id"], payload["period"])
        key = _dashboard_cache_key(payload)
        existing_task = _DASHBOARD_IN_FLIGHT.get(key)
        if existing_task and not existing_task.done():
            built = await existing_task
        else:
            task = asyncio.create_task(_build_dashboard_summary_response(store_id=payload["store_id"], period=payload["period"]))
            _DASHBOARD_IN_FLIGHT[key] = task
            try:
                built = await task
            finally:
                _DASHBOARD_IN_FLIGHT.pop(key, None)
        _dashboard_cache_set(payload, built)
        return built
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось собрать сводку dashboard: {exc}"}, status_code=500)


@router.post("/api/sales/overview/united-orders/refresh")
async def sales_overview_united_orders_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    date_from = str(body.get("date_from") or "").strip() or "2025-07-01"
    date_to = str(body.get("date_to") or "").strip()
    try:
        result = await refresh_sales_overview_history(date_from=date_from, date_to=date_to)
        invalidate_sales_overview_dashboard_cache()
        schedule_sales_overview_dashboard_cache_warm()
        return result
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить историю заказов Маркета: {exc}"}, status_code=500)


@router.post("/api/sales/overview/current-month/refresh")
async def sales_overview_current_month_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    store_id = str(body.get("store_id") or "").strip()
    if not store_id:
        return JSONResponse({"ok": False, "message": "store_id обязателен"}, status_code=400)
    try:
        result = await refresh_sales_overview_order_rows_current_month_for_store(store_uid=f"yandex_market:{store_id}")
        invalidate_sales_overview_dashboard_cache()
        schedule_sales_overview_dashboard_cache_warm()
        return result
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить текущий месяц продаж: {exc}"}, status_code=500)


@router.post("/api/sales/overview/today/refresh")
async def sales_overview_today_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    store_id = str(body.get("store_id") or "").strip()
    if not store_id:
        return JSONResponse({"ok": False, "message": "store_id обязателен"}, status_code=400)
    try:
        result = await refresh_sales_overview_order_rows_today_for_store(store_uid=f"yandex_market:{store_id}")
        invalidate_sales_overview_dashboard_cache()
        schedule_sales_overview_dashboard_cache_warm()
        return result
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить заказы дня: {exc}"}, status_code=500)


@router.post("/api/sales/overview/united-netting/refresh")
async def sales_overview_united_netting_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    date_from = str(body.get("date_from") or "").strip() or "2025-07-01"
    date_to = str(body.get("date_to") or "").strip() or "2026-03-11"
    try:
        result = await refresh_sales_united_netting_history(date_from=date_from, date_to=date_to)
        invalidate_sales_overview_dashboard_cache()
        schedule_sales_overview_dashboard_cache_warm()
        return result
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить united netting report: {exc}"}, status_code=500)


@router.get("/api/sales/overview/cogs-source")
async def sales_overview_cogs_source(store_id: str):
    sid = str(store_id or "").strip()
    if not sid:
        return JSONResponse({"ok": False, "message": "store_id обязателен"}, status_code=400)
    try:
        settings = get_pricing_store_settings(store_uid=f"yandex_market:{sid}") or {}
        return {
            "ok": True,
            "source": {
                "type": settings.get("overview_cogs_source_type"),
                "sourceId": settings.get("overview_cogs_source_id"),
                "sourceName": settings.get("overview_cogs_source_name"),
                "skuColumn": settings.get("overview_cogs_order_column"),
                "extraColumn": settings.get("overview_cogs_sku_column"),
                "valueColumn": settings.get("overview_cogs_value_column"),
            },
        }
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось получить источник себестоимости: {exc}"}, status_code=500)


@router.post("/api/sales/overview/cogs-source")
async def sales_overview_cogs_source_upsert(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    store_id = str(body.get("store_id") or "").strip()
    if not store_id:
        return JSONResponse({"ok": False, "message": "store_id обязателен"}, status_code=400)
    try:
        settings = upsert_pricing_store_settings(
            store_uid=f"yandex_market:{store_id}",
            values={
                "overview_cogs_source_type": body.get("type"),
                "overview_cogs_source_id": body.get("sourceId"),
                "overview_cogs_source_name": body.get("sourceName"),
                "overview_cogs_order_column": body.get("skuColumn"),
                "overview_cogs_sku_column": body.get("extraColumn"),
                "overview_cogs_value_column": body.get("valueColumn"),
            },
        )
        refresh_sales_overview_cogs_source_for_store(store_uid=f"yandex_market:{store_id}")
        await refresh_sales_overview_order_rows_for_store(store_uid=f"yandex_market:{store_id}")
        invalidate_sales_overview_dashboard_cache()
        schedule_sales_overview_dashboard_cache_warm()
        return {
            "ok": True,
            "source": {
                "type": settings.get("overview_cogs_source_type"),
                "sourceId": settings.get("overview_cogs_source_id"),
                "sourceName": settings.get("overview_cogs_source_name"),
                "skuColumn": settings.get("overview_cogs_order_column"),
                "extraColumn": settings.get("overview_cogs_sku_column"),
                "valueColumn": settings.get("overview_cogs_value_column"),
            },
        }
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось сохранить источник себестоимости: {exc}"}, status_code=500)
