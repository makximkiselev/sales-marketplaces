from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo
import calendar

from backend.services.db import _connect, is_postgres_backend
from backend.services.pricing_currency_helpers import (
    convert_price_between_currencies as _convert_price_between_currencies,
    get_pricing_fx_usd_rub_rate_for_date as _get_pricing_fx_usd_rub_rate_for_date,
    promo_matches_installed_price as _promo_matches_installed_price,
    resolve_goods_report_currency as _resolve_goods_report_currency,
    to_num_simple as _to_num,
)
from backend.services.pricing_runtime_bridge import (
    build_market_promo_details,
    get_cbr_usd_rub_rate_for_date as _get_cbr_usd_rub_rate_for_date,
    get_prices_context,
    get_prices_overview,
    get_prices_tree,
    profit_for_price_with_ads_rate as _profit_for_price_with_ads_rate,
    refresh_attractiveness_data,
    refresh_boost_data,
    refresh_promos_data,
    refresh_sales_coinvest_data,
    target_met as _target_met,
)
from backend.services.store_data_model import (
    _load_sales_overview_order_rows_combined,
    append_pricing_daily_plan_history_bulk,
    append_pricing_strategy_iteration_history_bulk,
    append_pricing_strategy_history_bulk,
    clear_pricing_strategy_results_for_store,
    get_active_pricing_promo_campaigns,
    get_fx_rates_cache,
    get_pricing_attractiveness_results_map,
    get_pricing_boost_results_map,
    get_pricing_price_results_map,
    get_pricing_promo_coinvest_settings_map,
    get_pricing_promo_offer_results_map,
    get_pricing_promo_offer_raw_map,
    get_pricing_promo_results_map,
    get_pricing_store_settings,
    get_pricing_strategy_iteration_latest_map,
    get_pricing_strategy_results_map,
    get_yandex_goods_price_report_map,
    get_yandex_goods_price_report_prev_map,
    upsert_pricing_strategy_results_bulk,
)

_STRATEGY_CACHE: dict[str, dict] = {}
_STRATEGY_CACHE_GEN = 1
_STRATEGY_CACHE_MAX = 400
MSK = ZoneInfo("Europe/Moscow")
STRATEGY_ITERATION_WAIT_SECONDS = 60
_STRATEGY_TRACE_FILE = Path(__file__).resolve().parents[2] / "data" / "logs" / "pricing_strategy_trace.jsonl"
logger = logging.getLogger("uvicorn.error")


def _cache_key(name: str, payload: dict) -> str:
    raw = json.dumps({"name": name, "gen": _STRATEGY_CACHE_GEN, "payload": payload}, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_get(name: str, payload: dict):
    key = _cache_key(name, payload)
    got = _STRATEGY_CACHE.get(key)
    return copy.deepcopy(got) if isinstance(got, dict) else None


def _cache_set(name: str, payload: dict, value: dict):
    key = _cache_key(name, payload)
    if len(_STRATEGY_CACHE) >= _STRATEGY_CACHE_MAX:
        _STRATEGY_CACHE.clear()
    _STRATEGY_CACHE[key] = copy.deepcopy(value)


def invalidate_strategy_cache():
    global _STRATEGY_CACHE_GEN
    _STRATEGY_CACHE.clear()
    _STRATEGY_CACHE_GEN += 1


def _append_strategy_trace(entry: dict[str, Any]) -> None:
    try:
        _STRATEGY_TRACE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(entry or {})
        payload.setdefault("logged_at", datetime.now(MSK).astimezone().isoformat())
        with _STRATEGY_TRACE_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def _round_money(v: float | None) -> float | None:
    if v is None:
        return None
    return float(int(round(v)))


def _round_pct(v: float | None) -> float | None:
    if v is None:
        return None
    return round(float(v), 2)




def _is_stable_report_value(current_value: float | None, previous_value: float | None) -> bool:
    if current_value in (None, 0) or previous_value in (None, 0):
        return False
    current = float(current_value)
    previous = float(previous_value)
    delta_abs = abs(current - previous)
    if delta_abs <= 1.0:
        return True
    baseline = max(abs(previous), 1.0)
    return (delta_abs / baseline) <= 0.005


def _build_profit_first_daily_plan(
    *,
    plan_revenue_total: float | None,
    plan_profit_total: float | None,
    month_revenue_before_today: float | None,
    month_profit_before_today: float | None,
    minimum_profit_percent: float | None,
    weighted_day_profit_pct: float | None,
    calc_date: date,
) -> dict[str, float | None]:
    days_in_month = calendar.monthrange(calc_date.year, calc_date.month)[1]
    remaining_days_in_month = max(1, days_in_month - calc_date.day + 1)

    remaining_revenue_total = (
        max(0.0, float(plan_revenue_total) - float(month_revenue_before_today or 0.0))
        if plan_revenue_total not in (None, 0)
        else None
    )
    remaining_profit_total = (
        max(0.0, float(plan_profit_total) - float(month_profit_before_today or 0.0))
        if plan_profit_total not in (None, 0)
        else None
    )
    base_plan_profit_pct = (
        (float(plan_profit_total) / float(plan_revenue_total)) * 100.0
        if plan_profit_total not in (None, 0) and plan_revenue_total not in (None, 0)
        else None
    )
    minimum_pct = float(minimum_profit_percent or 0.0) if minimum_profit_percent not in (None, "") else None
    feasible_profit_pct = base_plan_profit_pct
    if weighted_day_profit_pct not in (None, 0):
        feasible_profit_pct = min(
            float(base_plan_profit_pct or weighted_day_profit_pct),
            float(weighted_day_profit_pct),
        )
    if feasible_profit_pct in (None, 0) and minimum_pct not in (None, 0):
        feasible_profit_pct = float(minimum_pct)
    if minimum_pct not in (None, 0):
        feasible_profit_pct = max(float(minimum_pct), float(feasible_profit_pct or minimum_pct))

    revenue_needed_for_profit = (
        float(remaining_profit_total) / (float(feasible_profit_pct) / 100.0)
        if remaining_profit_total not in (None, 0) and feasible_profit_pct not in (None, 0) and feasible_profit_pct > 0
        else None
    )
    effective_remaining_revenue = None
    if remaining_revenue_total not in (None, 0) and revenue_needed_for_profit not in (None, 0):
        effective_remaining_revenue = max(float(remaining_revenue_total), float(revenue_needed_for_profit))
    elif revenue_needed_for_profit not in (None, 0):
        effective_remaining_revenue = float(revenue_needed_for_profit)
    elif remaining_revenue_total not in (None, 0):
        effective_remaining_revenue = float(remaining_revenue_total)

    planned_revenue_daily = (
        float(effective_remaining_revenue) / float(remaining_days_in_month)
        if effective_remaining_revenue not in (None, 0)
        else None
    )
    planned_profit_daily = (
        float(remaining_profit_total) / float(remaining_days_in_month)
        if remaining_profit_total not in (None, 0)
        else None
    )
    planned_profit_pct = (
        (float(planned_profit_daily) / float(planned_revenue_daily)) * 100.0
        if planned_profit_daily not in (None, 0) and planned_revenue_daily not in (None, 0)
        else None
    )
    return {
        "remaining_days_in_month": float(remaining_days_in_month),
        "remaining_revenue_total": remaining_revenue_total,
        "remaining_profit_total": remaining_profit_total,
        "base_plan_profit_pct": base_plan_profit_pct,
        "feasible_profit_pct": feasible_profit_pct,
        "revenue_needed_for_profit": revenue_needed_for_profit,
        "planned_revenue_daily": planned_revenue_daily,
        "planned_profit_daily": planned_profit_daily,
        "planned_profit_pct": planned_profit_pct,
    }


def _build_sales_plan_summary(
    *,
    stores: list[dict[str, Any]],
    store_totals: dict[str, dict[str, Any]],
    calc_date: date,
) -> dict[str, Any]:
    by_store: dict[str, dict[str, Any]] = {}
    overall_plan_revenue_rub = 0.0
    overall_plan_profit_rub = 0.0
    overall_adjusted_plan_revenue_rub = 0.0
    overall_adjusted_plan_profit_rub = 0.0
    overall_fact_revenue_rub = 0.0
    overall_fact_profit_rub = 0.0

    for store in stores:
        suid = str(store.get("store_uid") or "").strip()
        if not suid:
            continue
        totals = store_totals.get(suid) or {}
        plan_revenue = _to_num(totals.get("planned_revenue"))
        plan_profit = _to_num(totals.get("target_profit_rub"))
        fact_revenue = _to_num(totals.get("finalized_today_revenue"))
        fact_profit = _to_num(totals.get("finalized_today_profit"))
        operational_revenue = _to_num(totals.get("today_revenue"))
        operational_profit = _to_num(totals.get("today_profit"))
        month_revenue_before_today = _to_num(totals.get("month_revenue_before_today"))
        month_profit_before_today = _to_num(totals.get("month_profit_before_today"))
        minimum_profit_percent = _to_num(totals.get("minimum_profit_percent"))
        fact_profit_pct = (
            (float(fact_profit) / float(fact_revenue)) * 100.0
        ) if fact_revenue not in (None, 0) and fact_profit is not None else None
        base_day_plan = _build_profit_first_daily_plan(
            plan_revenue_total=plan_revenue,
            plan_profit_total=plan_profit,
            month_revenue_before_today=month_revenue_before_today,
            month_profit_before_today=month_profit_before_today,
            minimum_profit_percent=minimum_profit_percent,
            weighted_day_profit_pct=None,
            calc_date=calc_date,
        )
        adjusted_day_plan = _build_profit_first_daily_plan(
            plan_revenue_total=plan_revenue,
            plan_profit_total=plan_profit,
            month_revenue_before_today=month_revenue_before_today,
            month_profit_before_today=month_profit_before_today,
            minimum_profit_percent=minimum_profit_percent,
            weighted_day_profit_pct=fact_profit_pct,
            calc_date=calc_date,
        )
        planned_revenue_daily = _to_num(base_day_plan.get("planned_revenue_daily"))
        planned_profit_daily = _to_num(base_day_plan.get("planned_profit_daily"))
        planned_profit_pct = _to_num(base_day_plan.get("planned_profit_pct"))
        adjusted_planned_revenue_daily = _to_num(adjusted_day_plan.get("planned_revenue_daily"))
        adjusted_planned_profit_daily = _to_num(adjusted_day_plan.get("planned_profit_daily"))
        adjusted_planned_profit_pct = _to_num(adjusted_day_plan.get("planned_profit_pct"))

        by_store[suid] = {
            "store_uid": suid,
            "label": str(store.get("label") or store.get("store_id") or suid),
            "currency_code": "RUB",
            "planned_revenue_daily": planned_revenue_daily,
            "planned_profit_daily": planned_profit_daily,
            "planned_profit_pct": planned_profit_pct,
            "adjusted_planned_revenue_daily": adjusted_planned_revenue_daily,
            "adjusted_planned_profit_daily": adjusted_planned_profit_daily,
            "adjusted_planned_profit_pct": adjusted_planned_profit_pct,
            "fact_revenue": fact_revenue,
            "fact_profit": fact_profit,
            "fact_profit_pct": fact_profit_pct,
            "operational_revenue": operational_revenue,
            "operational_profit": operational_profit,
            "operational_profit_pct": (
                (float(operational_profit) / float(operational_revenue)) * 100.0
            ) if operational_revenue not in (None, 0) and operational_profit is not None else None,
            "minimum_profit_percent": minimum_profit_percent,
        }

        plan_revenue_rub = float(planned_revenue_daily or 0.0)
        plan_profit_rub = float(planned_profit_daily or 0.0)
        adjusted_plan_revenue_rub = float(adjusted_planned_revenue_daily or 0.0)
        adjusted_plan_profit_rub = float(adjusted_planned_profit_daily or 0.0)
        fact_revenue_rub = float(fact_revenue or 0.0)
        fact_profit_rub = float(fact_profit or 0.0)
        overall_plan_revenue_rub += float(plan_revenue_rub)
        overall_plan_profit_rub += float(plan_profit_rub)
        overall_adjusted_plan_revenue_rub += float(adjusted_plan_revenue_rub)
        overall_adjusted_plan_profit_rub += float(adjusted_plan_profit_rub)
        overall_fact_revenue_rub += float(fact_revenue_rub)
        overall_fact_profit_rub += float(fact_profit_rub)

    overall_planned_profit_pct = (
        (overall_plan_profit_rub / overall_plan_revenue_rub) * 100.0
    ) if overall_plan_revenue_rub > 0 else None
    overall_adjusted_planned_profit_pct = (
        (overall_adjusted_plan_profit_rub / overall_adjusted_plan_revenue_rub) * 100.0
    ) if overall_adjusted_plan_revenue_rub > 0 else None
    overall_fact_profit_pct = (
        (overall_fact_profit_rub / overall_fact_revenue_rub) * 100.0
    ) if overall_fact_revenue_rub > 0 else None

    return {
        "overall": {
            "label": "Все магазины",
            "currency_code": "RUB",
            "planned_revenue_daily": overall_plan_revenue_rub,
            "planned_profit_daily": overall_plan_profit_rub,
            "planned_profit_pct": overall_planned_profit_pct,
            "adjusted_planned_revenue_daily": overall_adjusted_plan_revenue_rub,
            "adjusted_planned_profit_daily": overall_adjusted_plan_profit_rub,
            "adjusted_planned_profit_pct": overall_adjusted_planned_profit_pct,
            "fact_revenue": overall_fact_revenue_rub,
            "fact_profit": overall_fact_profit_rub,
            "fact_profit_pct": overall_fact_profit_pct,
            "operational_revenue": sum(float((_to_num((store_totals.get(str(store.get("store_uid") or "").strip()) or {}).get("today_revenue")) or 0.0)) for store in stores),
            "operational_profit": sum(float((_to_num((store_totals.get(str(store.get("store_uid") or "").strip()) or {}).get("today_profit")) or 0.0)) for store in stores),
            "operational_profit_pct": (
                (
                    sum(float((_to_num((store_totals.get(str(store.get("store_uid") or "").strip()) or {}).get("today_profit")) or 0.0)) for store in stores)
                    / sum(float((_to_num((store_totals.get(str(store.get("store_uid") or "").strip()) or {}).get("today_revenue")) or 0.0)) for store in stores)
                ) * 100.0
            ) if sum(float((_to_num((store_totals.get(str(store.get("store_uid") or "").strip()) or {}).get("today_revenue")) or 0.0)) for store in stores) > 0 else None,
        },
        "by_store": by_store,
    }




def _filter_strategy_stores(stores: list[dict[str, Any]], *, scope: str, platform: str, store_id: str) -> list[dict[str, Any]]:
    platform_norm = str(platform or "").strip().lower()
    store_norm = str(store_id or "").strip()
    filtered: list[dict[str, Any]] = []
    for store in stores:
        store_platform = str(store.get("platform") or "").strip().lower()
        store_id_value = str(store.get("store_id") or "").strip()
        if store_platform != "yandex_market":
            continue
        if str(scope or "").strip().lower() == "store":
            if platform_norm and store_platform != platform_norm:
                continue
            if store_norm and store_id_value != store_norm:
                continue
        filtered.append(store)
    return filtered


def _parse_order_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    if " " in raw:
        head = raw.split(" ", 1)[0].strip()
        return _parse_order_date(head)
    return None


def _parse_order_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in (
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=MSK)
        except Exception:
            continue
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=MSK)
    except Exception:
        return None


def _status_kind(status: str) -> str:
    raw = str(status or "").strip().lower()
    if not raw:
        return "other"
    if "отмен" in raw or "невыкуп" in raw:
        return "ignore"
    if "возврат" in raw:
        return "return"
    if "доставлен" in raw:
        return "delivered"
    if "оформ" in raw or "отгруж" in raw or "processing" in raw or "pickup" in raw or "delivery" in raw:
        return "open"
    return "other"


def _arc_elasticity(
    *,
    current_qty: float,
    previous_qty: float,
    current_price: float | None,
    previous_price: float | None,
) -> float | None:
    if current_price in (None, 0) or previous_price in (None, 0):
        return None
    if current_qty < 0 or previous_qty < 0:
        return None
    qty_avg = (current_qty + previous_qty) / 2.0
    price_avg = (float(current_price) + float(previous_price)) / 2.0
    if qty_avg <= 0 or price_avg <= 0:
        return None
    qty_delta = current_qty - previous_qty
    price_delta = float(current_price) - float(previous_price)
    if abs(price_delta) < 1e-9:
        return None
    pct_qty = qty_delta / qty_avg
    pct_price = price_delta / price_avg
    if abs(pct_price) < 1e-9:
        return None
    return pct_qty / pct_price


def _compose_strategy_label(*, promo: bool, attractiveness: bool, boost: bool) -> str:
    parts: list[str] = []
    if promo:
        parts.append("промо")
    if attractiveness:
        parts.append("привлекательность")
    if boost:
        parts.append("буст")
    return " + ".join(parts) if parts else "базовая"


def _compose_strategy_code(*, promo: bool, attractiveness: bool, boost: bool) -> str:
    parts: list[str] = []
    if promo:
        parts.append("promo")
    if attractiveness:
        parts.append("attractiveness")
    if boost:
        parts.append("boost")
    return "+".join(parts) if parts else "base"


def _decision_code_from_label(label: str | None) -> str:
    normalized = str(label or "").strip().lower()
    if not normalized:
        return "observe"
    mapping = {
        "наблюдать": "observe",
        "снизить цену": "lower_price",
        "повысить цену": "raise_price",
        "тест промо": "test_promo",
        "тест буста": "test_boost",
        "защитить прибыль": "protect_profit",
        "фокус на другой магазин": "focus_other_store",
        "усилить продажи": "increase_sales",
        "2 промо + выгодно + буст": "promo2_profitable_boost",
        "2 промо + выгодно": "promo2_profitable",
        "1 промо + выгодно + буст": "promo1_profitable_boost",
        "1 промо + выгодно": "promo1_profitable",
        "2 промо + умеренно + буст": "promo2_moderate_boost",
        "2 промо + умеренно": "promo2_moderate",
        "1 промо + умеренно + буст": "promo1_moderate_boost",
        "1 промо + умеренно": "promo1_moderate",
        "буст + выгодно": "boost_profitable",
        "буст + умеренно": "boost_moderate",
        "выгодная цена + буст": "profitable_boost",
        "выгодная цена": "profitable",
        "выгодно": "profitable",
        "умеренная цена + буст": "moderate_boost",
        "умеренная цена": "moderate",
        "умеренно": "moderate",
        "буст": "boost_only",
        "невыгодная цена": "overpriced",
    }
    return mapping.get(normalized, normalized.replace(" ", "_"))


def _build_plan_fact_summary(
    *,
    stores: list[dict[str, Any]],
    store_totals: dict[str, dict[str, float | None]],
    today: date,
) -> dict[str, Any]:
    by_store: dict[str, dict[str, Any]] = {}
    overall_plan_revenue = 0.0
    overall_plan_profit = 0.0
    overall_fact_revenue = 0.0
    overall_fact_profit = 0.0
    has_overall_plan_revenue = False
    has_overall_plan_profit = False
    has_overall_fact_revenue = False
    has_overall_fact_profit = False

    for store in stores:
        suid = str(store.get("store_uid") or "").strip()
        if not suid:
            continue
        totals = store_totals.get(suid) or {}
        plan_revenue = _to_num(totals.get("planned_revenue"))
        plan_profit = _to_num(totals.get("target_profit_rub"))
        fact_revenue = _to_num(totals.get("today_revenue"))
        fact_profit = _to_num(totals.get("today_profit"))
        month_revenue_before_today = _to_num(totals.get("month_revenue_before_today"))
        month_profit_before_today = _to_num(totals.get("month_profit_before_today"))
        minimum_profit_percent = _to_num(totals.get("minimum_profit_percent"))
        fact_profit_pct = (
            (float(fact_profit) / float(fact_revenue)) * 100.0
        ) if fact_profit not in (None, 0) and fact_revenue not in (None, 0) else None
        day_plan = _build_profit_first_daily_plan(
            plan_revenue_total=plan_revenue,
            plan_profit_total=plan_profit,
            month_revenue_before_today=month_revenue_before_today,
            month_profit_before_today=month_profit_before_today,
            minimum_profit_percent=minimum_profit_percent,
            weighted_day_profit_pct=fact_profit_pct,
            calc_date=today,
        )
        daily_plan_revenue = _to_num(day_plan.get("planned_revenue_daily"))
        daily_plan_profit = _to_num(day_plan.get("planned_profit_daily"))
        daily_plan_profit_pct = _to_num(day_plan.get("planned_profit_pct"))

        by_store[suid] = {
            "store_uid": suid,
            "label": str(store.get("label") or "").strip(),
            "currency_code": str(store.get("currency_code") or "RUB").strip().upper() or "RUB",
            "plan_revenue": daily_plan_revenue,
            "plan_profit_pct": daily_plan_profit_pct,
            "plan_profit_abs": daily_plan_profit,
            "fact_revenue": fact_revenue,
            "fact_profit_pct": fact_profit_pct,
            "fact_profit_abs": fact_profit,
        }

        if daily_plan_revenue not in (None, 0):
            overall_plan_revenue += float(daily_plan_revenue)
            has_overall_plan_revenue = True
        if daily_plan_profit not in (None, 0):
            overall_plan_profit += float(daily_plan_profit)
            has_overall_plan_profit = True
        if fact_revenue not in (None, 0):
            overall_fact_revenue += float(fact_revenue)
            has_overall_fact_revenue = True
        if fact_profit not in (None, 0):
            overall_fact_profit += float(fact_profit)
            has_overall_fact_profit = True

    overall = {
        "plan_revenue": overall_plan_revenue if has_overall_plan_revenue else None,
        "plan_profit_pct": ((overall_plan_profit / overall_plan_revenue) * 100.0) if has_overall_plan_profit and has_overall_plan_revenue and overall_plan_revenue > 0 else None,
        "plan_profit_abs": overall_plan_profit if has_overall_plan_profit else None,
        "fact_revenue": overall_fact_revenue if has_overall_fact_revenue else None,
        "fact_profit_pct": ((overall_fact_profit / overall_fact_revenue) * 100.0) if has_overall_fact_profit and has_overall_fact_revenue and overall_fact_revenue > 0 else None,
        "fact_profit_abs": overall_fact_profit if has_overall_fact_profit else None,
    }
    return {"overall": overall, "by_store": by_store}


def _strategy_sort_metric_value(row: dict[str, Any], key: str) -> float:
    fact_sales_revenue_map = row.get("fact_sales_revenue_by_store") or {}
    fact_sales_qty_map = row.get("fact_sales_by_store") or {}
    sku_plan_qty_map = row.get("sku_sales_plan_qty_by_store") or {}
    sku_plan_revenue_map = row.get("sku_sales_plan_revenue_by_store") or {}
    unit_profit_abs_map = row.get("planned_unit_profit_abs_by_store") or {}
    unit_profit_pct_map = row.get("planned_unit_profit_pct_by_store") or {}
    fact_profit_abs_map = row.get("fact_economy_abs_by_store") or {}
    fact_profit_pct_map = row.get("fact_economy_pct_by_store") or {}
    store_uids = {
        *([str(k) for k in fact_sales_revenue_map.keys()] if isinstance(fact_sales_revenue_map, dict) else []),
        *([str(k) for k in fact_sales_qty_map.keys()] if isinstance(fact_sales_qty_map, dict) else []),
        *([str(k) for k in sku_plan_qty_map.keys()] if isinstance(sku_plan_qty_map, dict) else []),
        *([str(k) for k in sku_plan_revenue_map.keys()] if isinstance(sku_plan_revenue_map, dict) else []),
        *([str(k) for k in unit_profit_abs_map.keys()] if isinstance(unit_profit_abs_map, dict) else []),
        *([str(k) for k in unit_profit_pct_map.keys()] if isinstance(unit_profit_pct_map, dict) else []),
        *([str(k) for k in fact_profit_abs_map.keys()] if isinstance(fact_profit_abs_map, dict) else []),
        *([str(k) for k in fact_profit_pct_map.keys()] if isinstance(fact_profit_pct_map, dict) else []),
    }
    total = 0.0
    for suid in store_uids:
        qty_plan = float(_to_num((sku_plan_qty_map or {}).get(suid)) or 0.0)
        revenue_plan = float(_to_num((sku_plan_revenue_map or {}).get(suid)) or 0.0)
        unit_profit_abs = float(_to_num((unit_profit_abs_map or {}).get(suid)) or 0.0)
        unit_profit_pct = float(_to_num((unit_profit_pct_map or {}).get(suid)) or 0.0)
        fact_revenue = float(_to_num((fact_sales_revenue_map or {}).get(suid)) or 0.0)
        fact_qty = float(_to_num((fact_sales_qty_map or {}).get(suid)) or 0.0)
        fact_profit_abs = float(_to_num((fact_profit_abs_map or {}).get(suid)) or 0.0)
        fact_profit_pct = float(_to_num((fact_profit_pct_map or {}).get(suid)) or 0.0)
        plan_profit_abs = qty_plan * unit_profit_abs
        if key == "sku_plan_revenue":
            total += revenue_plan
        elif key == "sku_plan_qty":
            total += qty_plan
        elif key == "sku_plan_profit_abs":
            total += plan_profit_abs
        elif key == "sku_plan_profit_pct":
            total += unit_profit_pct
        elif key == "fact_sales_revenue":
            total += fact_revenue
        elif key == "fact_sales_qty":
            total += fact_qty
        elif key == "fact_profit_abs":
            total += fact_profit_abs
        elif key == "fact_profit_pct":
            total += fact_profit_pct
        elif key == "profit_completion_pct":
            total += ((fact_profit_abs / plan_profit_abs) * 100.0) if plan_profit_abs else 0.0
    return total


def _build_sales_metrics(store_uids: list[str], skus: list[str]) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, float | None]]]:
    suids = [str(x or "").strip() for x in store_uids if str(x or "").strip()]
    sku_list = [str(x or "").strip() for x in skus if str(x or "").strip()]
    if not suids or not sku_list:
        return {}, {}

    today = datetime.now(MSK).date()
    yesterday = today - timedelta(days=1)
    week_start = today - timedelta(days=6)
    prev_week_start = today - timedelta(days=13)
    prev_week_end = today - timedelta(days=7)
    sixty_start = today - timedelta(days=59)
    month_start = today.replace(day=1)
    elapsed_days = max(1, (today - month_start).days + 1)

    metrics: dict[str, dict[str, dict[str, Any]]] = {suid: {} for suid in suids}
    store_totals: dict[str, dict[str, float | None]] = {
        suid: {
            "today_revenue": 0.0,
            "today_profit": 0.0,
            "finalized_today_revenue": 0.0,
            "finalized_today_profit": 0.0,
            "month_revenue_before_today": 0.0,
            "month_profit_before_today": 0.0,
            "planned_revenue": None,
            "target_profit_rub": None,
            "minimum_profit_percent": None,
        }
        for suid in suids
    }

    sku_set = set(sku_list)
    rows: list[dict[str, Any]] = []
    for suid in suids:
        for row in _load_sales_overview_order_rows_combined(store_uid=suid):
            sku = str(row.get("sku") or "").strip()
            if sku and sku in sku_set:
                rows.append(
                    {
                        "store_uid": suid,
                        "sku": sku,
                        "order_created_date": row.get("order_created_date"),
                        "order_created_at": row.get("order_created_at"),
                        "item_status": row.get("item_status"),
                        "sale_price": row.get("sale_price"),
                        "sale_price_with_coinvest": row.get("sale_price_with_coinvest"),
                        "profit": row.get("profit"),
                        "uses_planned_costs": row.get("uses_planned_costs"),
                    }
                )

    for suid in suids:
        settings = get_pricing_store_settings(store_uid=suid) or {}
        planned_revenue = _to_num(settings.get("planned_revenue"))
        target_profit_rub = _to_num(settings.get("target_profit_rub"))
        target_profit_pct = _to_num(settings.get("target_profit_percent"))
        minimum_profit_percent = _to_num(settings.get("minimum_profit_percent"))
        if (target_profit_rub in (None, 0)) and planned_revenue not in (None, 0) and target_profit_pct not in (None, 0):
            target_profit_rub = float(planned_revenue) * (float(target_profit_pct) / 100.0)
        store_totals[suid]["planned_revenue"] = planned_revenue
        store_totals[suid]["target_profit_rub"] = target_profit_rub
        store_totals[suid]["minimum_profit_percent"] = minimum_profit_percent

    for row in rows:
        suid = str(row["store_uid"] or "").strip()
        sku = str(row["sku"] or "").strip()
        if not suid or not sku:
            continue
        status_kind = _status_kind(str(row["item_status"] or ""))
        if status_kind == "ignore":
            continue
        created_day = _parse_order_date(row["order_created_date"]) or (
            (_parse_order_datetime(row["order_created_at"]) or datetime.now(MSK)).date()
        )
        if created_day is None:
            continue
        sale_price = float(_to_num(row["sale_price"]) or 0.0)
        buyer_price = _to_num(row["sale_price_with_coinvest"])
        if buyer_price is None:
            buyer_price = sale_price
        profit = float(_to_num(row["profit"]) or 0.0)
        uses_planned_costs = bool(row.get("uses_planned_costs"))
        coinvest_pct = None
        if sale_price > 0 and buyer_price is not None:
            coinvest_pct = ((sale_price - float(buyer_price)) / sale_price) * 100.0

        item = metrics[suid].setdefault(
            sku,
            {
                "today_sales": 0,
                "today_revenue": 0.0,
                "today_profit": 0.0,
                "yesterday_sales": 0,
                "yesterday_revenue": 0.0,
                "yesterday_profit": 0.0,
                "yesterday_buyer_price_sum": 0.0,
                "yesterday_buyer_price_count": 0,
                "avg_check_sum": 0.0,
                "avg_check_count": 0,
                "coinvest_sum": 0.0,
                "coinvest_count": 0,
                "current_week_qty": 0,
                "current_week_price_sum": 0.0,
                "previous_week_qty": 0,
                "previous_week_price_sum": 0.0,
                "sixty_day_qty": 0,
                "sixty_day_price_sum": 0.0,
                "month_qty": 0,
            },
        )

        if created_day == today and status_kind in {"delivered", "open"}:
            item["today_sales"] += 1
            item["today_revenue"] += sale_price
            item["today_profit"] += profit
            store_totals[suid]["today_revenue"] = float(store_totals[suid]["today_revenue"] or 0.0) + sale_price
            store_totals[suid]["today_profit"] = float(store_totals[suid]["today_profit"] or 0.0) + profit
            if status_kind == "delivered" and not uses_planned_costs:
                store_totals[suid]["finalized_today_revenue"] = float(store_totals[suid]["finalized_today_revenue"] or 0.0) + sale_price
                store_totals[suid]["finalized_today_profit"] = float(store_totals[suid]["finalized_today_profit"] or 0.0) + profit
        elif month_start <= created_day < today and status_kind in {"delivered", "open"}:
            store_totals[suid]["month_revenue_before_today"] = float(store_totals[suid]["month_revenue_before_today"] or 0.0) + sale_price
            store_totals[suid]["month_profit_before_today"] = float(store_totals[suid]["month_profit_before_today"] or 0.0) + profit
        if created_day == yesterday and status_kind in {"delivered", "open"}:
            item["yesterday_sales"] += 1
            item["yesterday_revenue"] += sale_price
            item["yesterday_profit"] += profit
            item["yesterday_buyer_price_sum"] += float(buyer_price or 0.0)
            item["yesterday_buyer_price_count"] += 1

        if status_kind == "delivered":
            if week_start <= created_day <= today:
                item["current_week_qty"] += 1
                item["current_week_price_sum"] += float(buyer_price or 0.0)
            if prev_week_start <= created_day <= prev_week_end:
                item["previous_week_qty"] += 1
                item["previous_week_price_sum"] += float(buyer_price or 0.0)
            if sixty_start <= created_day <= today:
                item["sixty_day_qty"] += 1
                item["sixty_day_price_sum"] += float(buyer_price or 0.0)
            if sixty_start <= created_day <= today and coinvest_pct is not None:
                item["coinvest_sum"] += float(coinvest_pct)
                item["coinvest_count"] += 1
            if month_start <= created_day <= today:
                item["avg_check_sum"] += float(buyer_price or 0.0)
                item["avg_check_count"] += 1
        if month_start <= created_day <= today and status_kind in {"delivered", "open"}:
            item["month_qty"] += 1

    for suid, by_sku in metrics.items():
        for sku, item in by_sku.items():
            today_revenue = float(item["today_revenue"] or 0.0)
            today_profit = float(item["today_profit"] or 0.0)
            yesterday_sales = int(item["yesterday_sales"] or 0)
            today_sales = int(item["today_sales"] or 0)
            week_avg_daily = float(item["current_week_qty"] or 0.0) / 7.0
            month_avg_daily = float(item["month_qty"] or 0.0) / float(elapsed_days)
            forecast_sales = int(round(max(float(today_sales), (week_avg_daily + month_avg_daily) / 2.0)))
            current_week_avg_price = (
                float(item["current_week_price_sum"]) / float(item["current_week_qty"])
                if float(item["current_week_qty"] or 0.0) > 0
                else None
            )
            previous_week_avg_price = (
                float(item["previous_week_price_sum"]) / float(item["previous_week_qty"])
                if float(item["previous_week_qty"] or 0.0) > 0
                else None
            )
            sixty_day_avg_price = (
                float(item["sixty_day_price_sum"]) / float(item["sixty_day_qty"])
                if float(item["sixty_day_qty"] or 0.0) > 0
                else None
            )
            elasticity = _arc_elasticity(
                current_qty=float(item["current_week_qty"] or 0.0),
                previous_qty=float(item["previous_week_qty"] or 0.0),
                current_price=current_week_avg_price,
                previous_price=previous_week_avg_price,
            )
            sales_delta_pct = None
            if yesterday_sales > 0:
                sales_delta_pct = ((today_sales - yesterday_sales) / yesterday_sales) * 100.0
            elif today_sales > 0:
                sales_delta_pct = 100.0
            today_profit_pct = ((today_profit / today_revenue) * 100.0) if today_revenue > 0 else None
            yesterday_profit = float(item["yesterday_profit"] or 0.0)
            economy_delta_pct = None
            if abs(yesterday_profit) > 1e-9:
                economy_delta_pct = ((today_profit - yesterday_profit) / abs(yesterday_profit)) * 100.0
            elif today_profit > 0:
                economy_delta_pct = 100.0
            month_avg_check = (float(item["avg_check_sum"]) / float(item["avg_check_count"])) if int(item["avg_check_count"] or 0) > 0 else None
            item["avg_check"] = month_avg_check or current_week_avg_price or sixty_day_avg_price
            item["yesterday_avg_buyer_price"] = (
                float(item["yesterday_buyer_price_sum"]) / float(item["yesterday_buyer_price_count"])
            ) if int(item["yesterday_buyer_price_count"] or 0) > 0 else None
            item["coinvest_pct"] = (float(item["coinvest_sum"]) / float(item["coinvest_count"])) if int(item["coinvest_count"] or 0) > 0 else None
            item["elasticity"] = elasticity
            item["forecast_sales"] = forecast_sales
            item["week_avg_daily"] = week_avg_daily
            item["month_avg_daily"] = month_avg_daily
            item["current_week_avg_price"] = current_week_avg_price
            item["previous_week_avg_price"] = previous_week_avg_price
            item["sixty_day_avg_price"] = sixty_day_avg_price
            item["sales_delta_pct"] = sales_delta_pct
            item["fact_economy_abs"] = today_profit
            item["fact_economy_pct"] = today_profit_pct
            item["economy_delta_pct"] = economy_delta_pct

    return metrics, store_totals


def _resolve_target_drr_pct(row_metrics: dict[str, Any] | None) -> float | None:
    metric = row_metrics if isinstance(row_metrics, dict) else {}
    calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else {}
    rate = _to_num(calc_ctx.get("ads_rate"))
    if rate is None:
        return None
    return round(float(rate) * 100.0, 2)


def _profit_for_price_and_boost(
    *,
    price: float | None,
    boost_pct: float | None,
    calc_ctx: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    if price is None or not isinstance(calc_ctx, dict):
        return None, None
    has_required_ctx = any(
        key in calc_ctx and calc_ctx.get(key) not in (None, "")
        for key in ("dep_rate", "tax_rate", "fixed_cost")
    )
    if not has_required_ctx:
        return None, None
    return _profit_for_price_with_ads_rate(
        price=float(price),
        calc_ctx=calc_ctx,
        ads_rate_override=max(0.0, float(boost_pct or 0.0)) / 100.0,
    )


def _resolve_attractiveness_selection(
    *,
    attr_metric: dict[str, Any] | None,
    row_metrics: dict[str, Any] | None,
) -> dict[str, float | None]:
    metric = attr_metric if isinstance(attr_metric, dict) else {}
    price_metric = row_metrics if isinstance(row_metrics, dict) else {}
    calc_ctx = price_metric.get("calc_ctx") if isinstance(price_metric.get("calc_ctx"), dict) else {}
    chosen_price = _to_num(metric.get("attractiveness_chosen_price"))
    chosen_boost = _to_num(metric.get("attractiveness_chosen_boost_bid_percent"))
    target_price = _to_num(price_metric.get("target_price"))
    target_profit_abs = _to_num(price_metric.get("target_profit_abs"))
    target_profit_pct = _to_num(price_metric.get("target_profit_pct"))
    mrc_price = _to_num(price_metric.get("mrc_price"))
    mrc_profit_abs = _to_num(price_metric.get("mrc_profit_abs"))
    mrc_profit_pct = _to_num(price_metric.get("mrc_profit_pct"))
    if chosen_price is None:
        return {
            "price": None,
            "boost_pct": None,
            "profit_abs": None,
            "profit_pct": None,
        }
    if mrc_price is not None and abs(float(chosen_price) - float(mrc_price)) < 0.5:
        return {
            "price": chosen_price,
            "boost_pct": chosen_boost,
            "profit_abs": mrc_profit_abs,
            "profit_pct": mrc_profit_pct,
        }
    if target_price is not None and abs(float(chosen_price) - float(target_price)) < 0.5:
        return {
            "price": chosen_price,
            "boost_pct": chosen_boost if chosen_boost is not None else _resolve_internal_economy_boost_pct(
                boost_metric=None,
                fallback_bid_pct=_resolve_target_drr_pct(price_metric),
            ),
            "profit_abs": target_profit_abs,
            "profit_pct": target_profit_pct,
        }
    profit_abs, profit_pct = _profit_for_price_and_boost(price=chosen_price, boost_pct=chosen_boost, calc_ctx=calc_ctx)
    return {
        "price": chosen_price,
        "boost_pct": chosen_boost,
        "profit_abs": profit_abs,
        "profit_pct": profit_pct,
    }


def _build_attr_profitable_candidate(
    *,
    attr_metric: dict[str, Any] | None,
    row_metrics: dict[str, Any] | None,
) -> dict[str, float | None]:
    metric = attr_metric if isinstance(attr_metric, dict) else {}
    price_metric = row_metrics if isinstance(row_metrics, dict) else {}
    calc_ctx = price_metric.get("calc_ctx") if isinstance(price_metric.get("calc_ctx"), dict) else {}
    profitable_price = _to_num(metric.get("attractiveness_profitable_price"))
    if profitable_price is None:
        return {"price": None, "boost_pct": None, "profit_abs": None, "profit_pct": None}
    target_profit_pct = _to_num(price_metric.get("target_profit_pct"))
    target_profit_abs = _to_num(price_metric.get("target_profit_abs"))
    target_drr_pct = _resolve_internal_economy_boost_pct(
        boost_metric=None,
        fallback_bid_pct=_resolve_target_drr_pct(price_metric),
    )
    boost_pct, profit_abs, profit_pct = _max_ads_rate_for_price(
        price=profitable_price,
        calc_ctx=calc_ctx,
        target_pct=target_profit_pct,
        target_abs=target_profit_abs,
        max_ads_rate_pct=target_drr_pct,
    )
    if boost_pct is None:
        profit_abs, profit_pct = _profit_for_price_and_boost(
            price=profitable_price,
            boost_pct=0.0,
            calc_ctx=calc_ctx,
        )
        boost_pct = 0.0 if _meets_target(target_pct=target_profit_pct, target_abs=target_profit_abs, profit_pct=profit_pct, profit_abs=profit_abs) else None
    return {
        "price": profitable_price,
        "boost_pct": boost_pct,
        "profit_abs": profit_abs,
        "profit_pct": profit_pct,
    }


def _resolve_promo_selection(
    *,
    promo_summary: dict[str, Any] | None,
    attr_selection: dict[str, float | None] | None,
    row_metrics: dict[str, Any] | None,
) -> dict[str, float | None]:
    summary = promo_summary if isinstance(promo_summary, dict) else {}
    metric = row_metrics if isinstance(row_metrics, dict) else {}
    calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else {}
    selected_items = summary.get("promo_selected_items") if isinstance(summary.get("promo_selected_items"), list) else []
    if not selected_items:
        return {
            "price": None,
            "boost_pct": None,
            "profit_abs": None,
            "profit_pct": None,
        }
    has_fit = any(
        str(item.get("status") or "").strip().lower().startswith("проходит")
        for item in selected_items
        if isinstance(item, dict)
    )
    if not has_fit:
        return {
            "price": None,
            "boost_pct": None,
            "profit_abs": None,
            "profit_pct": None,
        }
    selected_price = _to_num(summary.get("promo_selected_price"))
    selected_boost = _to_num(summary.get("promo_selected_boost_bid_percent"))
    selected_profit_abs = _to_num(summary.get("promo_selected_profit_abs"))
    selected_profit_pct = _to_num(summary.get("promo_selected_profit_pct"))
    if selected_price is None:
        return {
            "price": None,
            "boost_pct": None,
            "profit_abs": None,
            "profit_pct": None,
        }
    attr_price = _to_num((attr_selection or {}).get("price"))
    if (selected_profit_abs is None and selected_profit_pct is None) and attr_price is not None and abs(float(selected_price) - float(attr_price)) < 0.5:
        return {
            "price": selected_price,
            "boost_pct": selected_boost if selected_boost is not None else _to_num((attr_selection or {}).get("boost_pct")),
            "profit_abs": _to_num((attr_selection or {}).get("profit_abs")),
            "profit_pct": _to_num((attr_selection or {}).get("profit_pct")),
        }
    if selected_profit_abs is None and selected_profit_pct is None:
        selected_profit_abs, selected_profit_pct = _profit_for_price_and_boost(
            price=selected_price,
            boost_pct=selected_boost,
            calc_ctx=calc_ctx,
        )
    return {
        "price": selected_price,
        "boost_pct": selected_boost,
        "profit_abs": selected_profit_abs,
        "profit_pct": selected_profit_pct,
    }


def _filter_promo_summary_to_active(
    *,
    promo_summary: dict[str, Any] | None,
    active_promo_ids: set[str],
) -> dict[str, Any]:
    summary = dict(promo_summary or {})
    if not active_promo_ids:
        return {
            **summary,
            "promo_selected_items": [],
            "promo_selected_price": None,
            "promo_selected_boost_bid_percent": None,
            "promo_selected_profit_abs": None,
            "promo_selected_profit_pct": None,
        }
    selected_items = summary.get("promo_selected_items") if isinstance(summary.get("promo_selected_items"), list) else []
    filtered_items = [
        item for item in selected_items
        if isinstance(item, dict) and str(item.get("promo_id") or "").strip() in active_promo_ids
    ]
    if filtered_items:
        summary["promo_selected_items"] = filtered_items
        return summary
    return {
        **summary,
        "promo_selected_items": [],
        "promo_selected_price": None,
        "promo_selected_boost_bid_percent": None,
        "promo_selected_profit_abs": None,
        "promo_selected_profit_pct": None,
    }

def _resolve_attr_status(attr_metric: dict[str, Any] | None, platform: str) -> str:
    metric = attr_metric if isinstance(attr_metric, dict) else {}
    chosen = _to_num(metric.get("attractiveness_chosen_price"))
    profitable = _to_num(metric.get("attractiveness_profitable_price"))
    moderate = _to_num(metric.get("attractiveness_moderate_price"))
    overpriced = _to_num(metric.get("attractiveness_overpriced_price"))
    has_any = any(v is not None for v in (profitable, moderate, overpriced))
    if not has_any:
        return "profitable"
    if chosen is None:
        return "profitable"
    if platform == "yandex_market":
        if profitable is not None and chosen <= profitable:
            return "profitable"
        if moderate is not None:
            return "moderate" if chosen <= moderate else "overpriced"
        return "moderate"
    if profitable is not None and chosen <= profitable:
        return "profitable"
    if moderate is not None and chosen <= moderate:
        return "moderate"
    return "overpriced"


def _resolve_attr_status_for_price(
    *,
    price: float | None,
    attr_metric: dict[str, Any] | None,
    platform: str,
) -> str:
    metric = attr_metric if isinstance(attr_metric, dict) else {}
    chosen = _to_num(metric.get("attractiveness_chosen_price"))
    profitable = _to_num(metric.get("attractiveness_profitable_price"))
    moderate = _to_num(metric.get("attractiveness_moderate_price"))
    overpriced = _to_num(metric.get("attractiveness_overpriced_price"))
    if price is None:
        return _resolve_attr_status(metric, platform)
    has_any = any(v is not None for v in (chosen, profitable, moderate, overpriced))
    if not has_any:
        return "unknown"
    if platform == "yandex_market":
        if profitable is not None and price <= profitable:
            return "profitable"
        if moderate is not None:
            return "moderate" if price <= moderate else "overpriced"
        return "moderate"
    if profitable is not None and price <= profitable:
        return "profitable"
    if moderate is not None and price <= moderate:
        return "moderate"
    return "overpriced"


def _resolve_promo_participation(
    *,
    final_price: float | None,
    promo_summary: dict[str, Any] | None,
    promo_offers: list[dict[str, Any]],
) -> bool:
    if final_price is None:
        return False
    selected = _selected_offer_by_price(promo_offers, final_price)
    if isinstance(selected, dict):
        mode = str(selected.get("promo_fit_mode") or "").strip().lower()
        if mode in {"with_ads", "without_ads"}:
            return True
    summary = promo_summary if isinstance(promo_summary, dict) else {}
    selected_items = summary.get("promo_selected_items") if isinstance(summary.get("promo_selected_items"), list) else []
    has_fit = any(
        str(item.get("status") or "").strip().lower().startswith("проходит")
        for item in selected_items
        if isinstance(item, dict)
    )
    selected_price = _to_num(summary.get("promo_selected_price"))
    if has_fit and selected_price is not None and float(final_price) <= float(selected_price) + 1.01:
        return True
    return False


def _resolve_boost_share(
    *,
    boost_bid_pct: float | None,
    boost_metric: dict[str, Any] | None,
) -> float | None:
    bid = _to_num(boost_bid_pct)
    metric = boost_metric if isinstance(boost_metric, dict) else {}
    if bid is None or bid <= 0:
        return None
    thresholds = [
        (95.0, _to_num(metric.get("bid_95"))),
        (80.0, _to_num(metric.get("bid_80"))),
        (60.0, _to_num(metric.get("bid_60"))),
        (30.0, _to_num(metric.get("bid_30"))),
    ]
    usable = [(share, required) for share, required in thresholds if required is not None and required > 0]
    if not usable:
        return None
    for share, required in usable:
        if bid >= float(required) - 0.01:
            return share
    return 0.0


def _parse_iso_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=MSK)
    except Exception:
        return None


def _recommended_boost_steps(boost_metric: dict[str, Any] | None) -> list[float]:
    metric = boost_metric if isinstance(boost_metric, dict) else {}
    recommended = _to_num(metric.get("recommended_bid"))
    candidates = [
        _to_num(metric.get("bid_30")),
        _to_num(metric.get("bid_60")),
        _to_num(metric.get("bid_80")),
        _to_num(metric.get("bid_95")),
        recommended,
    ]
    values = sorted({round(float(v), 2) for v in candidates if v not in (None, 0) and float(v) > 0})
    if recommended not in (None, 0):
        cap = round(float(recommended), 2)
        values = [v for v in values if v <= cap + 0.01]
        if cap > 0 and all(abs(v - cap) > 0.01 for v in values):
            values.append(cap)
            values.sort()
    return values


def _next_recommended_boost_step(*, current_bid_pct: float | None, boost_metric: dict[str, Any] | None) -> float | None:
    current = round(float(current_bid_pct or 0.0), 2)
    steps = _recommended_boost_steps(boost_metric)
    for step in steps:
        if step > current + 0.01:
            return step
    if steps:
        return steps[-1]
    return None


def _resolve_market_export_boost_pct(
    *,
    boost_bid_pct: float | None,
    boost_metric: dict[str, Any] | None,
) -> float:
    base_bid = max(0.0, float(boost_bid_pct or 0.0))
    if base_bid < 0.01:
        return 0.0
    metric = boost_metric if isinstance(boost_metric, dict) else {}
    explicit_steps = [
        _to_num(metric.get("bid_30")),
        _to_num(metric.get("bid_60")),
        _to_num(metric.get("bid_80")),
        _to_num(metric.get("bid_95")),
    ]
    explicit_steps = [round(float(step), 2) for step in explicit_steps if step not in (None, 0) and float(step) > 0]
    if explicit_steps:
        market_bid = explicit_steps[0]
    else:
        recommended_bid = _to_num(metric.get("recommended_bid"))
        if recommended_bid not in (None, 0):
            market_bid = round(float(recommended_bid), 2)
        else:
            market_bid = round(base_bid * 2.0, 2)
    return 0.0 if market_bid < 0.01 else round(float(market_bid), 2)


def _resolve_internal_economy_boost_pct(
    *,
    boost_metric: dict[str, Any] | None,
    fallback_bid_pct: float | None = None,
) -> float:
    market_bid = _resolve_market_export_boost_pct(
        boost_bid_pct=fallback_bid_pct,
        boost_metric=boost_metric,
    )
    if market_bid > 0.01:
        return round(float(market_bid), 2)
    base_bid = max(0.0, float(fallback_bid_pct or 0.0))
    return 0.0 if base_bid < 0.01 else round(base_bid, 2)


def _next_recommended_boost_step_with_cap(
    *,
    current_bid_pct: float | None,
    boost_metric: dict[str, Any] | None,
    max_allowed_bid_pct: float | None,
) -> float | None:
    current = round(float(current_bid_pct or 0.0), 2)
    cap = _to_num(max_allowed_bid_pct)
    if cap is None or cap <= current + 0.01:
        return None
    steps = [step for step in _recommended_boost_steps(boost_metric) if step <= float(cap) + 0.01]
    for step in steps:
        if step > current + 0.01:
            return step
    safe_cap = round(float(cap), 2)
    if safe_cap > current + 0.01:
        return safe_cap
    return None


def _same_num(left: float | None, right: float | None, tol: float = 0.5) -> bool:
    if left in (None, "") and right in (None, ""):
        return True
    if left in (None, "") or right in (None, ""):
        return False
    try:
        return abs(float(left) - float(right)) <= float(tol)
    except Exception:
        return False


def _strategy_window_minutes(control_state: str) -> int:
    normalized = str(control_state or "").strip().lower()
    if normalized in {"promo_watch", "boost_test", "price_test"}:
        return 180
    return 60


def _first_safe_boost_step_for_price(
    *,
    price: float | None,
    calc_ctx: dict[str, Any] | None,
    boost_metric: dict[str, Any] | None,
    min_profit_pct: float | None,
) -> float:
    if price in (None, 0):
        return 0.0
    target_internal_boost = _resolve_internal_economy_boost_pct(
        boost_metric=boost_metric,
        fallback_bid_pct=0.0,
    )
    if target_internal_boost <= 0.01:
        return 0.0
    max_recommended = _next_recommended_boost_step(current_bid_pct=10**9, boost_metric=boost_metric)
    max_safe_boost, _, _ = _max_ads_rate_for_price(
        price=price,
        calc_ctx=calc_ctx,
        target_pct=min_profit_pct,
        target_abs=None,
        max_ads_rate_pct=min(float(max_recommended or target_internal_boost), float(target_internal_boost)),
    )
    if max_safe_boost in (None, 0):
        return 0.0
    return round(min(float(target_internal_boost), float(max_safe_boost)), 2)


def _resolve_simplified_strategy_decision(
    *,
    now_msk: datetime,
    previous_control_state: str,
    previous_control_state_started_at: datetime | None,
    previous_installed_price: float | None,
    previous_installed_boost: float | None,
    has_stock: bool,
    fact_sales: int,
    calc_ctx: dict[str, Any] | None,
    min_safe_pct: float,
    mrc_price: float | None,
    rrc_price: float | None,
    attr_metric: dict[str, Any] | None,
    attr_selection: dict[str, float | None] | None,
    attr_profitable: dict[str, float | None] | None,
    promo_selection: dict[str, float | None] | None,
    boost_metric: dict[str, Any] | None,
    platform_name: str,
) -> dict[str, Any]:
    promo_threshold_price = _to_num((promo_selection or {}).get("price"))
    profitable_attr_price = _to_num((attr_profitable or {}).get("price"))
    attr_threshold_price = _to_num((attr_selection or {}).get("price"))
    promo_threshold_attr_status = (
        _resolve_attr_status_for_price(
            price=promo_threshold_price,
            attr_metric=attr_metric,
            platform=platform_name,
        )
        if promo_threshold_price is not None
        else None
    )
    profitable_promo_price = (
        profitable_attr_price
        if profitable_attr_price is not None
        and promo_threshold_price is not None
        and profitable_attr_price <= float(promo_threshold_price) + 0.5
        and (mrc_price is None or profitable_attr_price >= float(mrc_price) - 0.5)
        else None
    )
    no_promo_threshold_price = attr_threshold_price or profitable_attr_price or mrc_price or rrc_price
    no_promo_attr_status = _resolve_attr_status_for_price(
        price=no_promo_threshold_price,
        attr_metric=attr_metric,
        platform=platform_name,
    )
    elapsed_minutes = (
        max(0.0, (now_msk - previous_control_state_started_at).total_seconds() / 60.0)
        if previous_control_state_started_at is not None
        else None
    )
    previous_state = str(previous_control_state or "").strip().lower()
    previous_boost = float(previous_installed_boost or 0.0)

    final_source = "mrc"
    final_price = mrc_price or no_promo_threshold_price or rrc_price
    final_boost = 0.0
    decision_label = "Наблюдать"
    tone = "warning"
    hypothesis = "Ожидаем следующий сигнал по товару."
    control_state = "stable"

    if not has_stock:
        final_price = mrc_price or final_price
        decision_label = "Наблюдать"
        tone = "warning"
        hypothesis = "Товара нет в остатке. Стратегию не двигаем и ждём пополнение."
        control_state = "stable"
    elif promo_threshold_price is not None:
        promo_work_price = profitable_promo_price or promo_threshold_price
        promo_safe_boost = (
            _first_safe_boost_step_for_price(
                price=promo_work_price,
                calc_ctx=calc_ctx,
                boost_metric=boost_metric,
                min_profit_pct=min_safe_pct,
            )
            if mrc_price not in (None, 0) and promo_work_price > float(mrc_price) + 0.5
            else 0.0
        )
        final_source = "promo"
        final_price = promo_work_price
        if fact_sales > 0:
            decision_label = "Наблюдать"
            tone = "positive"
            hypothesis = "Товар находится в актуальном промо и уже даёт продажи. Держим рабочую цену и ждём следующий сигнал спроса."
            control_state = "promo_watch"
        elif (
            previous_state == "boost_test"
            and _same_num(previous_installed_price, promo_work_price)
            and previous_boost > 0.01
        ):
            if elapsed_minutes is not None and elapsed_minutes >= 180.0:
                final_source = "mrc"
                final_price = mrc_price or promo_work_price
                final_boost = 0.0
                decision_label = "Снизить цену"
                tone = "warning"
                hypothesis = "Промо и первый безопасный буст не дали продаж за 3 часа. Возвращаем товар на МРЦ и дальше ждём."
                control_state = "price_test"
            else:
                final_boost = previous_boost or promo_safe_boost
                decision_label = "Тест буста"
                tone = "warning"
                hypothesis = "Держим цену входа в промо с первым безопасным бустом и ждём 3 часа, чтобы проверить продажи."
                control_state = "boost_test"
        elif previous_state == "promo_watch" and _same_num(previous_installed_price, promo_work_price):
            if elapsed_minutes is not None and elapsed_minutes >= 180.0:
                if promo_safe_boost > 0.01:
                    final_boost = promo_safe_boost
                    decision_label = "Тест буста"
                    tone = "warning"
                    hypothesis = "Продаж нет 3 часа на цене входа в промо. Добавляем первый безопасный буст в пределах запаса до МРЦ."
                    control_state = "boost_test"
                else:
                    final_source = "mrc"
                    final_price = mrc_price or promo_work_price
                    final_boost = 0.0
                    decision_label = "Снизить цену"
                    tone = "warning"
                    hypothesis = "Запаса до МРЦ для безопасного буста нет. Ставим МРЦ и продолжаем ждать продажи."
                    control_state = "price_test"
            else:
                final_boost = 0.0
                decision_label = "Тест промо"
                tone = "warning"
                hypothesis = (
                    "Есть актуальное промо и выгодная цена. Ставим порог входа в промо как рабочую цену и ждём продажи."
                    if promo_threshold_attr_status == "profitable"
                    else "Есть актуальное промо, но цена пока умеренная. Ставим выгодный вариант внутри порога промо и ждём продажи."
                )
                control_state = "promo_watch"
        else:
            final_boost = 0.0
            decision_label = "Тест промо"
            tone = "warning"
            hypothesis = (
                "Есть актуальное промо и выгодная цена. Ставим порог входа в промо как рабочую цену и ждём продажи."
                if promo_threshold_attr_status == "profitable"
                else "Есть актуальное промо, но цена пока умеренная. Ставим выгодный вариант внутри порога промо и ждём продажи."
            )
            control_state = "promo_watch"
    else:
        if no_promo_attr_status == "profitable":
            final_source = "attractiveness"
            final_price = no_promo_threshold_price or mrc_price or rrc_price
            final_boost = _first_safe_boost_step_for_price(
                price=final_price,
                calc_ctx=calc_ctx,
                boost_metric=boost_metric,
                min_profit_pct=min_safe_pct,
            )
            decision_label = "Усилить продажи" if final_boost > 0.01 else "Наблюдать"
            tone = "warning" if final_boost > 0.01 else "positive"
            hypothesis = "Промо сейчас нет, но цена уже выгодная. Держим этот порог и используем безопасный буст как основной драйвер спроса."
            control_state = "stable"
        elif no_promo_attr_status == "moderate":
            threshold_price = no_promo_threshold_price or mrc_price or rrc_price
            if threshold_price is not None and rrc_price is not None and threshold_price < float(rrc_price) - 0.5:
                final_price = threshold_price
            else:
                final_price = rrc_price or threshold_price or mrc_price
            final_source = "attractiveness"
            final_boost = _first_safe_boost_step_for_price(
                price=final_price,
                calc_ctx=calc_ctx,
                boost_metric=boost_metric,
                min_profit_pct=min_safe_pct,
            )
            decision_label = "Усилить продажи" if final_boost > 0.01 else "Наблюдать"
            tone = "warning" if final_boost > 0.01 else "positive"
            hypothesis = "Промо нет, поэтому работаем от ценового порога привлекательности и безопасного буста, пока не изменятся пороги или статус промо."
            control_state = "stable"
        else:
            final_source = "mrc"
            final_price = mrc_price or no_promo_threshold_price or rrc_price
            final_boost = 0.0
            decision_label = "Снизить цену" if fact_sales == 0 else "Наблюдать"
            tone = "warning"
            hypothesis = "Ни промо, ни выгодной цены сейчас нет. Возвращаемся к МРЦ и ждём новое изменение порогов."
            control_state = "price_test" if fact_sales == 0 else "stable"

    final_profit_abs, final_profit_pct = _profit_for_price_and_boost(
        price=final_price,
        boost_pct=final_boost,
        calc_ctx=calc_ctx,
    )
    state_changed = not (
        previous_state == control_state
        and _same_num(previous_installed_price, final_price)
        and _same_num(previous_installed_boost, final_boost, tol=0.01)
    )
    strategy_started_at_dt = now_msk if state_changed or previous_control_state_started_at is None else previous_control_state_started_at
    hypothesis_started_at_out = strategy_started_at_dt.isoformat()
    hypothesis_expires_at_out = (
        strategy_started_at_dt + timedelta(minutes=_strategy_window_minutes(control_state))
    ).isoformat()
    store_code = (
        "promo+boost"
        if final_source == "promo" and (final_boost or 0.0) > 0.01
        else "promo"
        if final_source == "promo"
        else "attractiveness+boost"
        if final_source == "attractiveness" and (final_boost or 0.0) > 0.01
        else "attractiveness"
        if final_source == "attractiveness"
        else "base"
    )
    return {
        "final_source": final_source,
        "final_price": final_price,
        "final_boost": final_boost,
        "final_profit_abs": final_profit_abs,
        "final_profit_pct": final_profit_pct,
        "decision_label": decision_label,
        "tone": tone,
        "hypothesis": hypothesis,
        "control_state": control_state,
        "hypothesis_started_at_out": hypothesis_started_at_out,
        "hypothesis_expires_at_out": hypothesis_expires_at_out,
        "control_state_started_at": hypothesis_started_at_out,
        "store_code": store_code,
    }


def _build_strategy_day_plan_context(
    *,
    suid: str,
    store_totals: dict[str, Any],
    today: date,
) -> dict[str, Any]:
    totals = (store_totals.get(suid) or {}) if isinstance(store_totals, dict) else {}
    plan_revenue = _to_num(totals.get("planned_revenue"))
    plan_profit = _to_num(totals.get("target_profit_rub"))
    today_revenue = _to_num(totals.get("today_revenue"))
    today_profit = _to_num(totals.get("today_profit"))
    month_revenue_before_today = _to_num(totals.get("month_revenue_before_today"))
    month_profit_before_today = _to_num(totals.get("month_profit_before_today"))
    minimum_profit_pct = _to_num(totals.get("minimum_profit_percent"))
    weighted_day_profit_pct = (
        (float(today_profit) / float(today_revenue)) * 100.0
        if today_revenue not in (None, 0) and today_profit is not None
        else None
    )
    day_plan = _build_profit_first_daily_plan(
        plan_revenue_total=plan_revenue,
        plan_profit_total=plan_profit,
        month_revenue_before_today=month_revenue_before_today,
        month_profit_before_today=month_profit_before_today,
        minimum_profit_percent=minimum_profit_pct,
        weighted_day_profit_pct=weighted_day_profit_pct,
        calc_date=today,
    )
    daily_plan_revenue = _to_num(day_plan.get("planned_revenue_daily"))
    daily_plan_profit = _to_num(day_plan.get("planned_profit_daily"))
    revenue_plan_pct = (
        (float(today_revenue) / float(daily_plan_revenue)) * 100.0
        if daily_plan_revenue not in (None, 0) and today_revenue is not None
        else None
    )
    profit_plan_pct = (
        (float(today_profit) / float(daily_plan_profit)) * 100.0
        if daily_plan_profit not in (None, 0) and today_profit is not None
        else None
    )
    min_safe_pct = float(minimum_profit_pct or 3.0)
    experimental_floor_pct = float(min_safe_pct)
    snapshot = {
        "store_uid": suid,
        "plan_date": today.isoformat(),
        "planned_revenue_daily": daily_plan_revenue,
        "planned_profit_daily": daily_plan_profit,
        "today_revenue": today_revenue,
        "today_profit": today_profit,
        "weighted_day_profit_pct": weighted_day_profit_pct,
        "minimum_profit_percent": minimum_profit_pct,
        "experimental_floor_pct": experimental_floor_pct,
    }
    return {
        "daily_plan_revenue": daily_plan_revenue,
        "daily_plan_profit": daily_plan_profit,
        "revenue_plan_pct": revenue_plan_pct,
        "profit_plan_pct": profit_plan_pct,
        "minimum_profit_pct": minimum_profit_pct,
        "min_safe_pct": min_safe_pct,
        "experimental_floor_pct": experimental_floor_pct,
        "snapshot": snapshot,
    }


def _forecast_strategy_sales(
    *,
    forecast_sales_raw: int,
    elasticity: float | None,
    final_price: float | None,
    current_week_avg_price: float | None,
    fact_sales: int,
    week_avg_daily: float,
    month_avg_daily: float,
    has_historical_demand: bool,
    has_stock: bool,
) -> int:
    forecast_sales = int(forecast_sales_raw or 0)
    if elasticity is not None and final_price not in (None, 0) and current_week_avg_price not in (None, 0):
        elasticity_effect = max(-6.0, min(6.0, float(elasticity)))
        base_qty = max(float(fact_sales), float(week_avg_daily), float(month_avg_daily))
        if base_qty > 0:
            price_delta = (float(final_price) - float(current_week_avg_price)) / float(current_week_avg_price)
            demand_factor = max(0.1, min(3.0, 1.0 + elasticity_effect * price_delta))
            forecast_sales = int(round(base_qty * demand_factor))
            if has_historical_demand:
                forecast_sales = max(forecast_sales, 1)
    elif has_historical_demand:
        forecast_sales = max(int(round(max(week_avg_daily, month_avg_daily))), 1)
    if not has_stock:
        return 0
    return forecast_sales


def _build_strategy_coinvest_metrics(
    *,
    installed_price_view: float | None,
    installed_profit_abs_view: float | None,
    installed_boost_view: float | None,
    elasticity: float | None,
    forecast_sales: int,
    sales_metric: dict[str, Any],
    goods_price_metric: dict[str, Any],
    promo_offers: list[dict[str, Any]],
    promo_coinvest_settings: dict[str, Any],
    store_currency_code: str,
    goods_report_currency: str,
    today: date,
) -> dict[str, Any]:
    forecast_profit_abs = None
    if installed_profit_abs_view is not None:
        forecast_profit_abs = round(float(installed_profit_abs_view) * float(forecast_sales or 0), 2)
    coinvest_pct = None
    on_display_price = _to_num(goods_price_metric.get("on_display_price"))
    installed_price_for_report_currency = _convert_price_between_currencies(
        installed_price_view,
        from_currency=store_currency_code,
        to_currency=goods_report_currency,
        calc_date=today,
    )
    if installed_price_for_report_currency not in (None, 0) and on_display_price not in (None, 0):
        try:
            implied_coinvest_pct = (
                1.0 - (float(on_display_price) / float(installed_price_for_report_currency))
            ) * 100.0
        except Exception:
            implied_coinvest_pct = None
        if implied_coinvest_pct is not None:
            coinvest_pct = round(float(implied_coinvest_pct), 2)
    if coinvest_pct in (None, 0):
        coinvest_pct = _to_num(sales_metric.get("coinvest_pct"))
    matched_promos = _promo_matches_installed_price(installed_price=installed_price_view, offers=promo_offers)
    promo_extra_discount_percent = None
    if matched_promos:
        promo_discount_values = [
            value
            for value in (
                _to_num((promo_coinvest_settings.get(str(offer.get("promo_id") or "").strip()) or {}).get("max_discount_percent"))
                for offer in matched_promos
            )
            if value is not None
        ]
        promo_extra_discount_percent = max(promo_discount_values) if promo_discount_values else None
    total_coinvest_pct = coinvest_pct
    if promo_extra_discount_percent not in (None, 0):
        total_coinvest_pct = round(float(total_coinvest_pct or 0.0) + float(promo_extra_discount_percent), 2)
    planned_price_with_coinvest = None
    if installed_price_view is not None:
        planned_price_with_coinvest = float(installed_price_view)
        if installed_price_for_report_currency not in (None, 0) and total_coinvest_pct not in (None, 0):
            planned_price_with_coinvest = round(
                float(installed_price_for_report_currency) * (1.0 - (float(total_coinvest_pct) / 100.0)),
                2,
            )
        elif on_display_price not in (None, 0):
            planned_price_with_coinvest = float(on_display_price)
        elif coinvest_pct not in (None, 0):
            planned_price_with_coinvest = round(
                float(installed_price_view) * (1.0 - (float(coinvest_pct) / 100.0)),
                2,
            )
    sku_sales_plan_qty = 0
    yesterday_sales_qty = int(sales_metric.get("yesterday_sales") or 0)
    yesterday_avg_buyer_price = _to_num(sales_metric.get("yesterday_avg_buyer_price"))
    if yesterday_sales_qty > 0:
        sku_sales_plan_qty = yesterday_sales_qty
        if elasticity is not None and planned_price_with_coinvest not in (None, 0) and yesterday_avg_buyer_price not in (None, 0):
            elasticity_effect = max(-6.0, min(6.0, float(elasticity)))
            price_delta = (float(planned_price_with_coinvest) - float(yesterday_avg_buyer_price)) / float(yesterday_avg_buyer_price)
            demand_factor = max(0.1, min(3.0, 1.0 + elasticity_effect * price_delta))
            sku_sales_plan_qty = max(0, int(round(float(yesterday_sales_qty) * demand_factor)))
    elif int(forecast_sales or 0) > 0:
        # If yesterday had no sales, fall back to the current forecast instead of zeroing the SKU plan.
        sku_sales_plan_qty = max(0, int(forecast_sales or 0))
    sku_sales_plan_revenue = None
    if installed_price_view not in (None, 0) and sku_sales_plan_qty > 0:
        sku_sales_plan_revenue = round(float(installed_price_view) * float(sku_sales_plan_qty), 2)
    resolved_boost_pct = _to_num(installed_boost_view)
    if resolved_boost_pct is None or abs(float(resolved_boost_pct)) < 0.01:
        resolved_boost_pct = 0.0
    return {
        "resolved_boost_pct": resolved_boost_pct,
        "forecast_profit_abs": forecast_profit_abs,
        "coinvest_pct": total_coinvest_pct,
        "planned_price_with_coinvest": planned_price_with_coinvest,
        "on_display_price": on_display_price,
        "sku_sales_plan_qty": int(sku_sales_plan_qty or 0),
        "sku_sales_plan_revenue": sku_sales_plan_revenue,
    }


def _promo_supports_price(offers: list[dict[str, Any]], price: float | None) -> bool:
    if price is None:
        return False
    for offer in offers:
        offer_price = _to_num(offer.get("promo_price"))
        if offer_price is not None and offer_price >= float(price):
            return True
    return False


def _lowest_safe_candidate(candidates: list[dict[str, Any]], *, min_profit_pct: float = 3.0) -> dict[str, Any] | None:
    threshold = float(min_profit_pct) - 0.05
    usable = [
        item
        for item in candidates
        if item.get("price") is not None and (_to_num(item.get("profit_pct")) or -999999.0) >= threshold
    ]
    if not usable:
        return None
    usable.sort(
        key=lambda item: (
            float(_to_num(item.get("price")) or 0.0),
            0 if str(item.get("source") or "") == "promo" else 1 if str(item.get("source") or "") == "attractiveness" else 2,
        )
    )
    return usable[0]


def _lowest_candidate_meeting_pct(candidates: list[dict[str, Any]], *, target_pct: float | None) -> dict[str, Any] | None:
    threshold = float(target_pct or 0.0) - 0.05
    usable = [
        item
        for item in candidates
        if item.get("price") is not None and (_to_num(item.get("profit_pct")) or -999999.0) >= threshold
    ]
    if not usable:
        return None
    usable.sort(
        key=lambda item: (
            float(_to_num(item.get("price")) or 0.0),
            0 if str(item.get("source") or "") == "promo" else 1 if str(item.get("source") or "") == "attractiveness" else 2,
        )
    )
    return usable[0]


def _highest_candidate_meeting_pct(candidates: list[dict[str, Any]], *, target_pct: float | None) -> dict[str, Any] | None:
    threshold = float(target_pct or 0.0) - 0.05
    usable = [
        item
        for item in candidates
        if item.get("price") is not None and (_to_num(item.get("profit_pct")) or -999999.0) >= threshold
    ]
    if not usable:
        return None
    usable.sort(
        key=lambda item: (
            -float(_to_num(item.get("price")) or 0.0),
            -float(_to_num(item.get("profit_pct")) or -999999.0),
        )
    )
    return usable[0]


def _highest_safe_candidate(candidates: list[dict[str, Any]], *, min_profit_pct: float = 3.0) -> dict[str, Any] | None:
    threshold = float(min_profit_pct) - 0.05
    usable = [
        item
        for item in candidates
        if item.get("price") is not None and (_to_num(item.get("profit_pct")) or -999999.0) >= threshold
    ]
    if not usable:
        return None
    usable.sort(
        key=lambda item: (
            -float(_to_num(item.get("price")) or 0.0),
            -float(_to_num(item.get("profit_pct")) or -999999.0),
        )
    )
    return usable[0]


def _meets_floor_pct(profit_pct: float | None, floor_pct: float | None, *, tolerance: float = 0.05) -> bool:
    if floor_pct is None:
        return True
    if profit_pct is None:
        return False
    return float(profit_pct) >= (float(floor_pct) - float(tolerance))


def _meets_target_pct(target_pct: float | None, profit_pct: float | None) -> bool:
    if target_pct is None:
        return (profit_pct or 0.0) >= 0.0
    if profit_pct is None:
        return False
    return float(profit_pct) >= float(target_pct)


def _meets_target(*, target_pct: float | None, target_abs: float | None, profit_pct: float | None, profit_abs: float | None) -> bool:
    if target_pct is not None and target_pct > 0:
        return profit_pct is not None and float(profit_pct) >= float(target_pct)
    if target_abs is not None and target_abs > 0:
        return profit_abs is not None and float(profit_abs) >= float(target_abs)
    return (profit_pct or 0.0) >= 0.0


def _max_ads_rate_for_price(
    *,
    price: float | None,
    calc_ctx: dict[str, Any] | None,
    target_pct: float | None,
    target_abs: float | None,
    max_ads_rate_pct: float | None,
) -> tuple[float | None, float | None, float | None]:
    if price is None or not isinstance(calc_ctx, dict):
        return None, None, None
    hi_pct = max(0.0, float(max_ads_rate_pct or 0.0))
    pa0, pp0 = _profit_for_price_with_ads_rate(price=float(price), calc_ctx=calc_ctx, ads_rate_override=0.0)
    if not _meets_target(target_pct=target_pct, target_abs=target_abs, profit_pct=pp0, profit_abs=pa0):
        return None, None, None
    if hi_pct <= 0:
        return 0.0, pa0, pp0
    hi_rate = hi_pct / 100.0
    pah, pph = _profit_for_price_with_ads_rate(price=float(price), calc_ctx=calc_ctx, ads_rate_override=hi_rate)
    if _meets_target(target_pct=target_pct, target_abs=target_abs, profit_pct=pph, profit_abs=pah):
        return hi_pct, pah, pph
    lo = 0.0
    best_rate = 0.0
    best_abs = pa0
    best_pct = pp0
    for _ in range(40):
        mid = (lo + hi_rate) / 2.0
        pa, pp = _profit_for_price_with_ads_rate(price=float(price), calc_ctx=calc_ctx, ads_rate_override=mid)
        if _meets_target(target_pct=target_pct, target_abs=target_abs, profit_pct=pp, profit_abs=pa):
            best_rate = mid * 100.0
            best_abs = pa
            best_pct = pp
            lo = mid
        else:
            hi_rate = mid
    return round(best_rate, 2), best_abs, best_pct


def _find_min_price_for_zone(
    *,
    calc_ctx: dict[str, Any] | None,
    target_pct: float | None,
    target_abs: float | None,
    max_ads_rate_pct: float | None,
    low_price: float | None,
    high_price: float | None,
) -> tuple[float | None, float | None, float | None, float | None]:
    if not isinstance(calc_ctx, dict):
        return None, None, None, None
    if low_price is None or high_price is None:
        return None, None, None, None
    lo = max(1.0, float(low_price))
    hi = max(lo, float(high_price))
    best_price: float | None = None
    best_bid: float | None = None
    best_abs: float | None = None
    best_pct: float | None = None
    for _ in range(50):
        mid = float(int(round((lo + hi) / 2.0)))
        bid_pct, pa, pp = _max_ads_rate_for_price(
            price=mid,
            calc_ctx=calc_ctx,
            target_pct=target_pct,
            target_abs=target_abs,
            max_ads_rate_pct=max_ads_rate_pct,
        )
        if bid_pct is not None:
            best_price = mid
            best_bid = bid_pct
            best_abs = pa
            best_pct = pp
            hi = max(lo, mid - 1.0)
        else:
            lo = mid + 1.0
    return best_price, best_bid, best_abs, best_pct


def _build_promo_items(offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    label_map = {
        "with_ads": "Проходит",
        "without_ads": "Проходит без ДРР",
        "rejected": "Не проходит",
    }
    for offer in offers:
        name = str(offer.get("promo_name") or offer.get("promo_id") or "").strip()
        mode = str(offer.get("promo_fit_mode") or "rejected").strip() or "rejected"
        key = (name, mode)
        if not name or key in seen:
            continue
        seen.add(key)
        out.append({"name": name, "status": label_map.get(mode, "Не проходит")})
    return out


def _pick_best_promo_offer(
    offers: list[dict[str, Any]],
    *,
    target_profit_pct: float | None,
) -> tuple[dict[str, Any] | None, bool]:
    valid = [o for o in offers if isinstance(o, dict)]
    if not valid:
        return None, False
    valid.sort(key=lambda x: (_to_num(x.get("promo_price")) is None, _to_num(x.get("promo_price")) or 0.0))

    passing: list[tuple[dict[str, Any], bool]] = []
    for offer in valid:
        mode = str(offer.get("promo_fit_mode") or "rejected")
        if mode == "with_ads":
            ok = _meets_target_pct(target_profit_pct, _to_num(offer.get("promo_profit_pct")))
            if ok:
                passing.append((offer, True))
        elif mode == "without_ads":
            ok = _meets_target_pct(target_profit_pct, _to_num(offer.get("promo_profit_pct")))
            if ok:
                passing.append((offer, False))
    if passing:
        passing.sort(key=lambda pair: (_to_num(pair[0].get("promo_price")) is None, _to_num(pair[0].get("promo_price")) or 0.0))
        return passing[0]
    return None, False


def _pick_best_promo_offer_for_floor(
    offers: list[dict[str, Any]],
    *,
    min_profit_pct: float | None,
) -> tuple[dict[str, Any] | None, bool]:
    valid = [o for o in offers if isinstance(o, dict) and _to_num(o.get("promo_price")) is not None]
    if not valid:
        return None, False
    floor_pct = float(min_profit_pct or 0.0) - 0.05
    passing: list[tuple[dict[str, Any], bool]] = []
    for offer in valid:
        profit_pct = _to_num(offer.get("promo_profit_pct"))
        if profit_pct is None or float(profit_pct) < floor_pct:
            continue
        mode = str(offer.get("promo_fit_mode") or "rejected").strip().lower()
        uses_ads = mode == "with_ads"
        passing.append((offer, uses_ads))
    if not passing:
        return None, False
    passing.sort(
        key=lambda pair: (
            _to_num(pair[0].get("promo_price")) is None,
            _to_num(pair[0].get("promo_price")) or 0.0,
            str(pair[0].get("promo_name") or ""),
        )
    )
    chosen, uses_ads = passing[0]
    return chosen, uses_ads


def _selected_offer_by_price(offers: list[dict[str, Any]], selected_price: float | None) -> dict[str, Any] | None:
    if selected_price is None:
        return None
    matched = [
        offer for offer in offers
        if _to_num(offer.get("promo_price")) is not None and float(selected_price) <= float(_to_num(offer.get("promo_price")) or 0.0) + 1.01
    ]
    if not matched:
        return None
    matched.sort(key=lambda offer: (0 if str(offer.get("promo_fit_mode") or "").strip().lower() in {"with_ads", "without_ads"} else 1, str(offer.get("promo_name") or "")))
    return matched[0]


def _target_price_profit_pct(row: dict[str, Any]) -> float | None:
    metrics = row.get("price_metrics_by_store") if isinstance(row.get("price_metrics_by_store"), dict) else {}
    return _to_num(metrics.get("target_profit_pct"))


def _recalculate_for_price_with_drr(price_row: dict[str, Any], price: float | None, drr_percent: float) -> tuple[float | None, float | None]:
    calc_ctx = price_row.get("calc_ctx") if isinstance(price_row.get("calc_ctx"), dict) else None
    if price is None or not isinstance(calc_ctx, dict):
        return None, None
    return _profit_for_price_with_ads_rate(price=price, calc_ctx=calc_ctx, ads_rate_override=max(0.0, float(drr_percent or 0.0)) / 100.0)


def _find_min_price_for_fixed_boost_target(
    *,
    calc_ctx: dict[str, Any] | None,
    boost_pct: float | None,
    target_pct: float | None,
    target_abs: float | None,
    low_price: float | None,
    high_price: float | None,
) -> tuple[float | None, float | None, float | None]:
    if not isinstance(calc_ctx, dict):
        return None, None, None
    if low_price is None or high_price is None:
        return None, None, None
    boost = max(0.0, float(boost_pct or 0.0))
    lo = max(1.0, float(low_price))
    hi = max(lo, float(high_price))
    best_price: float | None = None
    best_abs: float | None = None
    best_pct: float | None = None
    for _ in range(50):
        mid = float(int(round((lo + hi) / 2.0)))
        profit_abs, profit_pct = _profit_for_price_and_boost(
            price=mid,
            boost_pct=boost,
            calc_ctx=calc_ctx,
        )
        if _meets_target(target_pct=target_pct, target_abs=target_abs, profit_pct=profit_pct, profit_abs=profit_abs):
            best_price = mid
            best_abs = profit_abs
            best_pct = profit_pct
            hi = max(lo, mid - 1.0)
        else:
            lo = mid + 1.0
    return best_price, best_abs, best_pct


_ITERATION_CONFIGS: list[tuple[str, str]] = [
    ("mrc", "МРЦ"),
    ("mrc_with_boost", "МРЦ + буст"),
]


def _strategy_iteration_configs_for_mode(strategy_mode: str | None) -> list[tuple[str, str]]:
    mode = str(strategy_mode or "").strip().lower()
    if mode == "mrc":
        return [("mrc", "МРЦ")]
    return list(_ITERATION_CONFIGS)


def _iteration_rank(iteration_code: str) -> int:
    mapping = {"rrc_with_boost": 0, "rrc_no_ads": 1, "mrc_with_boost": 2, "mrc": 3}
    return mapping.get(str(iteration_code or "").strip(), 99)


def _attr_rank(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized == "profitable":
        return 2
    if normalized == "moderate":
        return 1
    return 0


def _iteration_target_from_metric(
    *,
    iteration_code: str,
    price_metric: dict[str, Any] | None,
    boost_metric: dict[str, Any] | None = None,
) -> dict[str, float | None]:
    metric = price_metric if isinstance(price_metric, dict) else {}
    calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else {}
    target_drr_pct = _resolve_internal_economy_boost_pct(
        boost_metric=boost_metric,
        fallback_bid_pct=_resolve_target_drr_pct(metric),
    )
    if iteration_code == "rrc_with_boost":
        tested_price = _to_num(metric.get("target_price"))
        profit_abs = _to_num(metric.get("target_profit_abs"))
        profit_pct = _to_num(metric.get("target_profit_pct"))
        mrc_price = _to_num(metric.get("mrc_price"))
        if (
            isinstance(calc_ctx, dict)
            and target_drr_pct > 0.01
            and tested_price not in (None, 0)
        ):
            recomputed_price, recomputed_abs, recomputed_pct = _find_min_price_for_fixed_boost_target(
                calc_ctx=calc_ctx,
                boost_pct=target_drr_pct,
                target_pct=_to_num(metric.get("target_profit_pct")),
                target_abs=_to_num(metric.get("target_profit_abs")),
                low_price=mrc_price or tested_price,
                high_price=tested_price,
            )
            if recomputed_price not in (None, 0):
                tested_price = recomputed_price
                profit_abs = recomputed_abs
                profit_pct = recomputed_pct
            else:
                tested_price = None
                profit_abs = None
                profit_pct = None
        return {
            "tested_price": tested_price,
            "tested_boost_pct": target_drr_pct,
            "profit_abs": profit_abs,
            "profit_pct": profit_pct,
        }
    if iteration_code == "mrc_with_boost":
        tested_price = _to_num(metric.get("mrc_with_boost_price"))
        profit_abs = _to_num(metric.get("mrc_with_boost_profit_abs"))
        profit_pct = _to_num(metric.get("mrc_with_boost_profit_pct"))
        mrc_price = _to_num(metric.get("mrc_price"))
        mrc_profit_abs = _to_num(metric.get("mrc_profit_abs"))
        mrc_profit_pct = _to_num(metric.get("mrc_profit_pct"))
        target_price = _to_num(metric.get("target_price"))
        if (
            isinstance(calc_ctx, dict)
            and target_drr_pct > 0.01
            and mrc_price not in (None, 0)
        ):
            recomputed_price, recomputed_abs, recomputed_pct = _find_min_price_for_fixed_boost_target(
                calc_ctx=calc_ctx,
                boost_pct=target_drr_pct,
                target_pct=mrc_profit_pct,
                target_abs=mrc_profit_abs,
                low_price=mrc_price,
                high_price=target_price or tested_price or mrc_price,
            )
            if recomputed_price not in (None, 0):
                tested_price = recomputed_price
                profit_abs = recomputed_abs
                profit_pct = recomputed_pct
            else:
                tested_price = None
                profit_abs = None
                profit_pct = None
        return {
            "tested_price": tested_price,
            "tested_boost_pct": target_drr_pct,
            "profit_abs": profit_abs,
            "profit_pct": profit_pct,
        }
    if iteration_code == "rrc_no_ads":
        return {
            "tested_price": _to_num(metric.get("rrc_no_ads_price")),
            "tested_boost_pct": 0.0,
            "profit_abs": _to_num(metric.get("rrc_no_ads_profit_abs")),
            "profit_pct": _to_num(metric.get("rrc_no_ads_profit_pct")),
        }
    return {
        "tested_price": _to_num(metric.get("mrc_price")),
        "tested_boost_pct": 0.0,
        "profit_abs": _to_num(metric.get("mrc_profit_abs")),
        "profit_pct": _to_num(metric.get("mrc_profit_pct")),
    }


def _count_promo_hits_for_price(*, tested_price: float | None, promo_offers_raw: list[dict[str, Any]]) -> int:
    if tested_price in (None, 0):
        return 0
    seen: set[str] = set()
    total = 0
    for offer in promo_offers_raw:
        if not isinstance(offer, dict):
            continue
        promo_id = str(offer.get("promo_id") or "").strip()
        if not promo_id or promo_id in seen:
            continue
        payload = offer.get("payload") if isinstance(offer.get("payload"), dict) else {}
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        discount = params.get("discountParams") if isinstance(params.get("discountParams"), dict) else {}
        threshold = _to_num(discount.get("maxPromoPrice")) or _to_num(offer.get("promo_price"))
        if threshold not in (None, 0) and float(tested_price) <= float(threshold) + 1.0:
            seen.add(promo_id)
            total += 1
    return total


def _scenario_sort_key(snapshot: dict[str, Any]) -> tuple[int, int, int]:
    return (
        -min(2, max(0, int(snapshot.get("promo_count") or 0))),
        -_attr_rank(str(snapshot.get("attractiveness_status") or "")),
        _iteration_rank(str(snapshot.get("iteration_code") or "")),
    )


def _build_strategy_iteration_snapshot(
    *,
    store_uid: str,
    sku: str,
    cycle_started_at: str,
    iteration_code: str,
    iteration_label: str,
    tested_price: float | None,
    tested_boost_pct: float | None,
    boost_metric: dict[str, Any] | None,
    attr_metric: dict[str, Any] | None,
    promo_offers: list[dict[str, Any]],
    promo_offers_raw: list[dict[str, Any]],
    base_promo_price: float | None,
    goods_price_metric: dict[str, Any] | None,
    source_updated_at: str | None,
) -> dict[str, Any]:
    attr_status = _resolve_attr_status_for_price(
        price=tested_price,
        attr_metric=attr_metric if isinstance(attr_metric, dict) else {},
        platform="yandex_market",
    )
    promo_check_price = base_promo_price if base_promo_price not in (None, 0) else tested_price
    promo_count = _count_promo_hits_for_price(tested_price=promo_check_price, promo_offers_raw=promo_offers_raw)
    _, promo_details = build_market_promo_details(
        promo_offers=promo_offers,
        promo_offers_raw=promo_offers_raw,
        installed_price_view=promo_check_price,
        effective_market_promo_status="",
        effective_market_promo_message="",
        promo_candidate_ready=True,
    )
    on_display_price = _to_num((goods_price_metric or {}).get("on_display_price"))
    coinvest_pct = None
    if tested_price not in (None, 0) and on_display_price not in (None, 0):
        try:
            coinvest_pct = round(max(0.0, (1.0 - (float(on_display_price) / float(tested_price))) * 100.0), 2)
        except Exception:
            coinvest_pct = None
    market_boost_bid_percent = _resolve_market_export_boost_pct(
        boost_bid_pct=tested_boost_pct,
        boost_metric=boost_metric if isinstance(boost_metric, dict) else {},
    )
    boost_share = _resolve_boost_share(
        boost_bid_pct=market_boost_bid_percent,
        boost_metric=boost_metric if isinstance(boost_metric, dict) else {},
    )
    return {
        "store_uid": store_uid,
        "sku": sku,
        "cycle_started_at": cycle_started_at,
        "iteration_code": iteration_code,
        "iteration_label": iteration_label,
        "tested_price": tested_price,
        "tested_boost_pct": tested_boost_pct,
        "market_boost_bid_percent": market_boost_bid_percent,
        "boost_share": boost_share,
        "promo_count": promo_count,
        "attractiveness_status": (
            "profitable" if attr_status == "profitable" else
            "moderate" if attr_status == "moderate" else
            "unknown" if attr_status == "unknown" else
            "overpriced"
        ),
        "coinvest_pct": coinvest_pct,
        "on_display_price": on_display_price,
        "promo_details": promo_details,
        "market_promo_status": "",
        "market_promo_message": "",
        "source_updated_at": source_updated_at,
        "captured_at": datetime.now(MSK).astimezone().isoformat(),
    }


def _fallback_rrc_no_ads_strategy(
    *,
    price_metric: dict[str, Any] | None,
    boost_metric: dict[str, Any] | None,
) -> tuple[float | None, float, float | None, float | None]:
    metric = price_metric if isinstance(price_metric, dict) else {}
    price = _to_num(metric.get("rrc_no_ads_price"))
    profit_abs = _to_num(metric.get("rrc_no_ads_profit_abs"))
    profit_pct = _to_num(metric.get("rrc_no_ads_profit_pct"))
    _ = boost_metric
    return price, 0.0, profit_abs, profit_pct


def _matrix_decision_meta(
    *,
    promo_count: int,
    attr_rank: int,
    uses_boost: bool,
) -> tuple[str, str, str, str]:
    if promo_count >= 2 and attr_rank >= 2 and uses_boost:
        return "2 промо + выгодно + буст", "positive", "promo2_profitable_boost", "Выгодная"
    if promo_count >= 2 and attr_rank >= 2:
        return "2 промо + выгодно", "positive", "promo2_profitable", "Выгодная"
    if promo_count == 1 and attr_rank >= 2 and uses_boost:
        return "1 промо + выгодно + буст", "positive", "promo1_profitable_boost", "Выгодная"
    if promo_count == 1 and attr_rank >= 2:
        return "1 промо + выгодно", "positive", "promo1_profitable", "Выгодная"
    if promo_count >= 2 and attr_rank >= 1 and uses_boost:
        return "2 промо + умеренно + буст", "warning", "promo2_moderate_boost", "Умеренная"
    if promo_count >= 2 and attr_rank >= 1:
        return "2 промо + умеренно", "warning", "promo2_moderate", "Умеренная"
    if promo_count == 1 and attr_rank >= 1 and uses_boost:
        return "1 промо + умеренно + буст", "warning", "promo1_moderate_boost", "Умеренная"
    if promo_count == 1 and attr_rank >= 1:
        return "1 промо + умеренно", "warning", "promo1_moderate", "Умеренная"
    if uses_boost and attr_rank >= 2:
        return "Выгодная цена + буст", "positive", "profitable_boost", "Выгодная"
    if attr_rank >= 2:
        return "Выгодная цена", "positive", "profitable", "Выгодная"
    if uses_boost and attr_rank >= 1:
        return "Умеренная цена + буст", "warning", "moderate_boost", "Умеренная"
    if attr_rank >= 1:
        return "Умеренная цена", "warning", "moderate", "Умеренная"
    return "Невыгодная цена", "warning", "overpriced_fallback", "Завышенная"


def _pick_lowest_qualifying_promo_offer_for_mrc(
    *,
    mrc_price: float | None,
    promo_offers: Sequence[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if mrc_price in (None, 0):
        return None
    candidates = [
        offer for offer in (promo_offers or [])
        if isinstance(offer, dict)
        and _to_num(offer.get("promo_price")) not in (None, 0)
        and float(_to_num(offer.get("promo_price")) or 0.0) >= float(mrc_price) - 0.5
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda offer: (
            _to_num(offer.get("promo_price")) is None,
            _to_num(offer.get("promo_price")) or 0.0,
            str(offer.get("promo_name") or offer.get("promo_id") or ""),
        )
    )
    return candidates[0]


def _decision_from_selected_scenario(
    *,
    selected: dict[str, Any] | None,
    price_metric: dict[str, Any] | None,
    boost_metric: dict[str, Any] | None = None,
    attr_metric: dict[str, Any] | None = None,
    promo_offers: Sequence[dict[str, Any]] | None = None,
    promo_offers_raw: Sequence[dict[str, Any]] | None = None,
    platform: str = "yandex_market",
) -> dict[str, Any]:
    metric = price_metric if isinstance(price_metric, dict) else {}
    calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else {}
    mrc_price = _to_num(metric.get("mrc_price"))
    mrc_profit_abs = _to_num(metric.get("mrc_profit_abs"))
    mrc_profit_pct = _to_num(metric.get("mrc_profit_pct"))
    promo_offer = _pick_lowest_qualifying_promo_offer_for_mrc(
        mrc_price=mrc_price,
        promo_offers=promo_offers,
    )
    if promo_offer is not None:
        promo_price = _to_num(promo_offer.get("promo_price"))
        max_boost_pct = None
        boosted_profit_abs = None
        boosted_profit_pct = None
        if promo_price not in (None, 0) and isinstance(calc_ctx, dict):
            max_boost_pct, boosted_profit_abs, boosted_profit_pct = _max_ads_rate_for_price(
                price=promo_price,
                calc_ctx=calc_ctx,
                target_pct=mrc_profit_pct,
                target_abs=mrc_profit_abs,
                max_ads_rate_pct=100.0,
            )
        final_boost = round(float(max_boost_pct or 0.0), 2) if (max_boost_pct or 0.0) > 0.5 else 0.0
        if final_boost > 0.5:
            final_profit_abs = boosted_profit_abs
            final_profit_pct = boosted_profit_pct
        else:
            final_profit_abs, final_profit_pct = _profit_for_price_and_boost(
                price=promo_price,
                boost_pct=0.0,
                calc_ctx=calc_ctx,
            )
        promo_count = _count_promo_hits_for_price(
            tested_price=promo_price,
            promo_offers_raw=list(promo_offers_raw or []),
        )
        attr_code = _resolve_attr_status_for_price(
            price=promo_price,
            attr_metric=attr_metric if isinstance(attr_metric, dict) else {},
            platform=platform,
        )
        attr_rank = _attr_rank(attr_code)
        decision_label, decision_tone, selection_reason, final_attr_status = _matrix_decision_meta(
            promo_count=promo_count,
            attr_rank=attr_rank,
            uses_boost=final_boost > 0.5,
        )
        return {
            "selected_iteration_code": "promo_with_boost" if final_boost > 0.5 else "promo",
            "final_price": promo_price,
            "final_boost": final_boost,
            "final_profit_abs": final_profit_abs,
            "final_profit_pct": final_profit_pct,
            "decision_label": decision_label,
            "decision_tone": decision_tone,
            "control_state": "stable",
            "hypothesis": "Товар проходит в промо от МРЦ. Базой ставим нижний проходной порог промо, а привлекательность оцениваем уже от этой цены.",
            "uses_promo": promo_count >= 1,
            "uses_boost": final_boost > 0.5,
            "final_attr_status": final_attr_status,
            "selection_reason": f"promo_threshold_from_mrc:{selection_reason}",
            "promo_count": promo_count,
        }
    if not isinstance(selected, dict):
        return {
            "selected_iteration_code": "mrc_with_boost",
            "final_price": _to_num(metric.get("mrc_with_boost_price")) or _to_num(metric.get("mrc_price")),
            "final_boost": _resolve_internal_economy_boost_pct(
                boost_metric=boost_metric,
                fallback_bid_pct=_resolve_target_drr_pct(metric),
            ),
            "final_profit_abs": _to_num(metric.get("mrc_with_boost_profit_abs")) or _to_num(metric.get("mrc_profit_abs")),
            "final_profit_pct": _to_num(metric.get("mrc_with_boost_profit_pct")) or _to_num(metric.get("mrc_profit_pct")),
            "decision_label": "МРЦ + буст",
            "decision_tone": "warning",
            "control_state": "stable",
            "hypothesis": "Для товара нет порогов привлекательности и нет промо-сигнала. Держим сценарий МРЦ + буст до следующего полного цикла.",
            "uses_promo": False,
            "uses_boost": True,
            "final_attr_status": "",
            "selection_reason": "fallback_mrc_with_boost_no_thresholds",
            "promo_count": 0,
        }

    selected_iteration_code = str(selected.get("iteration_code") or "").strip()
    target_meta = _iteration_target_from_metric(
        iteration_code=selected_iteration_code,
        price_metric=metric,
        boost_metric=boost_metric,
    )
    final_price = _to_num(selected.get("tested_price"))
    final_boost = _to_num(selected.get("tested_boost_pct")) or 0.0
    final_profit_abs = _to_num(target_meta.get("profit_abs"))
    final_profit_pct = _to_num(target_meta.get("profit_pct"))
    if selected_iteration_code == "rrc_with_boost" and final_boost > 0.01:
        calc_ctx = metric.get("calc_ctx") if isinstance(metric.get("calc_ctx"), dict) else {}
        lowered_price, lowered_profit_abs, lowered_profit_pct = _find_min_price_for_fixed_boost_target(
            calc_ctx=calc_ctx,
            boost_pct=final_boost,
            target_pct=_to_num(target_meta.get("profit_pct")),
            target_abs=_to_num(target_meta.get("profit_abs")),
            low_price=_to_num(metric.get("mrc_price")) or final_price,
            high_price=final_price,
        )
        if lowered_price not in (None, 0):
            final_price = lowered_price
            final_profit_abs = lowered_profit_abs
            final_profit_pct = lowered_profit_pct
        else:
            final_profit_abs, final_profit_pct = _profit_for_price_and_boost(
                price=final_price,
                boost_pct=final_boost,
                calc_ctx=calc_ctx,
            )

    promo_count = int(selected.get("promo_count") or 0)
    attr_rank = _attr_rank(str(selected.get("attractiveness_status") or ""))
    uses_promo = promo_count >= 1
    uses_boost = final_boost > 0.5
    decision_label, decision_tone, selection_reason, final_attr_status = _matrix_decision_meta(
        promo_count=promo_count,
        attr_rank=attr_rank,
        uses_boost=uses_boost,
    )

    return {
        "selected_iteration_code": selected_iteration_code,
        "final_price": final_price,
        "final_boost": final_boost,
        "final_profit_abs": final_profit_abs,
        "final_profit_pct": final_profit_pct,
        "decision_label": decision_label,
        "decision_tone": decision_tone,
        "control_state": "stable",
        "hypothesis": "Товар сохранён в итоговом сценарии полного часового перебора. Следующее решение будет принято на новом часовом цикле.",
        "uses_promo": uses_promo,
        "uses_boost": uses_boost,
        "final_attr_status": final_attr_status,
        "selection_reason": selection_reason,
        "promo_count": promo_count,
    }


def _resolve_portfolio_role(
    *,
    sales_metric: dict[str, Any] | None,
    store_revenue_total: float,
) -> str:
    metric = sales_metric if isinstance(sales_metric, dict) else {}
    fact_sales = int(metric.get("today_sales") or 0)
    forecast_sales = int(metric.get("forecast_sales") or 0)
    week_avg_daily = float(metric.get("week_avg_daily") or 0.0)
    month_avg_daily = float(metric.get("month_avg_daily") or 0.0)
    today_revenue = float(_to_num(metric.get("today_revenue")) or 0.0)
    revenue_share = (today_revenue / store_revenue_total) if store_revenue_total > 0 else 0.0

    if (
        fact_sales >= 3
        or forecast_sales >= 3
        or week_avg_daily >= 1.5
        or month_avg_daily >= 1.5
        or revenue_share >= 0.08
    ):
        return "A"
    if (
        fact_sales >= 1
        or forecast_sales >= 1
        or week_avg_daily >= 0.3
        or month_avg_daily >= 0.3
        or revenue_share >= 0.02
    ):
        return "B"
    return "C"


def _select_scenario_by_codes(
    *,
    candidates: list[dict[str, Any]],
    preferred_codes: Sequence[str],
) -> dict[str, Any] | None:
    preferred = [str(code or "").strip() for code in preferred_codes if str(code or "").strip()]
    for code in preferred:
        match = next((item for item in candidates if str(item.get("iteration_code") or "").strip() == code), None)
        if match is not None:
            return match
    return None


def _scenario_has_boost(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    return float(_to_num(snapshot.get("tested_boost_pct")) or 0.0) > 0.5


def _scenario_group_rank(snapshot: dict[str, Any] | None) -> int:
    if not isinstance(snapshot, dict):
        return 999
    promo_count = min(2, max(0, int(snapshot.get("promo_count") or 0)))
    attr_rank = _attr_rank(str(snapshot.get("attractiveness_status") or ""))
    has_boost = _scenario_has_boost(snapshot)
    if promo_count >= 2 and attr_rank >= 2 and has_boost:
        return 0
    if promo_count >= 2 and attr_rank >= 2:
        return 1
    if promo_count == 1 and attr_rank >= 2 and has_boost:
        return 2
    if promo_count == 1 and attr_rank >= 2:
        return 3
    if promo_count >= 2 and attr_rank >= 1 and has_boost:
        return 4
    if promo_count >= 2 and attr_rank >= 1:
        return 5
    if promo_count == 1 and attr_rank >= 1 and has_boost:
        return 6
    if promo_count == 1 and attr_rank >= 1:
        return 7
    if attr_rank >= 2 and has_boost:
        return 8
    if attr_rank >= 2:
        return 9
    if attr_rank >= 1 and has_boost:
        return 10
    if attr_rank >= 1:
        return 11
    return 12


def _choose_portfolio_scenario(
    *,
    ordered: list[dict[str, Any]],
    sales_metric: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str, str]:
    _ = sales_metric
    preferred_codes_by_rank: dict[int, list[str]] = {
        0: ["mrc_with_boost", "rrc_with_boost"],
        1: ["mrc", "rrc_no_ads"],
        2: ["mrc_with_boost", "rrc_with_boost"],
        3: ["mrc", "rrc_no_ads"],
        4: ["mrc_with_boost", "rrc_with_boost"],
        5: ["mrc", "rrc_no_ads"],
        6: ["mrc_with_boost", "rrc_with_boost"],
        7: ["mrc", "rrc_no_ads"],
        8: ["mrc_with_boost", "rrc_with_boost"],
        9: ["mrc", "rrc_no_ads"],
        10: ["mrc_with_boost", "rrc_with_boost"],
        11: ["mrc", "rrc_no_ads"],
        12: ["rrc_with_boost", "rrc_no_ads", "mrc_with_boost", "mrc"],
    }
    for rank in range(13):
        candidates = [row for row in ordered if _scenario_group_rank(row) == rank]
        if not candidates:
            continue
        selected = _select_scenario_by_codes(
            candidates=candidates,
            preferred_codes=preferred_codes_by_rank.get(rank, ["rrc_with_boost", "rrc_no_ads", "mrc_with_boost", "mrc"]),
        )
        if selected is None:
            selected = candidates[0]
        return selected, "matrix", f"matrix_rank_{rank}"

    if ordered:
        return ordered[0], "matrix", "matrix_fallback"
    return None, "matrix", "matrix_empty"


def _missing_market_signals(ordered: Sequence[dict[str, Any]] | None) -> bool:
    rows = [row for row in (ordered or []) if isinstance(row, dict)]
    if not rows:
        return True
    has_any_promo = any(int(row.get("promo_count") or 0) > 0 for row in rows)
    has_any_attr = any(
        str(row.get("attractiveness_status") or "").strip().lower() in {"profitable", "moderate", "overpriced"}
        for row in rows
    )
    return (not has_any_promo) and (not has_any_attr)


def _choose_promo_attr_boost_scenario(
    *,
    ordered: Sequence[dict[str, Any]],
    price_metric: dict[str, Any] | None,
    boost_metric: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str, str]:
    rows = [row for row in (ordered or []) if isinstance(row, dict)]
    if not rows:
        return None, "gate", "gate_empty"
    by_code = {
        str(row.get("iteration_code") or "").strip(): row
        for row in rows
        if str(row.get("iteration_code") or "").strip()
    }
    mrc = by_code.get("mrc")
    mrc_with_boost = by_code.get("mrc_with_boost")
    rrc_with_boost = by_code.get("rrc_with_boost")
    if mrc is None:
        return rows[0], "gate", "gate_fallback_first"

    metric = price_metric if isinstance(price_metric, dict) else {}
    rrc_price = _to_num(metric.get("target_price"))
    if rrc_with_boost is None:
        virtual_rrc_target = _iteration_target_from_metric(
            iteration_code="rrc_with_boost",
            price_metric=metric,
            boost_metric=boost_metric,
        )
        rrc_with_boost = {
            "iteration_code": "rrc_with_boost",
            "iteration_label": "РРЦ + буст",
            "tested_price": _to_num(virtual_rrc_target.get("tested_price")),
            "tested_boost_pct": _to_num(virtual_rrc_target.get("tested_boost_pct")),
            "market_boost_bid_percent": _to_num(virtual_rrc_target.get("tested_boost_pct")),
            "boost_share": None,
            "promo_count": 0,
            "attractiveness_status": (
                "profitable"
                if _attr_rank(str((mrc_with_boost or {}).get("attractiveness_status") or "")) >= 2
                else "moderate"
                if _attr_rank(str((mrc_with_boost or {}).get("attractiveness_status") or "")) >= 1
                else "overpriced"
            ),
            "coinvest_pct": None,
            "on_display_price": None,
            "promo_details": [],
        }

    def _boost_overlay_allowed(snapshot: dict[str, Any] | None) -> bool:
        if not isinstance(snapshot, dict):
            return False
        tested_boost = _to_num(snapshot.get("tested_boost_pct")) or 0.0
        tested_price = _to_num(snapshot.get("tested_price"))
        if tested_boost <= 0.5 or tested_price in (None, 0):
            return False
        return _attr_rank(str(snapshot.get("attractiveness_status") or "")) >= 1

    def _within_rrc_cap(snapshot: dict[str, Any] | None) -> bool:
        if not isinstance(snapshot, dict):
            return False
        tested_price = _to_num(snapshot.get("tested_price"))
        if tested_price in (None, 0):
            return False
        if rrc_price in (None, 0):
            return True
        return float(tested_price) < float(rrc_price) - 0.5

    def _strictest_promo_threshold(snapshot: dict[str, Any] | None) -> float | None:
        if not isinstance(snapshot, dict):
            return None
        thresholds = [
            _to_num(detail.get("threshold_price"))
            for detail in list(snapshot.get("promo_details") or [])
            if isinstance(detail, dict)
        ]
        thresholds = [value for value in thresholds if value not in (None, 0)]
        if not thresholds:
            return None
        return min(float(value) for value in thresholds)

    def _preserves_base_promo_group(boosted_snapshot: dict[str, Any] | None, *, base_promo_count: int) -> bool:
        if base_promo_count <= 0:
            return True
        tested_price = _to_num((boosted_snapshot or {}).get("tested_price"))
        strictest_threshold = _strictest_promo_threshold(mrc)
        if tested_price in (None, 0) or strictest_threshold in (None, 0):
            return False
        return float(tested_price) <= float(strictest_threshold) + 0.01

    def _rrc_boost_allowed(snapshot: dict[str, Any] | None) -> bool:
        if not isinstance(snapshot, dict):
            return False
        tested_boost = _to_num(snapshot.get("tested_boost_pct")) or 0.0
        tested_price = _to_num(snapshot.get("tested_price"))
        if tested_boost <= 0.5 or tested_price in (None, 0):
            return False
        return _attr_rank(str(snapshot.get("attractiveness_status") or "")) >= 1

    base_promo = min(2, max(0, int(mrc.get("promo_count") or 0)))
    base_attr_rank = _attr_rank(str(mrc.get("attractiveness_status") or ""))

    if _boost_overlay_allowed(mrc_with_boost):
        if base_promo >= 1 and base_attr_rank >= 2:
            if _within_rrc_cap(mrc_with_boost) and _preserves_base_promo_group(mrc_with_boost, base_promo_count=base_promo):
                return mrc_with_boost, "gate", "promo_profitable_with_boost_overlay"
            return mrc, "gate", "promo_profitable_keep_mrc_rrc_cap"
        if base_promo >= 1 and base_attr_rank >= 1:
            if _within_rrc_cap(mrc_with_boost) and _preserves_base_promo_group(mrc_with_boost, base_promo_count=base_promo):
                return mrc_with_boost, "gate", "promo_moderate_with_boost_overlay"
            return mrc, "gate", "promo_moderate_keep_mrc_rrc_cap"
        if base_promo == 0 and base_attr_rank >= 2:
            if _within_rrc_cap(mrc_with_boost):
                return mrc_with_boost, "gate", "profitable_with_boost_overlay"
            if _rrc_boost_allowed(rrc_with_boost):
                return rrc_with_boost, "gate", "profitable_capped_to_rrc_with_boost"
            return mrc, "gate", "profitable_base_mrc_rrc_cap"
        if base_promo == 0 and base_attr_rank >= 1:
            if _within_rrc_cap(mrc_with_boost):
                return mrc_with_boost, "gate", "moderate_with_boost_overlay"
            if _rrc_boost_allowed(rrc_with_boost):
                return rrc_with_boost, "gate", "moderate_capped_to_rrc_with_boost"
            return mrc, "gate", "moderate_base_mrc_rrc_cap"

    if base_promo >= 1 and base_attr_rank >= 2:
        return mrc, "gate", "promo_profitable_base_mrc"
    if base_promo >= 1 and base_attr_rank >= 1:
        return mrc, "gate", "promo_moderate_base_mrc"
    if base_promo == 0 and base_attr_rank >= 2:
        if _rrc_boost_allowed(rrc_with_boost):
            return rrc_with_boost, "gate", "profitable_fallback_rrc_with_boost"
        return mrc, "gate", "profitable_base_mrc"
    if base_promo == 0 and base_attr_rank >= 1:
        if _rrc_boost_allowed(rrc_with_boost):
            return rrc_with_boost, "gate", "moderate_fallback_rrc_with_boost"
        return mrc, "gate", "moderate_base_mrc"
    return mrc, "gate", "overpriced_base_mrc"


def _normalize_matrix_decision(
    *,
    decision_label: str | None,
    decision_tone: str | None,
    promo_details: Sequence[dict[str, Any]] | None,
    attractiveness_status: str | None,
    boost_pct: float | None,
) -> tuple[str, str, str]:
    label = str(decision_label or "").strip()
    normalized = label.lower()
    allowed = {
        "2 промо + выгодно + буст",
        "2 промо + выгодно",
        "1 промо + выгодно + буст",
        "1 промо + выгодно",
        "2 промо + умеренно + буст",
        "2 промо + умеренно",
        "1 промо + умеренно + буст",
        "1 промо + умеренно",
        "выгодная цена + буст",
        "выгодная цена",
        "умеренная цена + буст",
        "умеренная цена",
        "невыгодная цена",
    }
    if normalized in allowed:
        return label, str(decision_tone or "warning").strip() or "warning", _decision_code_from_label(label)

    details = list(promo_details or [])
    promo_count = sum(
        1
        for detail in details
        if str((detail or {}).get("status_label") or "").strip() in {"Участвует", "Ждёт подтверждения"}
    )
    attr = str(attractiveness_status or "").strip().lower()
    boost = float(boost_pct or 0.0)

    if promo_count >= 2 and attr == "выгодная" and boost > 0.5:
        resolved_label = "2 промо + выгодно + буст"
        tone = "positive"
    elif promo_count >= 2 and attr == "выгодная":
        resolved_label = "2 промо + выгодно"
        tone = "positive"
    elif promo_count == 1 and attr == "выгодная" and boost > 0.5:
        resolved_label = "1 промо + выгодно + буст"
        tone = "positive"
    elif promo_count == 1 and attr == "выгодная":
        resolved_label = "1 промо + выгодно"
        tone = "positive"
    elif promo_count >= 2 and attr == "умеренная" and boost > 0.5:
        resolved_label = "2 промо + умеренно + буст"
        tone = "warning"
    elif promo_count >= 2 and attr == "умеренная":
        resolved_label = "2 промо + умеренно"
        tone = "warning"
    elif promo_count == 1 and attr == "умеренная" and boost > 0.5:
        resolved_label = "1 промо + умеренно + буст"
        tone = "warning"
    elif promo_count == 1 and attr == "умеренная":
        resolved_label = "1 промо + умеренно"
        tone = "warning"
    elif boost > 0.5 and attr == "выгодная":
        resolved_label = "Выгодная цена + буст"
        tone = "positive"
    elif attr == "выгодная":
        resolved_label = "Выгодная цена"
        tone = "positive"
    elif boost > 0.5 and attr == "умеренная":
        resolved_label = "Умеренная цена + буст"
        tone = "warning"
    elif attr == "умеренная":
        resolved_label = "Умеренная цена"
        tone = "warning"
    else:
        resolved_label = "Невыгодная цена"
        tone = "warning"

    return resolved_label, tone, _decision_code_from_label(resolved_label)


def _materialize_strategy_from_iteration_rows(
    *,
    store_uid: str,
    rows_by_sku: dict[str, dict[str, dict[str, Any]]],
    price_map: dict[str, dict[str, Any]],
    boost_map: dict[str, dict[str, Any]],
    attr_map: dict[str, dict[str, Any]] | None = None,
    promo_offer_map: dict[str, list[dict[str, Any]]] | None = None,
    promo_offer_raw_map: dict[str, list[dict[str, Any]]] | None = None,
    sales_map: dict[str, dict[str, Any]] | None = None,
    strategy_mode: str = "mix",
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    now_iso = datetime.now(MSK).astimezone().isoformat()
    strategy_mode_norm = str(strategy_mode or "mix").strip().lower() or "mix"
    for sku, scenarios in rows_by_sku.items():
        price_metric = price_map.get(sku) or {}
        boost_metric = boost_map.get(sku) or {}
        _ = boost_metric
        ordered = sorted(
            [row for row in scenarios.values() if isinstance(row, dict)],
            key=_scenario_sort_key,
        )
        if strategy_mode_norm == "mrc":
            selected = next((row for row in ordered if str(row.get("iteration_code") or "").strip() == "mrc"), None)
            if selected is None and ordered:
                selected = ordered[0]
            portfolio_role = "A"
            portfolio_reason = "forced_mrc_mode"
        else:
            if _missing_market_signals(ordered):
                selected = next((row for row in ordered if str(row.get("iteration_code") or "").strip() == "mrc_with_boost"), None)
                if selected is None:
                    selected = next((row for row in ordered if str(row.get("iteration_code") or "").strip() == "mrc"), None)
                if selected is None and ordered:
                    selected = ordered[0]
                portfolio_role = "matrix"
                portfolio_reason = "missing_market_signals"
            else:
                selected, portfolio_role, portfolio_reason = _choose_promo_attr_boost_scenario(
                    ordered=ordered,
                    price_metric=price_metric,
                    boost_metric=boost_map.get(sku) or {},
                )
        if portfolio_reason == "missing_market_signals":
            selected_iteration_code = str((selected or {}).get("iteration_code") or "mrc_with_boost").strip() or "mrc_with_boost"
            target_meta = _iteration_target_from_metric(
                iteration_code=selected_iteration_code,
                price_metric=price_metric,
                boost_metric=boost_map.get(sku) or {},
            )
            final_boost = _to_num((selected or {}).get("tested_boost_pct")) or 0.0
            decision_meta = {
                "selected_iteration_code": selected_iteration_code,
                "final_price": _to_num((selected or {}).get("tested_price")),
                "final_boost": final_boost,
                "final_profit_abs": _to_num(target_meta.get("profit_abs")),
                "final_profit_pct": _to_num(target_meta.get("profit_pct")),
                "decision_label": "МРЦ + буст" if selected_iteration_code == "mrc_with_boost" else "МРЦ",
                "decision_tone": "warning",
                "control_state": "stable",
                "hypothesis": "У товара нет доступных сигналов промо и привлекательности. Используем МРЦ с бустом как fallback до следующего цикла.",
                "uses_promo": False,
                "uses_boost": final_boost > 0.5,
                "final_attr_status": "Нет сигнала",
                "selection_reason": "missing_market_signals_mrc_with_boost",
            }
        else:
            decision_meta = _decision_from_selected_scenario(
                selected=selected,
                price_metric=price_metric,
                boost_metric=boost_map.get(sku) or {},
                attr_metric=(attr_map or {}).get(sku) or {},
                promo_offers=(promo_offer_map or {}).get(sku) or [],
                promo_offers_raw=(promo_offer_raw_map or {}).get(sku) or [],
            )
        selected_iteration_code = str(decision_meta.get("selected_iteration_code") or "mrc").strip() or "mrc"
        final_price = _to_num(decision_meta.get("final_price"))
        final_boost = _to_num(decision_meta.get("final_boost")) or 0.0
        final_profit_abs = _to_num(decision_meta.get("final_profit_abs"))
        final_profit_pct = _to_num(decision_meta.get("final_profit_pct"))
        decision_label = str(decision_meta.get("decision_label") or "Умеренно").strip() or "Умеренно"
        decision_code = _decision_code_from_label(decision_label)
        decision_tone = str(decision_meta.get("decision_tone") or "warning").strip() or "warning"
        control_state = str(decision_meta.get("control_state") or "stable").strip() or "stable"
        hypothesis = str(decision_meta.get("hypothesis") or "").strip()
        uses_promo = bool(decision_meta.get("uses_promo"))
        uses_boost = bool(decision_meta.get("uses_boost"))
        final_attr_status = str(decision_meta.get("final_attr_status") or "Умеренная").strip() or "Умеренная"
        selection_reason = str(decision_meta.get("selection_reason") or "").strip()
        if strategy_mode_norm == "mrc":
            final_price = _to_num(price_metric.get("mrc_price"))
            final_profit_abs = _to_num(price_metric.get("mrc_profit_abs"))
            final_profit_pct = _to_num(price_metric.get("mrc_profit_pct"))
            final_boost = 0.0
            uses_boost = False
            selected_iteration_code = "mrc"
            selection_reason = "forced_mrc_mode"
            decision_code = _decision_code_from_label(decision_label)
        else:
            selection_reason = f"{portfolio_reason}:{selection_reason}"
        scenario_matrix = {
            code: {
                "tested_price": _to_num((row or {}).get("tested_price")),
                "tested_boost_pct": _to_num((row or {}).get("tested_boost_pct")),
                "market_boost_bid_percent": _to_num((row or {}).get("market_boost_bid_percent")),
                "boost_share": _to_num((row or {}).get("boost_share")),
                "promo_count": int((row or {}).get("promo_count") or 0),
                "attractiveness_status": str((row or {}).get("attractiveness_status") or "").strip(),
                "coinvest_pct": _to_num((row or {}).get("coinvest_pct")),
                "on_display_price": _to_num((row or {}).get("on_display_price")),
            }
            for code, row in scenarios.items()
        }
        out.append(
            {
                "store_uid": store_uid,
                "sku": sku,
                "strategy_code": selected_iteration_code,
                "strategy_label": "Часовой сценарий",
                "rrc_price": _to_num(price_metric.get("target_price")),
                "rrc_profit_abs": _to_num(price_metric.get("target_profit_abs")),
                "rrc_profit_pct": _to_num(price_metric.get("target_profit_pct")),
                "mrc_price": _to_num(price_metric.get("mrc_price")),
                "mrc_profit_abs": _to_num(price_metric.get("mrc_profit_abs")),
                "mrc_profit_pct": _to_num(price_metric.get("mrc_profit_pct")),
                "mrc_with_boost_price": _to_num(price_metric.get("mrc_with_boost_price")),
                "mrc_with_boost_profit_abs": _to_num(price_metric.get("mrc_with_boost_profit_abs")),
                "mrc_with_boost_profit_pct": _to_num(price_metric.get("mrc_with_boost_profit_pct")),
                "promo_price": _to_num(price_metric.get("rrc_no_ads_price")),
                "promo_profit_abs": _to_num(price_metric.get("rrc_no_ads_profit_abs")),
                "promo_profit_pct": _to_num(price_metric.get("rrc_no_ads_profit_pct")),
                "attractiveness_price": final_price,
                "attractiveness_profit_abs": final_profit_abs,
                "attractiveness_profit_pct": final_profit_pct,
                "installed_price": final_price,
                "installed_profit_abs": final_profit_abs,
                "installed_profit_pct": final_profit_pct,
                "boost_bid_percent": final_boost,
                "market_boost_bid_percent": (
                    0.0
                    if strategy_mode_norm == "mrc"
                    else _resolve_market_export_boost_pct(
                        boost_bid_pct=final_boost,
                        boost_metric=boost_map.get(sku) or {},
                    )
                ),
                "boost_share": _resolve_boost_share(
                    boost_bid_pct=(
                        0.0
                        if strategy_mode_norm == "mrc"
                        else _resolve_market_export_boost_pct(
                            boost_bid_pct=final_boost,
                            boost_metric=boost_map.get(sku) or {},
                        )
                    ),
                    boost_metric=boost_map.get(sku) or {},
                ),
                "decision_code": decision_code,
                "decision_label": decision_label,
                "decision_tone": decision_tone,
                "hypothesis": hypothesis,
                "hypothesis_started_at": now_iso,
                "hypothesis_expires_at": (datetime.now(MSK) + timedelta(hours=1)).astimezone().isoformat(),
                "control_state": control_state,
                "control_state_started_at": now_iso,
                "attractiveness_status": final_attr_status,
                "promo_count": int(decision_meta.get("promo_count") or int((selected or {}).get("promo_count") or 0)),
                "coinvest_pct": _to_num(selected.get("coinvest_pct")),
                "promo_items": [],
                "uses_promo": uses_promo,
                "uses_attractiveness": True,
                "uses_boost": uses_boost,
                "market_promo_status": "verified" if uses_promo else "",
                "market_promo_checked_at": now_iso,
                "market_promo_message": "",
                "cycle_started_at": str(selected.get("cycle_started_at") or now_iso).strip(),
                "selected_iteration_code": selected_iteration_code,
                "scenario_matrix": scenario_matrix,
                "source_updated_at": now_iso,
            }
        )
        _append_strategy_trace(
            {
                "kind": "final_selection",
                "store_uid": store_uid,
                "sku": sku,
                "selection_reason": selection_reason,
                "portfolio_role": portfolio_role,
                "decision_label": decision_label,
                "decision_code": decision_code,
                "selected_iteration_code": selected_iteration_code,
                "final_price": final_price,
                "final_boost": final_boost,
                "final_profit_abs": final_profit_abs,
                "final_profit_pct": final_profit_pct,
                "scenario_matrix": scenario_matrix,
            }
        )
    return out


async def run_strategy_hourly_market_cycle_for_store(
    *,
    store_uid: str,
    wait_seconds: int = STRATEGY_ITERATION_WAIT_SECONDS,
) -> dict[str, Any]:
    ctx = await get_prices_context()
    stores = [s for s in list(ctx.get("marketplace_stores") or []) if str(s.get("store_uid") or "").strip() == str(store_uid or "").strip()]
    if not stores:
        raise RuntimeError(f"Не найден магазин {store_uid}")
    store = stores[0]
    platform = str(store.get("platform") or "").strip().lower()
    store_id = str(store.get("store_id") or "").strip()
    if platform != "yandex_market" or not store_id:
        raise RuntimeError(f"Часовой strategy-cycle поддержан только для Яндекс.Маркета: {store_uid}")
    store_settings = get_pricing_store_settings(store_uid=store_uid) or {}
    strategy_mode = str(store_settings.get("strategy_mode") or "mix").strip().lower() or "mix"
    iteration_configs = _strategy_iteration_configs_for_mode(strategy_mode)

    overview = await get_prices_overview(
        scope="store",
        platform=platform,
        store_id=store_id,
        page=1,
        page_size=5000,
        force_refresh=False,
    )
    rows = list(overview.get("rows") or [])
    if not rows:
        return {"ok": True, "store_uid": store_uid, "tested_skus": 0, "iterations": []}
    skus = [str(row.get("sku") or "").strip() for row in rows if str(row.get("sku") or "").strip()]
    boost_map = get_pricing_boost_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    cycle_started_at = datetime.now(MSK).astimezone().isoformat()
    iteration_results: list[dict[str, Any]] = []
    cached_attr_map: dict[str, dict[str, Any]] | None = None
    cached_boost_map: dict[str, dict[str, Any]] | None = None

    logger.warning(
        "[pricing_strategy] обрабатывается магазин store_uid=%s store_id=%s strategy_mode=%s iterations=%s",
        store_uid,
        store_id,
        strategy_mode,
        len(iteration_configs),
    )

    promo_offer_map = get_pricing_promo_offer_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    promo_offer_raw_map = get_pricing_promo_offer_raw_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    cached_attr_map = get_pricing_attractiveness_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    cached_boost_map = get_pricing_boost_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    boost_map = cached_boost_map

    logger.warning(
        "[pricing_strategy] магазин=%s: сигналы собраны один раз перед симуляцией promo_skus=%s attr_skus=%s boost_skus=%s",
        store_uid,
        len(promo_offer_map),
        len(cached_attr_map),
        len(cached_boost_map),
    )

    for iteration_index, (iteration_code, iteration_label) in enumerate(iteration_configs, start=1):
        price_payload: dict[str, float] = {}
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            metric = ((row.get("price_metrics_by_store") or {}).get(store_uid) or {}) if isinstance(row.get("price_metrics_by_store"), dict) else {}
            tested_price = _to_num(
                _iteration_target_from_metric(
                    iteration_code=iteration_code,
                    price_metric=metric,
                    boost_metric=boost_map.get(sku) or {},
                ).get("tested_price")
            )
            if sku and tested_price not in (None, 0):
                price_payload[sku] = float(tested_price)
        logger.warning(
            "[pricing_strategy] магазин=%s этап=%s: рассчитан сценарий %s rows=%s preview=%s",
            store_uid,
            iteration_index,
            iteration_label,
            len(price_payload),
            list(price_payload.items())[:3],
        )
        export_result = {
            "kind": "simulation",
            "status": "simulated",
            "updated_cells": 0,
            "matched_rows": len(price_payload),
            "values_total": len(price_payload),
            "message": "simulation_only_no_market_apply",
        }
        logger.warning(
            "[pricing_strategy] магазин=%s этап=%s: выполнена симуляция без отправки цен result=%s",
            store_uid,
            iteration_index,
            export_result,
        )
        attr_map = cached_attr_map

        snapshot_rows: list[dict[str, Any]] = []
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            metric = ((row.get("price_metrics_by_store") or {}).get(store_uid) or {}) if isinstance(row.get("price_metrics_by_store"), dict) else {}
            target = _iteration_target_from_metric(
                iteration_code=iteration_code,
                price_metric=metric,
                boost_metric=boost_map.get(sku) or {},
            )
            snapshot_rows.append(
                _build_strategy_iteration_snapshot(
                    store_uid=store_uid,
                    sku=sku,
                    cycle_started_at=cycle_started_at,
                    iteration_code=iteration_code,
                    iteration_label=iteration_label,
                    tested_price=_to_num(target.get("tested_price")),
                    tested_boost_pct=_to_num(target.get("tested_boost_pct")),
                    boost_metric=boost_map.get(sku) or {},
                    attr_metric=attr_map.get(sku) or {},
                    promo_offers=list(promo_offer_map.get(sku) or []),
                    promo_offers_raw=list(promo_offer_raw_map.get(sku) or []),
                    base_promo_price=_to_num(metric.get("mrc_price")),
                    goods_price_metric={},
                    source_updated_at=str(row.get("updated_at") or "").strip() or None,
                )
            )
        append_pricing_strategy_iteration_history_bulk(rows=snapshot_rows)
        for snapshot in snapshot_rows:
            _append_strategy_trace(
                {
                    "kind": "iteration_snapshot",
                    "store_uid": store_uid,
                    "cycle_started_at": cycle_started_at,
                    "iteration_code": iteration_code,
                    "iteration_label": iteration_label,
                    "sku": str(snapshot.get("sku") or "").strip(),
                    "tested_price": _to_num(snapshot.get("tested_price")),
                    "tested_boost_pct": _to_num(snapshot.get("tested_boost_pct")),
                    "market_boost_bid_percent": _to_num(snapshot.get("market_boost_bid_percent")),
                    "boost_share": _to_num(snapshot.get("boost_share")),
                    "promo_count": int(snapshot.get("promo_count") or 0),
                    "attractiveness_status": str(snapshot.get("attractiveness_status") or "").strip(),
                    "on_display_price": _to_num(snapshot.get("on_display_price")),
                }
            )
        promo_any_count = sum(1 for snapshot in snapshot_rows if int(snapshot.get("promo_count") or 0) >= 1)
        promo_two_count = sum(1 for snapshot in snapshot_rows if int(snapshot.get("promo_count") or 0) >= 2)
        profitable_count = sum(1 for snapshot in snapshot_rows if str(snapshot.get("attractiveness_status") or "").strip() == "profitable")
        moderate_count = sum(1 for snapshot in snapshot_rows if str(snapshot.get("attractiveness_status") or "").strip() == "moderate")
        boost_recommended_count = sum(1 for snapshot in snapshot_rows if (_to_num(snapshot.get("market_boost_bid_percent")) or 0.0) > 0.01)
        logger.warning(
            "[pricing_strategy] магазин=%s этап=%s: собрано промо для %s | 2 промо у %s товаров",
            store_uid,
            iteration_label,
            promo_any_count,
            promo_two_count,
        )
        if iteration_index == 1:
            logger.warning(
                "[pricing_strategy] магазин=%s этап=%s: собрана привлекательность | выгодно=%s умеренно=%s",
                store_uid,
                iteration_label,
                profitable_count,
                moderate_count,
            )
            logger.warning(
                "[pricing_strategy] магазин=%s этап=%s: собран рекомендованный буст по товарам=%s",
                store_uid,
                iteration_label,
                boost_recommended_count,
            )
        else:
            logger.warning(
                "[pricing_strategy] магазин=%s этап=%s: используется единый срез привлекательности | выгодно=%s умеренно=%s",
                store_uid,
                iteration_label,
                profitable_count,
                moderate_count,
            )
            logger.warning(
                "[pricing_strategy] магазин=%s этап=%s: используется единый срез рекомендаций буста по товарам=%s",
                store_uid,
                iteration_label,
                boost_recommended_count,
            )
        iteration_results.append({"iteration_code": iteration_code, "iteration_label": iteration_label, "export": export_result, "snapshots_written": len(snapshot_rows)})

    price_map = {}
    for row in rows:
        sku = str(row.get("sku") or "").strip()
        metric = ((row.get("price_metrics_by_store") or {}).get(store_uid) or {}) if isinstance(row.get("price_metrics_by_store"), dict) else {}
        if sku and isinstance(metric, dict):
            price_map[sku] = dict(metric)
    sales_metrics_map, sales_totals_map = _build_sales_metrics([store_uid], skus)
    sales_map = dict(sales_metrics_map.get(store_uid) or {})
    store_revenue_total = float(_to_num((sales_totals_map.get(store_uid) or {}).get("today_revenue")) or 0.0)
    for sales_metric in sales_map.values():
        if isinstance(sales_metric, dict):
            sales_metric["_store_revenue_total"] = store_revenue_total
    boost_map = get_pricing_boost_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    attr_map = get_pricing_attractiveness_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    promo_offer_map = get_pricing_promo_offer_results_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    promo_offer_raw_map = get_pricing_promo_offer_raw_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    latest_iterations = get_pricing_strategy_iteration_latest_map(store_uids=[store_uid], skus=skus).get(store_uid, {})
    final_rows = _materialize_strategy_from_iteration_rows(
        store_uid=store_uid,
        rows_by_sku=latest_iterations,
        price_map=price_map,
        boost_map=boost_map,
        attr_map=attr_map,
        promo_offer_map=promo_offer_map,
        promo_offer_raw_map=promo_offer_raw_map,
        sales_map=sales_map,
        strategy_mode=strategy_mode,
    )
    if final_rows:
        append_pricing_strategy_history_bulk(rows=final_rows, captured_at=datetime.now(MSK).astimezone().isoformat())
        upsert_pricing_strategy_results_bulk(rows=final_rows)
    _append_strategy_trace(
        {
            "kind": "cycle_summary",
            "store_uid": store_uid,
            "cycle_started_at": cycle_started_at,
            "tested_skus": len(skus),
            "iterations": iteration_results,
            "final_rows": len(final_rows),
        }
    )
    invalidate_strategy_cache()
    return {
        "ok": True,
        "store_uid": store_uid,
        "tested_skus": len(skus),
        "iterations": iteration_results,
        "final_rows": len(final_rows),
        "cycle_started_at": cycle_started_at,
    }


async def get_strategy_context():
    cached = _cache_get("context", {})
    if cached:
        return cached
    ctx = await get_prices_context()
    _cache_set("context", {}, ctx)
    return ctx


async def get_strategy_tree(**kwargs):
    payload = dict(kwargs)
    cached = _cache_get("tree", payload)
    if cached:
        return cached
    tree = await get_prices_tree(
        tree_mode=kwargs.get("tree_mode", "marketplaces"),
        tree_source_store_id=kwargs.get("tree_source_store_id", ""),
        scope=kwargs.get("scope", "all"),
        platform=kwargs.get("platform", ""),
        store_id=kwargs.get("store_id", ""),
    )
    _cache_set("tree", payload, tree)
    return tree


async def prime_strategy_cache() -> None:
    try:
        ctx = await get_strategy_context()
        stores = list(ctx.get("marketplace_stores") or [])
        yandex_stores = [
            store for store in stores
            if str(store.get("platform") or "").strip().lower() == "yandex_market"
        ]
        if not yandex_stores:
            return
        first_store_uid = str(yandex_stores[0].get("store_uid") or "").strip()
        first_store_id = str(yandex_stores[0].get("store_id") or "").strip()
        common_params = {
            "scope": "all",
            "tree_mode": "marketplaces",
            "tree_source_store_id": first_store_uid,
        }
        await get_strategy_tree(**common_params)
        await get_strategy_overview(
            **common_params,
            page=1,
            page_size=50,
            strategy_filter="all",
            sales_filter="all",
            stock_filter="all",
            sort_key="fact_sales_revenue",
            sort_dir="desc",
        )
        if first_store_id:
            await get_strategy_tree(
                scope="store",
                platform="yandex_market",
                store_id=first_store_id,
                tree_mode="marketplaces",
                tree_source_store_id=first_store_uid,
            )
            await get_strategy_overview(
                scope="store",
                platform="yandex_market",
                store_id=first_store_id,
                tree_mode="marketplaces",
                tree_source_store_id=first_store_uid,
                page=1,
                page_size=50,
                strategy_filter="all",
                sales_filter="all",
                stock_filter="all",
                sort_key="fact_sales_revenue",
                sort_dir="desc",
            )
    except Exception as exc:
        logger.warning("[pricing_strategy] prime cache skipped error=%s", exc)


async def _load_strategy_overlay_for_store(*, platform: str, store_id: str, store_uid: str) -> dict[str, dict[str, Any]]:
    overlay: dict[str, dict[str, Any]] = {}
    page = 1
    page_size = 500
    while True:
        overview = await get_strategy_overview(
            scope="store",
            platform=platform,
            store_id=store_id,
            page=page,
            page_size=page_size,
            persist_plan_snapshot=(page == 1),
        )
        rows = list(overview.get("rows") or [])
        if not rows:
            break
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue
            installed_price = _to_num((row.get("final_price_by_store") or {}).get(store_uid))
            installed_profit_abs = _to_num((row.get("final_profit_abs_by_store") or {}).get(store_uid))
            installed_profit_pct = _to_num((row.get("final_profit_pct_by_store") or {}).get(store_uid))
            boost_bid = _to_num((row.get("final_boost_by_store") or {}).get(store_uid))
            decision_label_value = str((row.get("decision_by_store") or {}).get(store_uid, {}).get("label") or "").strip()
            promo_used = bool((row.get("promo_participation_by_store") or {}).get(store_uid)) or decision_label_value == "Тест промо"
            attractiveness_status = str((row.get("attractiveness_status_by_store") or {}).get(store_uid) or "").strip()
            uses_attractiveness = attractiveness_status in {"Выгодная", "Умеренная"}
            uses_boost = (boost_bid or 0.0) >= 0.5
            overlay[sku] = {
                "installed_price": installed_price,
                "installed_profit_abs": installed_profit_abs,
                "installed_profit_pct": installed_profit_pct,
                "boost_bid_percent": 0.0 if (boost_bid or 0.0) < 0.5 else round(float(boost_bid or 0.0), 2),
                "decision_code": str((row.get("decision_by_store") or {}).get(store_uid, {}).get("code") or "").strip(),
                "decision_label": decision_label_value,
                "decision_tone": str((row.get("decision_by_store") or {}).get(store_uid, {}).get("tone") or "").strip(),
                "hypothesis": str((row.get("hypothesis_by_store") or {}).get(store_uid) or "").strip(),
                "hypothesis_started_at": str((row.get("hypothesis_started_at_by_store") or {}).get(store_uid) or "").strip(),
                "hypothesis_expires_at": str((row.get("hypothesis_expires_at_by_store") or {}).get(store_uid) or "").strip(),
                "control_state": str((row.get("control_state_by_store") or {}).get(store_uid) or "").strip(),
                "control_state_started_at": str((row.get("control_state_started_at_by_store") or {}).get(store_uid) or "").strip(),
                "attractiveness_status": attractiveness_status,
                "uses_promo": promo_used,
                "uses_attractiveness": uses_attractiveness,
                "uses_boost": uses_boost,
                "market_promo_status": str((row.get("market_promo_status_by_store") or {}).get(store_uid) or "").strip(),
                "market_promo_checked_at": str((row.get("market_promo_checked_at_by_store") or {}).get(store_uid) or "").strip(),
                "market_promo_message": str((row.get("market_promo_message_by_store") or {}).get(store_uid) or "").strip(),
                "strategy_code": _compose_strategy_code(
                    promo=promo_used,
                    attractiveness=uses_attractiveness,
                    boost=uses_boost,
                ),
                "strategy_label": _compose_strategy_label(
                    promo=promo_used,
                    attractiveness=uses_attractiveness,
                    boost=uses_boost,
                ),
            }
        total_count = int(overview.get("total_count") or 0)
        if page * page_size >= total_count:
            break
        page += 1
    return overlay


async def refresh_strategy_data(
    *,
    store_uids: list[str] | None = None,
    sku_filter_map: dict[str, list[str] | set[str] | tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    invalidate_strategy_cache()
    logger.warning(
        "[pricing_strategy] refresh_strategy_data is deprecated and no longer writes pricing_strategy_results; use run_strategy_hourly_market_cycle_for_store via strategy_refresh"
    )
    ctx = await get_prices_context()
    stores = list(ctx.get("marketplace_stores") or [])
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [s for s in stores if str(s.get("store_uid") or "").strip() in selected]
    stores_rows: list[dict[str, Any]] = []
    for store in stores:
        store_uid = str(store.get("store_uid") or "").strip()
        if not store_uid:
            continue
        ph = "%s" if is_postgres_backend() else "?"
        with _connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS rows_count, MAX(calculated_at) AS max_calculated_at
                FROM pricing_strategy_results
                WHERE store_uid = {ph}
                """,
                (store_uid,),
            ).fetchone()
        item = dict(row) if row else {}
        stores_rows.append(
            {
                "store_uid": store_uid,
                "store_id": str(store.get("store_id") or "").strip(),
                "mode": "read_only",
                "skus_updated": int(item.get("rows_count") or 0),
                "max_calculated_at": str(item.get("max_calculated_at") or "").strip(),
            }
        )
    return {
        "ok": True,
        "stores_total": len(stores_rows),
        "stores_updated": len(stores_rows),
        "stores_skipped": 0,
        "stores": stores_rows,
        "stores_skipped_rows": [],
        "mode": "read_only",
    }


async def get_strategy_overview(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    strategy_filter: str = "all",
    sales_filter: str = "all",
    stock_filter: str = "all",
    sort_key: str = "fact_sales_revenue",
    sort_dir: str = "desc",
    page: int = 1,
    page_size: int = 50,
    persist_plan_snapshot: bool = False,
):
    today = datetime.now(MSK).date()
    now_msk = datetime.now(MSK)
    payload = {
        "scope": scope,
        "platform": platform,
        "store_id": store_id,
        "tree_mode": tree_mode,
        "tree_source_store_id": tree_source_store_id,
        "category_path": category_path,
        "search": search,
        "strategy_filter": strategy_filter,
        "sales_filter": sales_filter,
        "stock_filter": stock_filter,
        "sort_key": sort_key,
        "sort_dir": sort_dir,
        "page": page,
        "page_size": page_size,
        "persist_plan_snapshot": bool(persist_plan_snapshot),
    }
    cached = _cache_get("overview", payload)
    if cached:
        return cached

    stock_filter_norm = str(stock_filter or "all").strip().lower()
    sales_filter_norm = str(sales_filter or "all").strip().lower()
    warm_page_size = max(1000, int(page_size or 200), 200)
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
        page_size=warm_page_size,
        force_refresh=False,
    )
    rows_all = list(base.get("rows") or [])
    total_base = int(base.get("total_count") or len(rows_all))
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
            page_size=warm_page_size,
            force_refresh=False,
        )
        chunk = list(nxt.get("rows") or [])
        if not chunk:
            break
        rows_all.extend(chunk)
        loaded += len(chunk)
        if page_i > 200:
            break
    rows = rows_all
    stores = _filter_strategy_stores(
        list(base.get("stores") or []),
        scope=scope,
        platform=platform,
        store_id=store_id,
    )
    if any(str(store.get("currency_code") or "RUB").strip().upper() == "USD" for store in stores):
        try:
            await _get_cbr_usd_rub_rate_for_date(today)
        except Exception:
            pass
    store_uids = [str(s.get("store_uid") or "").strip() for s in stores if str(s.get("store_uid") or "").strip()]
    skus = [str(r.get("sku") or "").strip() for r in rows if str(r.get("sku") or "").strip()]
    strategy_map = get_pricing_strategy_results_map(store_uids=store_uids, skus=skus)
    attr_map = get_pricing_attractiveness_results_map(store_uids=store_uids, skus=skus)
    boost_map = get_pricing_boost_results_map(store_uids=store_uids, skus=skus)
    promo_summary_map = get_pricing_promo_results_map(store_uids=store_uids, skus=skus)
    promo_offer_map = get_pricing_promo_offer_results_map(store_uids=store_uids, skus=skus)
    promo_offer_raw_map = get_pricing_promo_offer_raw_map(store_uids=store_uids, skus=skus)
    promo_coinvest_settings_map = get_pricing_promo_coinvest_settings_map(store_uids=store_uids)
    goods_price_report_map = get_yandex_goods_price_report_map(store_uids=store_uids, skus=skus)
    goods_price_report_prev_map = get_yandex_goods_price_report_prev_map(store_uids=store_uids, skus=skus)
    sales_metrics, store_totals = _build_sales_metrics(store_uids, skus)
    sales_plan_summary = _build_sales_plan_summary(
        stores=stores,
        store_totals=store_totals,
        calc_date=today,
    )
    active_promo_ids_by_store = {
        suid: {
            str(item.get("promo_id") or "").strip()
            for item in get_active_pricing_promo_campaigns(store_uid=suid, as_of=now_msk.isoformat())
            if str(item.get("promo_id") or "").strip()
        }
        for suid in store_uids
    }
    day_plan_ctx_by_store = {
        suid: _build_strategy_day_plan_context(
            suid=suid,
            store_totals=store_totals,
            today=today,
        )
        for suid in store_uids
    }

    out_rows: list[dict[str, Any]] = []
    daily_plan_snapshots: dict[str, dict[str, Any]] = {}
    for row in rows:
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        stock_by_store = row.get("stock_by_store") or {}
        cogs_price_by_store = row.get("cogs_price_by_store") or {}
        decision_by_store: dict[str, dict[str, Any]] = {}
        installed_price_by_store: dict[str, float | None] = {}
        mrc_price_by_store: dict[str, float | None] = {}
        mrc_with_boost_price_by_store: dict[str, float | None] = {}
        rrc_price_by_store: dict[str, float | None] = {}
        promo_participation_by_store: dict[str, bool] = {}
        promo_details_by_store: dict[str, list[dict[str, Any]]] = {}
        attractiveness_status_by_store: dict[str, str] = {}
        boost_bid_by_store: dict[str, float | None] = {}
        market_boost_bid_by_store: dict[str, float | None] = {}
        boost_share_by_store: dict[str, float | None] = {}
        planned_unit_profit_abs_by_store: dict[str, float | None] = {}
        planned_unit_profit_pct_by_store: dict[str, float | None] = {}
        elasticity_by_store: dict[str, float | None] = {}
        coinvest_pct_by_store: dict[str, float | None] = {}
        avg_check_by_store: dict[str, float | None] = {}
        fact_sales_by_store: dict[str, int] = {}
        fact_sales_revenue_by_store: dict[str, float | None] = {}
        sku_sales_plan_qty_by_store: dict[str, int] = {}
        sku_sales_plan_revenue_by_store: dict[str, float | None] = {}
        forecast_sales_by_store: dict[str, int] = {}
        forecast_profit_abs_by_store: dict[str, float | None] = {}
        planned_price_with_coinvest_by_store: dict[str, float | None] = {}
        sales_delta_pct_by_store: dict[str, float | None] = {}
        final_price_by_store: dict[str, float | None] = {}
        final_boost_by_store: dict[str, float | None] = {}
        final_profit_abs_by_store: dict[str, float | None] = {}
        final_profit_pct_by_store: dict[str, float | None] = {}
        fact_economy_abs_by_store: dict[str, float | None] = {}
        fact_economy_pct_by_store: dict[str, float | None] = {}
        economy_delta_pct_by_store: dict[str, float | None] = {}
        hypothesis_by_store: dict[str, str] = {}
        hypothesis_started_at_by_store: dict[str, str] = {}
        hypothesis_expires_at_by_store: dict[str, str] = {}
        control_state_by_store: dict[str, str] = {}
        control_state_started_at_by_store: dict[str, str] = {}
        market_promo_status_by_store: dict[str, str] = {}
        market_promo_checked_at_by_store: dict[str, str] = {}
        market_promo_message_by_store: dict[str, str] = {}
        on_display_price_by_store: dict[str, float | None] = {}
        minimum_profit_percent_by_store: dict[str, float | None] = {}
        experimental_floor_pct_by_store: dict[str, float | None] = {}
        strategy_code_by_store: dict[str, str] = {}

        peer_snapshot: dict[str, dict[str, Any]] = {}
        for store in stores:
            suid = str(store.get("store_uid") or "").strip()
            sales_metric = ((sales_metrics.get(suid) or {}).get(sku) or {}) if isinstance(sales_metrics.get(suid), dict) else {}
            peer_snapshot[suid] = {
                "store_uid": suid,
                "store_label": str(store.get("label") or "").strip(),
                "fact_sales": int(sales_metric.get("today_sales") or 0),
                "forecast_sales": int(sales_metric.get("forecast_sales") or 0),
                "coinvest_pct": _to_num(sales_metric.get("coinvest_pct")),
                "fact_profit_pct": _to_num(sales_metric.get("fact_economy_pct")),
            }

        per_store: dict[str, dict[str, Any]] = {}
        for store in stores:
            suid = str(store.get("store_uid") or "").strip()
            platform_name = str(store.get("platform") or "").strip().lower()
            store_currency_code = str(store.get("currency_code") or "RUB").strip().upper() or "RUB"
            active_promo_ids = active_promo_ids_by_store.get(suid) or set()
            item = (strategy_map.get(suid) or {}).get(sku) or {}
            row_metrics = ((row.get("price_metrics_by_store") or {}).get(suid) or {}) if isinstance(row.get("price_metrics_by_store"), dict) else {}
            calc_ctx = row_metrics.get("calc_ctx") if isinstance(row_metrics.get("calc_ctx"), dict) else {}
            attr_metric = ((attr_map.get(suid) or {}).get(sku) or {}) if isinstance(attr_map.get(suid), dict) else {}
            promo_summary_raw = ((promo_summary_map.get(suid) or {}).get(sku) or {}) if isinstance(promo_summary_map.get(suid), dict) else {}
            promo_offers = list(((promo_offer_map.get(suid) or {}).get(sku) or [])) if isinstance(promo_offer_map.get(suid), dict) else []
            promo_offers_raw = list(((promo_offer_raw_map.get(suid) or {}).get(sku) or [])) if isinstance(promo_offer_raw_map.get(suid), dict) else []
            if active_promo_ids:
                promo_offers = [
                    offer for offer in promo_offers
                    if str(offer.get("promo_id") or "").strip() in active_promo_ids
                ]
                promo_offers_raw = [
                    offer for offer in promo_offers_raw
                    if str(offer.get("promo_id") or "").strip() in active_promo_ids
                ]
            else:
                promo_offers = []
                promo_offers_raw = []
            promo_summary = _filter_promo_summary_to_active(
                promo_summary=promo_summary_raw,
                active_promo_ids=active_promo_ids,
            )
            boost_metric = ((boost_map.get(suid) or {}).get(sku) or {}) if isinstance(boost_map.get(suid), dict) else {}
            goods_price_metric = ((goods_price_report_map.get(suid) or {}).get(sku) or {}) if isinstance(goods_price_report_map.get(suid), dict) else {}
            previous_goods_price_metric = ((goods_price_report_prev_map.get(suid) or {}).get(sku) or {}) if isinstance(goods_price_report_prev_map.get(suid), dict) else {}
            sales_metric = ((sales_metrics.get(suid) or {}).get(sku) or {}) if isinstance(sales_metrics.get(suid), dict) else {}
            promo_coinvest_settings = (promo_coinvest_settings_map.get(suid) or {}) if isinstance(promo_coinvest_settings_map.get(suid), dict) else {}
            goods_report_currency = _resolve_goods_report_currency(
                store_currency=store_currency_code,
                report_currency=str(goods_price_metric.get("currency") or store_currency_code),
                on_display_price=_to_num(goods_price_metric.get("on_display_price")),
            )

            rrc_price = _to_num(row_metrics.get("target_price"))
            mrc_price = _to_num(row_metrics.get("mrc_price"))
            no_attr_thresholds = not any(
                _to_num(attr_metric.get(key)) is not None
                for key in (
                    "attractiveness_profitable_price",
                    "attractiveness_moderate_price",
                    "attractiveness_overpriced_price",
                )
            )

            attr_selection = _resolve_attractiveness_selection(attr_metric=attr_metric, row_metrics=row_metrics)
            attr_profitable = _build_attr_profitable_candidate(attr_metric=attr_metric, row_metrics=row_metrics)
            promo_selection = _resolve_promo_selection(
                promo_summary=promo_summary,
                attr_selection=attr_selection,
                row_metrics=row_metrics,
            )

            current_week_avg_price = _to_num(sales_metric.get("current_week_avg_price"))
            elasticity = _to_num(sales_metric.get("elasticity"))
            forecast_sales = int(sales_metric.get("forecast_sales") or 0)
            fact_sales = int(sales_metric.get("today_sales") or 0)
            week_avg_daily = float(sales_metric.get("week_avg_daily") or 0.0)
            month_avg_daily = float(sales_metric.get("month_avg_daily") or 0.0)
            has_historical_demand = week_avg_daily >= 1.0 or month_avg_daily >= 1.0
            stock_qty = _to_num((stock_by_store or {}).get(suid))
            has_stock = stock_qty is None or stock_qty > 0

            day_plan_ctx = day_plan_ctx_by_store.get(suid) or {}
            revenue_plan_pct = _to_num(day_plan_ctx.get("revenue_plan_pct"))
            profit_plan_pct = _to_num(day_plan_ctx.get("profit_plan_pct"))
            minimum_profit_pct = _to_num(day_plan_ctx.get("minimum_profit_pct"))
            min_safe_pct = float(day_plan_ctx.get("min_safe_pct") or 3.0)
            experimental_floor_pct = float(day_plan_ctx.get("experimental_floor_pct") or min_safe_pct)
            daily_plan_snapshots[suid] = dict(day_plan_ctx.get("snapshot") or {})

            previous_control_state = str(item.get("control_state") or "").strip()
            previous_control_state_started_at = _parse_iso_dt(item.get("control_state_started_at"))
            effective_market_promo_status = str(item.get("market_promo_status") or "").strip()
            effective_market_promo_message = str(item.get("market_promo_message") or "").strip()
            has_iteration_strategy = bool(str(item.get("selected_iteration_code") or "").strip())

            if has_iteration_strategy:
                final_source = str(item.get("selected_iteration_code") or item.get("strategy_code") or "base").strip() or "base"
                final_price = _to_num(item.get("installed_price"))
                final_boost = _to_num(item.get("boost_bid_percent")) or 0.0
                final_profit_abs = _to_num(item.get("installed_profit_abs"))
                final_profit_pct = _to_num(item.get("installed_profit_pct"))
                decision_label = str(item.get("decision_label") or "Наблюдать").strip() or "Наблюдать"
                tone = str(item.get("decision_tone") or "warning").strip() or "warning"
                hypothesis = str(item.get("hypothesis") or "Ожидаем следующий сигнал по товару.").strip()
                control_state = str(item.get("control_state") or "stable").strip() or "stable"
                hypothesis_started_at_out = str(item.get("hypothesis_started_at") or now_msk.isoformat()).strip()
                hypothesis_expires_at_out = str(item.get("hypothesis_expires_at") or now_msk.isoformat()).strip()
                control_state_started_at = str(item.get("control_state_started_at") or hypothesis_started_at_out).strip()
                store_code = str(item.get("strategy_code") or final_source or "base").strip() or "base"
                target_price_metric = _to_num(row_metrics.get("target_price"))
                rrc_no_ads_price_metric = _to_num(row_metrics.get("rrc_no_ads_price"))
                mrc_price_metric = _to_num(row_metrics.get("mrc_price"))
                mrc_with_boost_price_metric = _to_num(row_metrics.get("mrc_with_boost_price"))
                if final_source == "rrc_with_boost" and final_price is not None and target_price_metric is not None and abs(float(final_price) - float(target_price_metric)) < 0.5:
                    final_profit_abs = _to_num(row_metrics.get("target_profit_abs"))
                    final_profit_pct = _to_num(row_metrics.get("target_profit_pct"))
                elif final_source == "rrc_no_ads" and final_price is not None and rrc_no_ads_price_metric is not None and abs(float(final_price) - float(rrc_no_ads_price_metric)) < 0.5:
                    final_profit_abs = _to_num(row_metrics.get("rrc_no_ads_profit_abs"))
                    final_profit_pct = _to_num(row_metrics.get("rrc_no_ads_profit_pct"))
                elif final_source == "mrc_with_boost" and final_price is not None and mrc_with_boost_price_metric is not None and abs(float(final_price) - float(mrc_with_boost_price_metric)) < 0.5:
                    final_profit_abs = _to_num(row_metrics.get("mrc_with_boost_profit_abs"))
                    final_profit_pct = _to_num(row_metrics.get("mrc_with_boost_profit_pct"))
                elif final_source == "mrc" and final_price is not None and mrc_price_metric is not None and abs(float(final_price) - float(mrc_price_metric)) < 0.5:
                    final_profit_abs = _to_num(row_metrics.get("mrc_profit_abs"))
                    final_profit_pct = _to_num(row_metrics.get("mrc_profit_pct"))
            else:
                simplified = _resolve_simplified_strategy_decision(
                    now_msk=now_msk,
                    previous_control_state=previous_control_state,
                    previous_control_state_started_at=previous_control_state_started_at,
                    previous_installed_price=_to_num(item.get("installed_price")),
                    previous_installed_boost=_to_num(item.get("boost_bid_percent")),
                    has_stock=has_stock,
                    fact_sales=fact_sales,
                    calc_ctx=calc_ctx,
                    min_safe_pct=min_safe_pct,
                    mrc_price=mrc_price,
                    rrc_price=rrc_price,
                    attr_metric=attr_metric,
                    attr_selection=attr_selection,
                    attr_profitable=attr_profitable,
                    promo_selection=promo_selection,
                    boost_metric=boost_metric,
                    platform_name=platform_name,
                )
                final_source = str(simplified.get("final_source") or "mrc").strip() or "mrc"
                final_price = _to_num(simplified.get("final_price"))
                final_boost = _to_num(simplified.get("final_boost")) or 0.0
                final_profit_abs = _to_num(simplified.get("final_profit_abs"))
                final_profit_pct = _to_num(simplified.get("final_profit_pct"))
                decision_label = str(simplified.get("decision_label") or "Наблюдать").strip() or "Наблюдать"
                tone = str(simplified.get("tone") or "warning").strip() or "warning"
                hypothesis = str(simplified.get("hypothesis") or "Ожидаем следующий сигнал по товару.").strip()
                control_state = str(simplified.get("control_state") or "stable").strip() or "stable"
                hypothesis_started_at_out = str(simplified.get("hypothesis_started_at_out") or now_msk.isoformat()).strip()
                hypothesis_expires_at_out = str(simplified.get("hypothesis_expires_at_out") or now_msk.isoformat()).strip()
                control_state_started_at = str(simplified.get("control_state_started_at") or hypothesis_started_at_out).strip()
                store_code = str(simplified.get("store_code") or "base").strip() or "base"

            if no_attr_thresholds and not promo_offers:
                final_source = "mrc_with_boost"
                final_price = _to_num(row_metrics.get("mrc_with_boost_price")) or _to_num(row_metrics.get("mrc_price"))
                final_boost = _resolve_internal_economy_boost_pct(
                    boost_metric=boost_metric,
                    fallback_bid_pct=_resolve_target_drr_pct(row_metrics),
                )
                final_profit_abs = _to_num(row_metrics.get("mrc_with_boost_profit_abs")) or _to_num(row_metrics.get("mrc_profit_abs"))
                final_profit_pct = _to_num(row_metrics.get("mrc_with_boost_profit_pct")) or _to_num(row_metrics.get("mrc_profit_pct"))
                decision_label = "МРЦ + буст"
                tone = "warning"
                hypothesis = "По товару нет порогов привлекательности. Держим сценарий МРЦ + буст как базовый вариант."
                control_state = "stable"
                store_code = "mrc_with_boost"

            forecast_sales = _forecast_strategy_sales(
                forecast_sales_raw=forecast_sales,
                elasticity=elasticity,
                final_price=final_price,
                current_week_avg_price=current_week_avg_price,
                fact_sales=fact_sales,
                week_avg_daily=week_avg_daily,
                month_avg_daily=month_avg_daily,
                has_historical_demand=has_historical_demand,
                has_stock=has_stock,
            )

            installed_price_view = final_price
            installed_boost_view = final_boost
            installed_profit_abs_view = final_profit_abs
            installed_profit_pct_view = final_profit_pct
            promo_candidate_ready = bool(control_state in {"promo_watch", "boost_test"}) or _resolve_promo_participation(
                final_price=installed_price_view,
                promo_summary=promo_summary,
                promo_offers=promo_offers,
            )
            promo_participates, promo_details = build_market_promo_details(
                promo_offers=promo_offers,
                promo_offers_raw=promo_offers_raw,
                installed_price_view=installed_price_view,
                effective_market_promo_status=effective_market_promo_status,
                effective_market_promo_message=effective_market_promo_message,
                promo_candidate_ready=promo_candidate_ready,
            )
            if has_iteration_strategy:
                attr_status_label = str(item.get("attractiveness_status") or "").strip()
            else:
                attr_status_code = _resolve_attr_status_for_price(
                    price=installed_price_view,
                    attr_metric=attr_metric,
                    platform=platform_name,
                )
                attr_status_label = (
                    "Выгодная" if attr_status_code == "profitable" else
                    "Умеренная" if attr_status_code == "moderate" else
                    "" if attr_status_code == "unknown" else
                    "Завышенная"
                )
            if no_attr_thresholds and not promo_offers:
                attr_status_label = ""
            decision_label, tone, decision_code = _normalize_matrix_decision(
                decision_label=decision_label,
                decision_tone=tone,
                promo_details=promo_details,
                attractiveness_status=attr_status_label,
                boost_pct=installed_boost_view,
            )
            market_export_boost = _resolve_market_export_boost_pct(
                boost_bid_pct=installed_boost_view,
                boost_metric=boost_metric,
            )
            boost_share = _resolve_boost_share(
                boost_bid_pct=market_export_boost,
                boost_metric=boost_metric,
            )
            coinvest_metrics = _build_strategy_coinvest_metrics(
                installed_price_view=installed_price_view,
                installed_profit_abs_view=installed_profit_abs_view,
                installed_boost_view=installed_boost_view,
                elasticity=elasticity,
                forecast_sales=forecast_sales,
                sales_metric=sales_metric,
                goods_price_metric=goods_price_metric,
                promo_offers=promo_offers,
                promo_coinvest_settings=promo_coinvest_settings,
                store_currency_code=store_currency_code,
                goods_report_currency=goods_report_currency,
                today=today,
            )
            resolved_boost_pct = _to_num(coinvest_metrics.get("resolved_boost_pct")) or 0.0
            forecast_profit_abs = _to_num(coinvest_metrics.get("forecast_profit_abs"))
            total_coinvest_pct = _to_num(coinvest_metrics.get("coinvest_pct"))
            planned_price_with_coinvest = _to_num(coinvest_metrics.get("planned_price_with_coinvest"))
            on_display_price = _to_num(coinvest_metrics.get("on_display_price"))
            sku_sales_plan_qty = int(coinvest_metrics.get("sku_sales_plan_qty") or 0)
            sku_sales_plan_revenue = _to_num(coinvest_metrics.get("sku_sales_plan_revenue"))

            per_store[suid] = {
                "store_uid": suid,
                "store_label": str(store.get("label") or "").strip(),
                "strategy_code": store_code,
                "decision_code": decision_code,
                "installed_price": installed_price_view,
                "installed_profit_abs": installed_profit_abs_view,
                "installed_profit_pct": installed_profit_pct_view,
                "installed_boost_pct": resolved_boost_pct,
                "market_export_boost_pct": market_export_boost,
                "promo_participates": promo_participates,
                "promo_details": promo_details,
                "attractiveness_status": attr_status_label,
                "boost_share": boost_share,
                "planned_unit_profit_abs": installed_profit_abs_view,
                "planned_unit_profit_pct": installed_profit_pct_view,
                "elasticity": elasticity,
                "coinvest_pct": total_coinvest_pct,
                "avg_check": _to_num(sales_metric.get("avg_check")),
                "fact_sales": int(sales_metric.get("today_sales") or 0),
                "fact_sales_revenue": _to_num(sales_metric.get("today_revenue")),
                "sku_sales_plan_qty": int(sku_sales_plan_qty or 0),
                "sku_sales_plan_revenue": sku_sales_plan_revenue,
                "forecast_sales": int(forecast_sales or 0),
                "forecast_profit_abs": forecast_profit_abs,
                "planned_price_with_coinvest": planned_price_with_coinvest,
                "sales_delta_pct": _to_num(sales_metric.get("sales_delta_pct")),
                "revenue_plan_pct": revenue_plan_pct,
                "final_price": installed_price_view,
                "final_boost_pct": resolved_boost_pct,
                "final_profit_abs": installed_profit_abs_view,
                "final_profit_pct": installed_profit_pct_view,
                "fact_economy_abs": _to_num(sales_metric.get("fact_economy_abs")),
                "fact_economy_pct": _to_num(sales_metric.get("fact_economy_pct")),
                "economy_delta_pct": _to_num(sales_metric.get("economy_delta_pct")),
                "decision_label": decision_label,
                "decision_tone": tone,
                "hypothesis": hypothesis,
                "hypothesis_started_at": hypothesis_started_at_out,
                "hypothesis_expires_at": hypothesis_expires_at_out,
                "control_state": control_state,
                "control_state_started_at": control_state_started_at,
                "market_promo_status": effective_market_promo_status,
                "market_promo_checked_at": str(item.get("market_promo_checked_at") or "").strip(),
                "market_promo_message": effective_market_promo_message,
                "on_display_price": on_display_price,
                "minimum_profit_percent": minimum_profit_pct,
                "experimental_floor_pct": experimental_floor_pct,
            }

        if not per_store:
            continue

        if strategy_filter and strategy_filter != "all":
            if not any(str(item.get("decision_code") or "").strip() == strategy_filter for item in per_store.values()):
                continue
        if sales_filter_norm == "with_sales":
            if not any(int(item.get("fact_sales") or 0) > 0 for item in per_store.values()):
                continue
        elif sales_filter_norm == "without_sales":
            if any(int(item.get("fact_sales") or 0) > 0 for item in per_store.values()):
                continue

        for suid, item in per_store.items():
            decision_by_store[suid] = {
                "label": str(item.get("decision_label") or "").strip(),
                "tone": str(item.get("decision_tone") or "warning").strip() or "warning",
                "code": str(item.get("decision_code") or "").strip(),
            }
            installed_price_by_store[suid] = _to_num(item.get("installed_price"))
            mrc_price_by_store[suid] = _to_num((((row.get("price_metrics_by_store") or {}).get(suid) or {}).get("mrc_price")))
            mrc_with_boost_price_by_store[suid] = _to_num((((row.get("price_metrics_by_store") or {}).get(suid) or {}).get("mrc_with_boost_price")))
            rrc_price_by_store[suid] = _to_num((((row.get("price_metrics_by_store") or {}).get(suid) or {}).get("target_price")))
            promo_participation_by_store[suid] = bool(item.get("promo_participates"))
            promo_details_by_store[suid] = list(item.get("promo_details") or [])
            attractiveness_status_by_store[suid] = str(item.get("attractiveness_status") or "").strip()
            boost_bid_by_store[suid] = _to_num(item.get("installed_boost_pct"))
            market_boost_bid_by_store[suid] = _to_num(item.get("market_export_boost_pct"))
            boost_share_by_store[suid] = _to_num(item.get("boost_share"))
            planned_unit_profit_abs_by_store[suid] = _to_num(item.get("planned_unit_profit_abs"))
            planned_unit_profit_pct_by_store[suid] = _to_num(item.get("planned_unit_profit_pct"))
            elasticity_by_store[suid] = _to_num(item.get("elasticity"))
            coinvest_pct_by_store[suid] = _to_num(item.get("coinvest_pct"))
            avg_check_by_store[suid] = _to_num(item.get("avg_check"))
            fact_sales_by_store[suid] = int(item.get("fact_sales") or 0)
            fact_sales_revenue_by_store[suid] = _to_num(item.get("fact_sales_revenue"))
            sku_sales_plan_qty_by_store[suid] = int(item.get("sku_sales_plan_qty") or 0)
            sku_sales_plan_revenue_by_store[suid] = _to_num(item.get("sku_sales_plan_revenue"))
            forecast_sales_by_store[suid] = int(item.get("forecast_sales") or 0)
            forecast_profit_abs_by_store[suid] = _to_num(item.get("forecast_profit_abs"))
            planned_price_with_coinvest_by_store[suid] = _to_num(item.get("planned_price_with_coinvest"))
            sales_delta_pct_by_store[suid] = _to_num(item.get("sales_delta_pct"))
            final_price_by_store[suid] = _to_num(item.get("final_price"))
            final_boost_by_store[suid] = _to_num(item.get("final_boost_pct"))
            final_profit_abs_by_store[suid] = _to_num(item.get("final_profit_abs"))
            final_profit_pct_by_store[suid] = _to_num(item.get("final_profit_pct"))
            fact_economy_abs_by_store[suid] = _to_num(item.get("fact_economy_abs"))
            fact_economy_pct_by_store[suid] = _to_num(item.get("fact_economy_pct"))
            economy_delta_pct_by_store[suid] = _to_num(item.get("economy_delta_pct"))
            hypothesis_by_store[suid] = str(item.get("hypothesis") or "").strip()
            hypothesis_started_at_by_store[suid] = str(item.get("hypothesis_started_at") or "").strip()
            hypothesis_expires_at_by_store[suid] = str(item.get("hypothesis_expires_at") or "").strip()
            control_state_by_store[suid] = str(item.get("control_state") or "").strip()
            control_state_started_at_by_store[suid] = str(item.get("control_state_started_at") or "").strip()
            market_promo_status_by_store[suid] = str(item.get("market_promo_status") or "").strip()
            market_promo_checked_at_by_store[suid] = str(item.get("market_promo_checked_at") or "").strip()
            market_promo_message_by_store[suid] = str(item.get("market_promo_message") or "").strip()
            on_display_price_by_store[suid] = _to_num(item.get("on_display_price"))
            minimum_profit_percent_by_store[suid] = _to_num(item.get("minimum_profit_percent"))
            experimental_floor_pct_by_store[suid] = _to_num(item.get("experimental_floor_pct"))
            strategy_code_by_store[suid] = str(item.get("decision_code") or "").strip()

        out_rows.append(
            {
                "sku": sku,
                "name": row.get("name"),
                "tree_path": row.get("tree_path") or [],
                "placements": row.get("placements") or {},
                "stock_by_store": stock_by_store,
                "cogs_price_by_store": cogs_price_by_store,
                "decision_by_store": decision_by_store,
                "installed_price_by_store": installed_price_by_store,
                "mrc_price_by_store": mrc_price_by_store,
                "mrc_with_boost_price_by_store": mrc_with_boost_price_by_store,
                "rrc_price_by_store": rrc_price_by_store,
                "promo_participation_by_store": promo_participation_by_store,
                "promo_details_by_store": promo_details_by_store,
                "attractiveness_status_by_store": attractiveness_status_by_store,
                "boost_bid_by_store": boost_bid_by_store,
                "market_boost_bid_by_store": market_boost_bid_by_store,
                "boost_share_by_store": boost_share_by_store,
                "planned_unit_profit_abs_by_store": planned_unit_profit_abs_by_store,
                "planned_unit_profit_pct_by_store": planned_unit_profit_pct_by_store,
                "elasticity_by_store": elasticity_by_store,
                "coinvest_pct_by_store": coinvest_pct_by_store,
                "avg_check_by_store": avg_check_by_store,
                "fact_sales_by_store": fact_sales_by_store,
                "fact_sales_revenue_by_store": fact_sales_revenue_by_store,
                "sku_sales_plan_qty_by_store": sku_sales_plan_qty_by_store,
                "sku_sales_plan_revenue_by_store": sku_sales_plan_revenue_by_store,
                "forecast_sales_by_store": forecast_sales_by_store,
                "forecast_profit_abs_by_store": forecast_profit_abs_by_store,
                "planned_price_with_coinvest_by_store": planned_price_with_coinvest_by_store,
                "sales_delta_pct_by_store": sales_delta_pct_by_store,
                "final_price_by_store": final_price_by_store,
                "final_boost_by_store": final_boost_by_store,
                "final_profit_abs_by_store": final_profit_abs_by_store,
                "final_profit_pct_by_store": final_profit_pct_by_store,
                "fact_economy_abs_by_store": fact_economy_abs_by_store,
                "fact_economy_pct_by_store": fact_economy_pct_by_store,
                "economy_delta_pct_by_store": economy_delta_pct_by_store,
            "hypothesis_by_store": hypothesis_by_store,
            "hypothesis_started_at_by_store": hypothesis_started_at_by_store,
            "hypothesis_expires_at_by_store": hypothesis_expires_at_by_store,
            "control_state_by_store": control_state_by_store,
            "control_state_started_at_by_store": control_state_started_at_by_store,
                "market_promo_status_by_store": market_promo_status_by_store,
                "market_promo_checked_at_by_store": market_promo_checked_at_by_store,
                "market_promo_message_by_store": market_promo_message_by_store,
                "on_display_price_by_store": on_display_price_by_store,
                "minimum_profit_percent_by_store": minimum_profit_percent_by_store,
                "experimental_floor_pct_by_store": experimental_floor_pct_by_store,
                "strategy_code_by_store": strategy_code_by_store,
                "updated_at": row.get("updated_at") or "",
            }
        )

    plan_summary_by_store = (sales_plan_summary or {}).get("by_store") if isinstance(sales_plan_summary, dict) else {}
    can_normalize_sku_plan = not any(
        [
            str(search or "").strip(),
            str(category_path or "").strip(),
            strategy_filter and strategy_filter != "all",
            sales_filter_norm != "all",
            stock_filter_norm != "all",
        ]
    )
    if can_normalize_sku_plan and isinstance(plan_summary_by_store, dict) and out_rows:
        raw_plan_totals_by_store: dict[str, float] = {}
        for out_row in out_rows:
            revenue_map = out_row.get("sku_sales_plan_revenue_by_store") or {}
            if not isinstance(revenue_map, dict):
                continue
            for suid, value in revenue_map.items():
                amount = _to_num(value)
                if amount not in (None, 0):
                    raw_plan_totals_by_store[suid] = float(raw_plan_totals_by_store.get(suid) or 0.0) + float(amount)

        for out_row in out_rows:
            revenue_map = out_row.get("sku_sales_plan_revenue_by_store") or {}
            qty_map = out_row.get("sku_sales_plan_qty_by_store") or {}
            price_map = out_row.get("installed_price_by_store") or {}
            if not isinstance(revenue_map, dict) or not isinstance(qty_map, dict) or not isinstance(price_map, dict):
                continue
            for suid, raw_value in list(revenue_map.items()):
                raw_amount = _to_num(raw_value)
                if raw_amount in (None, 0):
                    continue
                store_summary = plan_summary_by_store.get(suid) if isinstance(plan_summary_by_store.get(suid), dict) else {}
                planned_revenue_daily = _to_num((store_summary or {}).get("planned_revenue_daily"))
                raw_total = _to_num(raw_plan_totals_by_store.get(suid))
                if planned_revenue_daily in (None, 0) or raw_total in (None, 0):
                    continue
                scale = float(planned_revenue_daily) / float(raw_total)
                scaled_revenue = round(float(raw_amount) * scale, 2)
                revenue_map[suid] = scaled_revenue
                installed_price = _to_num(price_map.get(suid))
                if installed_price not in (None, 0):
                    scaled_qty = max(0, int(round(float(scaled_revenue) / float(installed_price))))
                    if scaled_revenue > 0 and scaled_qty == 0:
                        scaled_qty = 1
                    qty_map[suid] = scaled_qty

    allowed_sort_keys = {
        "sku_plan_revenue",
        "sku_plan_qty",
        "sku_plan_profit_abs",
        "sku_plan_profit_pct",
        "fact_sales_revenue",
        "fact_sales_qty",
        "fact_profit_abs",
        "fact_profit_pct",
        "profit_completion_pct",
    }
    sort_key_norm = str(sort_key or "fact_sales_revenue").strip()
    if sort_key_norm not in allowed_sort_keys:
        sort_key_norm = "fact_sales_revenue"
    sort_dir_norm = str(sort_dir or "desc").strip().lower()
    reverse_sort = sort_dir_norm != "asc"
    out_rows.sort(
        key=lambda item: (
            _strategy_sort_metric_value(item, sort_key_norm),
            str(item.get("sku") or ""),
        ),
        reverse=reverse_sort,
    )

    page_size_n = max(1, min(int(page_size or 50), 200))
    page_n = max(1, int(page or 1))
    total_filtered = len(out_rows)
    paged_rows = out_rows[(page_n - 1) * page_size_n:(page_n - 1) * page_size_n + page_size_n]

    resp = {
        "ok": True,
        "scope": base.get("scope"),
        "rows": paged_rows,
        "total_count": total_filtered,
        "page": page_n,
        "page_size": page_size_n,
        "stores": stores,
        "tree_source": base.get("tree_source"),
        "sales_plan_summary": sales_plan_summary,
    }
    if persist_plan_snapshot and daily_plan_snapshots:
        append_pricing_daily_plan_history_bulk(rows=list(daily_plan_snapshots.values()))
    _cache_set("overview", payload, resp)
    return resp


async def get_strategy_latest(*, limit: int = 200) -> dict[str, Any]:
    data = await get_strategy_overview(scope="all", page=1, page_size=max(1, min(int(limit or 200), 500)))
    items: list[dict[str, Any]] = []
    for row in data.get("rows") or []:
        sku = str(row.get("sku") or "").strip()
        labels = row.get("decision_by_store") or {}
        installed = row.get("final_price_by_store") or {}
        boosts = row.get("final_boost_by_store") or {}
        profits_pct = row.get("final_profit_pct_by_store") or {}
        for suid, decision in labels.items():
            items.append(
                {
                    "sku": sku,
                    "store_uid": suid,
                    "decision": str((decision or {}).get("label") or "").strip(),
                    "new_price": installed.get(suid),
                    "boost_bid": boosts.get(suid),
                    "op_pct": profits_pct.get(suid),
                }
            )
    return {"ok": True, "items": items[:limit]}
