from __future__ import annotations

import datetime
import logging
from zoneinfo import ZoneInfo
from typing import Any

from backend.services.pricing_strategy_service import get_strategy_overview
from backend.services.sales_coinvest_service import get_sales_coinvest_overview
from backend.services.store_data_model import (
    create_pricing_autopilot_decision,
    finalize_pricing_autopilot_decision,
    get_due_pricing_autopilot_decisions,
    get_latest_pricing_autopilot_snapshot_map,
    get_open_pricing_autopilot_decision_map,
    get_pricing_store_settings,
    upsert_pricing_autopilot_snapshots,
)

logger = logging.getLogger("uvicorn.error")
MSK = ZoneInfo("Europe/Moscow")


def _to_num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


async def _fetch_all_rows(fetcher, **base_kwargs) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows_out: list[dict[str, Any]] = []
    stores_out: list[dict[str, Any]] = []
    page = 1
    page_size = 500
    while True:
        data = await fetcher(page=page, page_size=page_size, **base_kwargs)
        rows = data.get("rows") if isinstance(data, dict) and isinstance(data.get("rows"), list) else []
        stores = data.get("stores") if isinstance(data, dict) and isinstance(data.get("stores"), list) else []
        if not stores_out and stores:
            stores_out = stores
        if not rows:
            break
        rows_out.extend(rows)
        total_count = int(data.get("total_count") or len(rows_out))
        if page * page_size >= total_count:
            break
        page += 1
    return rows_out, stores_out


def _is_prime_time(now_msk: datetime.datetime) -> bool:
    hour = now_msk.hour
    return 9 <= hour < 23


def _bucket_bounds(now_msk: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
    start = now_msk.replace(minute=0, second=0, microsecond=0)
    end = start + datetime.timedelta(hours=3)
    return start, end


def _target_field_names(settings: dict[str, Any]) -> tuple[str, str, float]:
    earning_mode = str(settings.get("earning_mode") or "margin").strip().lower()
    earning_unit = str(settings.get("earning_unit") or "percent").strip().lower()
    step = 1.0 if earning_unit == "percent" else 500.0
    if earning_mode == "margin":
        return (
            "target_margin_percent" if earning_unit == "percent" else "target_margin_rub",
            earning_unit,
            step,
        )
    return (
        "target_profit_percent" if earning_unit == "percent" else "target_profit_rub",
        earning_unit,
        step,
    )


def _store_payload(
    *,
    sku: str,
    store_uid: str,
    strategy_row: dict[str, Any] | None,
    today_item: dict[str, Any] | None,
    yesterday_item: dict[str, Any] | None,
    settings: dict[str, Any],
) -> dict[str, Any]:
    strategy = strategy_row if isinstance(strategy_row, dict) else {}
    today = today_item if isinstance(today_item, dict) else {}
    yesterday = yesterday_item if isinstance(yesterday_item, dict) else {}
    target_field, target_unit, step = _target_field_names(settings)
    return {
        "sku": sku,
        "store_uid": store_uid,
        "target_field": target_field,
        "target_unit": target_unit,
        "step": step,
        "current_target_value": _to_num(settings.get(target_field)),
        "installed_price": _to_num((strategy.get("installed_price_by_store") or {}).get(store_uid)),
        "boost_bid": _to_num((strategy.get("boost_bid_by_store") or {}).get(store_uid)),
        "attractiveness_status": str(((strategy.get("attractiveness_status_by_store") or {}).get(store_uid) or "")).strip(),
        "promo_count": len(((strategy.get("promo_items_by_store") or {}).get(store_uid) or [])),
        "stock": _to_num((strategy.get("stock_by_store") or {}).get(store_uid)),
        "today_count": int(today.get("mentions_count") or 0),
        "today_turnover": _to_num(today.get("turnover")) or 0.0,
        "today_avg_sale_price": _to_num(today.get("avg_sale_price")),
        "today_avg_coinvest_percent": _to_num(today.get("avg_coinvest_percent")),
        "today_on_display_price": _to_num((today.get("on_display_price_by_store") or {}).get(store_uid)),
        "today_on_display_delta_pct": _to_num((today.get("on_display_delta_pct_by_store") or {}).get(store_uid)),
        "yesterday_count": int(yesterday.get("mentions_count") or 0),
        "yesterday_turnover": _to_num(yesterday.get("turnover")) or 0.0,
        "yesterday_avg_sale_price": _to_num(yesterday.get("avg_sale_price")),
        "yesterday_avg_coinvest_percent": _to_num(yesterday.get("avg_coinvest_percent")),
        "yesterday_on_display_price": _to_num((yesterday.get("on_display_price_by_store") or {}).get(store_uid)),
        "yesterday_on_display_delta_pct": _to_num((yesterday.get("on_display_delta_pct_by_store") or {}).get(store_uid)),
    }


def _build_snapshot_rows(
    *,
    now_msk: datetime.datetime,
    strategy_rows: list[dict[str, Any]],
    today_rows: list[dict[str, Any]],
    yesterday_rows: list[dict[str, Any]],
    stores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    strategy_by_sku = {str(row.get("sku") or "").strip(): row for row in strategy_rows if str(row.get("sku") or "").strip()}
    today_by_sku = {str(row.get("sku") or "").strip(): row for row in today_rows if str(row.get("sku") or "").strip()}
    yesterday_by_sku = {str(row.get("sku") or "").strip(): row for row in yesterday_rows if str(row.get("sku") or "").strip()}
    skus = sorted(set(strategy_by_sku.keys()) | set(today_by_sku.keys()) | set(yesterday_by_sku.keys()))
    bucket_start, bucket_end = _bucket_bounds(now_msk)
    out: list[dict[str, Any]] = []
    for sku in skus:
        strategy_row = strategy_by_sku.get(sku) or {}
        today_row = today_by_sku.get(sku) or {}
        yesterday_row = yesterday_by_sku.get(sku) or {}
        today_lookup = {
            str(item.get("store_uid") or "").strip(): item
            for item in (today_row.get("by_store") or [])
            if isinstance(item, dict) and str(item.get("store_uid") or "").strip()
        }
        yesterday_lookup = {
            str(item.get("store_uid") or "").strip(): item
            for item in (yesterday_row.get("by_store") or [])
            if isinstance(item, dict) and str(item.get("store_uid") or "").strip()
        }
        for store in stores:
            store_uid = str(store.get("store_uid") or "").strip()
            if not store_uid:
                continue
            settings = get_pricing_store_settings(store_uid=store_uid) or {}
            payload = _store_payload(
                sku=sku,
                store_uid=store_uid,
                strategy_row=strategy_row,
                today_item=today_lookup.get(store_uid),
                yesterday_item=yesterday_lookup.get(store_uid),
                settings=settings,
            )
            out.append(
                {
                    "snapshot_at": now_msk.isoformat(),
                    "time_bucket_start": bucket_start.isoformat(),
                    "time_bucket_end": bucket_end.isoformat(),
                    "store_uid": store_uid,
                    "sku": sku,
                    "payload": payload,
                }
            )
    return out


def _evaluate_decision_result(*, baseline: dict[str, Any], current: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    baseline_delta = _to_num(baseline.get("today_on_display_delta_pct"))
    current_delta = _to_num(current.get("today_on_display_delta_pct"))
    baseline_count = int(baseline.get("today_count") or 0)
    current_count = int(current.get("today_count") or 0)
    improved_display = (
        baseline_delta is not None and current_delta is not None and current_delta > baseline_delta + 0.1
    )
    worse_display = (
        baseline_delta is not None and current_delta is not None and current_delta < baseline_delta - 0.1
    )
    if improved_display:
        return "confirmed", {"reason": "on_display_delta_improved", "baseline": baseline_delta, "current": current_delta}
    if worse_display:
        return "reverted", {"reason": "on_display_delta_worse", "baseline": baseline_delta, "current": current_delta}
    if current_count > baseline_count:
        return "confirmed", {"reason": "sales_count_improved", "baseline": baseline_count, "current": current_count}
    return "reverted", {"reason": "no_positive_change", "baseline_count": baseline_count, "current_count": current_count}


def _propose_action(payload: dict[str, Any]) -> dict[str, Any] | None:
    current_target = _to_num(payload.get("current_target_value"))
    if current_target is None or current_target <= 0:
        return None
    step = float(payload.get("step") or 0.0)
    if step <= 0:
        return None
    today_count = int(payload.get("today_count") or 0)
    yesterday_count = int(payload.get("yesterday_count") or 0)
    promo_count = int(payload.get("promo_count") or 0)
    target_unit = str(payload.get("target_unit") or "").strip()
    if yesterday_count == 0 and today_count == 1:
        return {
            "action_code": "wait_second_sale",
            "action_unit": target_unit,
            "action_value": 0.0,
            "previous_value": current_target,
            "proposed_value": current_target,
            "reason": {"reason": "first_sale_after_zero_previous_day"},
        }
    if today_count == 0:
        if promo_count == 0:
            proposed = max(0.01 if target_unit == "percent" else 1.0, current_target - step)
            if proposed < current_target:
                return {
                    "action_code": "decrease_target_for_demand",
                    "action_unit": target_unit,
                    "action_value": -step,
                    "previous_value": current_target,
                    "proposed_value": proposed,
                    "reason": {"reason": "zero_sales_no_promo"},
                }
        return {
            "action_code": "hold_and_watch",
            "action_unit": target_unit,
            "action_value": 0.0,
            "previous_value": current_target,
            "proposed_value": current_target,
            "reason": {"reason": "zero_sales_with_promo_or_no_room"},
        }
    if yesterday_count > 0 and today_count < yesterday_count:
        proposed = max(0.01 if target_unit == "percent" else 1.0, current_target - step)
        if proposed < current_target:
            return {
                "action_code": "decrease_target_to_restore_trend",
                "action_unit": target_unit,
                "action_value": -step,
                "previous_value": current_target,
                "proposed_value": proposed,
                "reason": {"reason": "today_worse_than_yesterday"},
            }
    proposed = current_target + step
    return {
        "action_code": "increase_target_to_test_profit",
        "action_unit": target_unit,
        "action_value": step,
        "previous_value": current_target,
        "proposed_value": proposed,
        "reason": {"reason": "sales_present_try_raise_profit"},
    }


async def run_pricing_autopilot_simulation() -> dict[str, Any]:
    now_msk = datetime.datetime.now(MSK)
    strategy_rows, stores = await _fetch_all_rows(get_strategy_overview, scope="all")
    today_rows, _ = await _fetch_all_rows(get_sales_coinvest_overview, scope="all", period="today")
    yesterday_rows, _ = await _fetch_all_rows(get_sales_coinvest_overview, scope="all", period="yesterday")
    snapshot_rows = _build_snapshot_rows(
        now_msk=now_msk,
        strategy_rows=strategy_rows,
        today_rows=today_rows,
        yesterday_rows=yesterday_rows,
        stores=stores,
    )
    snapshots_written = upsert_pricing_autopilot_snapshots(rows=snapshot_rows)

    store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    skus = [str(row.get("sku") or "").strip() for row in strategy_rows if str(row.get("sku") or "").strip()]
    latest_map = get_latest_pricing_autopilot_snapshot_map(store_uids=store_uids, skus=skus)

    due = get_due_pricing_autopilot_decisions(review_before=now_msk.isoformat())
    decisions_reviewed = 0
    decisions_confirmed = 0
    decisions_reverted = 0
    for decision in due:
        store_uid = str(decision.get("store_uid") or "").strip()
        sku = str(decision.get("sku") or "").strip()
        latest = ((latest_map.get(store_uid) or {}).get(sku) or {})
        payload = latest.get("payload") if isinstance(latest.get("payload"), dict) else {}
        baseline = (decision.get("reason") or {}).get("baseline_payload") if isinstance(decision.get("reason"), dict) else None
        if not isinstance(payload, dict) or not isinstance(baseline, dict):
            finalize_pricing_autopilot_decision(
                decision_id=int(decision.get("decision_id")),
                decision_status="expired",
                reviewed_at=now_msk.isoformat(),
                review_snapshot_id=latest.get("snapshot_id"),
                result={"reason": "missing_baseline_or_current_snapshot"},
            )
            decisions_reviewed += 1
            continue
        status, result = _evaluate_decision_result(baseline=baseline, current=payload)
        finalize_pricing_autopilot_decision(
            decision_id=int(decision.get("decision_id")),
            decision_status=status,
            reviewed_at=now_msk.isoformat(),
            review_snapshot_id=latest.get("snapshot_id"),
            result=result,
        )
        decisions_reviewed += 1
        if status == "confirmed":
            decisions_confirmed += 1
        elif status == "reverted":
            decisions_reverted += 1

    decisions_created = 0
    if _is_prime_time(now_msk):
        open_map = get_open_pricing_autopilot_decision_map(store_uids=store_uids, skus=skus)
        review_after = (now_msk + datetime.timedelta(hours=3)).isoformat()
        for store_uid, sku_map in latest_map.items():
            for sku, snapshot in sku_map.items():
                if ((open_map.get(store_uid) or {}).get(sku)) is not None:
                    continue
                payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
                proposal = _propose_action(payload)
                if not proposal:
                    continue
                reason = dict(proposal.get("reason") or {})
                reason["baseline_payload"] = payload
                create_pricing_autopilot_decision(
                    created_at=now_msk.isoformat(),
                    review_after=review_after,
                    store_uid=store_uid,
                    sku=sku,
                    decision_status="pending",
                    decision_mode="simulate",
                    action_code=str(proposal.get("action_code") or "").strip(),
                    action_unit=str(proposal.get("action_unit") or "").strip(),
                    action_value=_to_num(proposal.get("action_value")),
                    previous_value=_to_num(proposal.get("previous_value")),
                    proposed_value=_to_num(proposal.get("proposed_value")),
                    baseline_snapshot_id=int(snapshot.get("snapshot_id") or 0) or None,
                    reason=reason,
                )
                decisions_created += 1

    logger.warning(
        "[pricing_autopilot] simulate finished snapshots=%s reviewed=%s confirmed=%s reverted=%s created=%s prime_time=%s",
        snapshots_written,
        decisions_reviewed,
        decisions_confirmed,
        decisions_reverted,
        decisions_created,
        _is_prime_time(now_msk),
    )
    return {
        "ok": True,
        "simulate": True,
        "snapshot_at": now_msk.isoformat(),
        "prime_time": _is_prime_time(now_msk),
        "snapshots_written": snapshots_written,
        "decisions_reviewed": decisions_reviewed,
        "decisions_confirmed": decisions_confirmed,
        "decisions_reverted": decisions_reverted,
        "decisions_created": decisions_created,
    }
