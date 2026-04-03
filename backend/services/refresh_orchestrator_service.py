from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler

from backend.routers._shared import _catalog_marketplace_stores_context
from backend.services.pricing_runtime_bridge import (
    refresh_attractiveness_data,
    refresh_boost_data,
    refresh_promos_data,
    refresh_sales_coinvest_data,
)
from backend.services.store_data_model import (
    _connect,
    _connect_system,
    create_refresh_job_run,
    delete_dashboard_snapshots,
    get_monitoring_export_snapshot,
    get_pricing_cogs_snapshot_map,
    finish_refresh_job_run,
    get_pricing_store_settings,
    get_refresh_job_runs_latest,
    get_refresh_job_runs_latest_success,
    get_refresh_jobs,
    upsert_dashboard_snapshot,
    replace_pricing_cogs_snapshot_rows,
    update_refresh_job_run,
    upsert_refresh_job,
)
from backend.services.source_tables import replace_source_rows

logger = logging.getLogger("uvicorn.error")

REFRESH_SCHEDULER: BackgroundScheduler | None = None
_JOB_LOCKS: dict[str, threading.Lock] = {}
_MANUAL_QUEUE: deque[dict[str, Any]] = deque()
_QUEUE_LOCK = threading.Lock()
_QUEUE_EVENT = threading.Event()
_QUEUE_WORKER_STARTED = False
_RUN_STATE_LOCK = threading.Lock()
_ACTIVE_RUN_IDS: set[int] = set()
_QUEUED_RUN_IDS: set[int] = set()
MSK = ZoneInfo("Europe/Moscow")
_PRICING_MONITORING_SNAPSHOT_NAME = "page_pricing_monitoring"
_PRICING_MONITORING_EXPORTS_SNAPSHOT_NAME = "page_pricing_monitoring_exports"


def refresh_pricing_catalog_trees_from_sources(*args, **kwargs):
    from backend.services.pricing_catalog_tree_service import refresh_pricing_catalog_trees_from_sources as impl
    return impl(*args, **kwargs)


def _load_cogs_map_from_source(*args, **kwargs):
    from backend.services.pricing_prices_service import _load_cogs_map_from_source as impl
    return impl(*args, **kwargs)


async def refresh_prices_data(*args, **kwargs):
    from backend.services.pricing_prices_service import refresh_prices_data as impl
    return await impl(*args, **kwargs)


async def refresh_strategy_data(*args, **kwargs):
    from backend.services.pricing_strategy_service import refresh_strategy_data as impl
    return await impl(*args, **kwargs)


async def prime_strategy_cache(*args, **kwargs):
    from backend.services.pricing_strategy_service import prime_strategy_cache as impl
    return await impl(*args, **kwargs)


async def run_strategy_hourly_market_cycle_for_store(*args, **kwargs):
    from backend.services.pricing_strategy_service import run_strategy_hourly_market_cycle_for_store as impl
    return await impl(*args, **kwargs)


def _strategy_iteration_count_for_mode(strategy_mode: str | None) -> int:
    from backend.services.pricing_strategy_service import _strategy_iteration_configs_for_mode as impl
    return len(impl(strategy_mode))


async def export_strategy_outputs_for_store(*args, **kwargs):
    from backend.services.pricing_export_service import export_strategy_outputs_for_store as impl
    return await impl(*args, **kwargs)


async def refresh_sales_elasticity_data(*args, **kwargs):
    from backend.services.sales_elasticity_service import refresh_sales_elasticity_data as impl
    return await impl(*args, **kwargs)


async def refresh_sales_shelfs_statistics_history(*args, **kwargs):
    from backend.services.yandex_shelfs_statistics_report_service import refresh_sales_shelfs_statistics_history as impl
    return await impl(*args, **kwargs)


async def refresh_yandex_shelfs_statistics_for_store(*args, **kwargs):
    from backend.services.yandex_shelfs_statistics_report_service import refresh_yandex_shelfs_statistics_for_store as impl
    return await impl(*args, **kwargs)


async def refresh_sales_shows_boost_history(*args, **kwargs):
    from backend.services.yandex_shows_boost_report_service import refresh_sales_shows_boost_history as impl
    return await impl(*args, **kwargs)


async def refresh_yandex_shows_boost_for_store(*args, **kwargs):
    from backend.services.yandex_shows_boost_report_service import refresh_yandex_shows_boost_for_store as impl
    return await impl(*args, **kwargs)


async def refresh_sales_united_netting_history(*args, **kwargs):
    from backend.services.yandex_united_netting_report_service import refresh_sales_united_netting_history as impl
    return await impl(*args, **kwargs)


async def refresh_yandex_united_netting_history_for_store(*args, **kwargs):
    from backend.services.yandex_united_netting_report_service import refresh_yandex_united_netting_history_for_store as impl
    return await impl(*args, **kwargs)


def refresh_sales_overview_cogs_source_for_store(*args, **kwargs):
    from backend.services.yandex_united_orders_report_service import refresh_sales_overview_cogs_source_for_store as impl
    return impl(*args, **kwargs)


async def refresh_sales_overview_history(*args, **kwargs):
    from backend.services.yandex_united_orders_report_service import refresh_sales_overview_history as impl
    return await impl(*args, **kwargs)


async def refresh_sales_overview_order_rows_for_store(*args, **kwargs):
    from backend.services.yandex_united_orders_report_service import refresh_sales_overview_order_rows_for_store as impl
    return await impl(*args, **kwargs)


async def refresh_sales_overview_order_rows_current_month_for_store(*args, **kwargs):
    from backend.services.yandex_united_orders_report_service import refresh_sales_overview_order_rows_current_month_for_store as impl
    return await impl(*args, **kwargs)


async def refresh_sales_overview_order_rows_today_for_store(*args, **kwargs):
    from backend.services.yandex_united_orders_report_service import refresh_sales_overview_order_rows_today_for_store as impl
    return await impl(*args, **kwargs)


async def refresh_yandex_united_orders_history_for_store(*args, **kwargs):
    from backend.services.yandex_united_orders_report_service import refresh_yandex_united_orders_history_for_store as impl
    return await impl(*args, **kwargs)


FULL_CYCLE_JOB_CODES: list[str] = [
    "cogs_hourly_snapshot",
    "today_orders_30m_refresh",
    "sales_reports_hourly_refresh",
    "shelfs_statistics_refresh",
    "shows_boost_refresh",
    "boost_refresh",
    "attractiveness_refresh",
    "promos_refresh",
    "coinvest_refresh",
    "strategy_refresh",
    "catalog_daily_refresh",
]

HIDDEN_MONITORING_JOB_CODES: set[str] = {
    "prices_refresh",
    "boost_refresh",
    "attractiveness_refresh",
    "promos_refresh",
    "coinvest_refresh",
}

SCHEDULER_ROOT_JOB_CODES: set[str] = {
    "cogs_hourly_snapshot",
    "today_orders_30m_refresh",
    "sales_reports_hourly_refresh",
    "shelfs_statistics_refresh",
    "shows_boost_refresh",
    "strategy_refresh",
    "catalog_daily_refresh",
}

REPORT_WINDOW_AUTORESET_JOBS: set[str] = {
    "sales_reports_hourly_refresh",
    "shelfs_statistics_refresh",
    "shows_boost_refresh",
}

JOB_DEFAULTS: list[dict[str, Any]] = [
    {"job_code": "cogs_hourly_snapshot", "title": "Себестоимость из Google", "enabled": True, "schedule_kind": "interval", "interval_minutes": 60, "time_of_day": None},
    {"job_code": "today_orders_30m_refresh", "title": "Заказы сегодня", "enabled": True, "schedule_kind": "interval", "interval_minutes": 30, "time_of_day": None},
    {"job_code": "sales_reports_hourly_refresh", "title": "Отчеты продаж", "enabled": True, "schedule_kind": "interval", "interval_minutes": 60, "time_of_day": None},
    {"job_code": "shelfs_statistics_refresh", "title": "Полки", "enabled": True, "schedule_kind": "interval", "interval_minutes": 60, "time_of_day": None},
    {"job_code": "shows_boost_refresh", "title": "Буст показов", "enabled": True, "schedule_kind": "interval", "interval_minutes": 60, "time_of_day": None},
    {"job_code": "boost_refresh", "title": "Буст", "enabled": True, "schedule_kind": "interval", "interval_minutes": 30, "time_of_day": None},
    {"job_code": "coinvest_refresh", "title": "Соинвест", "enabled": True, "schedule_kind": "interval", "interval_minutes": 30, "time_of_day": None},
    {"job_code": "strategy_refresh", "title": "Стратегия ценообразования", "enabled": True, "schedule_kind": "interval", "interval_minutes": 30, "time_of_day": None},
    {"job_code": "catalog_daily_refresh", "title": "Дерево каталога и ассортимент", "enabled": True, "schedule_kind": "daily", "interval_minutes": None, "time_of_day": "00:00"},
]

JOB_INTERVAL_OFFSETS_MINUTES: dict[str, int] = {
    "cogs_hourly_snapshot": 0,
    "today_orders_30m_refresh": 1,
    "sales_reports_hourly_refresh": 2,
    "shelfs_statistics_refresh": 3,
    "shows_boost_refresh": 4,
    "boost_refresh": 6,
    "coinvest_refresh": 12,
    "strategy_refresh": 14,
}


def _aligned_interval_start(minutes: int, *, offset_minutes: int = 0) -> datetime:
    interval = max(1, int(minutes or 1))
    now_msk = datetime.now(MSK)
    midnight_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    offset = max(0, int(offset_minutes or 0)) % interval
    elapsed_minutes = int((now_msk - midnight_msk).total_seconds() // 60)
    shifted_elapsed = max(0, elapsed_minutes - offset)
    next_bucket = (((shifted_elapsed // interval) + 1) * interval) + offset
    start_msk = midnight_msk + timedelta(minutes=next_bucket)
    return start_msk.astimezone(timezone.utc)

JOB_META: dict[str, dict[str, Any]] = {
    "cogs_hourly_snapshot": {"kind": "google", "platform": "google_sheets", "supports_store_selection": False},
    "today_orders_30m_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "sales_reports_hourly_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "shelfs_statistics_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "shows_boost_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "boost_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "attractiveness_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "promos_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "coinvest_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "strategy_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
    "catalog_daily_refresh": {"kind": "api", "platform": "yandex_market", "supports_store_selection": False},
}

JOB_STALE_TIMEOUT_MINUTES: dict[str, int] = {
    "cogs_hourly_snapshot": 15,
    "today_orders_30m_refresh": 15,
    "sales_reports_hourly_refresh": 60,
    "shelfs_statistics_refresh": 60,
    "shows_boost_refresh": 60,
    "boost_refresh": 30,
    "attractiveness_refresh": 30,
    "promos_refresh": 30,
    "coinvest_refresh": 30,
    "strategy_refresh": 30,
    "catalog_daily_refresh": 60,
    "run_all": 120,
}

OBSOLETE_JOB_CODES: set[str] = {
    "prices_refresh",
}


def _delete_refresh_job_everywhere(job_code: str) -> None:
    code = str(job_code or "").strip()
    if not code:
        return
    for connector in (_connect, _connect_system):
        with connector() as conn:
            conn.execute(f"DELETE FROM refresh_job_runs WHERE job_code = {_placeholders(1)}", (code,))
            conn.execute(f"DELETE FROM refresh_jobs WHERE job_code = {_placeholders(1)}", (code,))
            conn.commit()


def _prune_obsolete_refresh_jobs() -> None:
    for code in OBSOLETE_JOB_CODES:
        try:
            _delete_refresh_job_everywhere(code)
        except Exception as exc:
            logger.warning("[refresh_orchestrator] failed to prune obsolete job_code=%s error=%s", code, exc)


def _ensure_manual_queue_worker() -> None:
    global _QUEUE_WORKER_STARTED
    if _QUEUE_WORKER_STARTED:
        return
    with _QUEUE_LOCK:
        if _QUEUE_WORKER_STARTED:
            return
        thread = threading.Thread(target=_manual_queue_worker, daemon=True)
        thread.start()
        _QUEUE_WORKER_STARTED = True


def _refresh_monitoring_page_snapshots() -> None:
    delete_dashboard_snapshots(snapshot_name=_PRICING_MONITORING_SNAPSHOT_NAME)
    delete_dashboard_snapshots(snapshot_name=_PRICING_MONITORING_EXPORTS_SNAPSHOT_NAME)
    monitoring_response = get_refresh_monitoring_snapshot()
    exports_response = get_monitoring_export_snapshot()
    upsert_dashboard_snapshot(
        snapshot_name=_PRICING_MONITORING_SNAPSHOT_NAME,
        cache_key=_PRICING_MONITORING_SNAPSHOT_NAME,
        response=monitoring_response,
    )
    upsert_dashboard_snapshot(
        snapshot_name=_PRICING_MONITORING_EXPORTS_SNAPSHOT_NAME,
        cache_key=_PRICING_MONITORING_EXPORTS_SNAPSHOT_NAME,
        response=exports_response,
    )


def _register_queued_run(run_id: int) -> None:
    rid = int(run_id or 0)
    if rid <= 0:
        return
    with _RUN_STATE_LOCK:
        _QUEUED_RUN_IDS.add(rid)


def _mark_run_running(run_id: int) -> None:
    rid = int(run_id or 0)
    if rid <= 0:
        return
    with _RUN_STATE_LOCK:
        _QUEUED_RUN_IDS.discard(rid)
        _ACTIVE_RUN_IDS.add(rid)


def _clear_tracked_run(run_id: int) -> None:
    rid = int(run_id or 0)
    if rid <= 0:
        return
    with _RUN_STATE_LOCK:
        _QUEUED_RUN_IDS.discard(rid)
        _ACTIVE_RUN_IDS.discard(rid)


def _is_run_tracked(run_id: int, raw_status: str) -> bool:
    rid = int(run_id or 0)
    if rid <= 0:
        return False
    status = str(raw_status or "").strip().lower()
    with _RUN_STATE_LOCK:
        if status == "queued":
            return rid in _QUEUED_RUN_IDS
        if status == "running":
            return rid in _ACTIVE_RUN_IDS
    return False


def _job_has_active_or_queued_run(job_code: str) -> bool:
    code = str(job_code or "").strip()
    if not code:
        return False
    latest = get_refresh_job_runs_latest().get(code) or {}
    run_id = int(latest.get("run_id") or 0)
    status = str(latest.get("status") or "").strip().lower()
    if status not in {"queued", "running"}:
        return False
    return _is_run_tracked(run_id, status)


def _recover_stale_job_run(job_code: str) -> dict[str, Any] | None:
    code = str(job_code or "").strip()
    if not code:
        return None
    latest = get_refresh_job_runs_latest().get(code) or {}
    run_id = int(latest.get("run_id") or 0)
    raw_status = str(latest.get("status") or "").strip().lower()
    started_at = str(latest.get("started_at") or "").strip()
    if run_id <= 0 or raw_status not in {"queued", "running"}:
        return None
    if not _is_stale_running(job_code=code, started_at=started_at):
        return None
    _clear_tracked_run(run_id)
    finish_refresh_job_run(
        run_id=run_id,
        status="error",
        message="Зависшее выполнение",
        meta={
            "error": "stale_run",
            "recovered_by": "manual_restart",
            "job_code": code,
        },
    )
    _refresh_monitoring_page_snapshots()
    return {
        "job_code": code,
        "run_id": run_id,
        "status": raw_status,
        "recovered": True,
    }


def _manual_queue_worker() -> None:
    while True:
        _QUEUE_EVENT.wait()
        item: dict[str, Any] | None = None
        with _QUEUE_LOCK:
            if _MANUAL_QUEUE:
                item = _MANUAL_QUEUE.popleft()
            if not _MANUAL_QUEUE:
                _QUEUE_EVENT.clear()
        if not item:
            continue
        run_id = int(item.get("run_id") or 0)
        mode = str(item.get("mode") or "").strip()
        code = str(item.get("job_code") or "").strip()
        trigger_source = str(item.get("trigger_source") or "manual").strip() or "manual"
        child_run_ids_raw = item.get("child_run_ids")
        child_run_ids = child_run_ids_raw if isinstance(child_run_ids_raw, dict) else {}
        try:
            if run_id > 0:
                _mark_run_running(run_id)
                update_refresh_job_run(
                    run_id=run_id,
                    status="running",
                    message="Запуск",
                    meta={"progress_percent": 0, "current_stage": code or "run_all"},
                )
            if mode == "run_all":
                asyncio.run(run_refresh_all(trigger_source=trigger_source, run_id=run_id, precreated_run_ids=child_run_ids))
            else:
                asyncio.run(run_refresh_job(code, trigger_source=trigger_source, run_id=run_id))
        except Exception as exc:
            logger.warning("[refresh_orchestrator] queued run failed mode=%s job_code=%s error=%s", mode, code, exc)
            if run_id > 0:
                finish_refresh_job_run(run_id=run_id, status="error", message=str(exc), meta={"error": str(exc)})
        finally:
            _clear_tracked_run(run_id)
            _refresh_monitoring_page_snapshots()


def bind_refresh_scheduler(scheduler: BackgroundScheduler) -> None:
    global REFRESH_SCHEDULER
    REFRESH_SCHEDULER = scheduler


def ensure_refresh_jobs_defaults() -> list[dict[str, Any]]:
    _prune_obsolete_refresh_jobs()
    existing = {str(row.get("job_code") or "").strip() for row in get_refresh_jobs()}
    for item in JOB_DEFAULTS:
        if str(item["job_code"]) in existing:
            continue
        upsert_refresh_job(job_code=item["job_code"], values=item)
    return get_refresh_jobs()


def _job_lock(job_code: str) -> threading.Lock:
    code = str(job_code or "").strip()
    if code not in _JOB_LOCKS:
        _JOB_LOCKS[code] = threading.Lock()
    return _JOB_LOCKS[code]


def _sync_cogs_snapshot_time() -> str:
    now = datetime.now(timezone.utc)
    bucket = now.replace(minute=0, second=0, microsecond=0)
    return bucket.isoformat()


def _changed_cogs_skus(
    *,
    previous: dict[str, dict[str, Any]],
    current: dict[str, dict[str, float]],
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for store_uid, current_rows in current.items():
        prev_rows = (
            previous.get(store_uid, {}).get("rows")
            if isinstance(previous.get(store_uid), dict)
            else {}
        ) or {}
        changed: list[str] = []
        all_skus = sorted({*prev_rows.keys(), *current_rows.keys()})
        for sku in all_skus:
            prev_val = prev_rows.get(sku)
            curr_val = current_rows.get(sku)
            try:
                if prev_val is None and curr_val is None:
                    continue
                if prev_val is None or curr_val is None or abs(float(prev_val) - float(curr_val)) > 1e-9:
                    changed.append(sku)
            except Exception:
                if prev_val != curr_val:
                    changed.append(sku)
        if changed:
            out[store_uid] = changed
    return out


def _stores_context_by_platform(platform: str = "") -> list[dict[str, Any]]:
    platform_norm = str(platform or "").strip().lower()
    stores = _catalog_marketplace_stores_context()
    if not platform_norm:
        return list(stores)
    return [store for store in stores if str(store.get("platform") or "").strip().lower() == platform_norm]


def _selected_store_uids(job_row: dict[str, Any], *, platform: str = "") -> list[str]:
    try:
        configured = job_row.get("stores_json")
        parsed = json.loads(str(configured or "[]") or "[]")
    except Exception:
        parsed = []
    selected = [str(x or "").strip() for x in parsed if str(x or "").strip()]
    stores = _stores_context_by_platform(platform)
    available = {str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()}
    if not selected:
        return sorted(available)
    return [uid for uid in selected if uid in available]


def _recent_reports_window() -> tuple[str, str]:
    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    current_month_start = now_msk.date().replace(day=1)
    previous_month_last_day = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_last_day.replace(day=1)
    return previous_month_start.isoformat(), now_msk.date().isoformat()


def _job_date_range(job_row: dict[str, Any]) -> tuple[str, str]:
    custom_from = str(job_row.get("date_from") or "").strip()
    custom_to = str(job_row.get("date_to") or "").strip()
    if custom_from and custom_to:
        return custom_from, custom_to
    return _recent_reports_window()


def _monitoring_job_date_range(job_code: str, job_row: dict[str, Any]) -> tuple[str | None, str | None]:
    code = str(job_code or "").strip()
    custom_from = str(job_row.get("date_from") or "").strip()
    custom_to = str(job_row.get("date_to") or "").strip()
    if code not in REPORT_WINDOW_AUTORESET_JOBS:
        return (custom_from or None, custom_to or None)
    now_msk = datetime.now(MSK).date()
    updated_at_raw = str(job_row.get("updated_at") or "").strip()
    updated_at_date = None
    if updated_at_raw:
        try:
            updated_at_dt = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
            if updated_at_dt.tzinfo is None:
                updated_at_dt = updated_at_dt.replace(tzinfo=timezone.utc)
            updated_at_date = updated_at_dt.astimezone(MSK).date()
        except Exception:
            updated_at_date = None
    stale_manual = False
    if custom_from and custom_to:
        try:
            custom_to_date = datetime.strptime(custom_to, "%Y-%m-%d").date()
            stale_manual = custom_to_date < now_msk and updated_at_date is not None and updated_at_date < now_msk
        except Exception:
            stale_manual = False
    if stale_manual:
        return _recent_reports_window()
    if custom_from and custom_to:
        return custom_from, custom_to
    return _recent_reports_window()


def _consume_report_job_date_range(job_code: str, job_row: dict[str, Any]) -> tuple[str, str]:
    code = str(job_code or "").strip()
    date_from, date_to = _job_date_range(job_row)
    if code not in REPORT_WINDOW_AUTORESET_JOBS:
        return date_from, date_to
    custom_from = str(job_row.get("date_from") or "").strip()
    custom_to = str(job_row.get("date_to") or "").strip()
    if not (custom_from and custom_to):
        return date_from, date_to
    try:
        upsert_refresh_job(
            job_code=code,
            values={
                "date_from": None,
                "date_to": None,
            },
        )
    except Exception as exc:
        logger.warning(
            "[refresh_orchestrator] failed to reset report date range job_code=%s error=%s",
            code,
            exc,
        )
    return date_from, date_to


def _reset_stale_report_job_ranges() -> None:
    today_msk = datetime.now(MSK).date()
    for job_row in get_refresh_jobs():
        code = str(job_row.get("job_code") or "").strip()
        if code not in REPORT_WINDOW_AUTORESET_JOBS:
            continue
        custom_from = str(job_row.get("date_from") or "").strip()
        custom_to = str(job_row.get("date_to") or "").strip()
        updated_at_raw = str(job_row.get("updated_at") or "").strip()
        if not (custom_from and custom_to and updated_at_raw):
            continue
        try:
            custom_to_date = datetime.strptime(custom_to, "%Y-%m-%d").date()
            updated_at_dt = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
            if updated_at_dt.tzinfo is None:
                updated_at_dt = updated_at_dt.replace(tzinfo=timezone.utc)
            updated_at_date = updated_at_dt.astimezone(MSK).date()
        except Exception:
            continue
        if custom_to_date >= today_msk or updated_at_date >= today_msk:
            continue
        try:
            upsert_refresh_job(job_code=code, values={"date_from": None, "date_to": None})
        except Exception as exc:
            logger.warning(
                "[refresh_orchestrator] failed to clear stale report range job_code=%s error=%s",
                code,
                exc,
            )


def _is_stale_running(*, job_code: str, started_at: str) -> bool:
    raw = str(started_at or "").strip()
    if not raw:
        return False
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    timeout_minutes = JOB_STALE_TIMEOUT_MINUTES.get(str(job_code or "").strip(), 30)
    return datetime.now(timezone.utc) - dt > timedelta(minutes=timeout_minutes)


def _snapshot_status(*, job_code: str, status: str, started_at: str) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"running", "queued"} and _is_stale_running(job_code=job_code, started_at=started_at):
        return "error"
    return str(status or "").strip()


def _snapshot_message(*, job_code: str, status: str, started_at: str, message: str) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"running", "queued"} and _is_stale_running(job_code=job_code, started_at=started_at):
        return "Зависшее выполнение"
    return str(message or "").strip()


def _job_freshness_limit_minutes(job_row: dict[str, Any], job_code: str) -> int:
    schedule_kind = str(job_row.get("schedule_kind") or "").strip().lower()
    interval_minutes = int(job_row.get("interval_minutes") or 0)
    code = str(job_code or "").strip()
    if schedule_kind == "interval" and interval_minutes > 0:
        return max(interval_minutes * 2, interval_minutes + 15)
    if schedule_kind == "daily":
        return 36 * 60
    return max(JOB_STALE_TIMEOUT_MINUTES.get(code, 30) * 2, 30)


def _job_freshness_snapshot(*, job_row: dict[str, Any], job_code: str, success_run: dict[str, Any] | None) -> dict[str, Any]:
    success = success_run if isinstance(success_run, dict) else {}
    success_finished_at = str(success.get("finished_at") or success.get("started_at") or "").strip()
    limit_minutes = _job_freshness_limit_minutes(job_row, job_code)
    if not success_finished_at:
        return {
            "freshness_status": "unknown",
            "freshness_minutes": None,
            "freshness_limit_minutes": limit_minutes,
            "last_success_at": "",
            "is_stale": False,
        }
    try:
        dt = datetime.fromisoformat(success_finished_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_minutes = max(0, int((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() // 60))
        is_stale = age_minutes > limit_minutes
        return {
            "freshness_status": "stale" if is_stale else "fresh",
            "freshness_minutes": age_minutes,
            "freshness_limit_minutes": limit_minutes,
            "last_success_at": success_finished_at,
            "is_stale": is_stale,
        }
    except Exception:
        return {
            "freshness_status": "unknown",
            "freshness_minutes": None,
            "freshness_limit_minutes": limit_minutes,
            "last_success_at": success_finished_at,
            "is_stale": False,
        }


def _derive_progress_percent(meta: dict[str, Any] | None, *, terminal: bool = False) -> int:
    if terminal:
        return 100
    payload = meta if isinstance(meta, dict) else {}
    explicit = 0
    try:
        explicit = int(payload.get("progress_percent") or 0)
    except Exception:
        explicit = 0
    store_statuses = payload.get("store_statuses") if isinstance(payload, dict) else None
    if not isinstance(store_statuses, list) or not store_statuses:
        return max(0, explicit)
    values: list[int] = []
    for item in store_statuses:
        if not isinstance(item, dict):
            continue
        try:
            values.append(max(0, min(100, int(item.get("progress_percent") or 0))))
        except Exception:
            continue
    if not values:
        return max(0, explicit)
    averaged = round(sum(values) / len(values))
    return max(max(0, explicit), averaged)


def _all_yandex_store_uids() -> list[str]:
    return [
        str(store.get("store_uid") or "").strip()
        for store in _stores_context_by_platform("yandex_market")
        if str(store.get("store_uid") or "").strip()
    ]


def _selected_yandex_stores(store_uids: list[str] | None = None) -> list[dict[str, Any]]:
    stores = _stores_context_by_platform("yandex_market")
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [store for store in stores if str(store.get("store_uid") or "").strip() in selected]
    return [store for store in stores if str(store.get("store_uid") or "").strip() and str(store.get("store_id") or "").strip()]


def _progress_store_statuses_by_steps(
    *,
    target_store_uids: list[str],
    per_store_done: dict[str, int],
    total_steps_per_store: int,
    current_store_uid: str | None = None,
    errors: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    err = errors or {}
    rows: list[dict[str, Any]] = []
    safe_total = max(1, int(total_steps_per_store or 1))
    for uid in target_store_uids:
        done = int(per_store_done.get(uid) or 0)
        progress = max(0, min(100, round(done / safe_total * 100)))
        if uid in err:
            rows.append({"store_uid": uid, "status": "error", "message": err[uid], "progress_percent": 100})
        elif done >= safe_total:
            rows.append({"store_uid": uid, "status": "success", "message": "", "progress_percent": 100})
        elif current_store_uid and uid == current_store_uid:
            rows.append({"store_uid": uid, "status": "running", "message": "Идет обновление", "progress_percent": max(progress, 1)})
        elif done > 0:
            rows.append({"store_uid": uid, "status": "running", "message": "Частично выполнено", "progress_percent": max(progress, 1)})
        else:
            rows.append({"store_uid": uid, "status": "idle", "message": "", "progress_percent": 0})
    return rows


def _progress_store_statuses(*, target_store_uids: list[str], current_store_uid: str | None = None, completed: set[str] | None = None, errors: dict[str, str] | None = None) -> list[dict[str, Any]]:
    done = completed or set()
    err = errors or {}
    rows: list[dict[str, Any]] = []
    for uid in target_store_uids:
        if uid in err:
            rows.append({"store_uid": uid, "status": "error", "message": err[uid], "progress_percent": 100})
        elif uid in done:
            rows.append({"store_uid": uid, "status": "success", "message": "", "progress_percent": 100})
        elif current_store_uid and uid == current_store_uid:
            rows.append({"store_uid": uid, "status": "running", "message": "Идет обновление", "progress_percent": 50})
        else:
            rows.append({"store_uid": uid, "status": "idle", "message": "", "progress_percent": 0})
    return rows


async def _run_parallel_store_job(
    *,
    run_id: int | None,
    stage: str,
    initial_message: str,
    success_message: str,
    stores: list[dict[str, Any]],
    task_factory: Callable[[dict[str, Any]], Any],
) -> dict[str, Any]:
    if not stores:
        raise RuntimeError("Не найдены магазины для обновления")
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    per_store_done: dict[str, int] = {uid: 0 for uid in target_store_uids}
    errors: dict[str, str] = {}
    results: list[dict[str, Any]] = []
    error_rows: list[dict[str, str]] = []
    _update_progress(
        run_id,
        progress_percent=0,
        message=initial_message,
        store_statuses=_progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=1,
            errors=errors,
        ),
        current_stage=stage,
    )
    tasks = [asyncio.create_task(task_factory(store)) for store in stores]
    completed = 0
    total = max(1, len(tasks))
    for task in asyncio.as_completed(tasks):
        item = await task
        store_uid = str(item.get("store_uid") or "").strip()
        completed += 1
        per_store_done[store_uid] = 1
        if item.get("ok"):
            results.append(item.get("result") or {})
        else:
            err_msg = str(item.get("error") or "Ошибка").strip()
            errors[store_uid] = err_msg
            error_rows.append(
                {
                    "store_uid": store_uid,
                    "store_id": str(item.get("store_id") or "").strip(),
                    "campaign_id": str(item.get("campaign_id") or "").strip(),
                    "error": err_msg,
                }
            )
        _update_progress(
            run_id,
            progress_percent=round(completed / total * 100),
            message=f"{initial_message.split(':', 1)[0]}: {store_uid}".strip(),
            store_statuses=_progress_store_statuses_by_steps(
                target_store_uids=target_store_uids,
                per_store_done=per_store_done,
                total_steps_per_store=1,
                current_store_uid=store_uid,
                errors=errors,
            ),
            current_stage=stage,
        )
    result = {
        "ok": len(error_rows) == 0,
        "stores": results,
        "errors": error_rows,
        "stores_skipped": error_rows,
        "store_statuses": _progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=1,
            errors=errors,
        ),
        "progress_percent": 100,
    }
    if result["ok"]:
        await prime_strategy_cache()
    _update_progress(
        run_id,
        progress_percent=100,
        message=success_message,
        store_statuses=result["store_statuses"],
        current_stage=stage,
    )
    return result


def _update_progress(run_id: int | None, *, progress_percent: int, message: str = "", store_statuses: list[dict[str, Any]] | None = None, current_stage: str | None = None) -> None:
    if not run_id:
        return
    update_refresh_job_run(
        run_id=run_id,
        status="running",
        message=message,
        meta={
            "progress_percent": max(0, min(100, int(progress_percent))),
            "store_statuses": list(store_statuses or []),
            "current_stage": str(current_stage or "").strip(),
        },
    )


async def _run_cogs_hourly_snapshot(
    *,
    store_uids: list[str] | None = None,
    run_id: int | None = None,
    run_prices_refresh: bool = True,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    stores = _catalog_marketplace_stores_context()
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [store for store in stores if str(store.get("store_uid") or "").strip() in selected]
    selected_store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    previous_snapshot_map = get_pricing_cogs_snapshot_map(
        store_uids=selected_store_uids,
        as_of_msk=datetime.now(MSK) - timedelta(hours=1),
    ) if selected_store_uids else {}
    current_cogs_by_store: dict[str, dict[str, float]] = {}
    store_statuses: list[dict[str, Any]] = []
    done: set[str] = set()
    errors: dict[str, str] = {}
    total = max(1, len(stores))
    for store in stores:
        store_uid = str(store.get("store_uid") or "").strip()
        if not store_uid:
            continue
        _update_progress(
            run_id,
            progress_percent=int(len(done) / total * 100),
            message=f"Себестоимость: {store_uid}",
            store_statuses=_progress_store_statuses(target_store_uids=[str(s.get("store_uid") or "").strip() for s in stores], current_store_uid=store_uid, completed=done, errors=errors),
            current_stage="cogs_hourly_snapshot",
        )
        settings = get_pricing_store_settings(store_uid=store_uid) or {}
        source_id = str(settings.get("cogs_source_id") or "").strip()
        sku_column = str(settings.get("cogs_sku_column") or "").strip()
        value_column = str(settings.get("cogs_value_column") or "").strip()
        if not source_id or not sku_column or not value_column:
            errors[store_uid] = "Источник себестоимости не настроен"
            store_statuses.append({"store_uid": store_uid, "status": "error", "message": "Источник себестоимости не настроен", "progress_percent": 100})
            continue
        try:
            cogs_map = _load_cogs_map_from_source(source_id=source_id, sku_column=sku_column, value_column=value_column)
            current_cogs_by_store[store_uid] = {
                str(sku or "").strip(): float(value)
                for sku, value in cogs_map.items()
                if str(sku or "").strip()
            }
            done.add(store_uid)
            store_statuses.append({"store_uid": store_uid, "status": "success", "message": f"Строк: {len(cogs_map)}", "progress_percent": 100})
        except Exception as exc:
            logger.warning("[refresh_orchestrator] cogs snapshot failed store_uid=%s error=%s", store_uid, exc)
            errors[store_uid] = str(exc)
            store_statuses.append({"store_uid": store_uid, "status": "error", "message": str(exc), "progress_percent": 100})
            continue
        for sku, cogs_value in cogs_map.items():
            rows.append(
                {
                    "store_uid": store_uid,
                    "sku": str(sku or "").strip(),
                    "cogs_value": cogs_value,
                    "source_id": source_id,
                }
            )
    loaded = replace_pricing_cogs_snapshot_rows(snapshot_at=_sync_cogs_snapshot_time(), rows=rows)
    changed_skus_by_store = _changed_cogs_skus(previous=previous_snapshot_map, current=current_cogs_by_store)
    prices_result: dict[str, Any] | None = None
    if run_prices_refresh:
        _update_progress(
            run_id,
            progress_percent=80,
            message="Себестоимость обновлена, запускаем цены",
            store_statuses=store_statuses,
            current_stage="cogs_hourly_snapshot",
        )
        prices_target_uids = sorted(changed_skus_by_store) if changed_skus_by_store else selected_store_uids
        prices_result = await _run_prices_refresh(
            store_uids=prices_target_uids,
            sku_filter_map=changed_skus_by_store or None,
            preload_cogs=False,
        )
    _update_progress(run_id, progress_percent=100, message="Себестоимость обновлена", store_statuses=store_statuses, current_stage="cogs_hourly_snapshot")
    return {
        "snapshot_at": _sync_cogs_snapshot_time(),
        "rows": loaded,
        "store_statuses": store_statuses,
        "progress_percent": 100,
        "changed_skus_by_store": changed_skus_by_store,
        "prices_refresh": prices_result,
    }


async def _run_today_orders_refresh(*, store_uids: list[str] | None = None, run_id: int | None = None) -> dict[str, Any]:
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для заказов сегодня")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            order_rows = await asyncio.wait_for(
                refresh_sales_overview_order_rows_today_for_store(store_uid=store_uid),
                timeout=300,
            )
            return {
                "ok": True,
                "store_uid": store_uid,
                "campaign_id": campaign_id,
                "result": {
                    "order_rows": order_rows,
                },
            }
        except Exception as exc:
            logger.warning("[refresh_orchestrator] today orders failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    result = await _run_parallel_store_job(
        run_id=run_id,
        stage="today_orders_30m_refresh",
        initial_message="Заказы сегодня: запуск",
        success_message="Заказы сегодня обновлены",
        stores=stores,
        task_factory=_task,
    )
    _invalidate_sales_overview_dashboard_after_job()
    return result


def _invalidate_sales_overview_dashboard_after_job() -> None:
    try:
        from backend.routers.sales_overview import invalidate_sales_overview_dashboard_cache, schedule_sales_overview_dashboard_cache_warm

        invalidate_sales_overview_dashboard_cache()
        schedule_sales_overview_dashboard_cache_warm()
    except Exception as exc:
        logger.warning("[refresh_orchestrator] failed to invalidate sales overview dashboard cache: %s", exc)


async def _run_sales_reports_refresh(*, store_uids: list[str] | None = None, run_id: int | None = None) -> dict[str, Any]:
    job_row = next((row for row in get_refresh_jobs() if str(row.get("job_code") or "").strip() == "sales_reports_hourly_refresh"), {})
    date_from, date_to = _consume_report_job_date_range("sales_reports_hourly_refresh", job_row)
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для отчётов продаж")
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores]
    total_steps = max(1, len(stores) * 4)
    completed_steps = 0
    per_store_done: dict[str, int] = {uid: 0 for uid in target_store_uids}
    errors: dict[str, str] = {}
    orders_success: list[dict[str, Any]] = []
    orders_errors: list[dict[str, str]] = []
    netting_success: list[dict[str, Any]] = []
    netting_errors: list[dict[str, str]] = []
    cogs_success: list[dict[str, Any]] = []
    cogs_errors: list[dict[str, str]] = []
    order_rows_success: list[dict[str, Any]] = []
    order_rows_errors: list[dict[str, str]] = []

    _update_progress(
        run_id,
        progress_percent=0,
        message=f"Отчеты продаж: {date_from} — {date_to}",
        store_statuses=_progress_store_statuses_by_steps(
                target_store_uids=target_store_uids,
                per_store_done=per_store_done,
                total_steps_per_store=4,
                errors=errors,
            ),
            current_stage="sales_reports_hourly_refresh",
        )

    async def _orders_task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_yandex_united_orders_history_for_store(
                store_uid=store_uid,
                campaign_id=campaign_id,
                date_from=date_from,
                date_to=date_to,
            )
            return {"kind": "orders", "ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] sales reports orders failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"kind": "orders", "ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    async def _netting_task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_yandex_united_netting_history_for_store(
                store_uid=store_uid,
                campaign_id=campaign_id,
                date_from=date_from,
                date_to=date_to,
            )
            return {"kind": "netting", "ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] sales reports netting failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"kind": "netting", "ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    report_tasks = [asyncio.create_task(_orders_task(store)) for store in stores] + [asyncio.create_task(_netting_task(store)) for store in stores]
    for task in asyncio.as_completed(report_tasks):
        item = await task
        store_uid = str(item.get("store_uid") or "").strip()
        kind = str(item.get("kind") or "").strip()
        completed_steps += 1
        per_store_done[store_uid] = min(2, int(per_store_done.get(store_uid) or 0) + 1)
        if item.get("ok"):
            if kind == "orders":
                orders_success.append(item.get("result") or {})
            else:
                netting_success.append(item.get("result") or {})
        else:
            err_msg = str(item.get("error") or "Ошибка").strip()
            errors[store_uid] = err_msg
            if kind == "orders":
                orders_errors.append({"store_uid": store_uid, "campaign_id": str(item.get("campaign_id") or "").strip(), "error": err_msg})
            else:
                netting_errors.append({"store_uid": store_uid, "campaign_id": str(item.get("campaign_id") or "").strip(), "error": err_msg})
        _update_progress(
            run_id,
            progress_percent=round(completed_steps / total_steps * 100),
            message=f"Отчеты продаж: {kind} {store_uid}",
            store_statuses=_progress_store_statuses_by_steps(
                target_store_uids=target_store_uids,
                per_store_done=per_store_done,
                total_steps_per_store=4,
                current_store_uid=store_uid,
                errors=errors,
            ),
            current_stage="sales_reports_hourly_refresh",
        )

    cogs_tasks = [asyncio.create_task(asyncio.to_thread(refresh_sales_overview_cogs_source_for_store, store_uid=str(store.get("store_uid") or "").strip())) for store in stores]
    for task in asyncio.as_completed(cogs_tasks):
        try:
            item = await task
            store_uid = str(item.get("store_uid") or "").strip()
            cogs_success.append(item)
            per_store_done[store_uid] = 3
        except Exception as exc:
            store_uid = ""
            err_msg = str(exc)
            cogs_errors.append({"store_uid": store_uid, "error": err_msg})
        completed_steps += 1
        _update_progress(
            run_id,
            progress_percent=round(completed_steps / total_steps * 100),
            message=f"Отчеты продаж: себестоимость {store_uid or ''}".strip(),
            store_statuses=_progress_store_statuses_by_steps(
                target_store_uids=target_store_uids,
                per_store_done=per_store_done,
                total_steps_per_store=4,
                current_store_uid=store_uid or None,
                errors=errors,
            ),
            current_stage="sales_reports_hourly_refresh",
        )

    async def _order_rows_task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        try:
            result = await refresh_sales_overview_order_rows_for_store(store_uid=store_uid)
            return {"ok": True, "store_uid": store_uid, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] sales reports order rows failed store_uid=%s error=%s", store_uid, exc)
            return {"ok": False, "store_uid": store_uid, "error": str(exc)}

    order_rows_tasks = [asyncio.create_task(_order_rows_task(store)) for store in stores]
    for task in asyncio.as_completed(order_rows_tasks):
        item = await task
        store_uid = str(item.get("store_uid") or "").strip()
        completed_steps += 1
        if item.get("ok"):
            order_rows_success.append(item.get("result") or {})
            per_store_done[store_uid] = 4
        else:
            err_msg = str(item.get("error") or "Ошибка").strip()
            errors[store_uid] = err_msg
            order_rows_errors.append({"store_uid": store_uid, "error": err_msg})
        _update_progress(
            run_id,
            progress_percent=round(completed_steps / total_steps * 100),
            message=f"Отчеты продаж: по заказам {store_uid}".strip(),
            store_statuses=_progress_store_statuses_by_steps(
                target_store_uids=target_store_uids,
                per_store_done=per_store_done,
                total_steps_per_store=4,
                current_store_uid=store_uid or None,
                errors=errors,
            ),
            current_stage="sales_reports_hourly_refresh",
        )

    if not orders_success and not netting_success:
        raise RuntimeError(
            "Не удалось обновить ни один магазин Маркета: "
            + "; ".join(f"{x.get('campaign_id','')}: {x.get('error','')}" for x in orders_errors + netting_errors)
        )
    result = {
        "orders": {"ok": bool(orders_success), "date_from": date_from, "date_to": date_to, "stores": orders_success, "errors": orders_errors},
        "netting": {"ok": bool(netting_success), "date_from": date_from, "date_to": date_to, "stores": netting_success, "errors": netting_errors},
        "cogs_refresh": {"ok": len(cogs_errors) == 0, "stores": cogs_success, "errors": cogs_errors},
        "orders_rows_refresh": {"ok": len(order_rows_errors) == 0, "stores": order_rows_success, "errors": order_rows_errors},
        "store_statuses": _progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=4,
            errors=errors,
        ),
        "progress_percent": 100,
    }
    _update_progress(run_id, progress_percent=100, message="Отчеты продаж обновлены", store_statuses=result["store_statuses"], current_stage="sales_reports_hourly_refresh")
    return result


async def _run_shelfs_statistics_refresh(*, store_uids: list[str] | None = None, run_id: int | None = None) -> dict[str, Any]:
    job_row = next((row for row in get_refresh_jobs() if str(row.get("job_code") or "").strip() == "shelfs_statistics_refresh"), {})
    date_from, date_to = _consume_report_job_date_range("shelfs_statistics_refresh", job_row)
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для полок")
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores]
    per_store_done: dict[str, int] = {uid: 0 for uid in target_store_uids}
    errors: dict[str, str] = {}
    results: list[dict[str, Any]] = []
    error_rows: list[dict[str, str]] = []
    _update_progress(run_id, progress_percent=0, message=f"Полки: {date_from} — {date_to}", store_statuses=_progress_store_statuses_by_steps(target_store_uids=target_store_uids, per_store_done=per_store_done, total_steps_per_store=1, errors=errors), current_stage="shelfs_statistics_refresh")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_yandex_shelfs_statistics_for_store(
                store_uid=store_uid,
                campaign_id=campaign_id,
                date_from=date_from,
                date_to=date_to,
            )
            return {"ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] shelfs failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    tasks = [asyncio.create_task(_task(store)) for store in stores]
    completed = 0
    total = max(1, len(tasks))
    for task in asyncio.as_completed(tasks):
        item = await task
        store_uid = str(item.get("store_uid") or "").strip()
        completed += 1
        per_store_done[store_uid] = 1
        if item.get("ok"):
            results.append(item.get("result") or {})
        else:
            err_msg = str(item.get("error") or "Ошибка").strip()
            errors[store_uid] = err_msg
            error_rows.append({"store_uid": store_uid, "campaign_id": str(item.get("campaign_id") or "").strip(), "error": err_msg})
        _update_progress(
            run_id,
            progress_percent=round(completed / total * 100),
            message=f"Полки: {store_uid}",
            store_statuses=_progress_store_statuses_by_steps(target_store_uids=target_store_uids, per_store_done=per_store_done, total_steps_per_store=1, current_store_uid=store_uid, errors=errors),
            current_stage="shelfs_statistics_refresh",
        )
    result = {"ok": len(error_rows) == 0, "date_from": date_from, "date_to": date_to, "stores": results, "errors": error_rows}
    result["store_statuses"] = _progress_store_statuses_by_steps(target_store_uids=target_store_uids, per_store_done=per_store_done, total_steps_per_store=1, errors=errors)
    result["progress_percent"] = 100
    _update_progress(run_id, progress_percent=100, message="Полки обновлены", store_statuses=result["store_statuses"], current_stage="shelfs_statistics_refresh")
    return result


async def _run_shows_boost_refresh(*, store_uids: list[str] | None = None, run_id: int | None = None) -> dict[str, Any]:
    job_row = next((row for row in get_refresh_jobs() if str(row.get("job_code") or "").strip() == "shows_boost_refresh"), {})
    date_from, date_to = _consume_report_job_date_range("shows_boost_refresh", job_row)
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для буста показов")
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores]
    per_store_done: dict[str, int] = {uid: 0 for uid in target_store_uids}
    errors: dict[str, str] = {}
    results: list[dict[str, Any]] = []
    error_rows: list[dict[str, str]] = []
    _update_progress(run_id, progress_percent=0, message=f"Буст показов: {date_from} — {date_to}", store_statuses=_progress_store_statuses_by_steps(target_store_uids=target_store_uids, per_store_done=per_store_done, total_steps_per_store=1, errors=errors), current_stage="shows_boost_refresh")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_yandex_shows_boost_for_store(
                store_uid=store_uid,
                campaign_id=campaign_id,
                date_from=date_from,
                date_to=date_to,
            )
            return {"ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] shows boost failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    tasks = [asyncio.create_task(_task(store)) for store in stores]
    completed = 0
    total = max(1, len(tasks))
    for task in asyncio.as_completed(tasks):
        item = await task
        store_uid = str(item.get("store_uid") or "").strip()
        completed += 1
        per_store_done[store_uid] = 1
        if item.get("ok"):
            results.append(item.get("result") or {})
        else:
            err_msg = str(item.get("error") or "Ошибка").strip()
            errors[store_uid] = err_msg
            error_rows.append({"store_uid": store_uid, "campaign_id": str(item.get("campaign_id") or "").strip(), "error": err_msg})
        _update_progress(
            run_id,
            progress_percent=round(completed / total * 100),
            message=f"Буст показов: {store_uid}",
            store_statuses=_progress_store_statuses_by_steps(target_store_uids=target_store_uids, per_store_done=per_store_done, total_steps_per_store=1, current_store_uid=store_uid, errors=errors),
            current_stage="shows_boost_refresh",
        )
    result = {"ok": len(error_rows) == 0, "date_from": date_from, "date_to": date_to, "stores": results, "errors": error_rows}
    result["store_statuses"] = _progress_store_statuses_by_steps(target_store_uids=target_store_uids, per_store_done=per_store_done, total_steps_per_store=1, errors=errors)
    result["progress_percent"] = 100
    _update_progress(run_id, progress_percent=100, message="Буст показов обновлен", store_statuses=result["store_statuses"], current_stage="shows_boost_refresh")
    return result


async def _run_boost_refresh(*, run_id: int | None = None, store_uids: list[str] | None = None) -> dict[str, Any]:
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для буста")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_boost_data(refresh_base=True, store_uids=[store_uid])
            return {"ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] boost failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    return await _run_parallel_store_job(
        run_id=run_id,
        stage="boost_refresh",
        initial_message="Буст: запуск",
        success_message="Буст обновлен",
        stores=stores,
        task_factory=_task,
    )


async def _run_prices_refresh(
    *,
    run_id: int | None = None,
    store_uids: list[str] | None = None,
    sku_filter_map: dict[str, list[str] | set[str] | tuple[str, ...]] | None = None,
    cascade_promos: bool = False,
    preload_cogs: bool = True,
) -> dict[str, Any]:
    _update_progress(run_id, progress_percent=5, message="Цены: запуск", current_stage="prices_refresh")
    if preload_cogs:
        _update_progress(run_id, progress_percent=10, message="Цены: обновляем себестоимость", current_stage="prices_refresh")
        await _run_cogs_hourly_snapshot(
            store_uids=store_uids,
            run_prices_refresh=False,
        )
        _update_progress(run_id, progress_percent=20, message="Цены: считаем цены", current_stage="prices_refresh")
    result = await refresh_prices_data(store_uids=store_uids)
    stores = _selected_yandex_stores(store_uids)
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    promos_result: dict[str, Any] | None = None
    if cascade_promos and target_store_uids:
        _update_progress(
            run_id,
            progress_percent=85,
            message="Цены: запускаем промо",
            current_stage="prices_refresh",
        )
        promos_result = await _run_promos_refresh(store_uids=target_store_uids)
    out = {
        **(result if isinstance(result, dict) else {"result": result}),
        "promos_refresh": promos_result,
        "store_statuses": [
            {
                "store_uid": uid,
                "status": "success",
                "message": "",
                "progress_percent": 100,
            }
            for uid in target_store_uids
        ],
        "progress_percent": 100,
    }
    _update_progress(run_id, progress_percent=100, message="Цены обновлены", store_statuses=out["store_statuses"], current_stage="prices_refresh")
    return out


async def _run_attractiveness_refresh(
    *,
    run_id: int | None = None,
    store_uids: list[str] | None = None,
    cascade_strategy: bool = False,
) -> dict[str, Any]:
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для привлекательности")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_attractiveness_data(refresh_base=False, store_uids=[store_uid])
            return {"ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] attractiveness failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    result = await _run_parallel_store_job(
        run_id=run_id,
        stage="attractiveness_refresh",
        initial_message="Привлекательность: запуск",
        success_message="Привлекательность обновлена",
        stores=stores,
        task_factory=_task,
    )
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    if cascade_strategy and target_store_uids:
        result["strategy_refresh"] = await _run_strategy_refresh(store_uids=target_store_uids)
    return result


async def _run_promos_refresh(
    *,
    run_id: int | None = None,
    store_uids: list[str] | None = None,
    cascade_attractiveness: bool = False,
) -> dict[str, Any]:
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для промо")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_promos_data(refresh_base=False, store_uids=[store_uid])
            return {"ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] promos failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    result = await _run_parallel_store_job(
        run_id=run_id,
        stage="promos_refresh",
        initial_message="Промо: запуск",
        success_message="Промо обновлено",
        stores=stores,
        task_factory=_task,
    )
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    if cascade_attractiveness and target_store_uids:
        result["attractiveness_refresh"] = await _run_attractiveness_refresh(
            store_uids=target_store_uids,
            cascade_strategy=True,
        )
    return result


async def _run_coinvest_refresh(*, run_id: int | None = None, store_uids: list[str] | None = None) -> dict[str, Any]:
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для соинвеста")

    async def _task(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        try:
            result = await refresh_sales_coinvest_data(mode="today", manual=False, refresh_sales=False, store_uids=[store_uid])
            return {"ok": True, "store_uid": store_uid, "campaign_id": campaign_id, "result": result}
        except Exception as exc:
            logger.warning("[refresh_orchestrator] coinvest failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    return await _run_parallel_store_job(
        run_id=run_id,
        stage="coinvest_refresh",
        initial_message="Соинвест: запуск",
        success_message="Соинвест обновлен",
        stores=stores,
        task_factory=_task,
    )


async def _run_strategy_refresh(*, run_id: int | None = None, store_uids: list[str] | None = None) -> dict[str, Any]:
    stores = _selected_yandex_stores(store_uids)
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для стратегии")
    target_store_uids = [str(store.get("store_uid") or "").strip() for store in stores if str(store.get("store_uid") or "").strip()]
    per_store_done: dict[str, int] = {uid: 0 for uid in target_store_uids}
    errors: dict[str, str] = {}
    results: list[dict[str, Any]] = []
    error_rows: list[dict[str, str]] = []
    total_steps_per_store = 3
    _update_progress(
        run_id,
        progress_percent=0,
        message="Стратегия: запуск",
        store_statuses=_progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=total_steps_per_store,
            errors=errors,
        ),
        current_stage="strategy_refresh",
    )
    _update_progress(
        run_id,
        progress_percent=3,
        message="Стратегия: сбор буста",
        store_statuses=_progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=total_steps_per_store,
            errors=errors,
        ),
        current_stage="strategy_refresh",
    )
    try:
        await refresh_boost_data(refresh_base=False, store_uids=target_store_uids)
    except Exception:
        pass
    _update_progress(
        run_id,
        progress_percent=8,
        message="Стратегия: пересчёт цен",
        store_statuses=_progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=total_steps_per_store,
            errors=errors,
        ),
        current_stage="strategy_refresh",
    )
    await _run_prices_refresh(store_uids=target_store_uids, cascade_promos=False)
    _update_progress(
        run_id,
        progress_percent=12,
        message="Стратегия: сбор промо и привлекательности",
        store_statuses=_progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=total_steps_per_store,
            errors=errors,
        ),
        current_stage="strategy_refresh",
    )
    await refresh_promos_data(refresh_base=False, store_uids=target_store_uids)
    await refresh_attractiveness_data(refresh_base=False, store_uids=target_store_uids)
    for store in stores:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        store_settings = get_pricing_store_settings(store_uid=store_uid) or {}
        strategy_mode = str(store_settings.get("strategy_mode") or "mix").strip().lower() or "mix"
        iteration_count = _strategy_iteration_count_for_mode(strategy_mode)

        def _step_progress(done_steps: int, message: str) -> None:
            per_store_done[store_uid] = done_steps
            overall_progress = round(
                (
                    sum(int(per_store_done.get(uid) or 0) for uid in target_store_uids)
                    / max(1, len(target_store_uids) * total_steps_per_store)
                ) * 100
            )
            _update_progress(
                run_id,
                progress_percent=max(5, overall_progress),
                message=message,
                store_statuses=_progress_store_statuses_by_steps(
                    target_store_uids=target_store_uids,
                    per_store_done=per_store_done,
                    total_steps_per_store=total_steps_per_store,
                    current_store_uid=store_uid,
                    errors=errors,
                ),
                current_stage="strategy_refresh",
            )
        try:
            _step_progress(0, f"Стратегия: {store_uid} • {iteration_count} итерации")
            strategy_result = await run_strategy_hourly_market_cycle_for_store(store_uid=store_uid)
            _step_progress(1, f"Стратегия: {store_uid} • финальный экспорт")
            export_result = await export_strategy_outputs_for_store(store_uid=store_uid)
            _step_progress(2, f"Стратегия: {store_uid} • обновление соинвеста")
            coinvest_result = await refresh_sales_coinvest_data(
                mode="today",
                manual=False,
                refresh_sales=False,
                store_uids=[store_uid],
            )
            results.append(
                {
                    "strategy": strategy_result,
                    "export": export_result,
                    "coinvest": coinvest_result,
                }
            )
            per_store_done[store_uid] = total_steps_per_store
        except Exception as exc:
            logger.warning("[refresh_orchestrator] strategy failed store_uid=%s campaign_id=%s error=%s", store_uid, campaign_id, exc)
            err_msg = str(exc)
            errors[store_uid] = err_msg
            error_rows.append({"store_uid": store_uid, "campaign_id": campaign_id, "error": err_msg})
        _update_progress(
            run_id,
            progress_percent=round(
                (
                    sum(int(per_store_done.get(uid) or 0) for uid in target_store_uids)
                    / max(1, len(target_store_uids) * total_steps_per_store)
                ) * 100
            ),
            message=f"Стратегия: {store_uid}",
            store_statuses=_progress_store_statuses_by_steps(
                target_store_uids=target_store_uids,
                per_store_done=per_store_done,
                total_steps_per_store=total_steps_per_store,
                current_store_uid=store_uid,
                errors=errors,
            ),
            current_stage="strategy_refresh",
        )
    result = {
        "ok": len(error_rows) == 0,
        "stores": results,
        "errors": error_rows,
        "stores_skipped": error_rows,
        "store_statuses": _progress_store_statuses_by_steps(
            target_store_uids=target_store_uids,
            per_store_done=per_store_done,
            total_steps_per_store=total_steps_per_store,
            errors=errors,
        ),
        "progress_percent": 100,
    }
    _update_progress(
        run_id,
        progress_percent=100,
        message="Стратегия обновлена",
        store_statuses=result["store_statuses"],
        current_stage="strategy_refresh",
    )
    return result


async def _run_catalog_daily_refresh(*, store_uids: list[str] | None = None, run_id: int | None = None) -> dict[str, Any]:
    _update_progress(run_id, progress_percent=5, message="Каталог: запуск", current_stage="catalog_daily_refresh")
    result = refresh_pricing_catalog_trees_from_sources()
    if asyncio.iscoroutine(result):
        result = await result
    target_store_uids = [str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()] or [
        str(store.get("store_uid") or "").strip() for store in _stores_context_by_platform("yandex_market") if str(store.get("store_uid") or "").strip()
    ]
    out = {
        **(result if isinstance(result, dict) else {"result": result}),
        "store_statuses": [{"store_uid": uid, "status": "success", "message": "", "progress_percent": 100} for uid in target_store_uids],
        "progress_percent": 100,
    }
    _update_progress(run_id, progress_percent=100, message="Каталог обновлен", store_statuses=out["store_statuses"], current_stage="catalog_daily_refresh")
    return out


async def _run_pricing_hourly_cycle() -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    for code in (
        "cogs_hourly_snapshot",
        "today_orders_30m_refresh",
        "sales_reports_hourly_refresh",
        "shelfs_statistics_refresh",
        "shows_boost_refresh",
    ):
        result = await _run_job_body(code)
        steps.append({"job_code": code, "result": result})
    return {"steps": steps}


async def _run_job_body(job_code: str, *, run_id: int | None = None) -> dict[str, Any]:
    code = str(job_code or "").strip()
    job_row = next((row for row in get_refresh_jobs() if str(row.get("job_code") or "").strip() == code), {})
    meta = JOB_META.get(code, {})
    selected_store_uids = _selected_store_uids(job_row, platform=str(meta.get("platform") or "")) or _all_yandex_store_uids()
    runners: dict[str, Callable[[], Any]] = {
        "pricing_hourly_cycle": _run_pricing_hourly_cycle,
        "cogs_hourly_snapshot": lambda: _run_cogs_hourly_snapshot(store_uids=selected_store_uids, run_id=run_id),
        "prices_refresh": lambda: _run_prices_refresh(store_uids=selected_store_uids, run_id=run_id),
        "today_orders_30m_refresh": lambda: _run_today_orders_refresh(store_uids=selected_store_uids, run_id=run_id),
        "sales_reports_hourly_refresh": lambda: _run_sales_reports_refresh(store_uids=selected_store_uids, run_id=run_id),
        "shelfs_statistics_refresh": lambda: _run_shelfs_statistics_refresh(store_uids=selected_store_uids, run_id=run_id),
        "shows_boost_refresh": lambda: _run_shows_boost_refresh(store_uids=selected_store_uids, run_id=run_id),
        "boost_refresh": lambda: _run_boost_refresh(run_id=run_id, store_uids=selected_store_uids),
        "attractiveness_refresh": lambda: _run_attractiveness_refresh(run_id=run_id, store_uids=selected_store_uids),
        "promos_refresh": lambda: _run_promos_refresh(run_id=run_id, store_uids=selected_store_uids),
        "coinvest_refresh": lambda: _run_coinvest_refresh(run_id=run_id, store_uids=selected_store_uids),
        "strategy_refresh": lambda: _run_strategy_refresh(run_id=run_id, store_uids=selected_store_uids),
        "catalog_daily_refresh": lambda: _run_catalog_daily_refresh(store_uids=selected_store_uids, run_id=run_id),
    }
    runner = runners.get(code)
    if runner is None:
        raise ValueError(f"Неизвестный job_code: {code}")
    result = runner()
    if asyncio.iscoroutine(result):
        return await result
    return result


async def run_refresh_job(job_code: str, *, trigger_source: str = "manual", run_id: int | None = None) -> dict[str, Any]:
    code = str(job_code or "").strip()
    lock = _job_lock(code)
    if not lock.acquire(blocking=False):
        if str(trigger_source or "").strip().lower().startswith("scheduler"):
            return {"ok": True, "job_code": code, "skipped": True, "reason": "already_running"}
        raise RuntimeError(f"Задача {code} уже выполняется")
    local_run_id = run_id or create_refresh_job_run(job_code=code, trigger_source=trigger_source, meta={})
    _mark_run_running(local_run_id)
    try:
        result = await _run_job_body(code, run_id=local_run_id)
        finish_refresh_job_run(run_id=local_run_id, status="success", message="ok", meta=result if isinstance(result, dict) else {"result": result})
        return {"ok": True, "job_code": code, "run_id": local_run_id, "result": result}
    except Exception as exc:
        finish_refresh_job_run(run_id=local_run_id, status="error", message=str(exc), meta={"error": str(exc)})
        raise
    finally:
        _clear_tracked_run(local_run_id)
        _refresh_monitoring_page_snapshots()
        lock.release()


async def run_refresh_all(
    *,
    trigger_source: str = "manual",
    run_id: int | None = None,
    precreated_run_ids: dict[str, int] | None = None,
) -> dict[str, Any]:
    run_id = run_id or create_refresh_job_run(job_code="run_all", trigger_source=trigger_source, meta={"progress_percent": 0})
    _mark_run_running(run_id)
    steps: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total_phases = 5
    child_run_ids = {
        str(code).strip(): int(child_id or 0)
        for code, child_id in (precreated_run_ids or {}).items()
        if str(code).strip()
    }

    def _set_phase_progress(phase_index: int, stage: str, message: str) -> None:
        update_refresh_job_run(
            run_id=run_id,
            status="running",
            message=message,
            meta={
                "progress_percent": int(((phase_index - 1) / total_phases) * 100),
                "current_stage": stage,
                "steps": steps,
                "errors": errors,
            },
        )

    try:
        try:
            _set_phase_progress(1, "cogs_hourly_snapshot", "Этап: cogs_hourly_snapshot")
            result = await run_refresh_job(
                "cogs_hourly_snapshot",
                trigger_source=trigger_source,
                run_id=child_run_ids.get("cogs_hourly_snapshot") or None,
            )
            steps.append({"job_code": "cogs_hourly_snapshot", "result": result.get("result")})
        except Exception as exc:
            errors.append({"job_code": "cogs_hourly_snapshot", "error": str(exc)})

        try:
            _set_phase_progress(2, "today_orders_30m_refresh", "Этап: today_orders_30m_refresh")
            result = await run_refresh_job(
                "today_orders_30m_refresh",
                trigger_source=trigger_source,
                run_id=child_run_ids.get("today_orders_30m_refresh") or None,
            )
            steps.append({"job_code": "today_orders_30m_refresh", "result": result.get("result")})
        except Exception as exc:
            errors.append({"job_code": "today_orders_30m_refresh", "error": str(exc)})

        _set_phase_progress(3, "reports_bundle", "Этап: отчеты продаж / полки / буст показов")
        report_codes = [
            "sales_reports_hourly_refresh",
            "shelfs_statistics_refresh",
            "shows_boost_refresh",
        ]
        report_results = await asyncio.gather(
            *[
                run_refresh_job(
                    code,
                    trigger_source=trigger_source,
                    run_id=child_run_ids.get(code) or None,
                )
                for code in report_codes
            ],
            return_exceptions=True,
        )
        for code, result in zip(report_codes, report_results):
            if isinstance(result, Exception):
                errors.append({"job_code": code, "error": str(result)})
            else:
                steps.append({"job_code": code, "result": result.get("result")})

        for phase_index, code in enumerate(
            ["strategy_refresh", "catalog_daily_refresh"],
            start=4,
        ):
            _set_phase_progress(phase_index, code, f"Этап: {code}")
            try:
                result = await run_refresh_job(
                    code,
                    trigger_source=trigger_source,
                    run_id=child_run_ids.get(code) or None,
                )
                steps.append({"job_code": code, "result": result.get("result")})
            except Exception as exc:
                errors.append({"job_code": code, "error": str(exc)})

        final = {"ok": len(errors) == 0, "steps": steps, "errors": errors, "progress_percent": 100}
        finish_refresh_job_run(run_id=run_id, status="success" if not errors else "error", message="ok" if not errors else "Завершено с ошибками", meta=final)
        return {"ok": len(errors) == 0, "run_id": run_id, "steps": steps, "errors": errors}
    finally:
        _clear_tracked_run(run_id)
        _refresh_monitoring_page_snapshots()


def _spawn_async_job(coro: Callable[[], Any]) -> None:
    def _runner() -> None:
        asyncio.run(coro())
    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()


def start_refresh_job(job_code: str, *, trigger_source: str = "manual") -> dict[str, Any]:
    code = str(job_code or "").strip()
    _ensure_manual_queue_worker()
    _recover_stale_job_run(code)
    if trigger_source != "manual" and _job_has_active_or_queued_run(code):
        latest = get_refresh_job_runs_latest().get(code) or {}
        return {
            "ok": True,
            "job_code": code,
            "started": False,
            "queued": False,
            "skipped": True,
            "reason": "already_running",
            "run_id": int(latest.get("run_id") or 0) or None,
        }
    if trigger_source == "manual":
        if _job_has_active_or_queued_run(code):
            latest = get_refresh_job_runs_latest().get(code) or {}
            return {
                "ok": True,
                "job_code": code,
                "started": False,
                "queued": False,
                "skipped": True,
                "reason": "already_running",
                "run_id": int(latest.get("run_id") or 0) or None,
            }
        run_id = create_refresh_job_run(
            job_code=code,
            trigger_source=f"{trigger_source}:queued",
            status="queued",
            message="В очереди",
            meta={"progress_percent": 0, "current_stage": code},
        )
        _register_queued_run(run_id)
        with _QUEUE_LOCK:
            _MANUAL_QUEUE.append({"mode": "job", "job_code": code, "run_id": run_id, "trigger_source": trigger_source})
            _QUEUE_EVENT.set()
        _refresh_monitoring_page_snapshots()
        return {"ok": True, "job_code": code, "started": True, "queued": True, "run_id": run_id}
    _spawn_async_job(lambda: run_refresh_job(code, trigger_source=trigger_source))
    _refresh_monitoring_page_snapshots()
    return {"ok": True, "job_code": code, "started": True}


def start_refresh_all(*, trigger_source: str = "manual") -> dict[str, Any]:
    _ensure_manual_queue_worker()
    run_id = create_refresh_job_run(
        job_code="run_all",
        trigger_source=f"{trigger_source}:queued",
        status="queued",
        message="В очереди",
        meta={"progress_percent": 0, "current_stage": "cogs_hourly_snapshot"},
    )
    _register_queued_run(run_id)
    child_run_ids: dict[str, int] = {}
    for code in FULL_CYCLE_JOB_CODES:
        child_run_id = create_refresh_job_run(
            job_code=code,
            trigger_source=f"{trigger_source}:queued",
            status="queued",
            message="В очереди",
            meta={"progress_percent": 0, "current_stage": code},
        )
        _register_queued_run(child_run_id)
        child_run_ids[code] = child_run_id
    with _QUEUE_LOCK:
        _MANUAL_QUEUE.append(
            {
                "mode": "run_all",
                "run_id": run_id,
                "trigger_source": trigger_source,
                "child_run_ids": child_run_ids,
            }
        )
        _QUEUE_EVENT.set()
    _refresh_monitoring_page_snapshots()
    return {"ok": True, "started": True, "queued": True, "run_id": run_id, "child_run_ids": child_run_ids}


def get_refresh_monitoring_snapshot() -> dict[str, Any]:
    ensure_refresh_jobs_defaults()
    _reset_stale_report_job_ranges()
    jobs = [
        row
        for row in get_refresh_jobs()
        if str(row.get("job_code") or "").strip() not in HIDDEN_MONITORING_JOB_CODES
    ]
    latest_runs = get_refresh_job_runs_latest()
    latest_success_runs = get_refresh_job_runs_latest_success()
    run_all = latest_runs.get("run_all") or {}
    order_map = {str(item["job_code"]): idx for idx, item in enumerate(JOB_DEFAULTS)}
    jobs.sort(key=lambda row: order_map.get(str(row.get("job_code") or "").strip(), 10_000))
    rows: list[dict[str, Any]] = []
    platform_stores: dict[str, list[dict[str, Any]]] = {}
    for store in _catalog_marketplace_stores_context():
        platform = str(store.get("platform") or "").strip().lower()
        store_uid = str(store.get("store_uid") or "").strip()
        if not platform or not store_uid:
            continue
        platform_stores.setdefault(platform, []).append(
            {
                "store_uid": store_uid,
                "store_id": str(store.get("store_id") or "").strip(),
                "store_name": str(store.get("store_name") or store.get("label") or store.get("store_id") or "").strip(),
                "platform": platform,
                "platform_label": str(store.get("platform_label") or platform).strip(),
            }
        )
    for job in jobs:
        code = str(job.get("job_code") or "").strip()
        display_date_from, display_date_to = _monitoring_job_date_range(code, job)
        run = latest_runs.get(code) or {}
        run_id = int(run.get("run_id") or 0) if run else 0
        raw_status = str(run.get("status") or "").strip()
        orphaned = raw_status.lower() in {"running", "queued"} and not _is_run_tracked(run_id, raw_status)
        normalized_status = _snapshot_status(job_code=code, status=raw_status, started_at=str(run.get("started_at") or "").strip())
        if orphaned:
            normalized_status = "error"
        normalized_message = _snapshot_message(
            job_code=code,
            status=raw_status,
            started_at=str(run.get("started_at") or "").strip(),
            message=str(run.get("message") or "").strip(),
        )
        if orphaned:
            normalized_message = "Прервано при перезапуске"
        stale_active = normalized_status == "error" and raw_status.lower() in {"running", "queued"}
        meta = JOB_META.get(code, {})
        try:
            selected_store_uids = json.loads(str(job.get("stores_json") or "[]") or "[]")
        except Exception:
            selected_store_uids = []
        try:
            last_meta = json.loads(str(run.get("meta_json") or "{}") or "{}")
        except Exception:
            last_meta = {}
        freshness = _job_freshness_snapshot(
            job_row=job,
            job_code=code,
            success_run=latest_success_runs.get(code) or {},
        )
        rows.append(
            {
                **job,
                "date_from": display_date_from,
                "date_to": display_date_to,
                "kind": str(meta.get("kind") or "system"),
                "platform": str(meta.get("platform") or "pricing"),
                "supports_store_selection": bool(meta.get("supports_store_selection")),
                "selected_store_uids": [str(x or "").strip() for x in selected_store_uids if str(x or "").strip()],
                "store_statuses": list(last_meta.get("store_statuses") or []) if isinstance(last_meta, dict) else [],
                "last_started_at": str(run.get("started_at") or "").strip(),
                "last_finished_at": str(run.get("finished_at") or "").strip(),
                "last_status": normalized_status,
                "last_message": normalized_message,
                "last_run_id": run_id,
                "progress_percent": _derive_progress_percent(last_meta, terminal=normalized_status in {"success", "error"}),
                "current_stage": str(last_meta.get("current_stage") or "").strip() if isinstance(last_meta, dict) else "",
                **freshness,
            }
        )
    try:
        run_all_meta = json.loads(str(run_all.get("meta_json") or "{}") or "{}")
    except Exception:
        run_all_meta = {}
    run_all_id = int(run_all.get("run_id") or 0) if run_all else 0
    run_all_raw_status = str(run_all.get("status") or "").strip()
    run_all_orphaned = run_all_raw_status.lower() in {"running", "queued"} and not _is_run_tracked(run_all_id, run_all_raw_status)
    run_all_status = _snapshot_status(
        job_code="run_all",
        status=run_all_raw_status,
        started_at=str(run_all.get("started_at") or "").strip(),
    )
    if run_all_orphaned:
        run_all_status = "error"
    run_all_message = _snapshot_message(
        job_code="run_all",
        status=run_all_raw_status,
        started_at=str(run_all.get("started_at") or "").strip(),
        message=str(run_all.get("message") or "").strip(),
    )
    if run_all_orphaned:
        run_all_message = "Прервано при перезапуске"
    return {
        "ok": True,
        "rows": rows,
        "platform_stores": platform_stores,
        "run_all": {
            "last_started_at": str(run_all.get("started_at") or "").strip(),
            "last_finished_at": str(run_all.get("finished_at") or "").strip(),
            "last_status": run_all_status,
            "last_message": run_all_message,
            "progress_percent": _derive_progress_percent(run_all_meta, terminal=run_all_status in {"success", "error"}),
            "current_stage": str(run_all_meta.get("current_stage") or "").strip() if isinstance(run_all_meta, dict) else "",
        },
    }


def configure_refresh_scheduler() -> None:
    scheduler = REFRESH_SCHEDULER
    if scheduler is None:
        return
    ensure_refresh_jobs_defaults()
    jobs = get_refresh_jobs()
    known_ids = {f"refresh_orchestrator:{row['job_code']}" for row in jobs}
    for job in scheduler.get_jobs():
        if str(job.id).startswith("refresh_orchestrator:") and job.id not in known_ids:
            scheduler.remove_job(job.id)
    for row in jobs:
        code = str(row.get("job_code") or "").strip()
        job_id = f"refresh_orchestrator:{code}"
        enabled = bool(int(row.get("enabled") or 0))
        if not enabled or code not in SCHEDULER_ROOT_JOB_CODES:
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
            continue
        schedule_kind = str(row.get("schedule_kind") or "interval").strip().lower()
        if schedule_kind == "daily":
            raw_time = str(row.get("time_of_day") or "00:00").strip() or "00:00"
            hour_s, minute_s = (raw_time.split(":", 1) + ["00"])[:2]
            scheduler.add_job(
                _run_refresh_job_sync,
                trigger="cron",
                hour=int(hour_s or 0),
                minute=int(minute_s or 0),
                id=job_id,
                replace_existing=True,
                kwargs={"job_code": code, "trigger_source": "scheduler"},
            )
        else:
            minutes = max(1, int(row.get("interval_minutes") or 60))
            offset_minutes = JOB_INTERVAL_OFFSETS_MINUTES.get(code, 0)
            scheduler.add_job(
                _run_refresh_job_sync,
                trigger="interval",
                minutes=minutes,
                start_date=_aligned_interval_start(minutes, offset_minutes=offset_minutes),
                id=job_id,
                replace_existing=True,
                kwargs={"job_code": code, "trigger_source": "scheduler"},
            )


def _run_refresh_job_sync(*, job_code: str, trigger_source: str = "scheduler") -> None:
    asyncio.run(run_refresh_job(job_code, trigger_source=trigger_source))
