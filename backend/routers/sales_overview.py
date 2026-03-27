from __future__ import annotations

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
from backend.services.store_data_model import get_pricing_store_settings, upsert_pricing_store_settings

router = APIRouter()


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
        return await get_sales_overview_history(
            page=page,
            page_size=page_size,
            store_id=store_id,
            item_status=item_status,
            period=period,
            date_from=date_from,
            date_to=date_to,
        )
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
        return await get_sales_overview_problem_orders(
            page=page,
            page_size=page_size,
            store_id=store_id,
            period=period,
            date_from=date_from,
            date_to=date_to,
        )
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


@router.post("/api/sales/overview/united-orders/refresh")
async def sales_overview_united_orders_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    date_from = str(body.get("date_from") or "").strip() or "2025-07-01"
    date_to = str(body.get("date_to") or "").strip()
    try:
        return await refresh_sales_overview_history(date_from=date_from, date_to=date_to)
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
        return await refresh_sales_overview_order_rows_current_month_for_store(store_uid=f"yandex_market:{store_id}")
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить текущий месяц продаж: {exc}"}, status_code=500)


@router.post("/api/sales/overview/today/refresh")
async def sales_overview_today_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    store_id = str(body.get("store_id") or "").strip()
    if not store_id:
        return JSONResponse({"ok": False, "message": "store_id обязателен"}, status_code=400)
    try:
        return await refresh_sales_overview_order_rows_today_for_store(store_uid=f"yandex_market:{store_id}")
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить заказы дня: {exc}"}, status_code=500)


@router.post("/api/sales/overview/united-netting/refresh")
async def sales_overview_united_netting_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    date_from = str(body.get("date_from") or "").strip() or "2025-07-01"
    date_to = str(body.get("date_to") or "").strip() or "2026-03-11"
    try:
        return await refresh_sales_united_netting_history(date_from=date_from, date_to=date_to)
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
