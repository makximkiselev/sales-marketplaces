from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.services.sales_coinvest_service import (
    get_sales_coinvest_context,
    get_sales_coinvest_overview,
    get_sales_coinvest_tree,
    refresh_sales_coinvest_data,
    save_sales_coinvest_promo_adjustments,
)

router = APIRouter()


@router.get("/api/sales/coinvest/context")
async def sales_coinvest_context():
    return await get_sales_coinvest_context()


@router.get("/api/sales/coinvest/tree")
async def sales_coinvest_tree(
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    stock_filter: str = "all",
):
    return await get_sales_coinvest_tree(
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
        stock_filter=stock_filter,
    )


@router.get("/api/sales/coinvest/overview")
async def sales_coinvest_overview(
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    page: int = 1,
    page_size: int = 200,
    period: str = "today",
    date_from: str = "",
    date_to: str = "",
    stock_filter: str = "all",
):
    try:
        return await get_sales_coinvest_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            page=page,
            page_size=page_size,
            period=period,
            date_from=date_from,
            date_to=date_to,
            stock_filter=stock_filter,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)


@router.post("/api/sales/coinvest/refresh")
async def sales_coinvest_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    mode = str(body.get("mode") or "recent").strip().lower()
    manual = bool(body.get("manual"))
    try:
        return await refresh_sales_coinvest_data(mode=mode, manual=manual)
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить соинвест: {exc}"}, status_code=500)


@router.post("/api/sales/coinvest/promo-adjustments")
async def sales_coinvest_promo_adjustments(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    store_uid = str(body.get("store_uid") or "").strip()
    rows = body.get("rows") if isinstance(body.get("rows"), list) else []
    if not store_uid:
        return JSONResponse({"ok": False, "message": "store_uid обязателен"}, status_code=400)
    try:
        return await save_sales_coinvest_promo_adjustments(store_uid=store_uid, rows=rows)
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось сохранить скидки акций: {exc}"}, status_code=500)
