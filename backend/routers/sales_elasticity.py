from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.services.sales_elasticity_service import (
    get_sales_elasticity_context,
    get_sales_elasticity_overview,
    get_sales_elasticity_tree,
    refresh_sales_elasticity_data,
)

router = APIRouter()


@router.get("/api/sales/elasticity/context")
async def sales_elasticity_context():
    return await get_sales_elasticity_context()


@router.get("/api/sales/elasticity/tree")
async def sales_elasticity_tree(
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    stock_filter: str = "all",
):
    return await get_sales_elasticity_tree(
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
        stock_filter=stock_filter,
    )


@router.get("/api/sales/elasticity/overview")
async def sales_elasticity_overview(
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
):
    try:
        return await get_sales_elasticity_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter,
            page=page,
            page_size=page_size,
            period=period,
            date_from=date_from,
            date_to=date_to,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)


@router.post("/api/sales/elasticity/refresh")
async def sales_elasticity_refresh(payload: dict | None = None):
    body = payload if isinstance(payload, dict) else {}
    mode = str(body.get("mode") or "recent").strip().lower()
    manual = bool(body.get("manual"))
    try:
        return await refresh_sales_elasticity_data(mode=mode, manual=manual)
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось обновить продажи Маркета: {exc}"}, status_code=500)
