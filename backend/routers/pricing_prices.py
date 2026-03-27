from __future__ import annotations

from fastapi import APIRouter

from backend.services.pricing_prices_service import (
    get_prices_context,
    get_prices_tree,
    get_prices_overview,
    refresh_prices_data,
)

router = APIRouter()


@router.get("/api/pricing/prices/context")
async def pricing_prices_context():
    return await get_prices_context()


@router.get("/api/pricing/prices/tree")
async def pricing_prices_tree(
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


@router.get("/api/pricing/prices/overview")
async def pricing_prices_overview(
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
    return await get_prices_overview(
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


@router.post("/api/pricing/prices/refresh")
async def pricing_prices_refresh():
    return await refresh_prices_data()
