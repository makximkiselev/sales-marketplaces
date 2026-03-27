from __future__ import annotations

from fastapi import APIRouter

from backend.services.pricing_promos_service import (
    get_promos_context,
    get_promos_overview,
    get_promos_tree,
    refresh_promos_data,
)

router = APIRouter()


@router.get("/api/pricing/promos/context")
async def pricing_promos_context():
    return await get_promos_context()


@router.get("/api/pricing/promos/tree")
async def pricing_promos_tree(
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
):
    return await get_promos_tree(
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
    )


@router.get("/api/pricing/promos/overview")
async def pricing_promos_overview(
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    stock_filter: str = "all",
    page: int = 1,
    page_size: int = 200,
):
    return await get_promos_overview(
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


@router.post("/api/pricing/promos/refresh")
async def pricing_promos_refresh():
    return await refresh_promos_data()
