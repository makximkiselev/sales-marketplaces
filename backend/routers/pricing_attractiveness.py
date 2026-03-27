from __future__ import annotations

from fastapi import APIRouter

from backend.services.pricing_attractiveness_service import (
    get_attractiveness_context,
    get_attractiveness_overview,
    get_attractiveness_tree,
    refresh_attractiveness_data,
)

router = APIRouter()


@router.get("/api/pricing/attractiveness/context")
async def pricing_attractiveness_context():
    return await get_attractiveness_context()


@router.get("/api/pricing/attractiveness/tree")
async def pricing_attractiveness_tree(
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
):
    return await get_attractiveness_tree(
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
    )


@router.get("/api/pricing/attractiveness/overview")
async def pricing_attractiveness_overview(
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    status_filter: str = "all",
    stock_filter: str = "all",
    page: int = 1,
    page_size: int = 50,
):
    return await get_attractiveness_overview(
        scope=scope,
        platform=platform,
        store_id=store_id,
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        category_path=category_path,
        search=search,
        status_filter=status_filter,
        stock_filter=stock_filter,
        page=page,
        page_size=page_size,
    )


@router.post("/api/pricing/attractiveness/refresh")
async def pricing_attractiveness_refresh():
    return await refresh_attractiveness_data()
