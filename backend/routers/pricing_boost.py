from __future__ import annotations

from fastapi import APIRouter

from backend.services.pricing_boost_service import (
    get_boost_context,
    get_boost_overview,
    get_boost_tree,
    refresh_boost_data,
)

router = APIRouter()


@router.get("/api/pricing/boost/context")
async def pricing_boost_context():
    return await get_boost_context()


@router.get("/api/pricing/boost/tree")
async def pricing_boost_tree(
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
):
    return await get_boost_tree(
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
    )


@router.get("/api/pricing/boost/overview")
async def pricing_boost_overview(
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    stock_filter: str = "all",
    report_date: str = "",
    page: int = 1,
    page_size: int = 200,
):
    return await get_boost_overview(
        scope=scope,
        platform=platform,
        store_id=store_id,
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        category_path=category_path,
        search=search,
        stock_filter=stock_filter,
        report_date=report_date,
        page=page,
        page_size=page_size,
    )


@router.post("/api/pricing/boost/refresh")
async def pricing_boost_refresh():
    return await refresh_boost_data()
