from __future__ import annotations

from fastapi import APIRouter

from backend.services.refresh_orchestrator_service import run_refresh_job
from backend.services.pricing_strategy_service import (
    get_strategy_context,
    get_strategy_latest,
    get_strategy_overview,
    get_strategy_tree,
)

router = APIRouter()


@router.get("/api/pricing/strategy/context")
async def pricing_strategy_context():
    return await get_strategy_context()


@router.get("/api/pricing/strategy/tree")
async def pricing_strategy_tree(
    tree_mode: str = "marketplaces",
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
):
    return await get_strategy_tree(
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
    )


@router.get("/api/pricing/strategy/overview")
async def pricing_strategy_overview(
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
):
    return await get_strategy_overview(
        scope=scope,
        platform=platform,
        store_id=store_id,
        tree_mode=tree_mode,
        tree_source_store_id=tree_source_store_id,
        category_path=category_path,
        search=search,
        strategy_filter=strategy_filter,
        sales_filter=sales_filter,
        stock_filter=stock_filter,
        sort_key=sort_key,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )


@router.post("/api/pricing/strategy/refresh")
async def pricing_strategy_refresh():
    return await run_refresh_job("strategy_refresh", trigger_source="manual")


@router.get("/api/pricing/decision/latest")
async def pricing_decision_latest(limit: int = 200):
    return await get_strategy_latest(limit=limit)


@router.get("/api/pricing/decision")
async def pricing_decision(limit: int = 200):
    return await get_strategy_latest(limit=limit)
