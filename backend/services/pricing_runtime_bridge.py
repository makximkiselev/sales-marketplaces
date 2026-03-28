from __future__ import annotations


async def refresh_attractiveness_data(*args, **kwargs):
    from backend.services.pricing_attractiveness_service import refresh_attractiveness_data as impl
    return await impl(*args, **kwargs)


async def refresh_boost_data(*args, **kwargs):
    from backend.services.pricing_boost_service import refresh_boost_data as impl
    return await impl(*args, **kwargs)


def get_cbr_usd_rub_rate_for_date(*args, **kwargs):
    from backend.services.pricing_prices_service import _get_cbr_usd_rub_rate_for_date as impl
    return impl(*args, **kwargs)


async def get_prices_context(*args, **kwargs):
    from backend.services.pricing_prices_service import get_prices_context as impl
    return await impl(*args, **kwargs)


async def get_prices_overview(*args, **kwargs):
    from backend.services.pricing_prices_service import get_prices_overview as impl
    return await impl(*args, **kwargs)


async def get_prices_tree(*args, **kwargs):
    from backend.services.pricing_prices_service import get_prices_tree as impl
    return await impl(*args, **kwargs)


def build_market_promo_details(*args, **kwargs):
    from backend.services.pricing_promos_service import build_market_promo_details as impl
    return impl(*args, **kwargs)


async def refresh_promos_data(*args, **kwargs):
    from backend.services.pricing_promos_service import refresh_promos_data as impl
    return await impl(*args, **kwargs)


def profit_for_price_with_ads_rate(*args, **kwargs):
    from backend.services.pricing_boost_service import _profit_for_price_with_ads_rate as impl
    return impl(*args, **kwargs)


def target_met(*args, **kwargs):
    from backend.services.pricing_boost_service import _target_met as impl
    return impl(*args, **kwargs)


async def refresh_sales_coinvest_data(*args, **kwargs):
    from backend.services.sales_coinvest_service import refresh_sales_coinvest_data as impl
    return await impl(*args, **kwargs)
