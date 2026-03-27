from __future__ import annotations

import datetime
from typing import Any

from backend.services.pricing_prices_service import (
    _get_cbr_usd_rub_rate_for_date,
    get_prices_context,
    get_prices_overview,
    get_prices_tree,
)
from backend.services.sales_elasticity_service import (
    _build_sales_metrics_map,
    _period_bounds,
    refresh_sales_elasticity_data,
)
from backend.services.store_data_model import (
    get_active_pricing_promo_campaigns,
    get_fx_rates_cache,
    get_pricing_promo_coinvest_settings_map,
    get_pricing_promo_coinvest_settings,
    get_pricing_promo_offer_results_map,
    get_pricing_strategy_results_map,
    get_yandex_goods_price_report_map,
    upsert_pricing_promo_coinvest_settings,
)
from backend.services.yandex_goods_prices_report_service import refresh_yandex_goods_prices_report_for_store


def _to_num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _convert_price_between_currencies(
    value: float | None,
    *,
    from_currency: str,
    to_currency: str,
    calc_date: datetime.date,
) -> float | None:
    if value in (None, 0):
        return value
    source = str(from_currency or "RUB").strip().upper() or "RUB"
    target = str(to_currency or "RUB").strip().upper() or "RUB"
    if source == target:
        return float(value)
    rate = _get_pricing_fx_usd_rub_rate_for_date(calc_date)
    if not rate or rate <= 0:
        return float(value)
    if source == "USD" and target in {"RUB", "RUR"}:
        return float(value) * float(rate)
    if source in {"RUB", "RUR"} and target == "USD":
        return float(value) / float(rate)
    return float(value)


def _get_pricing_fx_usd_rub_rate_for_date(calc_date: datetime.date) -> float | None:
    key = calc_date.isoformat()
    try:
        cached = get_fx_rates_cache(source="cbr", pair="USD_RUB")
        rows = cached.get("rows") if isinstance(cached, dict) else None
        if isinstance(rows, list) and rows:
            by_date: dict[str, float] = {}
            for row in rows:
                rate_date = str(row.get("date") or "").strip()
                try:
                    rate_value = float(row.get("rate"))
                except Exception:
                    continue
                if rate_date and rate_value > 0:
                    by_date[rate_date] = rate_value
            if by_date:
                best_date = max(by_date.keys())
                return float(by_date[best_date])
    except Exception:
        return None
    return None


def _resolve_goods_report_currency(*, store_currency: str, report_currency: str, on_display_price: float | None) -> str:
    store_code = str(store_currency or "RUB").strip().upper() or "RUB"
    report_code = str(report_currency or store_code).strip().upper() or store_code
    if store_code == "USD" and on_display_price not in (None, 0):
        return "RUB"
    return report_code


def _summary_round(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _promo_matches_installed_price(*, installed_price: float | None, offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if installed_price in (None, 0) or not offers:
        return []
    matched: list[dict[str, Any]] = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        fit_mode = str(offer.get("promo_fit_mode") or "").strip().lower()
        if fit_mode not in {"with_ads", "without_ads"}:
            continue
        promo_price = _to_num(offer.get("promo_price"))
        if promo_price in (None, 0):
            continue
        if float(installed_price) <= float(promo_price) + 1.0:
            matched.append(offer)
    return matched


async def _load_all_filtered_price_rows(
    *,
    scope: str,
    platform: str,
    store_id: str,
    tree_source_store_id: str,
    category_path: str,
    search: str,
    stock_filter: str,
    total_count: int,
) -> list[dict[str, Any]]:
    if total_count <= 0:
        return []
    page_size = min(500, max(1, total_count))
    page = 1
    loaded_rows: list[dict[str, Any]] = []
    while len(loaded_rows) < total_count:
        resp = await get_prices_overview(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_mode="marketplaces",
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter,
            page=page,
            page_size=page_size,
            force_refresh=False,
        )
        chunk = resp.get("rows") if isinstance(resp, dict) and isinstance(resp.get("rows"), list) else []
        if not chunk:
            break
        loaded_rows.extend(chunk)
        if len(chunk) < page_size:
            break
        page += 1
        if page > 500:
            break
    return loaded_rows


async def get_sales_coinvest_context():
    ctx = await get_prices_context()
    stores = ctx.get("marketplace_stores") if isinstance(ctx, dict) and isinstance(ctx.get("marketplace_stores"), list) else []
    promo_adjustments_by_store: dict[str, list[dict[str, Any]]] = {}
    now_msk = datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=3))).isoformat()
    for store in stores:
        suid = str(store.get("store_uid") or "").strip()
        if not suid:
            continue
        promo_columns = get_active_pricing_promo_campaigns(store_uid=suid, as_of=now_msk)
        saved = {
            str(row.get("promo_id") or "").strip(): row
            for row in get_pricing_promo_coinvest_settings(store_uid=suid)
            if str(row.get("promo_id") or "").strip()
        }
        promo_adjustments_by_store[suid] = [
            {
                "promo_id": str(item.get("promo_id") or "").strip(),
                "promo_name": str(item.get("promo_name") or "").strip() or str(item.get("promo_id") or "").strip(),
                "max_discount_percent": _to_num((saved.get(str(item.get("promo_id") or "").strip()) or {}).get("max_discount_percent")),
            }
            for item in promo_columns
            if str(item.get("promo_id") or "").strip()
        ]
    return {**ctx, "promo_adjustments_by_store": promo_adjustments_by_store}


async def get_sales_coinvest_tree(
    *,
    tree_source_store_id: str = "",
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    stock_filter: str = "all",
):
    return await get_prices_tree(
        tree_mode="marketplaces",
        tree_source_store_id=tree_source_store_id,
        scope=scope,
        platform=platform,
        store_id=store_id,
    )


async def get_sales_coinvest_overview(
    *,
    scope: str = "all",
    platform: str = "",
    store_id: str = "",
    tree_source_store_id: str = "",
    category_path: str = "",
    search: str = "",
    page: int = 1,
    page_size: int = 200,
    period: str = "month",
    date_from: str = "",
    date_to: str = "",
    stock_filter: str = "all",
) -> dict[str, Any]:
    base = await get_prices_overview(
        scope=scope,
        platform=platform,
        store_id=store_id,
        tree_mode="marketplaces",
        tree_source_store_id=tree_source_store_id,
        category_path=category_path,
        search=search,
        stock_filter=stock_filter,
        page=page,
        page_size=page_size,
        force_refresh=False,
    )
    rows = base.get("rows") if isinstance(base, dict) and isinstance(base.get("rows"), list) else []
    total_count = int(base.get("total_count") or len(rows) or 0)
    summary_rows = rows
    if total_count > len(rows):
        summary_rows = await _load_all_filtered_price_rows(
            scope=scope,
            platform=platform,
            store_id=store_id,
            tree_source_store_id=tree_source_store_id,
            category_path=category_path,
            search=search,
            stock_filter=stock_filter,
            total_count=total_count,
        )
    stores = base.get("stores") if isinstance(base, dict) and isinstance(base.get("stores"), list) else []
    if not stores:
        return {**base, "summary": {"catalog_items_count": 0, "report_coverage_percent": None}, "series": [], "comparison_enabled": False}

    today = datetime.date.today()
    if any(str(store.get("currency_code") or "RUB").strip().upper() == "USD" for store in stores):
        try:
            await _get_cbr_usd_rub_rate_for_date(today)
        except Exception:
            pass
    current_from, current_to, _, _ = _period_bounds(period=period, today=today, date_from=date_from, date_to=date_to)
    store_uid_map = {
        str(store.get("store_uid") or "").strip(): store
        for store in stores
        if str(store.get("store_uid") or "").strip()
    }
    store_uids = list(store_uid_map.keys())
    page_skus = [str(row.get("sku") or "").strip() for row in rows if str(row.get("sku") or "").strip()]
    summary_skus = [str(row.get("sku") or "").strip() for row in summary_rows if str(row.get("sku") or "").strip()]
    skus = list(dict.fromkeys([*summary_skus, *page_skus]))
    strategy_map = get_pricing_strategy_results_map(store_uids=store_uids, skus=skus)
    report_map = get_yandex_goods_price_report_map(store_uids=store_uids, skus=skus)
    promo_offer_map = get_pricing_promo_offer_results_map(store_uids=store_uids, skus=skus)
    promo_discount_settings_map = get_pricing_promo_coinvest_settings_map(store_uids=store_uids)
    sales_map = _build_sales_metrics_map(
        stores=stores,
        skus=skus,
        date_from=current_from,
        date_to=current_to,
    )

    out_rows: list[dict[str, Any]] = []

    for row in rows:
        sku = str(row.get("sku") or "").strip()
        installed_price_by_store: dict[str, float | None] = {}
        effective_coinvest_percent_by_store: dict[str, float | None] = {}
        effective_on_display_price_by_store: dict[str, float | None] = {}
        promo_extra_discount_percent_by_store: dict[str, float | None] = {}
        total_coinvest_percent_by_store: dict[str, float | None] = {}
        total_on_display_price_by_store: dict[str, float | None] = {}
        by_store: list[dict[str, Any]] = []

        for suid, store_meta in store_uid_map.items():
            strategy_summary = ((strategy_map.get(suid) or {}).get(sku) or {}) if isinstance(strategy_map.get(suid), dict) else {}
            report_summary = ((report_map.get(suid) or {}).get(sku) or {}) if isinstance(report_map.get(suid), dict) else {}
            promo_offers = list(((promo_offer_map.get(suid) or {}).get(sku) or [])) if isinstance(promo_offer_map.get(suid), dict) else []
            sale_item = ((sales_map.get(suid) or {}).get(sku) or {}) if isinstance(sales_map.get(suid), dict) else {}
            promo_discount_settings = (promo_discount_settings_map.get(suid) or {}) if isinstance(promo_discount_settings_map.get(suid), dict) else {}

            installed_price = _to_num(strategy_summary.get("installed_price"))
            installed_price_by_store[suid] = installed_price

            store_currency = str(store_meta.get("currency_code") or "RUB").strip().upper() or "RUB"
            on_display_price = _to_num(report_summary.get("on_display_price"))
            report_currency = _resolve_goods_report_currency(
                store_currency=store_currency,
                report_currency=str(report_summary.get("currency") or store_currency),
                on_display_price=on_display_price,
            )
            fallback_on_display_price = _to_num(sale_item.get("avg_payment_price"))
            effective_on_display_price = on_display_price if on_display_price not in (None, 0) else fallback_on_display_price
            effective_on_display_price_by_store[suid] = effective_on_display_price

            effective_coinvest_percent = None
            installed_price_for_display_currency = _convert_price_between_currencies(
                installed_price,
                from_currency=store_currency,
                to_currency=report_currency,
                calc_date=today,
            )
            if installed_price_for_display_currency not in (None, 0) and on_display_price not in (None, 0):
                effective_coinvest_percent = round(
                    (1.0 - (float(on_display_price) / float(installed_price_for_display_currency))) * 100.0,
                    2,
                )
            else:
                effective_coinvest_percent = _to_num(sale_item.get("avg_coinvest_percent"))
            effective_coinvest_percent_by_store[suid] = effective_coinvest_percent

            matched_promos = _promo_matches_installed_price(installed_price=installed_price, offers=promo_offers)
            promo_extra_discount_percent = None
            if matched_promos:
                promo_discount_values = [
                    value
                    for value in (
                        _to_num((promo_discount_settings.get(str(offer.get("promo_id") or "").strip()) or {}).get("max_discount_percent"))
                        for offer in matched_promos
                    )
                    if value is not None
                ]
                promo_extra_discount_percent = max(promo_discount_values) if promo_discount_values else None
            promo_extra_discount_percent_by_store[suid] = promo_extra_discount_percent

            total_coinvest_percent = effective_coinvest_percent
            if promo_extra_discount_percent not in (None, 0):
                total_coinvest_percent = round(float(total_coinvest_percent or 0.0) + float(promo_extra_discount_percent), 2)
            total_coinvest_percent_by_store[suid] = total_coinvest_percent

            total_on_display_price = effective_on_display_price
            if installed_price_for_display_currency not in (None, 0):
                if total_coinvest_percent not in (None, 0):
                    total_on_display_price = round(
                        float(installed_price_for_display_currency) * (1.0 - (float(total_coinvest_percent) / 100.0)),
                        2,
                    )
                elif effective_on_display_price is None:
                    total_on_display_price = float(installed_price_for_display_currency)
            total_on_display_price_by_store[suid] = total_on_display_price

            by_store.append(
                {
                    "store_uid": suid,
                    "store_id": str(store_meta.get("store_id") or ""),
                    "label": str(store_meta.get("label") or suid),
                    "platform_label": str(store_meta.get("platform_label") or ""),
                    "mentions_count": int(sale_item.get("mentions_count") or 0),
                    "avg_sale_price": _to_num(sale_item.get("avg_sale_price")),
                    "avg_payment_price": _to_num(sale_item.get("avg_payment_price")),
                    "avg_coinvest_percent": _to_num(sale_item.get("avg_coinvest_percent")),
                }
            )

        out_rows.append(
            {
                "sku": sku,
                "name": str(row.get("name") or ""),
                "tree_path": row.get("tree_path") if isinstance(row.get("tree_path"), list) else [],
                "placements": row.get("placements") or {},
                "stock_by_store": row.get("stock_by_store") or {},
                "installed_price_by_store": installed_price_by_store,
                "promo_extra_discount_percent_by_store": promo_extra_discount_percent_by_store,
                "effective_coinvest_percent_by_store": effective_coinvest_percent_by_store,
                "effective_on_display_price_by_store": effective_on_display_price_by_store,
                "total_coinvest_percent_by_store": total_coinvest_percent_by_store,
                "total_on_display_price_by_store": total_on_display_price_by_store,
                "by_store": by_store,
            }
        )

    summary_installed_values: list[float] = []
    summary_on_display_values: list[float] = []
    summary_coinvest_values: list[float] = []
    summary_rows_with_report = 0
    for sku in summary_skus:
        row_has_report = False
        for suid, store_meta in store_uid_map.items():
            strategy_summary = ((strategy_map.get(suid) or {}).get(sku) or {}) if isinstance(strategy_map.get(suid), dict) else {}
            report_summary = ((report_map.get(suid) or {}).get(sku) or {}) if isinstance(report_map.get(suid), dict) else {}
            promo_offers = list(((promo_offer_map.get(suid) or {}).get(sku) or [])) if isinstance(promo_offer_map.get(suid), dict) else []
            sale_item = ((sales_map.get(suid) or {}).get(sku) or {}) if isinstance(sales_map.get(suid), dict) else {}
            promo_discount_settings = (promo_discount_settings_map.get(suid) or {}) if isinstance(promo_discount_settings_map.get(suid), dict) else {}

            installed_price = _to_num(strategy_summary.get("installed_price"))
            store_currency = str(store_meta.get("currency_code") or "RUB").strip().upper() or "RUB"
            on_display_price = _to_num(report_summary.get("on_display_price"))
            report_currency = _resolve_goods_report_currency(
                store_currency=store_currency,
                report_currency=str(report_summary.get("currency") or store_currency),
                on_display_price=on_display_price,
            )
            if on_display_price not in (None, 0):
                row_has_report = True
            effective_on_display_price = on_display_price if on_display_price not in (None, 0) else _to_num(sale_item.get("avg_payment_price"))
            installed_price_for_display_currency = _convert_price_between_currencies(
                installed_price,
                from_currency=store_currency,
                to_currency=report_currency,
                calc_date=today,
            )
            effective_coinvest_percent = None
            if installed_price_for_display_currency not in (None, 0) and on_display_price not in (None, 0):
                effective_coinvest_percent = round(
                    (1.0 - (float(on_display_price) / float(installed_price_for_display_currency))) * 100.0,
                    2,
                )
            else:
                effective_coinvest_percent = _to_num(sale_item.get("avg_coinvest_percent"))

            matched_promos = _promo_matches_installed_price(installed_price=installed_price, offers=promo_offers)
            promo_extra_discount_percent = None
            if matched_promos:
                promo_discount_values = [
                    value
                    for value in (
                        _to_num((promo_discount_settings.get(str(offer.get("promo_id") or "").strip()) or {}).get("max_discount_percent"))
                        for offer in matched_promos
                    )
                    if value is not None
                ]
                promo_extra_discount_percent = max(promo_discount_values) if promo_discount_values else None
            total_coinvest_percent = effective_coinvest_percent
            if promo_extra_discount_percent not in (None, 0):
                total_coinvest_percent = round(float(total_coinvest_percent or 0.0) + float(promo_extra_discount_percent), 2)
            total_on_display_price = effective_on_display_price
            if installed_price_for_display_currency not in (None, 0):
                if total_coinvest_percent not in (None, 0):
                    total_on_display_price = round(
                        float(installed_price_for_display_currency) * (1.0 - (float(total_coinvest_percent) / 100.0)),
                        2,
                    )
                elif effective_on_display_price is None:
                    total_on_display_price = float(installed_price_for_display_currency)

            if installed_price is not None:
                summary_installed_values.append(float(installed_price))
            if total_on_display_price is not None:
                summary_on_display_values.append(float(total_on_display_price))
            if total_coinvest_percent is not None:
                summary_coinvest_values.append(float(total_coinvest_percent))
        if row_has_report:
            summary_rows_with_report += 1

    summary = {
        "catalog_items_count": total_count,
        "report_items_count": summary_rows_with_report,
        "report_coverage_percent": round((summary_rows_with_report / total_count) * 100.0, 2) if total_count > 0 else None,
        "avg_installed_price": _summary_round(summary_installed_values),
        "avg_on_display_price": _summary_round(summary_on_display_values),
        "avg_coinvest_percent": _summary_round(summary_coinvest_values),
    }

    return {
        **base,
        "rows": out_rows,
        "summary": summary,
        "series": [],
        "comparison_enabled": False,
        "period": str(period or "month").strip().lower() or "month",
        "current_from": current_from.isoformat(),
        "current_to": current_to.isoformat(),
    }


async def save_sales_coinvest_promo_adjustments(*, store_uid: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    saved = upsert_pricing_promo_coinvest_settings(store_uid=store_uid, rows=rows)
    return {"ok": True, "store_uid": str(store_uid or "").strip(), "saved": saved}


async def refresh_sales_coinvest_data(
    *,
    mode: str = "recent",
    manual: bool = False,
    refresh_sales: bool = True,
    store_uids: list[str] | None = None,
) -> dict[str, Any]:
    out = (
        await refresh_sales_elasticity_data(mode=mode, manual=manual, store_uids=store_uids)
        if refresh_sales
        else {"ok": True, "mode": mode, "manual": manual}
    )
    ctx = await get_sales_coinvest_context()
    stores = ctx.get("marketplace_stores") if isinstance(ctx, dict) and isinstance(ctx.get("marketplace_stores"), list) else []
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [store for store in stores if str(store.get("store_uid") or "").strip() in selected]
    report_results: list[dict[str, Any]] = []
    report_errors: list[dict[str, Any]] = []
    for store in stores:
        if str(store.get("platform") or "").strip().lower() != "yandex_market":
            continue
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        if not store_uid or not campaign_id:
            continue
        try:
            report_results.append(
                await refresh_yandex_goods_prices_report_for_store(
                    store_uid=store_uid,
                    campaign_id=campaign_id,
                )
            )
        except Exception as exc:
            report_errors.append({"store_uid": store_uid, "store_id": campaign_id, "error": str(exc)})
    if not report_results and report_errors:
        raise RuntimeError(
            "Не удалось получить ни один goods prices report: "
            + "; ".join(f"{row.get('store_id','')}: {row.get('error','')}" for row in report_errors)
        )
    return {**out, "goods_price_reports": report_results, "goods_price_report_errors": report_errors}
