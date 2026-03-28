from __future__ import annotations

from datetime import date
from typing import Any


def to_num_simple(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def get_pricing_fx_usd_rub_rate_for_date(calc_date: date) -> float | None:
    _ = calc_date
    try:
        from backend.services.store_data_model import get_fx_rates_cache

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


def convert_price_between_currencies(
    value: float | None,
    *,
    from_currency: str,
    to_currency: str,
    calc_date: date,
) -> float | None:
    if value in (None, 0):
        return value
    source = str(from_currency or "RUB").strip().upper() or "RUB"
    target = str(to_currency or "RUB").strip().upper() or "RUB"
    if source == target:
        return float(value)
    rate = get_pricing_fx_usd_rub_rate_for_date(calc_date)
    if not rate or rate <= 0:
        return float(value)
    if source == "USD" and target in {"RUB", "RUR"}:
        return float(value) * float(rate)
    if source in {"RUB", "RUR"} and target == "USD":
        return float(value) / float(rate)
    return float(value)


def resolve_goods_report_currency(*, store_currency: str, report_currency: str, on_display_price: float | None) -> str:
    store_code = str(store_currency or "RUB").strip().upper() or "RUB"
    report_code = str(report_currency or store_code).strip().upper() or store_code
    if store_code == "USD" and on_display_price not in (None, 0):
        return "RUB"
    return report_code


def promo_matches_installed_price(*, installed_price: float | None, offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if installed_price in (None, 0) or not offers:
        return []
    matched: list[dict[str, Any]] = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        fit_mode = str(offer.get("promo_fit_mode") or "").strip().lower()
        if fit_mode not in {"with_ads", "without_ads"}:
            continue
        promo_price = to_num_simple(offer.get("promo_price"))
        if promo_price in (None, 0):
            continue
        if float(installed_price) <= float(promo_price) + 1.0:
            matched.append(offer)
    return matched


def summary_round(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)
