from __future__ import annotations

import re
from typing import Any, Callable

from backend.routers._shared import _catalog_path_from_row, _read_source_rows, _safe_source_table_name
from backend.services.source_tables import get_registered_source_table


def leaf_path_from_row(row: dict[str, Any]) -> str:
    path = _catalog_path_from_row(row)
    return " / ".join([p for p in path if str(p).strip()])


def num0(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def clamp_rate(value: float) -> float:
    if value < 0:
        return 0.0
    if value > 0.99:
        return 0.99
    return value


def to_num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        pass
    raw = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", raw)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def norm_col_name(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def compute_max_weight_kg(platform: str, dims: dict[str, Any]) -> float | None:
    weight = to_num(dims.get("weight_kg"))
    width = to_num(dims.get("width_cm"))
    length = to_num(dims.get("length_cm"))
    height = to_num(dims.get("height_cm"))
    divisor = 1000.0 if str(platform or "").strip().lower() == "yandex_market" else 5000.0
    volumetric = None
    if width is not None and length is not None and height is not None:
        volumetric = (float(width) * float(length) * float(height)) / divisor
    if weight is not None and volumetric is not None:
        return max(float(weight), float(volumetric))
    if weight is not None:
        return float(weight)
    if volumetric is not None:
        return float(volumetric)
    return None


def profit_for_price(
    *,
    price: float,
    dep_rate: float,
    tax_rate: float,
    fixed_cost: float,
    apply_tax: bool = True,
    handling_mode: str = "fixed",
    handling_fixed: float = 0.0,
    handling_percent: float = 0.0,
    handling_min: float = 0.0,
    handling_max: float = 0.0,
) -> tuple[float, float]:
    mode = str(handling_mode or "fixed").strip().lower()
    if mode == "percent":
        raw = price * (float(handling_percent) / 100.0)
        lower = max(0.0, float(handling_min or 0.0))
        upper = float(handling_max or 0.0)
        if upper > 0:
            handling_cost = min(max(raw, lower), upper)
        else:
            handling_cost = max(raw, lower)
    else:
        handling_cost = float(handling_fixed or 0.0)

    base_after_costs = price * (1.0 - dep_rate) - fixed_cost - handling_cost
    tax = (price * tax_rate) if apply_tax else 0.0
    profit_abs = base_after_costs - tax
    profit_pct = (profit_abs / price) if price > 0 else 0.0
    return profit_abs, profit_pct


def resolve_source_table_name(source_id: str) -> str:
    sid = str(source_id or "").strip()
    if not sid:
        return ""
    table_name = str(get_registered_source_table(sid) or "").strip()
    if table_name:
        _safe_source_table_name(table_name)
    return table_name


def load_numeric_map_from_source(
    *,
    source_id: str,
    sku_column: str,
    value_column: str,
    price_fallback_column: str = "price",
    row_loader: Callable[[str], list[dict[str, Any]]] | None = None,
) -> dict[str, float]:
    table_name = resolve_source_table_name(source_id)
    if not table_name:
        return {}
    rows = (row_loader or _read_source_rows)(table_name)
    sku_col = str(sku_column or "").strip()
    value_col = str(value_column or "").strip()
    sku_col_norm = norm_col_name(sku_col)
    value_col_norm = norm_col_name(value_col)
    out: dict[str, float] = {}
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        sku_raw = None
        if sku_col and payload:
            sku_raw = payload.get(sku_col)
            if sku_raw in (None, "") and sku_col_norm:
                for key, val in payload.items():
                    if norm_col_name(str(key)) == sku_col_norm:
                        sku_raw = val
                        break
        if sku_raw in (None, ""):
            sku_raw = row.get("sku")
        sku = str(sku_raw or "").strip()
        if not sku:
            continue
        if value_col:
            val_raw = payload.get(value_col) if payload else None
            if val_raw in (None, "") and value_col_norm and payload:
                for key, val in payload.items():
                    if norm_col_name(str(key)) == value_col_norm:
                        val_raw = val
                        break
            if val_raw in (None, "") and value_col.lower() == price_fallback_column.lower():
                val_raw = row.get("price")
        else:
            val_raw = row.get("price")
        num = to_num(val_raw)
        if num is None:
            continue
        out[sku] = float(num)
    return out


def idx_of(header_norm: list[str], key: str) -> int:
    normalized = norm_col_name(key)
    if not normalized:
        return -1
    for index, value in enumerate(header_norm):
        if value == normalized:
            return index
    return -1
