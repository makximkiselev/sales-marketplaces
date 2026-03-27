from __future__ import annotations

from collections import Counter
import re
from typing import Any

from backend.routers._shared import _catalog_marketplace_stores_context, _catalog_path_from_row, _read_source_rows
from backend.services.store_data_model import (
    replace_pricing_catalog_sku_paths,
    replace_pricing_category_tree,
    upsert_store_dataset,
)


def _choose_best_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    path_counter = Counter(tuple(candidate.get("path") or []) for candidate in candidates if candidate.get("path"))
    ranked = sorted(
        candidates,
        key=lambda candidate: (
            -(path_counter.get(tuple(candidate.get("path") or []), 0)),
            -len(candidate.get("path") or []),
            str(candidate.get("store_uid") or ""),
        ),
    )
    return ranked[0] if ranked else None


def _normalize_text_tokens(value: str) -> list[str]:
    text = re.sub(r"[^a-zа-я0-9]+", " ", str(value or "").lower())
    return [part for part in text.split() if part]


def _path_parts(path: list[str]) -> tuple[str, list[str], str, str]:
    clean = [str(part or "").strip() for part in path if str(part or "").strip()]
    if not clean:
        return "", [], "", ""
    if len(clean) == 1:
        return clean[0], [], "", ""
    if len(clean) == 2:
        return clean[0], [], clean[1], ""
    return clean[0], clean[1:-2], clean[-2], clean[-1]


def _adapt_cross_platform_path(path: list[str], local_paths: list[list[str]]) -> list[str]:
    if not path or not local_paths:
        return path
    category, sub_levels, brand, line = _path_parts(path)
    target_leaf = sub_levels[-1] if sub_levels else ""
    target_leaf_tokens = set(_normalize_text_tokens(target_leaf))
    brand_lc = brand.lower()
    line_lc = line.lower()

    exact_prefixes: Counter[tuple[str, ...]] = Counter()
    similar_prefixes: Counter[tuple[str, ...]] = Counter()

    for local_path in local_paths:
        local_category, local_sub_levels, local_brand, local_line = _path_parts(local_path)
        if not local_path or local_category != category:
            continue
        prefix = tuple([local_category, *local_sub_levels])
        if brand_lc and line_lc and local_brand.lower() == brand_lc and local_line.lower() == line_lc:
            exact_prefixes[prefix] += 1
            continue
        local_leaf_tokens = set(_normalize_text_tokens(local_sub_levels[-1] if local_sub_levels else ""))
        if target_leaf_tokens and local_leaf_tokens and target_leaf_tokens.intersection(local_leaf_tokens):
            similar_prefixes[prefix] += 1

    if exact_prefixes:
        chosen_prefix = list(exact_prefixes.most_common(1)[0][0])
        return [*chosen_prefix, brand, line] if brand or line else chosen_prefix
    if similar_prefixes:
        chosen_prefix = list(similar_prefixes.most_common(1)[0][0])
        return [*chosen_prefix, brand, line] if brand or line else chosen_prefix
    return path


def refresh_pricing_catalog_trees_from_sources() -> dict[str, Any]:
    stores = [store for store in _catalog_marketplace_stores_context() if store.get("table_name")]
    store_rows_by_uid: dict[str, list[dict[str, Any]]] = {}
    store_paths_by_uid: dict[str, dict[str, list[str]]] = {}
    row_count_by_uid: dict[str, int] = {}
    stores_by_platform: dict[str, list[dict[str, Any]]] = {}
    all_skus: set[str] = set()

    updated = 0
    skipped = 0

    for store in stores:
        store_uid = str(store.get("store_uid") or "").strip()
        store_id = str(store.get("store_id") or "").strip()
        table_name = str(store.get("table_name") or "").strip()
        platform = str(store.get("platform") or "").strip().lower()
        if not store_uid or not store_id or not table_name or not platform:
            skipped += 1
            continue

        rows = _read_source_rows(table_name)
        store_rows_by_uid[store_uid] = rows
        row_count_by_uid[store_uid] = len(rows)
        stores_by_platform.setdefault(platform, []).append(store)
        local_paths: dict[str, list[str]] = {}
        grouped: dict[tuple[str, tuple[str, ...]], int] = {}

        for row in rows:
            sku = str(row.get("sku") or "").strip()
            path = _catalog_path_from_row(row)
            if sku:
                all_skus.add(sku)
            if not sku or not path:
                continue
            local_paths[sku] = path
            category = str(path[0] or "").strip() or "Не определено"
            sub_levels = [str(part or "").strip() for part in path[1:] if str(part or "").strip()][:5]
            key = (category, tuple(sub_levels))
            grouped[key] = grouped.get(key, 0) + 1

        store_paths_by_uid[store_uid] = local_paths
        dataset_key = upsert_store_dataset(
            store_uid=store_uid,
            store_id=store_id,
            task_code="pricing_catalog_tree",
            title=f"{store_id}: pricing_catalog_tree",
            status="ready",
            row_count=len(grouped),
            meta={
                "platform": platform,
                "source_table": table_name,
                "raw_items_count": len(rows),
                "tree_rows_count": len(grouped),
                "source_kind": "catalog_refresh",
            },
        )
        replace_pricing_category_tree(
            dataset_key=dataset_key,
            store_uid=store_uid,
            rows=[
                {
                    "category": category,
                    "subcategory_levels": list(levels),
                    "items_count": count,
                }
                for (category, levels), count in grouped.items()
            ],
        )
        updated += 1

    for priority_platform, platform_stores in stores_by_platform.items():
        anchor_store = max(platform_stores, key=lambda store: row_count_by_uid.get(str(store.get("store_uid") or ""), 0))
        anchor_store_uid = str(anchor_store.get("store_uid") or "").strip()
        same_platform_store_uids = [str(store.get("store_uid") or "").strip() for store in platform_stores if str(store.get("store_uid") or "").strip()]
        same_platform_existing_paths = [
            path
            for store_uid in same_platform_store_uids
            for path in (store_paths_by_uid.get(store_uid) or {}).values()
            if path
        ]
        other_platform_store_uids = [
            store_uid
            for store_uid in store_rows_by_uid.keys()
            if store_uid not in same_platform_store_uids
        ]

        resolved_rows: list[dict[str, Any]] = []
        for sku in sorted(all_skus):
            anchor_path = (store_paths_by_uid.get(anchor_store_uid) or {}).get(sku)
            if anchor_path:
                resolved_rows.append(
                    {
                        "sku": sku,
                        "anchor_store_uid": anchor_store_uid,
                        "source_store_uid": anchor_store_uid,
                        "resolved_category": anchor_path[0] if anchor_path else "",
                        "resolved_subcategory_levels": anchor_path[1:],
                        "leaf_path": " / ".join(anchor_path),
                        "resolution_kind": "anchor",
                    }
                )
                continue

            same_platform_candidates = [
                {
                    "store_uid": store_uid,
                    "path": (store_paths_by_uid.get(store_uid) or {}).get(sku),
                }
                for store_uid in same_platform_store_uids
                if (store_paths_by_uid.get(store_uid) or {}).get(sku)
            ]
            chosen = _choose_best_candidate(same_platform_candidates)
            if chosen and chosen.get("path"):
                path = list(chosen["path"])
                resolved_rows.append(
                    {
                        "sku": sku,
                        "anchor_store_uid": anchor_store_uid,
                        "source_store_uid": str(chosen.get("store_uid") or "").strip(),
                        "resolved_category": path[0] if path else "",
                        "resolved_subcategory_levels": path[1:],
                        "leaf_path": " / ".join(path),
                        "resolution_kind": "same_platform_fallback",
                    }
                )
                continue

            cross_platform_candidates = [
                {
                    "store_uid": store_uid,
                    "path": (store_paths_by_uid.get(store_uid) or {}).get(sku),
                }
                for store_uid in other_platform_store_uids
                if (store_paths_by_uid.get(store_uid) or {}).get(sku)
            ]
            chosen = _choose_best_candidate(cross_platform_candidates)
            if chosen and chosen.get("path"):
                path = _adapt_cross_platform_path(list(chosen["path"]), same_platform_existing_paths)
                resolved_rows.append(
                    {
                        "sku": sku,
                        "anchor_store_uid": anchor_store_uid,
                        "source_store_uid": str(chosen.get("store_uid") or "").strip(),
                        "resolved_category": path[0] if path else "",
                        "resolved_subcategory_levels": path[1:],
                        "leaf_path": " / ".join(path),
                        "resolution_kind": "cross_platform_fallback",
                    }
                )
                continue

            resolved_rows.append(
                {
                    "sku": sku,
                    "anchor_store_uid": anchor_store_uid,
                    "source_store_uid": "",
                    "resolved_category": "Не определено",
                    "resolved_subcategory_levels": [],
                    "leaf_path": "Не определено",
                    "resolution_kind": "undefined",
                }
            )

        replace_pricing_catalog_sku_paths(priority_platform=priority_platform, rows=resolved_rows)

    return {
        "ok": True,
        "stores_total": len(stores),
        "stores_updated": updated,
        "stores_skipped": skipped,
        "platforms_total": len(stores_by_platform),
        "sku_total": len(all_skus),
    }
