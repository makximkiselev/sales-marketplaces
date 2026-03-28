from __future__ import annotations

import copy
import hashlib
import json
from typing import Any


def make_cache_key(name: str, payload: dict[str, Any], generation: int) -> str:
    raw = json.dumps(
        {"name": name, "gen": generation, "payload": payload},
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def cache_get_copy(cache: dict[str, dict], key: str) -> dict[str, Any] | None:
    got = cache.get(key)
    return copy.deepcopy(got) if isinstance(got, dict) else None


def cache_set_copy(cache: dict[str, dict], key: str, value: dict[str, Any], max_size: int) -> None:
    if len(cache) >= max_size:
        cache.clear()
    cache[key] = copy.deepcopy(value)
