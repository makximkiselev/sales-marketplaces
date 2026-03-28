from __future__ import annotations

import io
import json
import zipfile
from datetime import date, timedelta
from typing import Any, Callable


def iter_month_ranges(date_from: str, date_to: str) -> list[tuple[str, str]]:
    start = date.fromisoformat(str(date_from or "").strip())
    end = date.fromisoformat(str(date_to or "").strip())
    if start > end:
        raise ValueError("date_from не может быть больше date_to")
    ranges: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        next_month = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        ranges.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + timedelta(days=1)
    return ranges


def walk_payload_rows(payload: Any, *, sheet_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add_row(value: Any, row_index: int) -> None:
        if isinstance(value, dict):
            rows.append({"sheet_name": sheet_name, "row_index": row_index, "payload": value})
        else:
            rows.append({"sheet_name": sheet_name, "row_index": row_index, "payload": {"value": value}})

    if isinstance(payload, list):
        for idx, item in enumerate(payload, start=1):
            add_row(item, idx)
        return rows

    if isinstance(payload, dict):
        list_keys = [key for key, value in payload.items() if isinstance(value, list)]
        if list_keys:
            for key in list_keys:
                nested_sheet = f"{sheet_name}:{key}"
                nested_rows = walk_payload_rows(payload.get(key), sheet_name=nested_sheet)
                if nested_rows:
                    rows.extend(nested_rows)
            if rows:
                return rows
        rows.append({"sheet_name": sheet_name, "row_index": 1, "payload": payload})
        return rows

    rows.append({"sheet_name": sheet_name, "row_index": 1, "payload": {"value": payload}})
    return rows


def parse_report_bytes(
    content: bytes,
    *,
    is_target_sheet: Callable[[str, Any], bool],
) -> list[dict[str, Any]]:
    parsed_rows: list[dict[str, Any]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in sorted(zf.namelist(), key=lambda item: item.lower()):
                if not name.lower().endswith(".json"):
                    continue
                try:
                    raw = zf.read(name)
                    payload = json.loads(raw.decode("utf-8"))
                except Exception:
                    continue
                if not is_target_sheet(name, payload):
                    continue
                parsed_rows.extend(walk_payload_rows(payload, sheet_name=name))
    except zipfile.BadZipFile:
        try:
            payload = json.loads(content.decode("utf-8"))
            if is_target_sheet("report.json", payload):
                parsed_rows.extend(walk_payload_rows(payload, sheet_name="report.json"))
        except Exception:
            pass
    return parsed_rows
