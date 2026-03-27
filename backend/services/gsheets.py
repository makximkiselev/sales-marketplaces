import base64
import json
import re
import time
import gspread
from google.oauth2.service_account import Credentials
from backend.services.integrations import get_google_credentials

# === Скоупы для доступа (на запись) ===
GS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_gs_client = None


def _is_google_quota_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return "quota exceeded" in text or "read requests per minute per user" in text or "[429]" in text or "429" in text


def _with_gsheets_retry(fn, *, attempts: int = 4, base_sleep: float = 1.0):
    last_exc: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if not _is_google_quota_error(exc) or attempt >= attempts:
                raise
            time.sleep(base_sleep * attempt)
    if last_exc is not None:
        raise last_exc
    return None


def _column_letter(index: int) -> str:
    if index <= 0:
        raise ValueError("index должен быть > 0")
    letters = ""
    current = int(index)
    while current > 0:
        current, rem = divmod(current - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _load_service_account_info(raw_json_override: str | None = None, raw_b64_override: str | None = None) -> dict:
    """
    Источник ключа Google Service Account:
    1) integrations.google.service_account_json (raw JSON)
    2) integrations.google.service_account_b64 (base64 от JSON)
    """
    if raw_json_override is not None or raw_b64_override is not None:
        raw_json = (raw_json_override or "").strip()
        raw_b64 = (raw_b64_override or "").strip()
    else:
        raw_json, raw_b64 = get_google_credentials()
    if raw_json:
        if not raw_json.lstrip().startswith("{"):
            raise RuntimeError(
                "Ожидается полный JSON ключ сервисного аккаунта, а не Key ID. "
                "Скачайте ключ через Service Accounts -> Keys -> Create new key -> JSON."
            )
        try:
            return json.loads(raw_json)
        except Exception as e:
            raise RuntimeError(
                f"Google service_account_json невалидный JSON: {e}. "
                "Нужен полный JSON-файл ключа сервисного аккаунта."
            ) from e

    if raw_b64:
        try:
            decoded = base64.b64decode(raw_b64).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"Google service_account_b64 невалиден: {e}") from e

    raise FileNotFoundError(
        "Google credentials не найдены в БД интеграций "
        "(google.service_account_json или google.service_account_b64)"
    )


def get_gs_client(raw_json_override: str | None = None, raw_b64_override: str | None = None):
    """
    Создаёт и кэширует gspread-клиент.
    """
    global _gs_client
    use_override = raw_json_override is not None or raw_b64_override is not None
    if not use_override and _gs_client:
        return _gs_client

    creds_info = _load_service_account_info(
        raw_json_override=raw_json_override,
        raw_b64_override=raw_b64_override,
    )
    creds = Credentials.from_service_account_info(creds_info, scopes=GS_SCOPES)
    client = gspread.authorize(creds)
    if not use_override:
        _gs_client = client
    return client


def parse_sheet_id(url: str) -> str:
    """
    Извлекаем spreadsheet_id из URL Google Sheets.
    """
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url or "")
    return m.group(1) if m else (url or "").strip()


def list_worksheets(
    spreadsheet_id: str,
    raw_json_override: str | None = None,
    raw_b64_override: str | None = None,
):
    gc = get_gs_client(
        raw_json_override=raw_json_override,
        raw_b64_override=raw_b64_override,
    )
    sh = _with_gsheets_retry(lambda: gc.open_by_key(spreadsheet_id))
    return [ws.title for ws in _with_gsheets_retry(lambda: sh.worksheets())]


def read_sheet_preview(
    spreadsheet_id: str,
    worksheet: str | None = None,
    limit: int = 5,
    raw_json_override: str | None = None,
    raw_b64_override: str | None = None,
):
    """
    Читаем несколько строк (по умолчанию 5) из листа для предпросмотра.
    """
    gc = get_gs_client(
        raw_json_override=raw_json_override,
        raw_b64_override=raw_b64_override,
    )
    sh = _with_gsheets_retry(lambda: gc.open_by_key(spreadsheet_id))
    ws = _with_gsheets_retry(lambda: sh.worksheet(worksheet)) if worksheet else _with_gsheets_retry(lambda: sh.sheet1)
    rows = _with_gsheets_retry(lambda: ws.get_all_values())
    return {
        "worksheet": ws.title,
        "total_rows": len(rows),
        "preview": rows[:limit] if limit else rows
    }


def read_sheet_all(
    spreadsheet_id: str,
    worksheet: str | None = None,
    raw_json_override: str | None = None,
    raw_b64_override: str | None = None,
):
    """
    Читаем все строки из листа.
    """
    gc = get_gs_client(
        raw_json_override=raw_json_override,
        raw_b64_override=raw_b64_override,
    )
    sh = _with_gsheets_retry(lambda: gc.open_by_key(spreadsheet_id))
    ws = _with_gsheets_retry(lambda: sh.worksheet(worksheet)) if worksheet else _with_gsheets_retry(lambda: sh.sheet1)
    rows = _with_gsheets_retry(lambda: ws.get_all_values())
    return {
        "worksheet": ws.title,
        "total_rows": len(rows),
        "rows": rows
    }


def update_pricing_in_sheet(spreadsheet_url: str, updates: dict[str, dict], worksheet: str | None = "Прайс"):
    """
    Обновляет цены и ставки буста в Google Sheets батчево:
    - AM (39) → новая цена
    - AT (46) → ставка буста (boost_bid / 10000)
    """
    spreadsheet_id = parse_sheet_id(spreadsheet_url)
    gc = get_gs_client()
    sh = _with_gsheets_retry(lambda: gc.open_by_key(spreadsheet_id))
    ws = _with_gsheets_retry(lambda: sh.worksheet(worksheet)) if worksheet else _with_gsheets_retry(lambda: sh.sheet1)

    all_rows = _with_gsheets_retry(lambda: ws.get_all_values())
    batch_updates = []

    for row_idx, row in enumerate(all_rows, start=1):
        if len(row) < 5:
            continue
        sku = row[4].strip()
        if not sku or sku not in updates:
            continue

        entry = updates[sku]
        if "price" in entry:
            batch_updates.append({
                "range": f"U{row_idx}",
                "values": [[entry["price"]]]
            })
        if "boost" in entry:
            batch_updates.append({
                "range": f"AT{row_idx}",
                "values": [[entry["boost"]]]
            })

    if not batch_updates:
        print("⚠️ Нет данных для обновления в Google Sheets")
        return 0

    _with_gsheets_retry(lambda: ws.batch_update(batch_updates))
    print(f"✅ Обновлено строк в Google Sheets: {len(batch_updates)}")
    return len(batch_updates)


def push_pricing_to_sheet(spreadsheet_url: str, worksheet: str | None = "Прайс"):
    """
    Загружает pricing_decisions.json и обновляет Google Sheet батчево:
    - AM (39) → новая цена (всегда new_price, если есть)
    - AT (46) → ставка буста (boost_bid / 10000, иначе 0)
    """
    # Заглушка в урезанном backend: модуль pricing_control удален.
    # Функцию сохраняем для будущего этапа, но сейчас она ничего не делает.
    _ = (spreadsheet_url, worksheet)
    return 0


def update_sheet_column_by_sku(
    *,
    spreadsheet_id: str,
    worksheet: str | None,
    sku_column: str,
    value_column: str,
    values_by_sku: dict[str, object],
    present_skus: set[str] | None = None,
):
    sid = parse_sheet_id(spreadsheet_id)
    sku_col_name = str(sku_column or "").strip()
    value_col_name = str(value_column or "").strip()
    if not sid:
        raise ValueError("spreadsheet_id обязателен")
    if not sku_col_name or not value_col_name:
        raise ValueError("sku_column и value_column обязательны")
    if not isinstance(values_by_sku, dict) or not values_by_sku:
        return {"updated_cells": 0, "matched_rows": 0}

    gc = get_gs_client()
    sh = _with_gsheets_retry(lambda: gc.open_by_key(sid))
    ws = _with_gsheets_retry(lambda: sh.worksheet(worksheet)) if worksheet else _with_gsheets_retry(lambda: sh.sheet1)
    rows = _with_gsheets_retry(lambda: ws.get_all_values())
    if not rows:
        return {"updated_cells": 0, "matched_rows": 0}

    header = rows[0] if isinstance(rows[0], list) else []
    if not isinstance(header, list):
        return {"updated_cells": 0, "matched_rows": 0}
    header_map = {str(name or "").strip(): idx for idx, name in enumerate(header)}
    if sku_col_name not in header_map:
        raise ValueError(f"Не найден столбец SKU: {sku_col_name}")
    if value_col_name not in header_map:
        raise ValueError(f"Не найден столбец значения: {value_col_name}")

    sku_idx = int(header_map[sku_col_name])
    value_idx = int(header_map[value_col_name])
    updates: list[dict[str, object]] = []
    matched_rows = 0
    for row_index, row in enumerate(rows[1:], start=2):
        if not isinstance(row, list):
            continue
        sku = str(row[sku_idx] if sku_idx < len(row) else "").strip()
        if not sku:
            continue
        if sku in values_by_sku:
            cell_value = values_by_sku[sku]
        elif present_skus is not None and sku not in present_skus:
            cell_value = ""
        else:
            continue
        matched_rows += 1
        updates.append(
            {
                "range": f"{_column_letter(value_idx + 1)}{row_index}",
                "values": [[cell_value]],
            }
        )
    if not updates:
        return {"updated_cells": 0, "matched_rows": matched_rows}
    _with_gsheets_retry(lambda: ws.batch_update(updates))
    return {"updated_cells": len(updates), "matched_rows": matched_rows}


def update_sheet_columns_by_sku(
    *,
    spreadsheet_id: str,
    worksheet: str | None,
    sku_column: str,
    values_by_column: dict[str, dict[str, object]],
    present_skus: set[str] | None = None,
):
    sid = parse_sheet_id(spreadsheet_id)
    sku_col_name = str(sku_column or "").strip()
    if not sid:
        raise ValueError("spreadsheet_id обязателен")
    if not sku_col_name:
        raise ValueError("sku_column обязателен")
    usable_columns = {
        str(column or "").strip(): values
        for column, values in (values_by_column or {}).items()
        if str(column or "").strip() and isinstance(values, dict) and values
    }
    if not usable_columns:
        return {"updated_cells": 0, "matched_rows": 0, "updated_by_column": {}}

    gc = get_gs_client()
    sh = _with_gsheets_retry(lambda: gc.open_by_key(sid))
    ws = _with_gsheets_retry(lambda: sh.worksheet(worksheet)) if worksheet else _with_gsheets_retry(lambda: sh.sheet1)
    rows = _with_gsheets_retry(lambda: ws.get_all_values())
    if not rows:
        return {"updated_cells": 0, "matched_rows": 0, "updated_by_column": {}}

    header = rows[0] if isinstance(rows[0], list) else []
    if not isinstance(header, list):
        return {"updated_cells": 0, "matched_rows": 0, "updated_by_column": {}}
    header_map = {str(name or "").strip(): idx for idx, name in enumerate(header)}
    if sku_col_name not in header_map:
        raise ValueError(f"Не найден столбец SKU: {sku_col_name}")
    for value_col_name in usable_columns:
        if value_col_name not in header_map:
            raise ValueError(f"Не найден столбец значения: {value_col_name}")

    sku_idx = int(header_map[sku_col_name])
    updates: list[dict[str, object]] = []
    matched_rows = 0
    updated_by_column: dict[str, int] = {name: 0 for name in usable_columns}
    for row_index, row in enumerate(rows[1:], start=2):
        if not isinstance(row, list):
            continue
        sku = str(row[sku_idx] if sku_idx < len(row) else "").strip()
        if not sku:
            continue
        row_matched = False
        for value_col_name, values in usable_columns.items():
            if sku in values:
                cell_value = values[sku]
            elif present_skus is not None and sku not in present_skus:
                cell_value = ""
            else:
                continue
            row_matched = True
            value_idx = int(header_map[value_col_name])
            updates.append(
                {
                    "range": f"{_column_letter(value_idx + 1)}{row_index}",
                    "values": [[cell_value]],
                }
            )
            updated_by_column[value_col_name] += 1
        if row_matched:
            matched_rows += 1
    if not updates:
        return {"updated_cells": 0, "matched_rows": matched_rows, "updated_by_column": updated_by_column}

    _with_gsheets_retry(lambda: ws.batch_update(updates))
    return {"updated_cells": len(updates), "matched_rows": matched_rows, "updated_by_column": updated_by_column}
