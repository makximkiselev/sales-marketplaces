import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.db import migrate_legacy_json_if_missing, save_json

# === Пути ===
# Корень проекта: поднимаемся от services/Core/ до base_dir
BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "data" / "config"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
MAPPINGS_DIR = CONFIG_DIR / "mappings"
MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)

SOURCES_FILE = CONFIG_DIR / "sources.json"
PROMOTIONS_MAPPING_FILE = MAPPINGS_DIR / "promotions.json"
PROMOS_ORDERS_FILE = BASE_DIR / "data" / "promos_orders.json"
INTEGRATIONS_FILE = CONFIG_DIR / "integrations.json"
INTEGRATIONS_BACKUP_DIR = CONFIG_DIR / "integrations_backups"
INTEGRATIONS_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

KEY_SOURCES = "core.sources"
KEY_MAPPINGS_PROMOTIONS = "core.mappings.promotions"
KEY_PROMOS_ORDERS = "core.promos_orders"
KEY_INTEGRATIONS = "core.integrations"


def load_sources() -> list[dict[str, Any]]:
    """
    Загружаем список источников из sources.json.
    Если файл пустой/битый — возвращаем [].
    """
    try:
        data = migrate_legacy_json_if_missing(KEY_SOURCES, SOURCES_FILE, [])
        return data if isinstance(data, list) else []
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Данные источников повреждены: {e}")
    except Exception as e:
        raise RuntimeError(f"Ошибка чтения источников: {e}")


def get_source_by_id(source_id: str) -> dict[str, Any] | None:
    sid = str(source_id or "").strip()
    if not sid:
        return None
    for item in load_sources():
        if not isinstance(item, dict):
            continue
        current_id = str(item.get("id") or item.get("source_id") or "").strip()
        if current_id == sid:
            return item
    return None


def is_source_mode_enabled(source_id: str, mode: str, *, default: bool) -> bool:
    src = get_source_by_id(source_id)
    if not isinstance(src, dict):
        return False
    key = "mode_export" if str(mode or "").strip().lower() == "export" else "mode_import"
    value = src.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def save_sources(sources: list[dict[str, Any]]):
    """
    Сохраняем список источников в sources.json.
    """
    save_json(KEY_SOURCES, sources)


def seed_sources_if_empty():
    """
    Если файл отсутствует или пустой — создаём дефолтный sources.json.
    """
    try:
        data = load_sources()
        if data:
            return
    except RuntimeError:
        pass

    default_sources = [
        {
            "id": "catalog_prices",
            "title": "Каталог + прайс",
            "type": "gsheets",
            "spreadsheet_id": "1DhDdf5FbjIXShOhWN3g_xZdIjnLiMZUPkIAei-hG3Bc",
            "worksheet": "Прайс",
            "mapping": {}
        }
    ]
    save_sources(default_sources)


def load_promos_orders_payload() -> dict[str, Any]:
    default = {"orders": [], "updated_at": None}
    data = migrate_legacy_json_if_missing(KEY_PROMOS_ORDERS, PROMOS_ORDERS_FILE, default)
    if not isinstance(data, dict):
        return default
    if "orders" not in data or not isinstance(data["orders"], list):
        data["orders"] = []
    return data


def load_integrations() -> dict[str, Any]:
    default = {
        "yandex_market": {
            "api_key": "",
            "business_id": "",
            "connected_at": None,
        },
        "google": {
            "service_account_json": "",
            "service_account_b64": "",
            "active_account_id": "",
            "accounts": [],
        },
        "market_push_enabled": False,
        "data_import_enabled": True,
        "data_export_enabled": False,
        "platform_data_flow": {
            "yandex_market": {"import_enabled": True, "export_enabled": False},
            "ozon": {"import_enabled": True, "export_enabled": False},
            "wildberries": {"import_enabled": True, "export_enabled": False},
        },
        "ozon": {"connected": False},
        "wildberries": {"connected": False},
    }
    data: Any = None
    if INTEGRATIONS_FILE.exists():
        try:
            raw = INTEGRATIONS_FILE.read_text(encoding="utf-8").strip()
            parsed = json.loads(raw) if raw else default
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = None
    if not isinstance(data, dict):
        data = migrate_legacy_json_if_missing(KEY_INTEGRATIONS, INTEGRATIONS_FILE, default)
        if isinstance(data, dict):
            try:
                tmp_path = INTEGRATIONS_FILE.with_suffix(".json.tmp")
                tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp_path.replace(INTEGRATIONS_FILE)
            except Exception:
                pass
    if not isinstance(data, dict):
        return default
    if "data_import_enabled" not in data:
        data["data_import_enabled"] = True
    if "data_export_enabled" not in data:
        data["data_export_enabled"] = bool(data.get("market_push_enabled", False))
    if not isinstance(data.get("platform_data_flow"), dict):
        data["platform_data_flow"] = {}
    data["platform_data_flow"].setdefault(
        "yandex_market",
        {"import_enabled": bool(data["data_import_enabled"]), "export_enabled": bool(data["data_export_enabled"])},
    )
    data["platform_data_flow"].setdefault(
        "ozon",
        {"import_enabled": bool(data["data_import_enabled"]), "export_enabled": bool(data["data_export_enabled"])},
    )
    data["platform_data_flow"].setdefault(
        "wildberries",
        {"import_enabled": bool(data["data_import_enabled"]), "export_enabled": bool(data["data_export_enabled"])},
    )
    google = data.get("google")
    if not isinstance(google, dict):
        google = {"service_account_json": "", "service_account_b64": "", "active_account_id": "", "accounts": []}
    if not isinstance(google.get("accounts"), list):
        google["accounts"] = []
    if "active_account_id" not in google:
        google["active_account_id"] = ""
    data["google"] = google
    return data


def save_integrations(data: dict[str, Any]) -> None:
    payload = data or {}
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path = INTEGRATIONS_FILE.with_suffix(".json.tmp")
    tmp_path.write_text(serialized, encoding="utf-8")
    tmp_path.replace(INTEGRATIONS_FILE)
    try:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_path = INTEGRATIONS_BACKUP_DIR / f"integrations.{stamp}.json"
        backup_path.write_text(serialized, encoding="utf-8")
    except Exception:
        pass
    try:
        save_json(KEY_INTEGRATIONS, payload)
    except Exception:
        pass
