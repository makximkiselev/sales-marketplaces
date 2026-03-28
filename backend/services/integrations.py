from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.constants import Platform
from backend.services.storage import load_integrations, save_integrations

PLATFORMS = tuple(Platform)
GOOGLE_KEYS_DIR = Path(__file__).resolve().parents[2] / "data" / "config" / "google_keys"


def _as_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "on"}
    return default


def get_integrations() -> dict[str, Any]:
    data = load_integrations()
    if not isinstance(data, dict):
        return {}
    return data


def get_data_flow_settings() -> dict[str, Any]:
    data = get_integrations()
    import_enabled = _as_bool(data.get("data_import_enabled"), True)
    export_enabled = _as_bool(data.get("data_export_enabled", data.get("market_push_enabled")), False)
    platform_raw = data.get("platform_data_flow") if isinstance(data.get("platform_data_flow"), dict) else {}
    platforms: dict[str, dict[str, bool]] = {}
    for platform in PLATFORMS:
        p = platform_raw.get(platform) if isinstance(platform_raw.get(platform), dict) else {}
        platforms[platform] = {
            "import_enabled": _as_bool(p.get("import_enabled"), import_enabled),
            "export_enabled": _as_bool(p.get("export_enabled"), export_enabled),
        }
    return {
        "import_enabled": import_enabled,
        "export_enabled": export_enabled,
        "platforms": platforms,
    }


def _apply_on_obj(obj: dict[str, Any], *, import_enabled: bool | None, export_enabled: bool | None) -> None:
    if import_enabled is not None:
        obj["import_enabled"] = bool(import_enabled)
    if export_enabled is not None:
        obj["export_enabled"] = bool(export_enabled)


def _cascade_all_levels(
    data: dict[str, Any],
    *,
    import_enabled: bool | None = None,
    export_enabled: bool | None = None,
) -> None:
    if import_enabled is not None:
        data["data_import_enabled"] = bool(import_enabled)
    if export_enabled is not None:
        data["data_export_enabled"] = bool(export_enabled)
        data["market_push_enabled"] = bool(export_enabled)

    if not isinstance(data.get("platform_data_flow"), dict):
        data["platform_data_flow"] = {}
    for platform in PLATFORMS:
        p = data["platform_data_flow"].get(platform)
        if not isinstance(p, dict):
            p = {}
        _apply_on_obj(p, import_enabled=import_enabled, export_enabled=export_enabled)
        data["platform_data_flow"][platform] = p

    ym = data.get("yandex_market")
    if not isinstance(ym, dict):
        return
    accounts = ym.get("accounts")
    if not isinstance(accounts, list):
        return
    for account in accounts:
        if not isinstance(account, dict):
            continue
        _apply_on_obj(account, import_enabled=import_enabled, export_enabled=export_enabled)
        shops = account.get("shops")
        if not isinstance(shops, list):
            continue
        for shop in shops:
            if not isinstance(shop, dict):
                continue
            _apply_on_obj(shop, import_enabled=import_enabled, export_enabled=export_enabled)


def save_scoped_data_flow_settings(
    *,
    scope: str,
    platform: str | None = None,
    business_id: str | None = None,
    campaign_id: str | None = None,
    import_enabled: bool | None = None,
    export_enabled: bool | None = None,
) -> dict[str, Any]:
    if import_enabled is None and export_enabled is None:
        raise ValueError("Передайте import_enabled и/или export_enabled")

    scope = str(scope or "global").strip().lower()
    data = get_integrations()

    if scope == "global":
        # Верхний тоггл: меняем всё дерево (площадки + магазины)
        _cascade_all_levels(
            data,
            import_enabled=import_enabled,
            export_enabled=export_enabled,
        )
        save_integrations(data)
        return get_data_flow_settings()

    if scope == "platform":
        platform = str(platform or "").strip()
        if platform not in Platform.__members__.values():
            raise ValueError("Неизвестная площадка")
        if not isinstance(data.get("platform_data_flow"), dict):
            data["platform_data_flow"] = {}
        p = data["platform_data_flow"].get(platform)
        if not isinstance(p, dict):
            p = {}
        if import_enabled is not None:
            p["import_enabled"] = bool(import_enabled)
        if export_enabled is not None:
            p["export_enabled"] = bool(export_enabled)
        data["platform_data_flow"][platform] = p

        # Включение площадки включает верхний тоггл
        if import_enabled is True:
            data["data_import_enabled"] = True
        if export_enabled is True:
            data["data_export_enabled"] = True
            data["market_push_enabled"] = True

        # Площадка управляет всеми магазинами этой площадки
        if platform == Platform.YANDEX_MARKET:
            ym = data.get("yandex_market")
            if isinstance(ym, dict):
                accounts = ym.get("accounts")
                if isinstance(accounts, list):
                    for account in accounts:
                        if not isinstance(account, dict):
                            continue
                        _apply_on_obj(account, import_enabled=import_enabled, export_enabled=export_enabled)
                        shops = account.get("shops")
                        if not isinstance(shops, list):
                            continue
                        for shop in shops:
                            if not isinstance(shop, dict):
                                continue
                            _apply_on_obj(shop, import_enabled=import_enabled, export_enabled=export_enabled)
        save_integrations(data)
        return get_data_flow_settings()

    if scope == "account":
        platform = str(platform or Platform.YANDEX_MARKET).strip()
        if platform not in {Platform.YANDEX_MARKET, Platform.OZON}:
            raise ValueError("Для account поддержаны yandex_market и ozon")

        bid = str(business_id or "").strip()
        if not bid:
            raise ValueError("business_id/client_id обязателен")

        if platform == Platform.YANDEX_MARKET:
            ym = data.get(Platform.YANDEX_MARKET) or {}
            accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
            account = next((a for a in accounts if str((a or {}).get("business_id") or "").strip() == bid), None)
            if not isinstance(account, dict):
                raise ValueError("Аккаунт не найден")
            if import_enabled is not None:
                account["import_enabled"] = bool(import_enabled)
            if export_enabled is not None:
                account["export_enabled"] = bool(export_enabled)
            # Аккаунт управляет магазинами внутри себя
            shops = account.get("shops") if isinstance(account.get("shops"), list) else []
            for shop in shops:
                if not isinstance(shop, dict):
                    continue
                _apply_on_obj(shop, import_enabled=import_enabled, export_enabled=export_enabled)
            data[Platform.YANDEX_MARKET] = ym
        else:
            oz = data.get(Platform.OZON) if isinstance(data.get(Platform.OZON), dict) else {}
            accounts = oz.get("accounts") if isinstance(oz.get("accounts"), list) else []
            account = next((a for a in accounts if str((a or {}).get("client_id") or "").strip() == bid), None)
            if not isinstance(account, dict):
                raise ValueError("Кабинет Ozon не найден")
            if import_enabled is not None:
                account["import_enabled"] = bool(import_enabled)
            if export_enabled is not None:
                account["export_enabled"] = bool(export_enabled)
            stores = account.get("stores") if isinstance(account.get("stores"), list) else []
            for store in stores:
                if not isinstance(store, dict):
                    continue
                _apply_on_obj(store, import_enabled=import_enabled, export_enabled=export_enabled)
            oz["accounts"] = accounts
            data[Platform.OZON] = oz

        # Включение аккаунта включает площадку и верхний тоггл
        if import_enabled is True:
            data["data_import_enabled"] = True
        if export_enabled is True:
            data["data_export_enabled"] = True
            data["market_push_enabled"] = True
        if not isinstance(data.get("platform_data_flow"), dict):
            data["platform_data_flow"] = {}
        pf = data["platform_data_flow"].get(platform)
        if not isinstance(pf, dict):
            pf = {}
        if import_enabled is True:
            pf["import_enabled"] = True
        if export_enabled is True:
            pf["export_enabled"] = True
        data["platform_data_flow"][platform] = pf
        save_integrations(data)
        return get_data_flow_settings()

    if scope == "shop":
        platform = str(platform or Platform.YANDEX_MARKET).strip()
        if platform != Platform.YANDEX_MARKET:
            raise ValueError("Для shop пока поддержан только yandex_market")
        bid = str(business_id or "").strip()
        if not bid:
            raise ValueError("business_id обязателен")

        ym = data.get("yandex_market") or {}
        accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
        account = next((a for a in accounts if str((a or {}).get("business_id") or "").strip() == bid), None)
        if not isinstance(account, dict):
            raise ValueError("Аккаунт не найден")

        cid = str(campaign_id or "").strip()
        if not cid:
            raise ValueError("campaign_id обязателен")
        shops = account.get("shops") if isinstance(account.get("shops"), list) else []
        shop = next((s for s in shops if str((s or {}).get("campaign_id") or "").strip() == cid), None)
        if not isinstance(shop, dict):
            raise ValueError("Магазин не найден")
        if import_enabled is not None:
            shop["import_enabled"] = bool(import_enabled)
        if export_enabled is not None:
            shop["export_enabled"] = bool(export_enabled)

        # Включение магазина включает площадку и верхний тоггл
        if import_enabled is True:
            data["data_import_enabled"] = True
        if export_enabled is True:
            data["data_export_enabled"] = True
            data["market_push_enabled"] = True
        if not isinstance(data.get("platform_data_flow"), dict):
            data["platform_data_flow"] = {}
        yp = data["platform_data_flow"].get(Platform.YANDEX_MARKET)
        if not isinstance(yp, dict):
            yp = {}
        if import_enabled is True:
            yp["import_enabled"] = True
        if export_enabled is True:
            yp["export_enabled"] = True
        data["platform_data_flow"][Platform.YANDEX_MARKET] = yp

        data["yandex_market"] = ym
        save_integrations(data)
        return get_data_flow_settings()

    raise ValueError("Неизвестный scope")


def get_google_credentials() -> tuple[str, str]:
    data = get_integrations()
    g = data.get("google") or {}
    accounts = g.get("accounts") if isinstance(g.get("accounts"), list) else []
    active_id = str(g.get("active_account_id") or "").strip()
    if active_id and accounts:
        active = next((a for a in accounts if str((a or {}).get("id") or "").strip() == active_id), None)
        if isinstance(active, dict):
            raw_json = str(active.get("service_account_json") or "").strip()
            raw_b64 = str(active.get("service_account_b64") or "").strip()
            if raw_json or raw_b64:
                return raw_json, raw_b64

    if accounts:
        first = next((a for a in accounts if isinstance(a, dict)), None)
        if isinstance(first, dict):
            raw_json = str(first.get("service_account_json") or "").strip()
            raw_b64 = str(first.get("service_account_b64") or "").strip()
            if raw_json or raw_b64:
                return raw_json, raw_b64

    raw_json = str(g.get("service_account_json") or "").strip()
    raw_b64 = str(g.get("service_account_b64") or "").strip()
    if raw_json or raw_b64:
        return raw_json, raw_b64

    if GOOGLE_KEYS_DIR.exists():
        candidates = sorted(
            [path for path in GOOGLE_KEYS_DIR.iterdir() if path.is_file() and path.suffix.lower() == ".json"],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for candidate in candidates:
            try:
                raw_json = candidate.read_text(encoding="utf-8").strip()
            except Exception:
                continue
            if raw_json:
                return raw_json, ""
    return raw_json, raw_b64
