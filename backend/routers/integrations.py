from __future__ import annotations

import datetime

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import JSONResponse

from backend.routers._shared import (
    BASE_DIR,
    GSHEETS_MAPPING_TEMPLATES,
    _effective_flow,
    _extract_campaigns,
    _fetch_campaigns_payload,
    _fetch_ozon_seller_info,
    _get_catalog_import_config,
    _normalize_ozon_accounts,
    _normalize_ym_accounts,
    _now_iso,
    _persist_shop_health,
    _persist_source_health,
    _upsert_google_account,
    get_data_flow_settings,
    get_google_credentials,
    load_integrations,
    load_sources,
    parse_sheet_id,
    read_sheet_preview,
    save_integrations,
    save_scoped_data_flow_settings,
    upsert_store,
)
from backend.services.store_data_model import upsert_pricing_logistics_store_settings

router = APIRouter()


@router.get("/api/sources")
async def api_list_sources():
    from backend.routers._shared import _mapping_configured, load_sources
    items = load_sources()
    enriched = []
    for src in items:
        row = dict(src)
        row["mapping_configured"] = _mapping_configured(row)
        enriched.append(row)
    return {"ok": True, "items": enriched, "total_count": len(enriched)}


@router.get("/api/integrations")
async def api_get_integrations():
    data = load_integrations()
    ym = data.get("yandex_market") or {}
    ym_api = (ym.get("api_key") or "").strip()
    ym_business = (ym.get("business_id") or "").strip()

    g_json, g_b64 = get_google_credentials()
    google_configured = bool(g_json or g_b64)
    google = data.get("google") if isinstance(data.get("google"), dict) else {}
    g_accounts_raw = google.get("accounts") if isinstance(google.get("accounts"), list) else []
    g_accounts = []
    for acc in g_accounts_raw:
        if not isinstance(acc, dict):
            continue
        g_accounts.append(
            {
                "id": str(acc.get("id") or ""),
                "name": str(acc.get("name") or ""),
                "client_email": str(acc.get("client_email") or ""),
                "private_key_id": str(acc.get("private_key_id") or ""),
                "created_at": acc.get("created_at"),
            }
        )
    g_active_id = str(google.get("active_account_id") or "")

    ym_accounts = _normalize_ym_accounts(ym)
    ym_shops = [s for a in ym_accounts for s in (a.get("shops") or [])]
    ym_connected = bool(ym_accounts) or bool(ym_api and ym_business)
    oz = data.get("ozon") if isinstance(data.get("ozon"), dict) else {}
    oz_accounts = _normalize_ozon_accounts(oz)
    oz_connected = bool(oz_accounts)

    flow = get_data_flow_settings()
    global_flow = {
        "import_enabled": bool(flow.get("import_enabled", True)),
        "export_enabled": bool(flow.get("export_enabled", False)),
    }
    ym_platform_flow = ((flow.get("platforms") or {}).get("yandex_market") or {}) if isinstance(flow, dict) else {}
    oz_platform_flow = ((flow.get("platforms") or {}).get("ozon") or {}) if isinstance(flow, dict) else {}

    return {
        "ok": True,
        "data_flow": flow,
        "imports": {
            "catalog": _get_catalog_import_config(data),
        },
        "yandex_market": {
            "connected": ym_connected,
            "business_id": ym_business,
            "campaign_id": ym.get("campaign_id") or "",
            "campaign_name": ym.get("campaign_name") or "",
            "connected_at": ym.get("connected_at"),
            "data_flow": _effective_flow(
                global_flow=global_flow,
                platform_flow=ym_platform_flow,
            ),
            "accounts": [
                {
                    "business_id": a.get("business_id") or "",
                    "api_key": a.get("api_key") or "",
                    "connected_at": a.get("connected_at"),
                    "data_flow": _effective_flow(
                        global_flow=global_flow,
                        platform_flow=ym_platform_flow,
                        account=a,
                    ),
                    "shops": [
                        {
                            **s,
                            "data_flow": _effective_flow(
                                global_flow=global_flow,
                                platform_flow=ym_platform_flow,
                                account=a,
                                shop=s,
                            ),
                        }
                        for s in (a.get("shops") or [])
                    ],
                }
                for a in ym_accounts
            ],
            "shops": ym_shops,
        },
        "ozon": {
            "connected": oz_connected,
            "available": True,
            "message": "Подключение по Client-Id и Api-Key",
            "accounts": [
                {
                    "client_id": a.get("client_id") or "",
                    "api_key": a.get("api_key") or "",
                    "seller_id": a.get("seller_id") or "",
                    "seller_name": a.get("seller_name") or "",
                    "currency_code": str(a.get("currency_code") or "RUB"),
                    "fulfillment_model": str(a.get("fulfillment_model") or "FBO"),
                    "connected_at": a.get("connected_at"),
                    "data_flow": _effective_flow(
                        global_flow=global_flow,
                        platform_flow=oz_platform_flow,
                        account=a,
                    ),
                    "health_status": a.get("health_status"),
                    "health_message": a.get("health_message"),
                    "health_checked_at": a.get("health_checked_at"),
                    "stores": a.get("stores") or [],
                }
                for a in oz_accounts
            ],
            "data_flow": _effective_flow(
                global_flow=global_flow,
                platform_flow=oz_platform_flow,
            ),
        },
        "wildberries": {
            "connected": False,
            "available": False,
            "message": "Интеграция Wildberries в разработке",
        },
        "google": {
            "credentials_configured": google_configured,
            "active_account_id": g_active_id,
            "accounts": g_accounts,
        },
    }


@router.post("/api/integrations/data-flow")
async def update_data_flow(payload: dict):
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "message": "Некорректный payload"}, status_code=400)

    scope = str(payload.get("scope") or "global").strip().lower()
    platform = str(payload.get("platform") or "").strip() or None
    business_id = str(payload.get("business_id") or "").strip() or None
    campaign_id = str(payload.get("campaign_id") or "").strip() or None

    has_import = "import_enabled" in payload and payload.get("import_enabled") is not None
    has_export = "export_enabled" in payload and payload.get("export_enabled") is not None
    if not has_import and not has_export:
        return JSONResponse({"ok": False, "message": "Передайте import_enabled и/или export_enabled"}, status_code=400)

    import_value = payload.get("import_enabled") if has_import else None
    export_value = payload.get("export_enabled") if has_export else None
    if has_import and not isinstance(import_value, bool):
        return JSONResponse({"ok": False, "message": "import_enabled должен быть boolean"}, status_code=400)
    if has_export and not isinstance(export_value, bool):
        return JSONResponse({"ok": False, "message": "export_enabled должен быть boolean"}, status_code=400)

    try:
        settings = save_scoped_data_flow_settings(
            scope=scope,
            platform=platform,
            business_id=business_id,
            campaign_id=campaign_id,
            import_enabled=import_value,
            export_enabled=export_value,
        )
    except ValueError as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=400)

    return {"ok": True, "data_flow": settings}


@router.post("/api/integrations/yamarket/campaigns")
async def resolve_yandex_campaigns(payload: dict):
    api_key = str(payload.get("api_key") or "").strip()
    business_id = str(payload.get("business_id") or "").strip()

    if not api_key:
        return JSONResponse({"ok": False, "message": "Укажите API token Яндекс.Маркета"}, status_code=400)
    if not business_id:
        return JSONResponse({"ok": False, "message": "Укажите Business ID Яндекс.Маркета"}, status_code=400)

    try:
        payload = await _fetch_campaigns_payload(api_key)
        campaigns = _extract_campaigns(payload, business_id)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "message": f"Не удалось получить список магазинов Маркета: {e}"},
            status_code=400,
        )

    if not campaigns:
        return JSONResponse(
            {
                "ok": False,
                "message": "Для указанного Business ID не найдено магазинов (campaigns).",
                "items": [],
            },
            status_code=404,
        )

    auto_selected = campaigns[0] if len(campaigns) == 1 else None
    return {
        "ok": True,
        "items": campaigns,
        "need_select": len(campaigns) > 1,
        "auto_selected": auto_selected,
    }


@router.post("/api/integrations/yamarket/connect")
async def connect_yandex_market(payload: dict):
    api_key = str(payload.get("api_key") or "").strip()
    business_id = str(payload.get("business_id") or "").strip()
    campaign_id = str(payload.get("campaign_id") or "").strip()
    raw_campaign_ids = payload.get("campaign_ids")

    if not api_key:
        return JSONResponse({"ok": False, "message": "Укажите API token Яндекс.Маркета"}, status_code=400)
    if not business_id:
        return JSONResponse({"ok": False, "message": "Укажите Business ID Яндекс.Маркета"}, status_code=400)

    try:
        payload = await _fetch_campaigns_payload(api_key)
        campaigns = _extract_campaigns(payload, business_id)
    except Exception as e:
        return JSONResponse(
            {
                "ok": False,
                "message": f"Не удалось подключиться к Яндекс.Маркету: {e}",
            },
            status_code=400,
        )

    if not campaigns:
        return JSONResponse(
            {
                "ok": False,
                "message": "Для указанного Business ID не найдено магазинов (campaigns).",
            },
            status_code=404,
        )

    selected_ids: list[str] = []
    if isinstance(raw_campaign_ids, list):
        selected_ids = [str(x).strip() for x in raw_campaign_ids if str(x).strip()]
    if campaign_id and campaign_id not in selected_ids:
        selected_ids.append(campaign_id)

    if not selected_ids:
        if len(campaigns) == 1:
            selected_ids = [campaigns[0]["id"]]
        else:
            return JSONResponse(
                {
                    "ok": False,
                    "need_campaign_select": True,
                    "message": "Найдено несколько магазинов. Выберите один или несколько магазинов.",
                    "campaigns": campaigns,
                },
                status_code=409,
            )

    campaigns_by_id = {c["id"]: c for c in campaigns}
    selected_campaigns: list[dict] = []
    for cid in selected_ids:
        c = campaigns_by_id.get(cid)
        if not c:
            return JSONResponse(
                {"ok": False, "message": f"Магазин с campaign_id={cid} не найден для указанного Business ID."},
                status_code=400,
            )
        selected_campaigns.append(c)

    data = load_integrations()
    now = datetime.datetime.utcnow().isoformat() + "Z"
    ym = data.get("yandex_market") or {}
    accounts = _normalize_ym_accounts(ym)


    account = next((a for a in accounts if str(a.get("business_id") or "") == business_id), None)
    if not account:
        account = {"business_id": business_id, "api_key": api_key, "connected_at": now, "shops": []}
        accounts.append(account)
    else:
        account["api_key"] = api_key
        account["connected_at"] = now

    account_shops = account.get("shops") or []
    existing_shop_by_id = {str((s or {}).get("campaign_id") or ""): s for s in account_shops if isinstance(s, dict)}
    for selected in selected_campaigns:
        prev_shop = existing_shop_by_id.get(str(selected["id"]))
        new_shop = {
            "campaign_id": selected["id"],
            "campaign_name": selected["name"],
            "business_id": business_id,
            "currency_code": str((prev_shop or {}).get("currency_code") or "RUB").strip().upper() or "RUB",
            "fulfillment_model": str((prev_shop or {}).get("fulfillment_model") or "FBO").strip().upper() or "FBO",
            "connected_at": now,
            "health_status": None,
            "health_message": "",
            "health_checked_at": None,
        }
        replaced = False
        for i, s in enumerate(account_shops):
            if s["campaign_id"] == new_shop["campaign_id"]:
                account_shops[i] = new_shop
                replaced = True
                break
        if not replaced:
            account_shops.append(new_shop)
    account["shops"] = account_shops

    all_shops = [s for a in accounts for s in (a.get("shops") or [])]
    primary = selected_campaigns[0]

    data["yandex_market"] = {
        "api_key": api_key,
        "business_id": business_id,
        "campaign_id": primary["id"],  # legacy current
        "campaign_name": primary["name"],  # legacy current
        "connected_at": now,
        "accounts": accounts,
        "shops": all_shops,  # legacy flat view
    }
    save_integrations(data)
    return {
        "ok": True,
        "message": "Яндекс.Маркет подключен",
        "business_id": business_id,
        "campaigns": selected_campaigns,
        "shops_count": len(all_shops),
        "accounts_count": len(accounts),
    }


@router.post("/api/integrations/yamarket/accounts/delete")
async def delete_yandex_account(payload: dict):
    business_id = str(payload.get("business_id") or "").strip()
    if not business_id:
        return JSONResponse({"ok": False, "message": "business_id обязателен"}, status_code=400)

    data = load_integrations()
    ym = data.get("yandex_market") if isinstance(data.get("yandex_market"), dict) else {}
    accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
    before = len(accounts)
    accounts = [a for a in accounts if str((a or {}).get("business_id") or "").strip() != business_id]
    if len(accounts) == before:
        return JSONResponse({"ok": False, "message": "Аккаунт не найден"}, status_code=404)

    all_shops = [s for a in accounts if isinstance(a, dict) for s in (a.get("shops") or []) if isinstance(s, dict)]
    ym["accounts"] = accounts
    ym["shops"] = all_shops
    if accounts:
        first = accounts[0]
        ym["api_key"] = str(first.get("api_key") or "")
        ym["business_id"] = str(first.get("business_id") or "")
        first_shop = (first.get("shops") or [{}])[0] if isinstance(first.get("shops"), list) else {}
        ym["campaign_id"] = str((first_shop or {}).get("campaign_id") or "")
        ym["campaign_name"] = str((first_shop or {}).get("campaign_name") or "")
    else:
        ym["api_key"] = ""
        ym["business_id"] = ""
        ym["campaign_id"] = ""
        ym["campaign_name"] = ""

    data["yandex_market"] = ym
    save_integrations(data)
    return {"ok": True}


@router.post("/api/integrations/yamarket/shops/delete")
async def delete_yandex_shop(payload: dict):
    business_id = str(payload.get("business_id") or "").strip()
    campaign_id = str(payload.get("campaign_id") or "").strip()
    if not business_id or not campaign_id:
        return JSONResponse({"ok": False, "message": "business_id и campaign_id обязательны"}, status_code=400)

    data = load_integrations()
    ym = data.get("yandex_market") if isinstance(data.get("yandex_market"), dict) else {}
    accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
    changed = False
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        if str(acc.get("business_id") or "").strip() != business_id:
            continue
        shops = acc.get("shops") if isinstance(acc.get("shops"), list) else []
        before = len(shops)
        shops = [s for s in shops if str((s or {}).get("campaign_id") or "").strip() != campaign_id]
        if len(shops) != before:
            acc["shops"] = shops
            changed = True
        break

    if not changed:
        return JSONResponse({"ok": False, "message": "Магазин не найден"}, status_code=404)

    ym["accounts"] = accounts
    ym["shops"] = [s for a in accounts if isinstance(a, dict) for s in (a.get("shops") or []) if isinstance(s, dict)]
    data["yandex_market"] = ym
    save_integrations(data)
    return {"ok": True}


@router.post("/api/integrations/yamarket/shops/{campaign_id}/check")
async def check_yandex_shop(campaign_id: str, payload: dict | None = None):
    data = load_integrations()
    ym = data.get("yandex_market") or {}
    requested_business_id = str((payload or {}).get("business_id") or "").strip()
    accounts = _normalize_ym_accounts(ym)
    account = next((a for a in accounts if str(a.get("business_id") or "") == requested_business_id), None)

    api_key = str(
        (account or {}).get("api_key")
        or ym.get("api_key")
        or ""
    ).strip()
    business_id = str(
        (account or {}).get("business_id")
        or ym.get("business_id")
        or ""
    ).strip()

    if not api_key:
        _persist_shop_health(
            business_id=business_id,
            campaign_id=campaign_id,
            ok=False,
            message="Не найден API token Яндекс.Маркета.",
        )
        return JSONResponse({"ok": False, "message": "Не найден API token Яндекс.Маркета."}, status_code=400)
    if not business_id:
        _persist_shop_health(
            business_id=requested_business_id,
            campaign_id=campaign_id,
            ok=False,
            message="Не найден Business ID Яндекс.Маркета.",
        )
        return JSONResponse({"ok": False, "message": "Не найден Business ID Яндекс.Маркета."}, status_code=400)

    try:
        payload = await _fetch_campaigns_payload(api_key)
        campaigns = _extract_campaigns(payload, business_id)
    except Exception as e:
        msg = f"Ошибка проверки API: {e}"
        _persist_shop_health(
            business_id=business_id,
            campaign_id=campaign_id,
            ok=False,
            message=msg,
        )
        return JSONResponse({"ok": False, "message": msg}, status_code=400)

    target = next((c for c in campaigns if c["id"] == str(campaign_id)), None)
    if not target:
        msg = "Магазин не найден в текущем списке кампаний. Проверьте доступы и Business ID."
        _persist_shop_health(
            business_id=business_id,
            campaign_id=campaign_id,
            ok=False,
            message=msg,
        )
        return JSONResponse(
            {
                "ok": False,
                "message": msg,
            },
            status_code=404,
        )

    _persist_shop_health(
        business_id=business_id,
        campaign_id=campaign_id,
        ok=True,
        message="",
    )
    return {"ok": True, "campaign": target, "message": "Подключение активно", "checked_at": _now_iso()}


@router.post("/api/integrations/ozon/seller-info")
async def resolve_ozon_seller_info(payload: dict):
    client_id = str(payload.get("client_id") or "").strip()
    api_key = str(payload.get("api_key") or "").strip()
    if not client_id:
        return JSONResponse({"ok": False, "message": "Укажите Client ID Ozon"}, status_code=400)
    if not api_key:
        return JSONResponse({"ok": False, "message": "Укажите API key Ozon"}, status_code=400)
    try:
        seller = await _fetch_ozon_seller_info(client_id, api_key)
        return {
            "ok": True,
            "seller": {
                "seller_id": seller["seller_id"],
                "seller_name": seller["seller_name"],
            },
        }
    except Exception as e:
        return JSONResponse({"ok": False, "message": f"Не удалось получить SellerInfo Ozon: {e}"}, status_code=400)


@router.post("/api/integrations/ozon/connect")
async def connect_ozon(payload: dict):
    client_id = str(payload.get("client_id") or "").strip()
    api_key = str(payload.get("api_key") or "").strip()
    if not client_id:
        return JSONResponse({"ok": False, "message": "Укажите Client ID Ozon"}, status_code=400)
    if not api_key:
        return JSONResponse({"ok": False, "message": "Укажите API key Ozon"}, status_code=400)

    try:
        seller = await _fetch_ozon_seller_info(client_id, api_key)
    except Exception as e:
        return JSONResponse({"ok": False, "message": f"Не удалось подключиться к Ozon: {e}"}, status_code=400)

    seller_id = str(seller.get("seller_id") or "").strip() or client_id
    seller_name = str(seller.get("seller_name") or "").strip() or f"Ozon кабинет {seller_id}"
    now = _now_iso()

    data = load_integrations()
    oz = data.get("ozon") if isinstance(data.get("ozon"), dict) else {}
    accounts = _normalize_ozon_accounts(oz)
    account = next((a for a in accounts if str(a.get("client_id") or "") == client_id), None)
    store = {
        "store_id": seller_id,
        "store_name": seller_name,
        "currency_code": str((account or {}).get("currency_code") or "RUB").strip().upper() if account else "RUB",
        "fulfillment_model": str((account or {}).get("fulfillment_model") or "FBO").strip().upper() if account else "FBO",
        "connected_at": now,
        "health_status": None,
        "health_message": "",
        "health_checked_at": None,
    }
    if account is None:
        account = {
            "client_id": client_id,
            "api_key": api_key,
            "seller_id": seller_id,
            "seller_name": seller_name,
            "currency_code": "RUB",
            "fulfillment_model": "FBO",
            "connected_at": now,
            "stores": [store],
        }
        accounts.append(account)
    else:
        account["api_key"] = api_key
        account["seller_id"] = seller_id
        account["seller_name"] = seller_name
        account["currency_code"] = str(account.get("currency_code") or "RUB").strip().upper() or "RUB"
        account["fulfillment_model"] = str(account.get("fulfillment_model") or "FBO").strip().upper() or "FBO"
        account["connected_at"] = now
        stores = account.get("stores") if isinstance(account.get("stores"), list) else []
        if stores:
            stores[0] = store
        else:
            stores = [store]
        account["stores"] = stores

    oz["connected"] = True
    oz["connected_at"] = now
    oz["client_id"] = client_id
    oz["api_key"] = api_key
    oz["seller_id"] = seller_id
    oz["seller_name"] = seller_name
    oz["accounts"] = accounts
    data["ozon"] = oz
    save_integrations(data)
    ensure_source_table(
        f"ozon:{client_id}",
        source_type="ozon",
        title=f"Ozon {client_id}",
    )
    return {
        "ok": True,
        "seller": {"seller_id": seller_id, "seller_name": seller_name},
        "accounts_count": len(accounts),
    }


@router.post("/api/integrations/store-currency")
async def set_integration_store_currency(payload: dict):
    platform = str(payload.get("platform") or "").strip().lower()
    currency_code = str(payload.get("currency_code") or "").strip().upper()
    if currency_code not in {"RUB", "USD"}:
        return JSONResponse({"ok": False, "message": "currency_code должен быть RUB или USD"}, status_code=400)

    data = load_integrations()
    updated = False
    store_uid = ""
    store_name = ""
    store_id = ""

    if platform == "yandex_market":
        business_id = str(payload.get("business_id") or "").strip()
        campaign_id = str(payload.get("campaign_id") or "").strip()
        if not business_id or not campaign_id:
            return JSONResponse({"ok": False, "message": "business_id и campaign_id обязательны"}, status_code=400)
        ym = data.get("yandex_market") if isinstance(data.get("yandex_market"), dict) else {}
        accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
        for acc in accounts:
            if not isinstance(acc, dict):
                continue
            if str(acc.get("business_id") or "").strip() != business_id:
                continue
            shops = acc.get("shops") if isinstance(acc.get("shops"), list) else []
            for shop in shops:
                if not isinstance(shop, dict):
                    continue
                if str(shop.get("campaign_id") or "").strip() != campaign_id:
                    continue
                shop["currency_code"] = currency_code
                updated = True
                store_uid = f"yandex_market:{campaign_id}"
                store_id = campaign_id
                store_name = str(shop.get("campaign_name") or f"Магазин {campaign_id}")
                break
            if updated:
                break
        if not updated:
            return JSONResponse({"ok": False, "message": "Магазин не найден"}, status_code=404)
        ym["accounts"] = accounts
        ym["shops"] = [s for a in accounts if isinstance(a, dict) for s in (a.get("shops") or []) if isinstance(s, dict)]
        data["yandex_market"] = ym
        save_integrations(data)
        upsert_store(
            platform="yandex_market",
            store_id=store_id,
            store_name=store_name,
            currency_code=currency_code,
            fulfillment_model=str(shop.get("fulfillment_model") or "FBO"),
            business_id=business_id,
            account_id=business_id,
        )
    elif platform == "ozon":
        client_id = str(payload.get("client_id") or "").strip()
        if not client_id:
            return JSONResponse({"ok": False, "message": "client_id обязателен"}, status_code=400)
        oz = data.get("ozon") if isinstance(data.get("ozon"), dict) else {}
        accounts = oz.get("accounts") if isinstance(oz.get("accounts"), list) else []
        for acc in accounts:
            if not isinstance(acc, dict):
                continue
            if str(acc.get("client_id") or "").strip() != client_id:
                continue
            acc["currency_code"] = currency_code
            stores = acc.get("stores") if isinstance(acc.get("stores"), list) else []
            if stores and isinstance(stores[0], dict):
                stores[0]["currency_code"] = currency_code
                store_name = str(stores[0].get("store_name") or acc.get("seller_name") or f"Ozon кабинет {client_id}")
            else:
                store_name = str(acc.get("seller_name") or f"Ozon кабинет {client_id}")
            updated = True
            store_uid = f"ozon:{client_id}"
            store_id = client_id
            break
        if not updated:
            return JSONResponse({"ok": False, "message": "Кабинет Ozon не найден"}, status_code=404)
        oz["accounts"] = accounts
        data["ozon"] = oz
        save_integrations(data)
        upsert_store(
            platform="ozon",
            store_id=store_id,
            store_name=store_name,
            currency_code=currency_code,
            fulfillment_model=str(next((a.get("fulfillment_model") for a in accounts if str(a.get("client_id") or "") == client_id), "FBO") or "FBO"),
            seller_id=str(next((a.get("seller_id") for a in accounts if str(a.get("client_id") or "") == client_id), "") or ""),
            account_id=client_id,
        )
    else:
        return JSONResponse({"ok": False, "message": "Поддержаны только yandex_market и ozon"}, status_code=400)

    return {"ok": True, "platform": platform, "currency_code": currency_code, "store_uid": store_uid}


@router.post("/api/integrations/store-fulfillment")
async def set_integration_store_fulfillment(payload: dict):
    platform = str(payload.get("platform") or "").strip().lower()
    fulfillment_model = str(payload.get("fulfillment_model") or "").strip().upper()
    if fulfillment_model not in {"FBO", "FBS", "DBS", "EXPRESS"}:
        return JSONResponse({"ok": False, "message": "fulfillment_model должен быть FBO, FBS, DBS или EXPRESS"}, status_code=400)

    data = load_integrations()
    updated = False
    store_uid = ""
    store_name = ""
    store_id = ""

    if platform == "yandex_market":
        business_id = str(payload.get("business_id") or "").strip()
        campaign_id = str(payload.get("campaign_id") or "").strip()
        if not business_id or not campaign_id:
            return JSONResponse({"ok": False, "message": "business_id и campaign_id обязательны"}, status_code=400)
        ym = data.get("yandex_market") if isinstance(data.get("yandex_market"), dict) else {}
        accounts = ym.get("accounts") if isinstance(ym.get("accounts"), list) else []
        for acc in accounts:
            if not isinstance(acc, dict):
                continue
            if str(acc.get("business_id") or "").strip() != business_id:
                continue
            shops = acc.get("shops") if isinstance(acc.get("shops"), list) else []
            for shop in shops:
                if not isinstance(shop, dict):
                    continue
                if str(shop.get("campaign_id") or "").strip() != campaign_id:
                    continue
                shop["fulfillment_model"] = fulfillment_model
                updated = True
                store_uid = f"yandex_market:{campaign_id}"
                store_id = campaign_id
                store_name = str(shop.get("campaign_name") or f"Магазин {campaign_id}")
                break
            if updated:
                break
        if not updated:
            return JSONResponse({"ok": False, "message": "Магазин не найден"}, status_code=404)
        ym["accounts"] = accounts
        ym["shops"] = [s for a in accounts if isinstance(a, dict) for s in (a.get("shops") or []) if isinstance(s, dict)]
        data["yandex_market"] = ym
        save_integrations(data)
        upsert_store(
            platform="yandex_market",
            store_id=store_id,
            store_name=store_name,
            currency_code=str(next((s.get("currency_code") for a in accounts if isinstance(a, dict) for s in (a.get("shops") or []) if isinstance(s, dict) and str(s.get("campaign_id") or "") == campaign_id), "RUB") or "RUB"),
            fulfillment_model=fulfillment_model,
            business_id=business_id,
            account_id=business_id,
        )
        upsert_pricing_logistics_store_settings(store_uid=store_uid, values={"fulfillment_model": fulfillment_model})
    elif platform == "ozon":
        client_id = str(payload.get("client_id") or "").strip()
        if not client_id:
            return JSONResponse({"ok": False, "message": "client_id обязателен"}, status_code=400)
        oz = data.get("ozon") if isinstance(data.get("ozon"), dict) else {}
        accounts = oz.get("accounts") if isinstance(oz.get("accounts"), list) else []
        for acc in accounts:
            if not isinstance(acc, dict):
                continue
            if str(acc.get("client_id") or "").strip() != client_id:
                continue
            acc["fulfillment_model"] = fulfillment_model
            stores = acc.get("stores") if isinstance(acc.get("stores"), list) else []
            if stores and isinstance(stores[0], dict):
                stores[0]["fulfillment_model"] = fulfillment_model
                store_name = str(stores[0].get("store_name") or acc.get("seller_name") or f"Ozon кабинет {client_id}")
            else:
                store_name = str(acc.get("seller_name") or f"Ozon кабинет {client_id}")
            updated = True
            store_uid = f"ozon:{client_id}"
            store_id = client_id
            break
        if not updated:
            return JSONResponse({"ok": False, "message": "Кабинет Ozon не найден"}, status_code=404)
        oz["accounts"] = accounts
        data["ozon"] = oz
        save_integrations(data)
        acc = next((a for a in accounts if isinstance(a, dict) and str(a.get("client_id") or "") == client_id), {})
        upsert_store(
            platform="ozon",
            store_id=store_id,
            store_name=store_name,
            currency_code=str(acc.get("currency_code") or "RUB"),
            fulfillment_model=fulfillment_model,
            seller_id=str(acc.get("seller_id") or ""),
            account_id=client_id,
        )
        upsert_pricing_logistics_store_settings(store_uid=store_uid, values={"fulfillment_model": fulfillment_model})
    else:
        return JSONResponse({"ok": False, "message": "Поддержаны только yandex_market и ozon"}, status_code=400)

    return {"ok": True, "platform": platform, "fulfillment_model": fulfillment_model, "store_uid": store_uid}


@router.post("/api/integrations/ozon/accounts/delete")
async def delete_ozon_account(payload: dict):
    client_id = str(payload.get("client_id") or "").strip()
    if not client_id:
        return JSONResponse({"ok": False, "message": "client_id обязателен"}, status_code=400)
    data = load_integrations()
    oz = data.get("ozon") if isinstance(data.get("ozon"), dict) else {}
    accounts = _normalize_ozon_accounts(oz)
    before = len(accounts)
    accounts = [a for a in accounts if str(a.get("client_id") or "").strip() != client_id]
    if len(accounts) == before:
        return JSONResponse({"ok": False, "message": "Кабинет Ozon не найден"}, status_code=404)
    oz["accounts"] = accounts
    oz["connected"] = bool(accounts)
    if accounts:
        first = accounts[0]
        oz["client_id"] = str(first.get("client_id") or "")
        oz["api_key"] = str(first.get("api_key") or "")
        oz["seller_id"] = str(first.get("seller_id") or "")
        oz["seller_name"] = str(first.get("seller_name") or "")
    else:
        oz["client_id"] = ""
        oz["api_key"] = ""
        oz["seller_id"] = ""
        oz["seller_name"] = ""
    data["ozon"] = oz
    save_integrations(data)
    return {"ok": True}


@router.post("/api/integrations/ozon/accounts/{client_id}/check")
async def check_ozon_account(client_id: str):
    data = load_integrations()
    oz = data.get("ozon") if isinstance(data.get("ozon"), dict) else {}
    accounts = _normalize_ozon_accounts(oz)
    account = next((a for a in accounts if str(a.get("client_id") or "").strip() == str(client_id).strip()), None)
    if not account:
        return JSONResponse({"ok": False, "message": "Кабинет Ozon не найден"}, status_code=404)
    api_key = str(account.get("api_key") or "").strip()
    cid = str(account.get("client_id") or "").strip()
    try:
        seller = await _fetch_ozon_seller_info(cid, api_key)
        account["health_status"] = "ok"
        account["health_message"] = ""
        account["health_checked_at"] = _now_iso()
        account["seller_id"] = str(seller.get("seller_id") or account.get("seller_id") or "")
        account["seller_name"] = str(seller.get("seller_name") or account.get("seller_name") or "")
        stores = account.get("stores") if isinstance(account.get("stores"), list) else []
        if stores:
            stores[0]["health_status"] = "ok"
            stores[0]["health_message"] = ""
            stores[0]["health_checked_at"] = account["health_checked_at"]
        else:
            account["stores"] = [
                {
                    "store_id": account["seller_id"] or cid,
                    "store_name": account["seller_name"] or f"Ozon кабинет {cid}",
                    "connected_at": account.get("connected_at"),
                    "health_status": "ok",
                    "health_message": "",
                    "health_checked_at": account["health_checked_at"],
                }
            ]
        oz["accounts"] = accounts
        data["ozon"] = oz
        save_integrations(data)
        return {"ok": True, "seller": {"seller_id": account["seller_id"], "seller_name": account["seller_name"]}, "checked_at": account["health_checked_at"]}
    except Exception as e:
        account["health_status"] = "error"
        account["health_message"] = str(e)
        account["health_checked_at"] = _now_iso()
        stores = account.get("stores") if isinstance(account.get("stores"), list) else []
        if stores:
            stores[0]["health_status"] = "error"
            stores[0]["health_message"] = str(e)
            stores[0]["health_checked_at"] = account["health_checked_at"]
        oz["accounts"] = accounts
        data["ozon"] = oz
        save_integrations(data)
        return JSONResponse({"ok": False, "message": f"Ошибка проверки Ozon: {e}"}, status_code=400)


@router.post("/api/integrations/wildberries/connect")
async def connect_wildberries(_: dict):
    return JSONResponse(
        {"ok": False, "message": "Интеграция Wildberries пока в разработке"},
        status_code=501,
    )


@router.get("/api/integrations/gsheets/mapping-templates")
async def gsheets_mapping_templates():
    return {"ok": True, "items": GSHEETS_MAPPING_TEMPLATES}


@router.post("/api/integrations/gsheets/accounts/select")
async def select_gsheets_account(payload: dict):
    account_id = str(payload.get("account_id") or "").strip()
    if not account_id:
        return JSONResponse({"ok": False, "message": "account_id обязателен"}, status_code=400)
    data = load_integrations()
    google = data.get("google") if isinstance(data.get("google"), dict) else {}
    accounts = google.get("accounts") if isinstance(google.get("accounts"), list) else []
    exists = any(str((a or {}).get("id") or "").strip() == account_id for a in accounts if isinstance(a, dict))
    if not exists:
        return JSONResponse({"ok": False, "message": "Аккаунт не найден"}, status_code=404)
    google["active_account_id"] = account_id
    data["google"] = google
    save_integrations(data)
    return {"ok": True, "active_account_id": account_id}


@router.post("/api/integrations/gsheets/accounts/upsert")
async def upsert_gsheets_account(payload: dict):
    name = str(payload.get("name") or "").strip() or "Google Service Account"
    service_account_json = str(payload.get("service_account_json") or "").strip()
    if not service_account_json:
        return JSONResponse({"ok": False, "message": "Укажите Key (JSON)"}, status_code=400)
    try:
        account = _upsert_google_account(name=name, service_account_json=service_account_json)
    except ValueError as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=400)

    return {
        "ok": True,
        "account": account,
        "active_account_id": account["id"],
    }


@router.post("/api/integrations/gsheets/accounts/upload")
async def upload_gsheets_account_key(
    key_file: UploadFile = File(...),
    name: str = Form("Google Service Account"),
):
    filename = str(key_file.filename or "").strip().lower()
    if filename and not filename.endswith(".json"):
        return JSONResponse({"ok": False, "message": "Загрузите файл с расширением .json"}, status_code=400)

    try:
        raw_bytes = await key_file.read()
        service_account_json = raw_bytes.decode("utf-8").strip()
    except Exception as e:
        return JSONResponse({"ok": False, "message": f"Не удалось прочитать файл: {e}"}, status_code=400)

    if not service_account_json:
        return JSONResponse({"ok": False, "message": "Файл пустой"}, status_code=400)

    try:
        account = _upsert_google_account(name=str(name or "").strip() or "Google Service Account", service_account_json=service_account_json)
    except ValueError as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=400)

    return {
        "ok": True,
        "account": account,
        "active_account_id": account["id"],
    }


@router.post("/api/integrations/gsheets/accounts/delete")
async def delete_gsheets_account(payload: dict):
    account_id = str(payload.get("account_id") or "").strip()
    if not account_id:
        return JSONResponse({"ok": False, "message": "account_id обязателен"}, status_code=400)

    data = load_integrations()
    google = data.get("google") if isinstance(data.get("google"), dict) else {}
    accounts = google.get("accounts") if isinstance(google.get("accounts"), list) else []
    removed = next(
        (a for a in accounts if isinstance(a, dict) and str((a or {}).get("id") or "").strip() == account_id),
        None,
    )
    before = len(accounts)
    accounts = [a for a in accounts if str((a or {}).get("id") or "").strip() != account_id]
    if len(accounts) == before:
        return JSONResponse({"ok": False, "message": "Аккаунт не найден"}, status_code=404)

    google["accounts"] = accounts
    active_id = str(google.get("active_account_id") or "").strip()
    if active_id == account_id:
        google["active_account_id"] = str((accounts[0] or {}).get("id") or "") if accounts else ""
    data["google"] = google
    save_integrations(data)

    if isinstance(removed, dict):
        key_file_path = str(removed.get("key_file_path") or "").strip()
        if key_file_path:
            fp = BASE_DIR / key_file_path
            if fp.exists():
                try:
                    fp.unlink()
                except Exception:
                    pass

    return {"ok": True, "active_account_id": google.get("active_account_id") or ""}


@router.post("/api/integrations/gsheets/verify")
async def verify_gsheets_connection(payload: dict):
    raw_sheet = str(payload.get("spreadsheet_url") or payload.get("spreadsheet_id") or "").strip()
    spreadsheet_id = parse_sheet_id(raw_sheet)
    if not spreadsheet_id:
        return JSONResponse({"ok": False, "message": "Укажите URL или ID Google таблицы"}, status_code=400)

    service_account_json = str(payload.get("service_account_json") or "").strip()
    service_account_b64 = str(payload.get("service_account_b64") or "").strip()
    account_id = str(payload.get("account_id") or "").strip()
    worksheet = str(payload.get("worksheet") or "").strip() or None

    if account_id and not service_account_json and not service_account_b64:
        data = load_integrations()
        google = data.get("google") if isinstance(data.get("google"), dict) else {}
        accounts = google.get("accounts") if isinstance(google.get("accounts"), list) else []
        acc = next(
            (a for a in accounts if isinstance(a, dict) and str(a.get("id") or "").strip() == account_id),
            None,
        )
        if not acc:
            return JSONResponse({"ok": False, "message": "Google аккаунт не найден"}, status_code=404)
        service_account_json = str(acc.get("service_account_json") or "").strip()
        service_account_b64 = str(acc.get("service_account_b64") or "").strip()

    try:
        from backend.services.gsheets import list_worksheets

        worksheets = list_worksheets(
            spreadsheet_id,
            raw_json_override=service_account_json or None,
            raw_b64_override=service_account_b64 or None,
        )
        preview = read_sheet_preview(
            spreadsheet_id,
            worksheet=worksheet or (worksheets[0] if worksheets else None),
            limit=1,
            raw_json_override=service_account_json or None,
            raw_b64_override=service_account_b64 or None,
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "message": f"Не удалось подключиться к Google Sheets: {e}"},
            status_code=400,
        )

    headers = preview["preview"][0] if preview.get("preview") else []
    return {
        "ok": True,
        "spreadsheet_id": spreadsheet_id,
        "worksheets": worksheets,
        "headers": headers,
    }


@router.post("/api/integrations/gsheets/sources/{source_id}/check")
async def check_gsheets_source(source_id: str):
    sources = load_sources()
    src = next((s for s in sources if str(s.get("id") or "").strip() == str(source_id).strip()), None)
    if not src:
        return JSONResponse({"ok": False, "message": "Источник не найден"}, status_code=404)
    if str(src.get("type") or "").lower() != "gsheets":
        return JSONResponse({"ok": False, "message": "Источник не является Google Sheets"}, status_code=400)

    spreadsheet_id = parse_sheet_id(str(src.get("spreadsheet_id") or "").strip())
    worksheet = str(src.get("worksheet") or "").strip() or None
    if not spreadsheet_id:
        _persist_source_health(source_id=source_id, ok=False, message="Не задан spreadsheet_id")
        return JSONResponse({"ok": False, "message": "Не задан spreadsheet_id"}, status_code=400)

    try:
        preview = read_sheet_preview(spreadsheet_id, worksheet=worksheet, limit=1)
        _persist_source_health(source_id=source_id, ok=True, message="")
        headers = preview["preview"][0] if preview.get("preview") else []
        return {"ok": True, "message": "Источник доступен", "headers": headers, "checked_at": _now_iso()}
    except Exception as e:
        msg = str(e)
        _persist_source_health(source_id=source_id, ok=False, message=msg)
        return JSONResponse({"ok": False, "message": msg}, status_code=400)
