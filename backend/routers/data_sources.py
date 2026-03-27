from __future__ import annotations

import datetime

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.routers._shared import (
    GSHEETS_MAPPING_TEMPLATES,
    _mapping_configured,
    _normalize_source_id,
    _now_iso,
    _persist_source_health,
    _to_bool,
    ensure_source_table,
    load_integrations,
    load_sources,
    parse_sheet_id,
    read_sheet_preview,
    save_integrations,
    save_sources,
)

router = APIRouter()


@router.post("/api/data/sources/gsheets/connect")
async def connect_gsheets_source(payload: dict):
    title = str(payload.get("title") or "").strip() or "Google Sheets"
    worksheet = str(payload.get("worksheet") or "").strip() or None
    source_id = _normalize_source_id(str(payload.get("source_id") or ""))
    mode_import = _to_bool(payload.get("mode_import"), True)
    mode_export = _to_bool(payload.get("mode_export"), False)
    mapping_template = str(payload.get("mapping_template") or "custom").strip() or "custom"
    if mapping_template not in GSHEETS_MAPPING_TEMPLATES:
        mapping_template = "custom"
    mapping = payload.get("mapping") if isinstance(payload.get("mapping"), dict) else {}

    raw_sheet = str(payload.get("spreadsheet_url") or payload.get("spreadsheet_id") or "").strip()
    spreadsheet_id = parse_sheet_id(raw_sheet)
    if not spreadsheet_id:
        return JSONResponse({"ok": False, "message": "Укажите URL или ID Google таблицы"}, status_code=400)

    # Шаг 2: credentials бота для доступа к Google Sheets
    service_account_json = str(payload.get("service_account_json") or "").strip()
    service_account_b64 = str(payload.get("service_account_b64") or "").strip()
    account_id = str(payload.get("account_id") or "").strip()
    if account_id and not service_account_json and not service_account_b64:
        integrations = load_integrations()
        google = integrations.get("google") if isinstance(integrations.get("google"), dict) else {}
        accounts = google.get("accounts") if isinstance(google.get("accounts"), list) else []
        acc = next(
            (a for a in accounts if isinstance(a, dict) and str(a.get("id") or "").strip() == account_id),
            None,
        )
        if not acc:
            return JSONResponse({"ok": False, "message": "Google аккаунт не найден"}, status_code=404)
        service_account_json = str(acc.get("service_account_json") or "").strip()
        service_account_b64 = str(acc.get("service_account_b64") or "").strip()
        google["active_account_id"] = account_id
        integrations["google"] = google
        save_integrations(integrations)
    if service_account_json or service_account_b64:
        integrations = load_integrations()
        google = integrations.get("google") or {}
        if service_account_json:
            google["service_account_json"] = service_account_json
        if service_account_b64:
            google["service_account_b64"] = service_account_b64
        integrations["google"] = google
        save_integrations(integrations)

    try:
        preview = read_sheet_preview(
            spreadsheet_id,
            worksheet=worksheet,
            limit=1,
            raw_json_override=service_account_json or None,
            raw_b64_override=service_account_b64 or None,
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "message": f"Не удалось подключиться к Google Sheets: {e}"},
            status_code=400,
        )

    sources = load_sources()
    now = datetime.datetime.utcnow().isoformat() + "Z"

    existing = next((s for s in sources if s.get("id") == source_id), None)
    if existing:
        existing["title"] = title
        existing["type"] = "gsheets"
        existing["spreadsheet_id"] = spreadsheet_id
        existing["worksheet"] = preview.get("worksheet") or worksheet
        existing["last_refreshed"] = now
        existing["mode_import"] = mode_import
        existing["mode_export"] = mode_export
        existing["mapping_template"] = mapping_template
        existing["mapping"] = mapping or existing.get("mapping", {})
        saved = existing
    else:
        saved = {
            "id": source_id,
            "title": title,
            "type": "gsheets",
            "spreadsheet_id": spreadsheet_id,
            "worksheet": preview.get("worksheet") or worksheet,
            "mapping": mapping,
            "mapping_template": mapping_template,
            "mode_import": mode_import,
            "mode_export": mode_export,
            "last_refreshed": now,
        }
        sources.append(saved)

    save_sources(sources)
    ensure_source_table(
        f"gsheets:{source_id}",
        source_type="gsheets",
        title=title,
    )
    return {
        "ok": True,
        "message": "Google Sheets источник подключен",
        "item": {**saved, "mapping_configured": _mapping_configured(saved)},
    }


@router.post("/api/data/sources/delete")
async def delete_source(payload: dict):
    source_id = str(payload.get("source_id") or "").strip()
    if not source_id:
        return JSONResponse({"ok": False, "message": "source_id обязателен"}, status_code=400)
    sources = load_sources()
    before = len(sources)
    sources = [s for s in sources if str(s.get("id") or "").strip() != source_id]
    if len(sources) == before:
        return JSONResponse({"ok": False, "message": "Источник не найден"}, status_code=404)
    save_sources(sources)
    return {"ok": True}


@router.post("/api/data/sources/{source_id}/flow")
async def update_source_flow(source_id: str, payload: dict):
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "message": "Некорректный payload"}, status_code=400)

    has_import = "mode_import" in payload and payload.get("mode_import") is not None
    has_export = "mode_export" in payload and payload.get("mode_export") is not None
    if not has_import and not has_export:
        return JSONResponse({"ok": False, "message": "Передайте mode_import и/или mode_export"}, status_code=400)

    mode_import = payload.get("mode_import") if has_import else None
    mode_export = payload.get("mode_export") if has_export else None
    if has_import and not isinstance(mode_import, bool):
        return JSONResponse({"ok": False, "message": "mode_import должен быть boolean"}, status_code=400)
    if has_export and not isinstance(mode_export, bool):
        return JSONResponse({"ok": False, "message": "mode_export должен быть boolean"}, status_code=400)

    sources = load_sources()
    src = next((s for s in sources if str(s.get("id") or "").strip() == str(source_id).strip()), None)
    if not src:
        return JSONResponse({"ok": False, "message": "Источник не найден"}, status_code=404)

    if has_import:
        src["mode_import"] = bool(mode_import)
    if has_export:
        src["mode_export"] = bool(mode_export)
    save_sources(sources)

    row = dict(src)
    row["mapping_configured"] = _mapping_configured(row)
    return {"ok": True, "item": row}


@router.get("/api/data/sources/gsheets/test")
async def test_gsheets(which: str = "catalog_prices", worksheet: str | None = None, limit: int = 5):
    try:
        sources = load_sources()
        src = next((s for s in sources if s["id"] == which), None)
        if not src:
            return {"ok": False, "error": f"Источник {which} не найден"}
        preview = read_sheet_preview(
            src["spreadsheet_id"],
            worksheet=worksheet or src.get("worksheet"),
            limit=limit,
        )
        return {"ok": True, **preview}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.get("/api/data/sources/gsheets/headers")
async def gsheets_headers(which: str = "catalog_prices", worksheet: str | None = None):
    try:
        sources = load_sources()
        src = next((s for s in sources if s["id"] == which), None)
        if not src:
            return {"ok": False, "error": f"Источник {which} не найден"}

        preview = read_sheet_preview(
            src["spreadsheet_id"],
            worksheet=worksheet or src.get("worksheet"),
            limit=1,
        )
        headers = preview["preview"][0] if preview["preview"] else []

        mapping = src.get("mapping", {}).copy()
        if "НомерЗаказаМП" in headers and "order_id" not in mapping:
            mapping["order_id"] = "НомерЗаказаМП"

        return {"ok": True, "headers": headers, "mapping": mapping}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.post("/api/data/sources/mapping")
async def save_mapping(which: str, mapping: dict):
    try:
        sources = load_sources()
        for s in sources:
            if s["id"] == which:
                s["mapping"] = mapping
                save_sources(sources)
                return {"ok": True, "message": "Mapping сохранён"}
        return {"ok": False, "error": f"Источник {which} не найден"}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


def refresh_all_sources():
    sources = load_sources()
    updated = []
    for s in sources:
        if s.get("type") != "gsheets":
            continue
        try:
            _ = read_sheet_preview(
                s["spreadsheet_id"],
                worksheet=s.get("worksheet"),
                limit=1,
            )
            s["last_refreshed"] = datetime.datetime.utcnow().isoformat() + "Z"
            s["health_status"] = "ok"
            s["health_message"] = ""
            s["health_checked_at"] = _now_iso()
            updated.append({"id": s["id"], "status": "ok"})
        except Exception as e:
            s["health_status"] = "error"
            s["health_message"] = str(e)
            s["health_checked_at"] = _now_iso()
            updated.append({"id": s["id"], "status": "error", "error": str(e)})
    save_sources(sources)
    return updated


async def run_integrations_health_check() -> dict:
    """
    Периодическая проверка доступности интеграций:
    - Google Sheets источники
    - Яндекс.Маркет магазины
    """
    from backend.routers._shared import (
        _extract_campaigns,
        _fetch_campaigns_payload,
        _normalize_ym_accounts,
        _persist_shop_health,
        load_integrations,
    )

    gsheets_ok = 0
    gsheets_err = 0
    yandex_ok = 0
    yandex_err = 0

    # 1) Google Sheets
    sources = load_sources()
    for src in sources:
        if str(src.get("type") or "").lower() != "gsheets":
            continue
        sid = str(src.get("id") or "").strip()
        spreadsheet_id = parse_sheet_id(str(src.get("spreadsheet_id") or "").strip())
        worksheet = str(src.get("worksheet") or "").strip() or None
        if not spreadsheet_id:
            _persist_source_health(source_id=sid, ok=False, message="Не задан spreadsheet_id")
            gsheets_err += 1
            continue
        try:
            _ = read_sheet_preview(spreadsheet_id, worksheet=worksheet, limit=1)
            _persist_source_health(source_id=sid, ok=True, message="")
            gsheets_ok += 1
        except Exception as e:
            _persist_source_health(source_id=sid, ok=False, message=str(e))
            gsheets_err += 1

    # 2) Яндекс.Маркет (по аккаунтам и магазинам)
    integrations = load_integrations()
    ym = integrations.get("yandex_market") or {}
    accounts = _normalize_ym_accounts(ym)
    for acc in accounts:
        api_key = str(acc.get("api_key") or "").strip()
        business_id = str(acc.get("business_id") or "").strip()
        shops = acc.get("shops") if isinstance(acc.get("shops"), list) else []

        if not api_key or not business_id:
            for shop in shops:
                cid = str(shop.get("campaign_id") or "").strip()
                if not cid:
                    continue
                _persist_shop_health(
                    business_id=business_id,
                    campaign_id=cid,
                    ok=False,
                    message="Не задан API token или Business ID.",
                )
                yandex_err += 1
            continue

        try:
            payload = await _fetch_campaigns_payload(api_key)
            campaigns = _extract_campaigns(payload, business_id)
            campaign_ids = {str(c.get("id") or "").strip() for c in campaigns}
            for shop in shops:
                cid = str(shop.get("campaign_id") or "").strip()
                if not cid:
                    continue
                if cid in campaign_ids:
                    _persist_shop_health(business_id=business_id, campaign_id=cid, ok=True, message="")
                    yandex_ok += 1
                else:
                    _persist_shop_health(
                        business_id=business_id,
                        campaign_id=cid,
                        ok=False,
                        message="Магазин не найден в текущем списке кампаний.",
                    )
                    yandex_err += 1
        except Exception as e:
            err_msg = f"Ошибка проверки API: {e}"
            for shop in shops:
                cid = str(shop.get("campaign_id") or "").strip()
                if not cid:
                    continue
                _persist_shop_health(
                    business_id=business_id,
                    campaign_id=cid,
                    ok=False,
                    message=err_msg,
                )
                yandex_err += 1

    return {
        "ok": True,
        "checked_at": _now_iso(),
        "gsheets": {"ok": gsheets_ok, "error": gsheets_err},
        "yandex_market": {"ok": yandex_ok, "error": yandex_err},
    }


@router.get("/api/data/sources/mapping/refresh")
async def refresh_sources():
    try:
        result = refresh_all_sources()
        return {"ok": True, "updated": result}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# Планировщик из main.py управляет периодическими задачами.
