from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, timedelta
from typing import Any

import httpx

from backend.routers._shared import (
    YANDEX_BASE_URL,
    _catalog_marketplace_stores_context,
    _find_yandex_shop_credentials,
    _ym_headers,
)
from backend.services.store_data_model import replace_sales_shelfs_statistics_report_rows_for_period
from backend.services.yandex_json_report_helpers import iter_month_ranges as _iter_month_ranges
from backend.services.yandex_json_report_helpers import parse_report_bytes as _shared_parse_report_bytes

logger = logging.getLogger("uvicorn.error")

DEFAULT_SHELFS_DATE_FROM = "2025-07-01"
DEFAULT_SHELFS_DATE_TO = "2026-03-11"
_REPORT_INFO_POLL_INTERVAL_SEC = 30
_REPORT_INFO_MAX_ATTEMPTS = 12


def _is_target_sheet(sheet_name: str, payload: Any) -> bool:
    name = str(sheet_name or "").strip().lower()
    if "shelfs_statistics_summary" in name:
        return True
    if "общ" in name and "отчет" in name:
        return True
    if isinstance(payload, dict):
        local = json.dumps(payload, ensure_ascii=False).lower()
        if "общий отчет" in local or "общий отчёт" in local or "shelfs_statistics_summary" in local:
            return True
    return False

def _parse_report_bytes(content: bytes) -> list[dict[str, Any]]:
    return _shared_parse_report_bytes(content, is_target_sheet=_is_target_sheet)


async def _generate_report(*, business_id: str, campaign_id: str, api_key: str, date_from: str, date_to: str) -> str:
    url = f"{YANDEX_BASE_URL}/reports/shelf-statistics/generate"
    params = {"format": "JSON", "language": "RU"}
    body = {
        "businessId": int(business_id),
        "dateFrom": date_from,
        "dateTo": date_to,
        "attributionType": "SHOWS",
    }
    async with httpx.AsyncClient(timeout=90) as client:
        logger.warning(
            "[sales_shelfs] shelfs statistics generate request business_id=%s campaign_id=%s date_from=%s date_to=%s",
            business_id,
            campaign_id,
            date_from,
            date_to,
        )
        resp = await client.post(url, headers=_ym_headers(api_key), params=params, json=body)
        logger.warning(
            "[sales_shelfs] shelfs statistics generate response business_id=%s campaign_id=%s status=%s",
            business_id,
            campaign_id,
            resp.status_code,
        )
        if resp.status_code >= 400:
            logger.warning(
                "[sales_shelfs] shelfs statistics generate error business_id=%s campaign_id=%s body=%s",
                business_id,
                campaign_id,
                resp.text[:800],
            )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
    report_id = str(
        ((data.get("result") or {}) if isinstance(data.get("result"), dict) else {}).get("reportId")
        or data.get("reportId")
        or ""
    ).strip()
    if not report_id:
        raise RuntimeError("Маркет не вернул reportId для shelfs statistics report")
    return report_id


async def _wait_report_download_url(*, report_id: str, api_key: str) -> tuple[str, str, str]:
    url = f"{YANDEX_BASE_URL}/reports/info/{report_id}"
    last_error = ""
    async with httpx.AsyncClient(timeout=60) as client:
        for attempt in range(_REPORT_INFO_MAX_ATTEMPTS):
            resp = await client.get(url, headers=_ym_headers(api_key))
            if resp.status_code >= 400:
                last_error = resp.text[:400]
                resp.raise_for_status()
            data = resp.json() if resp.content else {}
            result = data.get("result") if isinstance(data.get("result"), dict) else data
            status = str(result.get("status") or data.get("status") or "").strip().upper()
            sub_status = str(result.get("subStatus") or data.get("subStatus") or "").strip().upper()
            file_url = str(result.get("file") or result.get("url") or result.get("fileUrl") or result.get("downloadUrl") or "").strip()
            file_name = str(result.get("fileName") or result.get("filename") or "").strip()
            logger.warning(
                "[sales_shelfs] shelfs statistics report info report_id=%s attempt=%s status=%s sub_status=%s file=%s",
                report_id,
                attempt + 1,
                status or "-",
                sub_status or "-",
                "set" if file_url else "-",
            )
            if file_url:
                return file_url, file_name, sub_status
            if status == "DONE" and sub_status == "NO_DATA":
                return "", file_name, sub_status
            if status in {"FAILED", "ERROR"}:
                raise RuntimeError(f"Маркет не смог подготовить shelfs statistics report: {data}")
            await asyncio.sleep(_REPORT_INFO_POLL_INTERVAL_SEC)
    raise RuntimeError(f"Не удалось дождаться shelfs statistics report: {last_error or report_id}")


async def refresh_yandex_shelfs_statistics_for_store(
    *,
    store_uid: str,
    campaign_id: str,
    date_from: str,
    date_to: str,
) -> dict[str, Any]:
    creds = _find_yandex_shop_credentials(campaign_id)
    if not creds:
        raise ValueError(f"credentials_not_found:{campaign_id}")
    business_id, _campaign_id, api_key = creds
    total_rows = 0
    chunks: list[dict[str, Any]] = []
    for chunk_from, chunk_to in _iter_month_ranges(date_from, date_to):
        report_id = await _generate_report(
            business_id=business_id,
            campaign_id=campaign_id,
            api_key=api_key,
            date_from=chunk_from,
            date_to=chunk_to,
        )
        file_url, file_name, sub_status = await _wait_report_download_url(report_id=report_id, api_key=api_key)
        rows: list[dict[str, Any]] = []
        if file_url:
            async with httpx.AsyncClient(timeout=180) as client:
                file_resp = await client.get(file_url)
                file_resp.raise_for_status()
            rows = _parse_report_bytes(file_resp.content)
        loaded = replace_sales_shelfs_statistics_report_rows_for_period(
            store_uid=store_uid,
            platform="yandex_market",
            date_from=chunk_from,
            date_to=chunk_to,
            report_id=report_id,
            rows=[{**row, "source_updated_at": report_id} for row in rows],
        )
        total_rows += loaded
        logger.warning(
            "[sales_shelfs] shelfs statistics loaded store_uid=%s campaign_id=%s report_id=%s date_from=%s date_to=%s rows=%s file_name=%s",
            store_uid,
            campaign_id,
            report_id,
            chunk_from,
            chunk_to,
            loaded,
            file_name,
        )
        chunks.append(
            {
                "report_id": report_id,
                "date_from": chunk_from,
                "date_to": chunk_to,
                "rows": loaded,
                "file_name": file_name,
                "sub_status": sub_status,
            }
        )
    return {
        "store_uid": store_uid,
        "campaign_id": campaign_id,
        "date_from": date_from,
        "date_to": date_to,
        "rows": total_rows,
        "chunks": chunks,
    }


async def refresh_sales_shelfs_statistics_history(
    *,
    date_from: str = DEFAULT_SHELFS_DATE_FROM,
    date_to: str = DEFAULT_SHELFS_DATE_TO,
    store_uids: list[str] | None = None,
) -> dict[str, Any]:
    stores = [
        store for store in _catalog_marketplace_stores_context()
        if str(store.get("platform") or "").strip().lower() == "yandex_market"
    ]
    selected = {str(x or "").strip() for x in (store_uids or []) if str(x or "").strip()}
    if selected:
        stores = [store for store in stores if str(store.get("store_uid") or "").strip() in selected]
    if not stores:
        raise RuntimeError("Не найдены магазины Яндекс.Маркета для shelfs statistics report")

    async def _run_store(store: dict[str, Any]) -> dict[str, Any]:
        store_uid = str(store.get("store_uid") or "").strip()
        campaign_id = str(store.get("store_id") or "").strip()
        if not store_uid or not campaign_id:
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": "store_uid_or_campaign_id_missing"}
        try:
            result = await refresh_yandex_shelfs_statistics_for_store(
                store_uid=store_uid,
                campaign_id=campaign_id,
                date_from=date_from,
                date_to=date_to,
            )
            return {"ok": True, "result": result}
        except Exception as exc:
            logger.warning(
                "[sales_shelfs] shelfs statistics refresh failed store_uid=%s campaign_id=%s error=%s",
                store_uid,
                campaign_id,
                exc,
            )
            return {"ok": False, "store_uid": store_uid, "campaign_id": campaign_id, "error": str(exc)}

    settled = await asyncio.gather(*[_run_store(store) for store in stores])
    results: list[dict[str, Any]] = [item["result"] for item in settled if item.get("ok")]
    errors: list[dict[str, str]] = [
        {
            "store_uid": str(item.get("store_uid") or "").strip(),
            "campaign_id": str(item.get("campaign_id") or "").strip(),
            "error": str(item.get("error") or "").strip(),
        }
        for item in settled
        if not item.get("ok")
    ]
    return {"ok": len(errors) == 0, "date_from": date_from, "date_to": date_to, "stores": results, "errors": errors}
