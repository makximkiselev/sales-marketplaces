import { API_BASE, apiGet, apiPost, apiPostOk } from "../../../lib/api";
import type { CampaignItem, IntegrationsPayload, SourceItem } from "./types";

export function fetchSources() {
  return apiGet<{ ok?: boolean; items?: SourceItem[] }>("/api/sources");
}

export function fetchIntegrations() {
  return apiGet<IntegrationsPayload & { ok?: boolean }>("/api/integrations");
}

export function postIntegrationFlow(payload: Record<string, unknown>) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/data-flow", payload);
}

export function postSourceFlow(sourceId: string, payload: { mode_import?: boolean; mode_export?: boolean }) {
  return apiPostOk<{ ok: boolean; message?: string }>(
    `/api/data/sources/${encodeURIComponent(sourceId)}/flow`,
    payload,
  );
}

export function postStoreCurrency(payload: {
  platform: "yandex_market" | "ozon";
  currency_code: "RUB" | "USD";
  business_id?: string;
  campaign_id?: string;
  client_id?: string;
}) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/store-currency", payload);
}

export function postStoreFulfillment(payload: {
  platform: "yandex_market" | "ozon";
  fulfillment_model: "FBO" | "FBS" | "DBS" | "EXPRESS";
  business_id?: string;
  campaign_id?: string;
  client_id?: string;
}) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/store-fulfillment", payload);
}

export async function fetchYandexCampaigns(apiKey: string, businessId: string): Promise<CampaignItem[]> {
  const data = await apiPostOk<{ ok: boolean; message?: string; items?: CampaignItem[] }>(
    "/api/integrations/yamarket/campaigns",
    { api_key: apiKey, business_id: businessId },
  );
  return data.items || [];
}

export function connectYandexAccount(payload: {
  api_key: string;
  business_id: string;
  campaign_ids: string[];
}) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/yamarket/connect", payload);
}

export function checkGsheetSource(sourceId: string) {
  return apiPost<unknown>(`/api/integrations/gsheets/sources/${sourceId}/check`);
}

export function checkYandexShop(campaignId: string, businessId: string) {
  return apiPost<unknown>(`/api/integrations/yamarket/shops/${campaignId}/check`, {
    business_id: businessId,
  });
}

export function verifyGsheets(payload: {
  spreadsheet_url: string;
  account_id?: string;
  worksheet?: string;
}) {
  return apiPostOk<{ ok: boolean; message?: string; worksheets?: string[] }>(
    "/api/integrations/gsheets/verify",
    payload,
  );
}

export function connectGsheetsSource(payload: {
  source_id: string;
  title: string;
  spreadsheet_url: string;
  worksheet: string;
  mode_import: boolean;
  mode_export: boolean;
  account_id?: string;
}) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/data/sources/gsheets/connect", payload);
}

export async function uploadGoogleAccountKey(file: File, name: string) {
  const fd = new FormData();
  fd.append("key_file", file);
  fd.append("name", name);
  const res = await fetch(`${API_BASE}/api/integrations/gsheets/accounts/upload`, {
    method: "POST",
    body: fd,
  });
  const data = await res.json();
  if (!res.ok || !data.ok) {
    throw new Error(data.message || "Не удалось сохранить аккаунт");
  }
  return data as { ok: boolean; active_account_id?: string; message?: string };
}

export function deleteGoogleAccount(accountId: string) {
  return apiPostOk<{ ok: boolean; message?: string; active_account_id?: string }>(
    "/api/integrations/gsheets/accounts/delete",
    { account_id: accountId },
  );
}

export function selectGoogleAccount(accountId: string) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/gsheets/accounts/select", {
    account_id: accountId,
  });
}

export function connectOzonAccount(payload: { client_id: string; api_key: string }) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/ozon/connect", payload);
}

export function deleteOzonAccount(clientId: string) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/ozon/accounts/delete", {
    client_id: clientId,
  });
}

export function checkOzonAccount(clientId: string) {
  return apiPost<unknown>(`/api/integrations/ozon/accounts/${encodeURIComponent(clientId)}/check`);
}

export function deleteYandexAccount(businessId: string) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/yamarket/accounts/delete", {
    business_id: businessId,
  });
}

export function deleteYandexShop(businessId: string, campaignId: string) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/integrations/yamarket/shops/delete", {
    business_id: businessId,
    campaign_id: campaignId,
  });
}

export function deleteGsheetSource(sourceId: string) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/data/sources/delete", {
    source_id: sourceId,
  });
}

export function postYandexShopCheckRaw(shopCampaignId: string, businessId: string) {
  return fetch(`${API_BASE}/api/integrations/yamarket/shops/${shopCampaignId}/check`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ business_id: businessId }),
  });
}

export function postOzonAccountCheckRaw(clientId: string) {
  return fetch(`${API_BASE}/api/integrations/ozon/accounts/${encodeURIComponent(clientId)}/check`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
}

export function postGsheetSourceCheckRaw(sourceId: string) {
  return fetch(`${API_BASE}/api/integrations/gsheets/sources/${sourceId}/check`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
}
