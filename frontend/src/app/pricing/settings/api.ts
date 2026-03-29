import { apiGetOk, apiGetParams, apiPost, apiPostOk } from "../../../lib/api";
import type {
  IntegrationsResponse,
  LogisticsStoreSettingsApi,
  MonitoringExportRunStoreResult,
  MonitoringExportRunAllResult,
  MonitoringExportStoreApi,
  PricingCategoryTreeApiRow,
  RefreshMonitoringRunAllApi,
  RefreshMonitoringRowApi,
  RefreshMonitoringStoreApi,
  SalesPlanRowApi,
  PricingStoreSettingsApi,
} from "./types";

export function fetchPricingSettingsIntegrations() {
  return apiGetOk<IntegrationsResponse>("/api/integrations");
}

export function fetchPricingCategoryTree(platform: string, storeId: string) {
  return apiGetParams<{
    ok: boolean;
    message?: string;
    rows?: PricingCategoryTreeApiRow[];
    store_settings?: PricingStoreSettingsApi;
  }>("/api/pricing/settings/category-tree", {
    platform,
    store_id: storeId,
  });
}

export function fetchPricingSalesPlan() {
  return apiGetOk<{
    ok: boolean;
    message?: string;
    rows?: SalesPlanRowApi[];
  }>("/api/pricing/settings/sales-plan");
}

export function fetchPricingMonitoring() {
  return apiGetOk<{
    ok: boolean;
    message?: string;
    rows?: RefreshMonitoringRowApi[];
    platform_stores?: Record<string, RefreshMonitoringStoreApi[]>;
    run_all?: RefreshMonitoringRunAllApi;
  }>("/api/pricing/settings/monitoring");
}

export function fetchPricingMonitoringExports() {
  return apiGetOk<{
    ok: boolean;
    message?: string;
    rows?: MonitoringExportStoreApi[];
  }>("/api/pricing/settings/monitoring/exports");
}

export function runPricingMonitoringAll() {
  return apiPostOk<{ ok: boolean; message?: string; started?: boolean }>(
    "/api/pricing/settings/monitoring/run-all",
    {},
  );
}

export function runPricingMonitoringJob(jobCode: string) {
  return apiPostOk<{ ok: boolean; message?: string; started?: boolean }>(
    "/api/pricing/settings/monitoring/run-job",
    { job_code: jobCode },
  );
}

export function savePricingMonitoringJob(payload: {
  job_code: string;
  enabled: boolean;
  schedule_kind: string;
  interval_minutes?: number | null;
  time_of_day?: string | null;
  date_from?: string | null;
  date_to?: string | null;
  stores?: string[];
}) {
  return apiPostOk<{ ok: boolean; message?: string; job?: RefreshMonitoringRowApi }>(
    "/api/pricing/settings/monitoring/job",
    payload,
  );
}

export function savePricingMonitoringExportConfig(payload: {
  store_uid: string;
  export_kind: "prices" | "ads";
  type?: string | null;
  sourceId?: string | null;
  sourceName?: string | null;
  skuColumn?: string | null;
  valueColumn?: string | null;
}) {
  return apiPostOk<{ ok: boolean; message?: string }>(
    "/api/pricing/settings/monitoring/export-config",
    payload,
  );
}

export function runPricingMonitoringExport(storeUid?: string) {
  return apiPost<{ ok: boolean; message?: string; result?: MonitoringExportRunAllResult | MonitoringExportRunStoreResult }>(
    "/api/pricing/settings/monitoring/export-run",
    storeUid ? { store_uid: storeUid } : {},
  );
}

export function fetchPricingLogistics(params: {
  platform: string;
  store_id: string;
  page: string;
  page_size: string;
  search: string;
}) {
  return apiGetParams<{
    ok: boolean;
    message?: string;
    store_settings?: LogisticsStoreSettingsApi;
    rows?: unknown[];
    total_count?: number;
    page?: number;
    page_size?: number;
    page_size_options?: number[];
  }>("/api/pricing/settings/logistics", params);
}

export function refreshPricingMarketItems(platform: string, storeId: string) {
  return apiGetParams<{ ok: boolean; message?: string }>("/api/pricing/settings/market-items", {
    platform,
    store_id: storeId,
  });
}

export function savePricingCategorySettings(payload: {
  platform: string;
  store_id: string;
  leaf_path: string;
  values: Record<string, string>;
}) {
  return apiPostOk<{ ok: boolean; message?: string }>("/api/pricing/settings/category-settings", payload);
}

export function savePricingStoreSettings(payload: {
  platform: string;
  store_id: string;
  store_name?: string;
  values: Record<string, unknown>;
}) {
  return apiPost<{ ok: boolean; message?: string; settings?: PricingStoreSettingsApi }>(
    "/api/pricing/settings/store-settings",
    payload,
  );
}

export function applyPricingCategoryDefaults(payload: {
  platform: string;
  store_id: string;
  commission_percent?: string;
  target_margin_percent: string;
  target_margin_rub: string;
  target_profit_rub: string;
  target_profit_percent: string;
  ads_percent: string;
}) {
  return apiPost<{ ok: boolean; message?: string }>(
    "/api/pricing/settings/category-settings/apply-defaults",
    payload,
  );
}

export function saveLogisticsStoreSettings(payload: {
  platform: string;
  store_id: string;
  values: LogisticsStoreSettingsApi;
}) {
  return apiPost<{ ok: boolean; message?: string; settings?: LogisticsStoreSettingsApi }>(
    "/api/pricing/settings/logistics/store-settings",
    payload,
  );
}

export function saveLogisticsProductSettings(payload: {
  platform: string;
  store_id: string;
  sku: string;
  values: Record<string, string>;
}) {
  return apiPostOk("/api/pricing/settings/logistics/product-settings", payload);
}
