export type StoreItem = {
  id: string;
  name: string;
  currencyCode: "RUB" | "USD";
  fulfillmentModel: "FBO" | "FBS" | "DBS" | "EXPRESS";
};

export type StoreTabItem = {
  key: string;
  platformId: string;
  platformLabel: string;
  storeId: string;
  storeName: string;
};

export type PlatformItem = {
  id: string;
  label: string;
  stores: StoreItem[];
};

export type IntegrationsResponse = {
  ok: boolean;
  message?: string;
  yandex_market?: {
    accounts?: Array<{
      business_id?: string;
      shops?: Array<{ campaign_id?: string; campaign_name?: string; currency_code?: string; fulfillment_model?: string }>;
    }>;
  };
  ozon?: {
    accounts?: Array<{
      client_id?: string;
      seller_id?: string;
      seller_name?: string;
      currency_code?: string;
      fulfillment_model?: string;
    }>;
  };
};

export type PricingMarketRow = {
  sku: string;
  name: string;
  category: string;
  subcategory: string;
};

export type PricingCategoryRow = {
  key: string;
  leafPath: string;
  category: string;
  subcategoryLevels: string[];
  itemsCount: number;
  values: {
    commission_percent: number | null;
    acquiring_percent: number | null;
    logistics_rub: number | null;
    ads_percent: number | null;
    returns_percent: number | null;
    tax_percent: number | null;
    other_expenses_rub: number | null;
    other_expenses_percent: number | null;
    cogs_rub: number | null;
    target_profit_rub: number | null;
    target_profit_percent: number | null;
    target_margin_rub: number | null;
    target_margin_percent: number | null;
  };
};

export type PricingCategoryTreeApiRow = {
  category?: string;
  subcategory_1?: string;
  subcategory_2?: string;
  subcategory_3?: string;
  subcategory_4?: string;
  subcategory_5?: string;
  leaf_path?: string;
  items_count?: number;
  commission_percent?: number | null;
  acquiring_percent?: number | null;
  logistics_rub?: number | null;
  ads_percent?: number | null;
  returns_percent?: number | null;
  tax_percent?: number | null;
  other_expenses_rub?: number | null;
  other_expenses_percent?: number | null;
  cogs_rub?: number | null;
  target_profit_rub?: number | null;
  target_profit_percent?: number | null;
  minimum_profit_percent?: number | null;
  target_margin_rub?: number | null;
  target_margin_percent?: number | null;
};

export type PricingStoreSettingsApi = {
  earning_mode?: "profit" | "margin";
  earning_unit?: "rub" | "percent";
  strategy_mode?: "mix" | "mrc";
  planned_revenue?: number | null;
  target_profit_rub?: number | null;
  target_profit_percent?: number | null;
  minimum_profit_percent?: number | null;
  target_margin_rub?: number | null;
  target_margin_percent?: number | null;
  target_drr_percent?: number | null;
  cogs_source_type?: "table" | "system" | null;
  cogs_source_id?: string | null;
  cogs_source_name?: string | null;
  cogs_sku_column?: string | null;
  cogs_value_column?: string | null;
  stock_source_type?: "table" | "system" | null;
  stock_source_id?: string | null;
  stock_source_name?: string | null;
  stock_sku_column?: string | null;
  stock_value_column?: string | null;
  updated_at?: string | null;
};

export type SalesPlanRowApi = {
  store_uid: string;
  platform: string;
  platform_label: string;
  store_id: string;
  store_name: string;
  currency_code: "RUB" | "USD" | string;
  earning_mode?: "profit" | "margin";
  strategy_mode?: "mix" | "mrc";
  planned_revenue?: number | null;
  target_drr_percent?: number | null;
  target_profit_rub?: number | null;
  target_profit_percent?: number | null;
  minimum_profit_percent?: number | null;
  target_margin_rub?: number | null;
  target_margin_percent?: number | null;
  updated_at?: string | null;
};

export type RefreshMonitoringRowApi = {
  job_code: string;
  title: string;
  enabled: number | boolean;
  schedule_kind: "interval" | "daily" | string;
  interval_minutes?: number | null;
  time_of_day?: string | null;
  date_from?: string | null;
  date_to?: string | null;
  kind?: "api" | "google" | "system" | string;
  platform?: string;
  supports_store_selection?: boolean;
  selected_store_uids?: string[];
  store_statuses?: Array<{ store_uid: string; status: string; message?: string }>;
  updated_at?: string | null;
  last_started_at?: string | null;
  last_finished_at?: string | null;
  last_status?: string | null;
  last_message?: string | null;
  last_run_id?: number | null;
  progress_percent?: number | null;
  current_stage?: string | null;
  freshness_status?: "fresh" | "stale" | "unknown" | string | null;
  freshness_minutes?: number | null;
  freshness_limit_minutes?: number | null;
  last_success_at?: string | null;
  is_stale?: boolean | null;
};

export type RefreshMonitoringStoreApi = {
  store_uid: string;
  store_id: string;
  store_name: string;
  platform: string;
  platform_label: string;
};

export type RefreshMonitoringRunAllApi = {
  last_started_at?: string | null;
  last_finished_at?: string | null;
  last_status?: string | null;
  last_message?: string | null;
  progress_percent?: number | null;
  current_stage?: string | null;
};

export type MonitoringExportConfigApi = {
  type?: "table" | "system" | null;
  sourceId?: string | null;
  sourceName?: string | null;
  skuColumn?: string | null;
  valueColumn?: string | null;
};

export type MonitoringExportStoreApi = {
  store_uid: string;
  platform: string;
  store_id: string;
  store_name: string;
  export_prices: MonitoringExportConfigApi;
  export_ads: MonitoringExportConfigApi;
};

export type MonitoringExportRunKindResult = {
  kind: "prices" | "ads" | "market_prices" | "market_promos" | "market_boosts" | string;
  status: "success" | "skipped" | "error" | string;
  message?: string;
  source_id?: string;
  updated_cells?: number;
  matched_rows?: number;
  values_total?: number;
};

export type MonitoringExportRunStoreResult = {
  ok: boolean;
  store_uid: string;
  store_id?: string;
  store_name?: string;
  results: MonitoringExportRunKindResult[];
};

export type MonitoringExportRunAllResult = {
  ok: boolean;
  stores: MonitoringExportRunStoreResult[];
};

export type CogsSource = {
  type: "table" | "system";
  sourceId: string;
  sourceName: string;
  skuColumn: string;
  extraColumn?: string;
  valueColumn: string;
};

export type StockSource = {
  type: "table" | "system";
  sourceId: string;
  sourceName: string;
  skuColumn: string;
  valueColumn: string;
};

export type SourceItem = {
  id: string;
  title?: string;
  type?: string;
  source_id?: string;
};

export type EditableFieldKey =
  | "commission_percent"
  | "acquiring_percent"
  | "logistics_rub"
  | "ads_percent"
  | "returns_percent"
  | "tax_percent"
  | "other_expenses_rub"
  | "other_expenses_percent"
  | "cogs_rub"
  | "target_profit_rub"
  | "target_profit_percent"
  | "target_margin_rub"
  | "target_margin_percent";

export type PricingTableColumn = {
  id: string;
  label: string;
  field?: EditableFieldKey;
  kind: "text" | "input";
  subIndex?: number;
};

export type LogisticsStoreSettingsApi = {
  fulfillment_model?: "FBO" | "FBS" | "DBS" | "EXPRESS";
  handling_mode?: "fixed" | "percent";
  handling_fixed_amount?: number | null;
  handling_percent?: number | null;
  handling_min_amount?: number | null;
  handling_max_amount?: number | null;
  delivery_cost_per_kg?: number | null;
  return_processing_cost?: number | null;
  disposal_cost?: number | null;
  updated_at?: string | null;
};

export type LogisticsRow = {
  sku: string;
  name: string;
  tree_path?: string[];
  logistics_cost_display?: number | string | null;
  width_cm?: number | null;
  length_cm?: number | null;
  height_cm?: number | null;
  weight_kg?: number | null;
  dimensions_inherited?: boolean;
  volumetric_weight_kg?: number | null;
  max_weight_kg?: number | null;
  cost_per_kg?: number | null;
  handling_cost_display?: string;
  delivery_to_client_cost?: number | null;
  return_processing_cost?: number | null;
  disposal_cost?: number | null;
  comments?: string;
  updated_at?: string;
};

export type LogisticsEditableFieldKey = "width_cm" | "length_cm" | "height_cm" | "weight_kg";
export type LogisticsNumericKey =
  | "handling_fixed_amount"
  | "handling_percent"
  | "handling_min_amount"
  | "handling_max_amount"
  | "delivery_cost_per_kg"
  | "return_processing_cost"
  | "disposal_cost";

export type LogisticsImportModalProps = {
  open: boolean;
  platform: string;
  storeId: string;
  onClose: () => void;
  onDone: () => Promise<void>;
};
