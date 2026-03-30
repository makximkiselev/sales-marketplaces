import type { CogsSource, StockSource } from "../../pricing/settings/types";

export type SourceItem = {
  id: string;
  title: string;
  type?: string;
  spreadsheet_id?: string;
  worksheet?: string;
  mode_import?: boolean;
  mode_export?: boolean;
  mapping_template?: string;
  last_refreshed?: string;
  health_status?: "ok" | "error" | string;
  health_message?: string;
  health_checked_at?: string;
  mapping?: Record<string, string>;
  mapping_configured?: boolean;
};

export type PlatformType = "yandex_market" | "ozon" | "wildberries";

export type CampaignItem = {
  id: string;
  name: string;
  business_id: string;
};

export type DeleteRequest =
  | { type: "yandex_shop"; business_id: string; campaign_id: string; name: string }
  | { type: "yandex_account"; business_id: string }
  | { type: "ozon_account"; client_id: string; name?: string }
  | { type: "gsheet_source"; source_id: string; name: string }
  | { type: "google_account"; account_id: string; name: string };

export type YandexShop = {
  campaign_id: string;
  campaign_name: string;
  business_id: string;
  connected_at?: string | null;
  health_status?: "ok" | "error" | string;
  health_message?: string;
  health_checked_at?: string;
  data_flow?: { import_enabled?: boolean; export_enabled?: boolean };
  currency_code?: "RUB" | "USD" | string;
  fulfillment_model?: "FBO" | "FBS" | "DBS" | "EXPRESS" | string;
};

export type StoreSourceBinding = {
  cogsSource: CogsSource | null;
  stockSource: StockSource | null;
  updatedAt?: string | null;
};

export type YandexAccount = {
  business_id: string;
  api_key?: string;
  connected_at?: string | null;
  health_status?: "ok" | "error" | string;
  health_message?: string;
  health_checked_at?: string;
  data_flow?: { import_enabled?: boolean; export_enabled?: boolean };
  currency_code?: "RUB" | "USD" | string;
  shops?: YandexShop[];
};

export type OzonStore = {
  store_id: string;
  store_name: string;
  connected_at?: string | null;
  health_status?: "ok" | "error" | string;
  health_message?: string;
  health_checked_at?: string;
};

export type OzonAccount = {
  client_id: string;
  api_key?: string;
  seller_id?: string;
  seller_name?: string;
  connected_at?: string | null;
  data_flow?: { import_enabled?: boolean; export_enabled?: boolean };
  currency_code?: "RUB" | "USD" | string;
  fulfillment_model?: "FBO" | "FBS" | "DBS" | "EXPRESS" | string;
  health_status?: "ok" | "error" | string;
  health_message?: string;
  health_checked_at?: string;
  stores?: OzonStore[];
};

export type IntegrationsPayload = {
  ok?: boolean;
  data_flow?: {
    import_enabled?: boolean;
    export_enabled?: boolean;
    platforms?: Record<string, { import_enabled?: boolean; export_enabled?: boolean }>;
  };
  yandex_market?: {
    connected?: boolean;
    business_id?: string;
    campaign_id?: string;
    campaign_name?: string;
    connected_at?: string | null;
    shops?: YandexShop[];
    accounts?: YandexAccount[];
    data_flow?: { import_enabled?: boolean; export_enabled?: boolean };
  };
  ozon?: {
    connected?: boolean;
    message?: string;
    accounts?: OzonAccount[];
    data_flow?: { import_enabled?: boolean; export_enabled?: boolean };
  };
  wildberries?: { connected?: boolean; message?: string };
  google?: {
    credentials_configured?: boolean;
    active_account_id?: string;
    accounts?: Array<{
      id: string;
      name?: string;
      client_email?: string;
      private_key_id?: string;
      created_at?: string;
    }>;
  };
};

export const platformMeta: Record<PlatformType, { label: string; desc: string }> = {
  yandex_market: { label: "Яндекс.Маркет", desc: "Подключение по API-ключу и Business ID" },
  ozon: { label: "Ozon", desc: "Подключение по Client ID и API key" },
  wildberries: { label: "Wildberries", desc: "Интеграция в разработке" },
};
