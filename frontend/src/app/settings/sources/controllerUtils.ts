import type { IntegrationsPayload, SourceItem } from "./types";

export function sortById(left?: string, right?: string) {
  return (left || "").localeCompare(right || "", "ru", { numeric: true, sensitivity: "base" });
}

export function getSortedYandexAccounts(accounts: NonNullable<IntegrationsPayload["yandex_market"]>["accounts"] = []) {
  return [...accounts]
    .sort((a, b) => sortById(a.business_id, b.business_id))
    .map((acc) => ({
      ...acc,
      shops: [...(acc.shops || [])].sort((a, b) => sortById(a.campaign_id, b.campaign_id)),
    }));
}

export function getSortedOzonAccounts(accounts: NonNullable<IntegrationsPayload["ozon"]>["accounts"] = []) {
  return [...accounts].sort((a, b) => sortById(a.client_id, b.client_id));
}

export function formatDateTime(value?: string | null): string {
  if (!value) return "Не проверялось";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "Не проверялось";
  return dt.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function formatRefreshLabel(value?: string | null): string {
  if (!value) return "-";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "-";
  return dt.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}
