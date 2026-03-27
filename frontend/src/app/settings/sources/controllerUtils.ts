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

export function extractLatestHealthTimestamp(srcItems: SourceItem[], intData: IntegrationsPayload): string | null {
  const ts: string[] = [];
  for (const s of srcItems) {
    if (s.health_checked_at) ts.push(s.health_checked_at);
  }
  for (const acc of intData.yandex_market?.accounts || []) {
    if (acc.health_checked_at) ts.push(acc.health_checked_at);
    for (const shop of acc.shops || []) {
      if (shop.health_checked_at) ts.push(shop.health_checked_at);
    }
  }
  for (const acc of intData.ozon?.accounts || []) {
    if (acc.health_checked_at) ts.push(acc.health_checked_at);
  }
  const latest = ts
    .map((v) => ({ raw: v, ms: new Date(v).getTime() }))
    .filter((x) => Number.isFinite(x.ms))
    .sort((a, b) => b.ms - a.ms)[0];
  return latest?.raw || null;
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

export function sourceModeLabel(src: SourceItem): string {
  const imp = Boolean(src.mode_import);
  const exp = Boolean(src.mode_export);
  if (imp && exp) return "Mix";
  if (exp) return "Экспорт";
  if (imp) return "Импорт";
  return "-";
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
