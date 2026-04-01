export const PRICING_SETTINGS_CTX_CACHE_KEY = "pricing_settings_ctx_v1";
export const PRICING_SETTINGS_TREE_CACHE_PREFIX = "pricing_settings_tree_v1:";
export const PRICING_SETTINGS_LOGISTICS_CACHE_PREFIX = "pricing_settings_logistics_v1:";
export const PRICING_SETTINGS_SALES_PLAN_CACHE_KEY = "pricing_settings_sales_plan_v1";
export const PRICING_SETTINGS_MONITORING_CACHE_KEY = "pricing_settings_monitoring_v1";

export function safeReadJson<T>(key: string): T | null {
  try {
    if (typeof window === "undefined") return null;
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export function safeWriteJson(key: string, value: unknown) {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // noop
  }
}

export function safeReadFreshJson<T>(key: string, ttlMs: number): T | null {
  const payload = safeReadJson<{ value?: T; savedAt?: number }>(key);
  if (!payload) return null;
  if (typeof payload.savedAt !== "number" || !Number.isFinite(payload.savedAt)) return null;
  if (Date.now() - payload.savedAt > ttlMs) return null;
  return (payload.value as T) ?? null;
}

export function safeWriteFreshJson(key: string, value: unknown) {
  safeWriteJson(key, { value, savedAt: Date.now() });
}

export function clearPricingSettingsCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(PRICING_SETTINGS_CTX_CACHE_KEY);
    window.localStorage.removeItem(PRICING_SETTINGS_SALES_PLAN_CACHE_KEY);
    window.localStorage.removeItem(PRICING_SETTINGS_MONITORING_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const k = window.localStorage.key(i);
      if (!k) continue;
      if (
        k.startsWith(PRICING_SETTINGS_TREE_CACHE_PREFIX) ||
        k.startsWith(PRICING_SETTINGS_LOGISTICS_CACHE_PREFIX)
      ) {
        keysToRemove.push(k);
      }
    }
    keysToRemove.forEach((k) => window.localStorage.removeItem(k));
  } catch {
    // noop
  }
}
