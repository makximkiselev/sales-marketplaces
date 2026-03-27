"use client";

export const PRICING_SETTINGS_CTX_CACHE_KEY = "pricing_settings_ctx_v1";
export const PRICING_SETTINGS_TREE_CACHE_PREFIX = "pricing_settings_tree_v1:";
export const PRICING_SETTINGS_LOGISTICS_CACHE_PREFIX = "pricing_settings_logistics_v1:";

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

export function clearPricingSettingsCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(PRICING_SETTINGS_CTX_CACHE_KEY);
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
