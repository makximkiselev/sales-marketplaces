"use client";

import { fetchPricingSettingsIntegrations, refreshPricingMarketItems } from "./api";
import { clearPricingSettingsCache, PRICING_SETTINGS_CTX_CACHE_KEY, safeReadJson, safeWriteJson } from "./cache";
import { buildPlatformsFromIntegrations } from "./controllerUtils";
import type { PlatformItem } from "./types";

export function getCachedPricingPlatforms() {
  const cached = safeReadJson<{ platforms: PlatformItem[] }>(PRICING_SETTINGS_CTX_CACHE_KEY);
  if (!cached?.platforms || !Array.isArray(cached.platforms)) return null;
  return cached.platforms;
}

export async function loadPricingPlatforms() {
  const data = await fetchPricingSettingsIntegrations();
  const platforms = buildPlatformsFromIntegrations(data);
  safeWriteJson(PRICING_SETTINGS_CTX_CACHE_KEY, { platforms });
  return platforms;
}

export function resolveNextStoreSelection(params: {
  nextPlatforms: PlatformItem[];
  activePlatform: string;
  activeStoreId: string;
}) {
  const { nextPlatforms, activePlatform, activeStoreId } = params;
  const currentPlatform = nextPlatforms.find((p) => p.id === activePlatform);
  if (currentPlatform) {
    const hasCurrentStore = currentPlatform.stores.some((s) => s.id === activeStoreId);
    return {
      activePlatform,
      activeStoreId: hasCurrentStore ? activeStoreId : currentPlatform.stores[0]?.id || "",
    };
  }
  const firstPlatform = nextPlatforms.find((p) => p.stores?.length) || nextPlatforms[0];
  return {
    activePlatform: firstPlatform?.id || "yandex_market",
    activeStoreId: firstPlatform?.stores?.[0]?.id || "",
  };
}

export async function refreshPricingStoreData(platform: string, storeId: string) {
  clearPricingSettingsCache();
  if (!platform || !storeId || (platform !== "yandex_market" && platform !== "ozon")) return;
  await refreshPricingMarketItems(platform, storeId);
}
