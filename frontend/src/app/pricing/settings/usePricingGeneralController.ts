"use client";

import { useEffect, useRef, useState } from "react";
import { savePricingStoreSettings } from "./api";
import { clearPricingSettingsCache } from "./cache";
import type { CogsSource, StockSource } from "./types";
import { showAppToast } from "../../../components/ui/toastBus";

export function usePricingGeneralController(params: {
  activePlatform: string;
  activeStoreId: string;
  itemsLoading: boolean;
}) {
  const { activePlatform, activeStoreId, itemsLoading } = params;
  const [earningMode, setEarningMode] = useState<"profit" | "margin">("margin");
  const [earningUnit, setEarningUnit] = useState<"rub" | "percent">("percent");
  const [targetProfit, setTargetProfit] = useState("");
  const [targetProfitPercent, setTargetProfitPercent] = useState("");
  const [targetMargin, setTargetMargin] = useState("");
  const [targetMarginRub, setTargetMarginRub] = useState("");
  const [targetDrr, setTargetDrr] = useState("");
  const [cogsSource, setCogsSource] = useState<CogsSource | null>(null);
  const [cogsModalOpen, setCogsModalOpen] = useState(false);
  const [stockSource, setStockSource] = useState<StockSource | null>(null);
  const [stockModalOpen, setStockModalOpen] = useState(false);
  const [storeSettingsSaving, setStoreSettingsSaving] = useState(false);
  const [storeSettingsError, setStoreSettingsError] = useState("");
  const [storeSettingsSavedAt, setStoreSettingsSavedAt] = useState("");
  const storeSettingsSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hydratedStoreKeyRef = useRef("");

  useEffect(() => {
    hydratedStoreKeyRef.current = "";
  }, [activePlatform, activeStoreId]);

  useEffect(() => {
    if (!activePlatform || !activeStoreId || itemsLoading || (activePlatform !== "yandex_market" && activePlatform !== "ozon")) return;
    if (hydratedStoreKeyRef.current !== `${activePlatform}:${activeStoreId}`) return;
    if (storeSettingsSaveTimerRef.current) clearTimeout(storeSettingsSaveTimerRef.current);
    storeSettingsSaveTimerRef.current = setTimeout(() => {
      setStoreSettingsSaving(true);
      setStoreSettingsError("");
      void savePricingStoreSettings({
        platform: activePlatform,
        store_id: activeStoreId,
        values: {
          earning_mode: earningMode,
          earning_unit: earningUnit,
          target_profit_rub: targetProfit,
          target_profit_percent: targetProfitPercent,
          target_margin_rub: targetMarginRub,
          target_margin_percent: targetMargin,
          target_drr_percent: targetDrr,
          cogs_source_type: cogsSource?.type ?? null,
          cogs_source_id: cogsSource?.sourceId ?? null,
          cogs_source_name: cogsSource?.sourceName ?? null,
          cogs_sku_column: cogsSource?.skuColumn ?? null,
          cogs_value_column: cogsSource?.valueColumn ?? null,
          stock_source_type: stockSource?.type ?? null,
          stock_source_id: stockSource?.sourceId ?? null,
          stock_source_name: stockSource?.sourceName ?? null,
          stock_sku_column: stockSource?.skuColumn ?? null,
          stock_value_column: stockSource?.valueColumn ?? null,
        },
      })
        .then((data) => {
          if (!data.ok) throw new Error(data.message || "Не удалось сохранить настройки магазина");
          setStoreSettingsSavedAt(String(data?.settings?.updated_at || new Date().toISOString()));
          setStoreSettingsError("");
          clearPricingSettingsCache();
          showAppToast({ message: "Данные сохранены" });
        })
        .catch((e) => { setStoreSettingsError(e instanceof Error ? e.message : String(e)); })
        .finally(() => { setStoreSettingsSaving(false); });
    }, 500);
  }, [activePlatform, activeStoreId, earningMode, earningUnit, targetProfit, targetProfitPercent, targetMarginRub, targetMargin, targetDrr, cogsSource, stockSource, itemsLoading]);

  useEffect(() => () => {
    if (storeSettingsSaveTimerRef.current) clearTimeout(storeSettingsSaveTimerRef.current);
  }, []);

  function markStoreSettingsHydrated() {
    if (!activePlatform || !activeStoreId) return;
    hydratedStoreKeyRef.current = `${activePlatform}:${activeStoreId}`;
  }

  const activeTargetValue = earningMode === "margin"
    ? (earningUnit === "percent" ? targetMargin : targetMarginRub)
    : (earningUnit === "percent" ? targetProfitPercent : targetProfit);

  function setActiveTargetValue(next: string) {
    if (earningMode === "margin") {
      if (earningUnit === "percent") setTargetMargin(next);
      else setTargetMarginRub(next);
      return;
    }
    if (earningUnit === "percent") setTargetProfitPercent(next);
    else setTargetProfit(next);
  }

  return {
    earningMode,
    earningUnit,
    targetProfit,
    targetProfitPercent,
    targetMargin,
    targetMarginRub,
    targetDrr,
    cogsSource,
    cogsModalOpen,
    stockSource,
    stockModalOpen,
    storeSettingsSaving,
    storeSettingsError,
    storeSettingsSavedAt,
    activeTargetValue,
    setEarningMode,
    setEarningUnit,
    setTargetProfit,
    setTargetProfitPercent,
    setTargetMargin,
    setTargetMarginRub,
    setTargetDrr,
    setCogsSource,
    setCogsModalOpen,
    setStockSource,
    setStockModalOpen,
    setStoreSettingsSavedAt,
    setStoreSettingsError,
    setActiveTargetValue,
    markStoreSettingsHydrated,
  };
}
