"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { applyPricingCategoryDefaults, fetchPricingCategoryTree, savePricingCategorySettings } from "./api";
import { clearPricingSettingsCache, PRICING_SETTINGS_TREE_CACHE_PREFIX, safeReadJson, safeWriteJson } from "./cache";
import { buildCategoryRows, buildPricingTableColumns, defaultFieldValue, formatNum, getCellKey, getUsedSubcategoryDepth } from "./controllerUtils";
import type { CogsSource, EditableFieldKey, PricingCategoryRow, PricingCategoryTreeApiRow, PricingStoreSettingsApi, PricingTableColumn } from "./types";
import { showAppToast } from "../../../components/ui/toastBus";

export function usePricingCategoryController(params: {
  activePlatform: string;
  activeStoreId: string;
  moneySign: string;
  earningMode: "profit" | "margin";
  earningUnit: "rub" | "percent";
  targetProfit: string;
  targetProfitPercent: string;
  targetMargin: string;
  targetMarginRub: string;
  targetDrr: string;
  onStoreSettingsLoaded: (settings: PricingStoreSettingsApi) => void;
}) {
  const { activePlatform, activeStoreId, moneySign, earningMode, earningUnit, targetProfit, targetProfitPercent, targetMargin, targetMarginRub, targetDrr, onStoreSettingsLoaded } = params;
  const [itemsLoading, setItemsLoading] = useState(false);
  const [itemsError, setItemsError] = useState("");
  const [categoryRows, setCategoryRows] = useState<PricingCategoryRow[]>([]);
  const [cellDrafts, setCellDrafts] = useState<Record<string, string>>({});
  const [cellSaving, setCellSaving] = useState<Record<string, boolean>>({});
  const saveTimersRef = useRef<Record<string, ReturnType<typeof setTimeout>>>({});
  const bulkApplyTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const bulkApplyBaselineRef = useRef<string>("");

  function applyTreeData(treeData: any) {
    const rows = Array.isArray(treeData?.rows) ? (treeData.rows as PricingCategoryTreeApiRow[]) : [];
    const storeSettings = (treeData?.store_settings || {}) as PricingStoreSettingsApi;
    onStoreSettingsLoaded(storeSettings);
    setCategoryRows(buildCategoryRows(rows));
    setCellDrafts({});
    setCellSaving({});
  }

  async function loadPricingCategoryTree() {
    if (!activePlatform || !activeStoreId || (activePlatform !== "yandex_market" && activePlatform !== "ozon")) {
      setCategoryRows([]);
      setItemsError("");
      return;
    }
    setItemsLoading(true);
    setItemsError("");
    try {
      const cacheKey = `${PRICING_SETTINGS_TREE_CACHE_PREFIX}${activePlatform}:${activeStoreId}`;
      const cached = safeReadJson<{ ok: boolean; message?: string; rows?: any[]; store_settings?: any }>(cacheKey);
      if (cached?.ok) {
        applyTreeData(cached);
        setItemsLoading(false);
        return;
      }
      const treeData = await fetchPricingCategoryTree(activePlatform, activeStoreId);
      safeWriteJson(cacheKey, treeData);
      applyTreeData(treeData);
    } catch (e) {
      setCategoryRows([]);
      setItemsError(e instanceof Error ? e.message : String(e));
    } finally {
      setItemsLoading(false);
    }
  }

  async function saveCellValue(row: PricingCategoryRow, field: EditableFieldKey, rawValue: string) {
    const key = getCellKey(row.leafPath || row.key, field);
    setCellSaving((prev) => ({ ...prev, [key]: true }));
    try {
      await savePricingCategorySettings({
        platform: activePlatform,
        store_id: activeStoreId,
        leaf_path: row.leafPath || row.key,
        values: { [field]: rawValue },
      });
      clearPricingSettingsCache();
      showAppToast({ message: "Данные сохранены" });
      const num = rawValue.trim() === "" ? null : Number(rawValue.replace(",", "."));
      setCategoryRows((prev) => prev.map((r) => r.key === row.key ? { ...r, values: { ...r.values, [field]: Number.isNaN(num as number) ? null : (num as number | null) } } : r));
    } catch (e) {
      setItemsError(e instanceof Error ? e.message : String(e));
    } finally {
      setCellSaving((prev) => ({ ...prev, [key]: false }));
    }
  }

  function flushSaveCell(row: PricingCategoryRow, field: EditableFieldKey, rawValue?: string) {
    const key = getCellKey(row.leafPath || row.key, field);
    if (saveTimersRef.current[key]) {
      clearTimeout(saveTimersRef.current[key]);
      delete saveTimersRef.current[key];
    }
    const nextRawValue = rawValue ?? cellDrafts[key] ?? formatNum(row.values[field]);
    void saveCellValue(row, field, nextRawValue ?? "");
  }

  function queueSaveCell(row: PricingCategoryRow, field: EditableFieldKey, rawValue: string) {
    const key = getCellKey(row.leafPath || row.key, field);
    setCellDrafts((prev) => ({ ...prev, [key]: rawValue }));
    if (saveTimersRef.current[key]) clearTimeout(saveTimersRef.current[key]);
    saveTimersRef.current[key] = setTimeout(() => { void saveCellValue(row, field, rawValue); }, 550);
  }

  useEffect(() => () => {
    for (const t of Object.values(saveTimersRef.current)) clearTimeout(t);
    if (bulkApplyTimerRef.current) clearTimeout(bulkApplyTimerRef.current);
  }, []);

  useEffect(() => {
    bulkApplyBaselineRef.current = "";
  }, [activePlatform, activeStoreId]);

  useEffect(() => {
    if (!activePlatform || !activeStoreId || itemsLoading || !categoryRows.length || (activePlatform !== "yandex_market" && activePlatform !== "ozon")) return;
    const signature = JSON.stringify({
      activePlatform,
      activeStoreId,
      targetProfit,
      targetProfitPercent,
      targetMarginRub,
      targetMargin,
      targetDrr,
    });
    // Do not auto-apply defaults on initial page load/store switch.
    // Only apply after the user actually changes the top-level values.
    if (!bulkApplyBaselineRef.current) {
      bulkApplyBaselineRef.current = signature;
      return;
    }
    if (bulkApplyBaselineRef.current === signature) return;
    bulkApplyBaselineRef.current = signature;
    if (bulkApplyTimerRef.current) clearTimeout(bulkApplyTimerRef.current);
    bulkApplyTimerRef.current = setTimeout(async () => {
      try {
        const data = await applyPricingCategoryDefaults({
          platform: activePlatform,
          store_id: activeStoreId,
          target_margin_percent: targetMargin,
          target_margin_rub: targetMarginRub,
          target_profit_rub: targetProfit,
          target_profit_percent: targetProfitPercent,
          ads_percent: targetDrr,
        });
        if (!data.ok) return;
        setCategoryRows((prev) => prev.map((row) => ({
          ...row,
          values: {
            ...row.values,
            ads_percent: targetDrr.trim() === "" ? row.values.ads_percent : Number(targetDrr.replace(",", ".")),
            target_profit_rub: targetProfit.trim() === "" ? row.values.target_profit_rub : Number(targetProfit.replace(",", ".")),
            target_profit_percent: targetProfitPercent.trim() === "" ? row.values.target_profit_percent : Number(targetProfitPercent.replace(",", ".")),
            target_margin_rub: targetMarginRub.trim() === "" ? row.values.target_margin_rub : Number(targetMarginRub.replace(",", ".")),
            target_margin_percent: targetMargin.trim() === "" ? row.values.target_margin_percent : Number(targetMargin.replace(",", ".")),
          },
        })));
      } catch {
        // noop
      }
    }, 650);
  }, [activePlatform, activeStoreId, targetProfit, targetProfitPercent, targetMarginRub, targetMargin, targetDrr, categoryRows.length, itemsLoading]);

  useEffect(() => {
    void loadPricingCategoryTree();
  }, [activePlatform, activeStoreId]);

  const usedSubcategoryDepth = useMemo(() => getUsedSubcategoryDepth(categoryRows), [categoryRows]);
  const tableColumns = useMemo<PricingTableColumn[]>(() => buildPricingTableColumns({ isProfitMode: earningMode === "profit", usedSubcategoryDepth, earningUnit, moneySign }), [earningMode, usedSubcategoryDepth, earningUnit, moneySign]);
  const defaultCategoryFieldValue = (field: EditableFieldKey) => defaultFieldValue(field, { targetDrr, targetMargin, targetMarginRub, targetProfit, targetProfitPercent });

  return {
    itemsLoading,
    itemsError,
    categoryRows,
    cellDrafts,
    cellSaving,
    tableColumns,
    loadPricingCategoryTree,
    getCellKey,
    defaultFieldValue: defaultCategoryFieldValue,
    formatNum,
    queueSaveCell,
    flushSaveCell,
    setItemsError,
  };
}
