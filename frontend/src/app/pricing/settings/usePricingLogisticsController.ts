"use client";

import { useEffect, useRef, useState } from "react";
import { fetchPricingLogistics, saveLogisticsProductSettings, saveLogisticsStoreSettings } from "./api";
import { clearPricingSettingsCache, PRICING_SETTINGS_LOGISTICS_CACHE_PREFIX, safeReadJson, safeWriteJson } from "./cache";
import { fmtCell, fmtInputNum, getLogisticsCellKey, numFromAny, toLiveLogisticsRow } from "./controllerUtils";
import type { LogisticsEditableFieldKey, LogisticsNumericKey, LogisticsRow, LogisticsStoreSettingsApi } from "./types";
import { showAppToast } from "../../../components/ui/toastBus";

export function usePricingLogisticsController(params: {
  activePlatform: string;
  activeStoreId: string;
  settingsTab: "general" | "logistics";
  moneySign: string;
}) {
  const { activePlatform, activeStoreId, settingsTab, moneySign } = params;
  const [logisticsStoreSettings, setLogisticsStoreSettings] = useState<LogisticsStoreSettingsApi>({
    fulfillment_model: "FBO",
    handling_mode: "fixed",
  });
  const [logisticsRows, setLogisticsRows] = useState<LogisticsRow[]>([]);
  const [logisticsLoading, setLogisticsLoading] = useState(false);
  const [logisticsError, setLogisticsError] = useState("");
  const [logisticsSearch, setLogisticsSearch] = useState("");
  const [logisticsPage, setLogisticsPage] = useState(1);
  const [logisticsPageSize, setLogisticsPageSize] = useState(50);
  const [logisticsTotal, setLogisticsTotal] = useState(0);
  const [logisticsPageSizeOptions, setLogisticsPageSizeOptions] = useState<number[]>([25, 50, 100, 200]);
  const [logisticsStoreSaving, setLogisticsStoreSaving] = useState(false);
  const [logisticsStoreSavedAt, setLogisticsStoreSavedAt] = useState("");
  const [logisticsStoreError, setLogisticsStoreError] = useState("");
  const [logisticsFieldErrors, setLogisticsFieldErrors] = useState<Record<"handling" | "delivery" | "return" | "disposal", string>>({
    handling: "",
    delivery: "",
    return: "",
    disposal: "",
  });
  const [logisticsNumericDrafts, setLogisticsNumericDrafts] = useState<Partial<Record<LogisticsNumericKey, string>>>({});
  const [logisticsCellDrafts, setLogisticsCellDrafts] = useState<Record<string, string>>({});
  const [logisticsCellSaving, setLogisticsCellSaving] = useState<Record<string, boolean>>({});
  const [logisticsEditingCell, setLogisticsEditingCell] = useState<Record<string, boolean>>({});
  const [logisticsImportOpen, setLogisticsImportOpen] = useState(false);
  const logisticsStoreSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const logisticsSearchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  async function loadLogisticsData(opts?: { page?: number; pageSize?: number; search?: string }) {
    if (!activePlatform || !activeStoreId || (activePlatform !== "yandex_market" && activePlatform !== "ozon")) {
      setLogisticsRows([]);
      setLogisticsError("");
      return;
    }
    const pageValue = opts?.page ?? logisticsPage;
    const pageSizeValue = opts?.pageSize ?? logisticsPageSize;
    const searchValue = opts?.search ?? logisticsSearch;
    setLogisticsLoading(true);
    setLogisticsError("");
    try {
      const logisticsParams = { platform: activePlatform, store_id: activeStoreId, page: String(pageValue), page_size: String(pageSizeValue), search: searchValue };
      const cacheKey = `${PRICING_SETTINGS_LOGISTICS_CACHE_PREFIX}${JSON.stringify(logisticsParams)}`;
      const cached = safeReadJson<any>(cacheKey);
      if (cached?.ok) {
        setLogisticsStoreSettings((prev) => ({ ...prev, ...(cached.store_settings || {}) }));
        setLogisticsRows(Array.isArray(cached.rows) ? (cached.rows as LogisticsRow[]) : []);
        setLogisticsTotal(Number(cached.total_count || 0));
        setLogisticsPage(Number(cached.page || pageValue || 1));
        setLogisticsPageSize(Number(cached.page_size || pageSizeValue || 50));
        setLogisticsPageSizeOptions(Array.isArray(cached.page_size_options) && cached.page_size_options.length ? cached.page_size_options.map((v: any) => Number(v)).filter((v: number) => Number.isFinite(v) && v > 0) : [25, 50, 100, 200]);
        setLogisticsStoreSavedAt(String(cached.store_settings?.updated_at || ""));
        setLogisticsStoreError("");
        setLogisticsEditingCell({});
        setLogisticsNumericDrafts({});
        setLogisticsLoading(false);
        return;
      }
      const data = await fetchPricingLogistics(logisticsParams);
      safeWriteJson(cacheKey, data);
      setLogisticsStoreSettings((prev) => ({ ...prev, ...(data.store_settings || {}) }));
      setLogisticsRows(Array.isArray(data.rows) ? (data.rows as LogisticsRow[]) : []);
      setLogisticsTotal(Number(data.total_count || 0));
      setLogisticsPage(Number(data.page || pageValue || 1));
      setLogisticsPageSize(Number(data.page_size || pageSizeValue || 50));
      setLogisticsPageSizeOptions(Array.isArray(data.page_size_options) && data.page_size_options.length ? data.page_size_options.map((v: any) => Number(v)).filter((v: number) => Number.isFinite(v) && v > 0) : [25, 50, 100, 200]);
      setLogisticsStoreSavedAt(String(data.store_settings?.updated_at || ""));
      setLogisticsStoreError("");
      setLogisticsEditingCell({});
      setLogisticsNumericDrafts({});
    } catch (e) {
      setLogisticsRows([]);
      setLogisticsError(e instanceof Error ? e.message : String(e));
    } finally {
      setLogisticsLoading(false);
    }
  }

  async function handleLogisticsImportDone() {
    clearPricingSettingsCache();
    await loadLogisticsData({ page: 1, pageSize: logisticsPageSize, search: logisticsSearch });
  }

  useEffect(() => {
    if (settingsTab !== "logistics") return;
    if (logisticsSearchTimerRef.current) clearTimeout(logisticsSearchTimerRef.current);
    logisticsSearchTimerRef.current = setTimeout(() => {
      void loadLogisticsData({ page: 1, pageSize: logisticsPageSize, search: logisticsSearch });
    }, 350);
  }, [settingsTab, activePlatform, activeStoreId, logisticsSearch, logisticsPageSize]);

  useEffect(() => {
    if (settingsTab !== "logistics") return;
    void loadLogisticsData();
  }, [settingsTab, activePlatform, activeStoreId, logisticsPage, logisticsPageSize]);

  useEffect(() => {
    if (settingsTab !== "logistics" || !activePlatform || !activeStoreId || logisticsLoading || (activePlatform !== "yandex_market" && activePlatform !== "ozon")) return;
    if (logisticsStoreSaveTimerRef.current) clearTimeout(logisticsStoreSaveTimerRef.current);
    logisticsStoreSaveTimerRef.current = setTimeout(() => {
      setLogisticsStoreSaving(true);
      setLogisticsStoreError("");
      void saveLogisticsStoreSettings({ platform: activePlatform, store_id: activeStoreId, values: logisticsStoreSettings })
        .then((data) => {
          if (!data.ok) throw new Error(data.message || "Не удалось сохранить настройки логистики");
          setLogisticsStoreSavedAt(String(data.settings?.updated_at || new Date().toISOString()));
          setLogisticsStoreError("");
          clearPricingSettingsCache();
        })
        .catch((e) => setLogisticsStoreError(e instanceof Error ? e.message : String(e)))
        .finally(() => setLogisticsStoreSaving(false));
    }, 450);
  }, [settingsTab, activePlatform, activeStoreId, logisticsStoreSettings, logisticsLoading]);

  function setLogisticsField<K extends keyof LogisticsStoreSettingsApi>(key: K, value: LogisticsStoreSettingsApi[K]) {
    setLogisticsStoreSettings((prev) => ({ ...prev, [key]: value }));
  }

  function setLogisticsNumericField(key: LogisticsNumericKey, raw: string) {
    const block: "handling" | "delivery" | "return" | "disposal" = key === "delivery_cost_per_kg" ? "delivery" : key === "return_processing_cost" ? "return" : key === "disposal_cost" ? "disposal" : "handling";
    const trimmed = raw.trim();
    setLogisticsNumericDrafts((prev) => ({ ...prev, [key]: raw }));
    if (trimmed === "") {
      setLogisticsFieldErrors((prev) => ({ ...prev, [block]: "" }));
      setLogisticsField(key, null);
      return;
    }
    const decimalLike = /^\d*(?:[.,]\d*)?$/.test(trimmed);
    if (!decimalLike) {
      setLogisticsFieldErrors((prev) => ({ ...prev, [block]: "Введите пожалуйста число, например 200, 300.1" }));
      return;
    }
    const parsed = Number(trimmed.replace(",", "."));
    if (Number.isFinite(parsed)) {
      setLogisticsFieldErrors((prev) => ({ ...prev, [block]: "" }));
      setLogisticsField(key, parsed);
      return;
    }
    setLogisticsFieldErrors((prev) => ({ ...prev, [block]: "" }));
  }

  function onLogisticsNumericBlur(key: LogisticsNumericKey) {
    const raw = logisticsNumericDrafts[key];
    if (raw == null) return;
    const trimmed = raw.trim();
    if (trimmed === "") {
      setLogisticsNumericDrafts((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      return;
    }
    const parsed = Number(trimmed.replace(",", "."));
    if (!Number.isFinite(parsed)) return;
    setLogisticsField(key, parsed);
    setLogisticsNumericDrafts((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }

  function getLogisticsNumericValue(key: LogisticsNumericKey, current: number | null | undefined): string {
    const draft = logisticsNumericDrafts[key];
    if (draft != null) return draft;
    return fmtInputNum(current);
  }

  function openLogisticsCellEditor(row: LogisticsRow, field: LogisticsEditableFieldKey) {
    const key = getLogisticsCellKey(row.sku, field);
    setLogisticsEditingCell((prev) => ({ ...prev, [key]: true }));
    setLogisticsCellDrafts((prev) => {
      if (prev[key] != null) return prev;
      const base = row[field];
      return { ...prev, [key]: base == null ? "" : String(base) };
    });
  }

  async function saveLogisticsCellValue(row: LogisticsRow, field: LogisticsEditableFieldKey, rawValue: string) {
    const key = getLogisticsCellKey(row.sku, field);
    setLogisticsCellSaving((prev) => ({ ...prev, [key]: true }));
    try {
      await saveLogisticsProductSettings({ platform: activePlatform, store_id: activeStoreId, sku: row.sku, values: { [field]: rawValue } });
      clearPricingSettingsCache();
      const num = rawValue.trim() === "" ? null : Number(rawValue.replace(",", "."));
      setLogisticsRows((prev) => prev.map((r) => {
        if (r.sku !== row.sku) return r;
        const next = { ...r, [field]: Number.isNaN(num as number) ? null : (num as number | null) } as LogisticsRow;
        const width = next.width_cm ?? null;
        const length = next.length_cm ?? null;
        const height = next.height_cm ?? null;
        const weight = next.weight_kg ?? null;
        const divisor = activePlatform === "yandex_market" ? 1000 : 5000;
        let volumetric: number | null = null;
        if (width != null && length != null && height != null) volumetric = Number((((width as number) * (length as number) * (height as number)) / divisor).toFixed(3));
        let maxWeight: number | null = null;
        if (weight != null && volumetric != null) maxWeight = Math.max(weight as number, volumetric);
        else if (weight != null) maxWeight = weight as number;
        else if (volumetric != null) maxWeight = volumetric;
        next.volumetric_weight_kg = volumetric;
        next.max_weight_kg = maxWeight == null ? null : Number(maxWeight.toFixed(3));
        return next;
      }));
      setLogisticsCellDrafts((prev) => { const next = { ...prev }; delete next[key]; return next; });
      setLogisticsEditingCell((prev) => { const next = { ...prev }; delete next[key]; return next; });
    } catch (e) {
      setLogisticsError(e instanceof Error ? e.message : String(e));
    } finally {
      setLogisticsCellSaving((prev) => ({ ...prev, [key]: false }));
    }
  }

  function setLogisticsCellDraftByKey(cellKey: string, rawValue: string) {
    setLogisticsCellDrafts((prev) => ({ ...prev, [cellKey]: rawValue }));
  }

  function commitLogisticsCell(row: LogisticsRow, field: LogisticsEditableFieldKey) {
    const key = getLogisticsCellKey(row.sku, field);
    const raw = logisticsCellDrafts[key] ?? "";
    const base = row[field];
    const normBase = base == null ? "" : String(base);
    const normRaw = raw.trim();
    if (normRaw !== "") {
      const parsed = Number(normRaw.replace(",", "."));
      if (!Number.isFinite(parsed)) {
        setLogisticsError("Введите пожалуйста число, например 200, 300.1");
        return;
      }
    }
    if (logisticsError === "Введите пожалуйста число, например 200, 300.1") setLogisticsError("");
    if (normRaw === normBase.trim()) {
      setLogisticsEditingCell((prev) => { const next = { ...prev }; delete next[key]; return next; });
      return;
    }
    void saveLogisticsCellValue(row, field, raw);
  }

  const toLiveLogisticsRowBound = (row: LogisticsRow) =>
    toLiveLogisticsRow(row, logisticsStoreSettings, moneySign, activePlatform);

  return {
    logisticsStoreSettings,
    logisticsRows,
    logisticsLoading,
    logisticsError,
    logisticsSearch,
    logisticsPage,
    logisticsPageSize,
    logisticsTotal,
    logisticsPageSizeOptions,
    logisticsStoreSaving,
    logisticsStoreSavedAt,
    logisticsStoreError,
    logisticsFieldErrors,
    logisticsCellDrafts,
    logisticsCellSaving,
    logisticsEditingCell,
    logisticsImportOpen,
    setLogisticsPage,
    setLogisticsPageSize,
    setLogisticsSearch,
    setLogisticsImportOpen,
    setLogisticsEditingCell,
    setLogisticsCellDrafts,
    setLogisticsField,
    setLogisticsNumericField,
    onLogisticsNumericBlur,
    getLogisticsNumericValue,
    toLiveLogisticsRow: toLiveLogisticsRowBound,
    fmtCell,
    getLogisticsCellKey,
    openLogisticsCellEditor,
    setLogisticsCellDraftByKey,
    commitLogisticsCell,
    handleLogisticsImportDone,
    loadLogisticsData,
  };
}
