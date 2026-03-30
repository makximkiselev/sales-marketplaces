import { useEffect, useMemo, useState } from "react";
import { fetchPricingMonitoring, fetchPricingSalesPlan, runPricingMonitoringAll, runPricingMonitoringJob, savePricingMonitoringJob, savePricingStoreSettings } from "./api";
import { usePricingCategoryController } from "./usePricingCategoryController";
import { usePricingGeneralController } from "./usePricingGeneralController";
import { usePricingLogisticsController } from "./usePricingLogisticsController";
import { getCachedPricingPlatforms, loadPricingPlatforms, refreshPricingStoreData, resolveNextStoreSelection } from "./controllerServices";
import { safeReadJson, safeWriteJson } from "./cache";
import type { PlatformItem, RefreshMonitoringRowApi, SalesPlanRowApi, StoreTabItem } from "./types";
import { showAppToast } from "../../../components/ui/toastBus";

const PRICING_SETTINGS_TAB_KEY = "pricing_settings_active_tab_v1";
type SettingsTab = "sales_plan" | "categories" | "logistics" | "monitoring";

function readInitialSettingsTab(): SettingsTab {
  const saved = safeReadJson<SettingsTab>(PRICING_SETTINGS_TAB_KEY);
  return saved === "sales_plan" || saved === "categories" || saved === "logistics" || saved === "monitoring"
    ? saved
    : "sales_plan";
}

export function usePricingSettingsController() {
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [platforms, setPlatforms] = useState<PlatformItem[]>([]);
  const [activePlatform, setActivePlatform] = useState("yandex_market");
  const [activeStoreId, setActiveStoreId] = useState("");
  const [settingsTab, setSettingsTab] = useState<SettingsTab>(() => readInitialSettingsTab());
  const [salesPlanRows, setSalesPlanRows] = useState<SalesPlanRowApi[]>([]);
  const [salesPlanLoading, setSalesPlanLoading] = useState(false);
  const [salesPlanError, setSalesPlanError] = useState("");
  const [salesPlanSaving, setSalesPlanSaving] = useState<Record<string, boolean>>({});
  const [monitoringRows, setMonitoringRows] = useState<RefreshMonitoringRowApi[]>([]);
  const [monitoringLoading, setMonitoringLoading] = useState(false);
  const [monitoringError, setMonitoringError] = useState("");
  const [monitoringSaving, setMonitoringSaving] = useState<Record<string, boolean>>({});
  const [monitoringRunning, setMonitoringRunning] = useState<Record<string, boolean>>({});
  const [monitoringRunAll, setMonitoringRunAll] = useState(false);

  const activeStores = useMemo(
    () => platforms.find((p) => p.id === activePlatform)?.stores || [],
    [platforms, activePlatform],
  );
  const storeTabs = useMemo<StoreTabItem[]>(
    () =>
      platforms.flatMap((platform) =>
        (platform.stores || []).map((store) => ({
          key: `${platform.id}:${store.id}`,
          platformId: platform.id,
          platformLabel: platform.label,
          storeId: store.id,
          storeName: store.name,
        })),
      ),
    [platforms],
  );
  const activeStoreTabKey = useMemo(() => `${activePlatform}:${activeStoreId}`, [activePlatform, activeStoreId]);
  const activeStoreCurrency = useMemo<"RUB" | "USD">(() => {
    const current = activeStores.find((s) => s.id === activeStoreId);
    return current?.currencyCode === "USD" ? "USD" : "RUB";
  }, [activeStores, activeStoreId]);
  const moneySign = activeStoreCurrency === "USD" ? "$" : "₽";

  const logistics = usePricingLogisticsController({
    activePlatform,
    activeStoreId,
    settingsTab: settingsTab === "logistics" ? "logistics" : "general",
    moneySign,
  });

  const general = usePricingGeneralController({
    activePlatform,
    activeStoreId,
    itemsLoading: false,
  });

  const categories = usePricingCategoryController({
    activePlatform,
    activeStoreId,
    moneySign,
    earningMode: general.earningMode,
    earningUnit: general.earningUnit,
    targetProfit: general.targetProfit,
    targetProfitPercent: general.targetProfitPercent,
    targetMargin: general.targetMargin,
    targetMarginRub: general.targetMarginRub,
    targetDrr: general.targetDrr,
    onStoreSettingsLoaded: (storeSettings) => {
      if (storeSettings.earning_mode === "profit" || storeSettings.earning_mode === "margin") general.setEarningMode(storeSettings.earning_mode);
      if (storeSettings.earning_unit === "rub" || storeSettings.earning_unit === "percent") general.setEarningUnit(storeSettings.earning_unit);
      general.setTargetProfit(storeSettings.target_profit_rub == null ? "" : String(storeSettings.target_profit_rub));
      general.setTargetProfitPercent(storeSettings.target_profit_percent == null ? "" : String(storeSettings.target_profit_percent));
      general.setTargetMargin(storeSettings.target_margin_percent == null ? "" : String(storeSettings.target_margin_percent));
      general.setTargetMarginRub(storeSettings.target_margin_rub == null ? "" : String(storeSettings.target_margin_rub));
      general.setTargetDrr(storeSettings.target_drr_percent == null ? "" : String(storeSettings.target_drr_percent));
      general.setCogsSource(
        storeSettings.cogs_source_id
          ? {
              type: storeSettings.cogs_source_type ?? "table",
              sourceId: storeSettings.cogs_source_id,
              sourceName: storeSettings.cogs_source_name ?? "",
              skuColumn: storeSettings.cogs_sku_column ?? "",
              valueColumn: storeSettings.cogs_value_column ?? "",
            }
          : null,
      );
      general.setStockSource(
        storeSettings.stock_source_id
          ? {
              type: storeSettings.stock_source_type ?? "table",
              sourceId: storeSettings.stock_source_id,
              sourceName: storeSettings.stock_source_name ?? "",
              skuColumn: storeSettings.stock_sku_column ?? "",
              valueColumn: storeSettings.stock_value_column ?? "",
            }
          : null,
      );
      general.setStoreSettingsSavedAt(String(storeSettings.updated_at || ""));
      general.setStoreSettingsError("");
      general.markStoreSettingsHydrated();
    },
  });

  async function loadContext(isRefresh = false) {
    if (!isRefresh) setLoading(true);
    setError("");
    try {
      if (!isRefresh) {
        const nextPlatforms = getCachedPricingPlatforms();
        if (nextPlatforms) {
          setPlatforms(nextPlatforms);
          const nextSelection = resolveNextStoreSelection({ nextPlatforms, activePlatform, activeStoreId });
          setActivePlatform(nextSelection.activePlatform);
          setActiveStoreId(nextSelection.activeStoreId);
          setLoading(false);
          return;
        }
      }
      const nextPlatforms: PlatformItem[] = await loadPricingPlatforms();
      setPlatforms(nextPlatforms);
      const nextSelection = resolveNextStoreSelection({ nextPlatforms, activePlatform, activeStoreId });
      setActivePlatform(nextSelection.activePlatform);
      setActiveStoreId(nextSelection.activeStoreId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  async function loadSalesPlanData() {
    setSalesPlanLoading(true);
    setSalesPlanError("");
    try {
      const data = await fetchPricingSalesPlan();
      setSalesPlanRows(Array.isArray(data.rows) ? data.rows : []);
    } catch (e) {
      setSalesPlanRows([]);
      setSalesPlanError(e instanceof Error ? e.message : String(e));
    } finally {
      setSalesPlanLoading(false);
    }
  }

  async function loadMonitoringData() {
    setMonitoringLoading(true);
    setMonitoringError("");
    try {
      const data = await fetchPricingMonitoring();
      setMonitoringRows(Array.isArray(data.rows) ? data.rows : []);
    } catch (e) {
      setMonitoringRows([]);
      setMonitoringError(e instanceof Error ? e.message : String(e));
    } finally {
      setMonitoringLoading(false);
    }
  }

  async function refreshStoreDataFromPlatform() {
    setRefreshing(true);
    categories.setItemsError("");
    try {
      await loadContext(true);
      await loadSalesPlanData();
      await loadMonitoringData();
      await refreshPricingStoreData(activePlatform, activeStoreId);
      if (settingsTab === "logistics") await logistics.loadLogisticsData();
      else if (settingsTab === "categories") await categories.loadPricingCategoryTree();
    } catch (e) {
      categories.setItemsError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshing(false);
    }
  }

  useEffect(() => { void loadContext(); }, []);
  useEffect(() => { void loadSalesPlanData(); }, []);
  useEffect(() => { void loadMonitoringData(); }, []);
  useEffect(() => {
    safeWriteJson(PRICING_SETTINGS_TAB_KEY, settingsTab);
  }, [settingsTab]);

  useEffect(() => {
    if (!activeStores.length) {
      setActiveStoreId("");
      return;
    }
    if (!activeStores.some((s) => s.id === activeStoreId)) setActiveStoreId(activeStores[0]?.id || "");
  }, [activeStores, activeStoreId]);

  function setActiveStoreTabKey(nextKey: string) {
    const normalized = String(nextKey || "").trim();
    if (!normalized) return;
    const found = storeTabs.find((item) => item.key === normalized);
    if (!found) return;
    setActivePlatform(found.platformId);
    setActiveStoreId(found.storeId);
  }

  async function saveSalesPlanRow(row: SalesPlanRowApi, values: Record<string, unknown>) {
    const platform = String(row.platform || "").trim().toLowerCase();
    const storeId = String(row.store_id || "").trim();
    const storeUid = String(row.store_uid || `${platform}:${storeId}`).trim();
    if (!platform || !storeId) return;
    setSalesPlanSaving((prev) => ({ ...prev, [storeUid]: true }));
    setSalesPlanError("");
    try {
      const data = await savePricingStoreSettings({
        platform,
        store_id: storeId,
        store_name: row.store_name,
        values,
      });
      if (!data.ok) throw new Error(data.message || "Не удалось сохранить план продаж");
      await loadSalesPlanData();
      if (platform === activePlatform && storeId === activeStoreId) {
        await categories.loadPricingCategoryTree();
      }
    } catch (e) {
      setSalesPlanError(e instanceof Error ? e.message : String(e));
    } finally {
      setSalesPlanSaving((prev) => ({ ...prev, [storeUid]: false }));
    }
  }

  async function saveSalesPlanRows(items: Array<{ row: SalesPlanRowApi; values: Record<string, unknown> }>) {
    const normalized = items.filter((item) => {
      const platform = String(item?.row?.platform || "").trim().toLowerCase();
      const storeId = String(item?.row?.store_id || "").trim();
      return Boolean(platform && storeId);
    });
    if (!normalized.length) return;

    const savingMapPatch = Object.fromEntries(
      normalized.map(({ row }) => [String(row.store_uid || `${row.platform}:${row.store_id}`).trim(), true]),
    );
    setSalesPlanSaving((prev) => ({ ...prev, ...savingMapPatch }));
    setSalesPlanError("");
    try {
      for (const { row, values } of normalized) {
        const platform = String(row.platform || "").trim().toLowerCase();
        const storeId = String(row.store_id || "").trim();
        const data = await savePricingStoreSettings({
          platform,
          store_id: storeId,
          store_name: row.store_name,
          values,
        });
        if (!data.ok) {
          throw new Error(data.message || `Не удалось сохранить план продаж для магазина ${row.store_name || storeId}`);
        }
      }
      await loadSalesPlanData();
      if (activePlatform && activeStoreId) {
        await categories.loadPricingCategoryTree();
      }
    } catch (e) {
      setSalesPlanError(e instanceof Error ? e.message : String(e));
    } finally {
      setSalesPlanSaving((prev) => {
        const next = { ...prev };
        for (const key of Object.keys(savingMapPatch)) next[key] = false;
        return next;
      });
    }
  }

  async function saveMonitoringJob(jobCode: string, values: { enabled: boolean; schedule_kind: string; interval_minutes?: number | null; time_of_day?: string | null }) {
    const code = String(jobCode || "").trim();
    if (!code) return;
    setMonitoringSaving((prev) => ({ ...prev, [code]: true }));
    setMonitoringError("");
    try {
      await savePricingMonitoringJob({ job_code: code, ...values });
      await loadMonitoringData();
    } catch (e) {
      setMonitoringError(e instanceof Error ? e.message : String(e));
    } finally {
      setMonitoringSaving((prev) => ({ ...prev, [code]: false }));
    }
  }

  async function runMonitoringJob(jobCode: string) {
    const code = String(jobCode || "").trim();
    if (!code) return;
    setMonitoringRunning((prev) => ({ ...prev, [code]: true }));
    setMonitoringError("");
    try {
      await runPricingMonitoringJob(code);
      showAppToast({
        message:
          code === "strategy_refresh"
            ? "Пересчет стратегии запущен, статус можно смотреть в Мониторинге."
            : "Обновление запущено",
      });
      await loadMonitoringData();
    } catch (e) {
      setMonitoringError(e instanceof Error ? e.message : String(e));
    } finally {
      setMonitoringRunning((prev) => ({ ...prev, [code]: false }));
    }
  }

  async function runMonitoringAll() {
    setMonitoringRunAll(true);
    setMonitoringError("");
    try {
      await runPricingMonitoringAll();
      showAppToast({ message: "Обновление запущено" });
      await loadMonitoringData();
    } catch (e) {
      setMonitoringError(e instanceof Error ? e.message : String(e));
    } finally {
      setMonitoringRunAll(false);
    }
  }

  return {
    loading,
    refreshing,
    error,
    platforms,
    activePlatform,
    activeStoreId,
    earningMode: general.earningMode,
    earningUnit: general.earningUnit,
    targetProfit: general.targetProfit,
    targetProfitPercent: general.targetProfitPercent,
    targetMargin: general.targetMargin,
    targetMarginRub: general.targetMarginRub,
    targetDrr: general.targetDrr,
    itemsLoading: categories.itemsLoading,
    itemsError: categories.itemsError,
    cogsSource: general.cogsSource,
    cogsModalOpen: general.cogsModalOpen,
    stockSource: general.stockSource,
    stockModalOpen: general.stockModalOpen,
    settingsTab,
    salesPlanRows,
    salesPlanLoading,
    salesPlanError,
    salesPlanSaving,
    monitoringRows,
    monitoringLoading,
    monitoringError,
    monitoringSaving,
    monitoringRunning,
    monitoringRunAll,
    categoryRows: categories.categoryRows,
    cellDrafts: categories.cellDrafts,
    cellSaving: categories.cellSaving,
    storeSettingsSaving: general.storeSettingsSaving,
    storeSettingsError: general.storeSettingsError,
    storeSettingsSavedAt: general.storeSettingsSavedAt,
    logisticsStoreSettings: logistics.logisticsStoreSettings,
    logisticsRows: logistics.logisticsRows,
    logisticsLoading: logistics.logisticsLoading,
    logisticsError: logistics.logisticsError,
    logisticsSearch: logistics.logisticsSearch,
    logisticsPage: logistics.logisticsPage,
    logisticsPageSize: logistics.logisticsPageSize,
    logisticsTotal: logistics.logisticsTotal,
    logisticsPageSizeOptions: logistics.logisticsPageSizeOptions,
    logisticsStoreSaving: logistics.logisticsStoreSaving,
    logisticsStoreSavedAt: logistics.logisticsStoreSavedAt,
    logisticsStoreError: logistics.logisticsStoreError,
    logisticsFieldErrors: logistics.logisticsFieldErrors,
    logisticsCellDrafts: logistics.logisticsCellDrafts,
    logisticsCellSaving: logistics.logisticsCellSaving,
    logisticsEditingCell: logistics.logisticsEditingCell,
    logisticsImportOpen: logistics.logisticsImportOpen,
    storeTabs,
    activeStoreTabKey,
    activeStores,
    moneySign,
    tableColumns: categories.tableColumns,
    loadPricingCategoryTree: categories.loadPricingCategoryTree,
    activeTargetValue: general.activeTargetValue,
    setActivePlatform,
    setActiveStoreId,
    setActiveStoreTabKey,
    setEarningMode: general.setEarningMode,
    setEarningUnit: general.setEarningUnit,
    setTargetDrr: general.setTargetDrr,
    setCogsSource: general.setCogsSource,
    setCogsModalOpen: general.setCogsModalOpen,
    setStockSource: general.setStockSource,
    setStockModalOpen: general.setStockModalOpen,
    setSettingsTab,
    setActiveTargetValue: general.setActiveTargetValue,
    loadContext,
    loadSalesPlanData,
    loadMonitoringData,
    refreshStoreDataFromPlatform,
    saveSalesPlanRow,
    saveSalesPlanRows,
    saveMonitoringJob,
    runMonitoringJob,
    runMonitoringAll,
    getCellKey: categories.getCellKey,
    defaultFieldValue: categories.defaultFieldValue,
    formatNum: categories.formatNum,
    queueSaveCell: categories.queueSaveCell,
    flushSaveCell: categories.flushSaveCell,
    applyColumnValue: categories.applyColumnValue,
    setLogisticsField: logistics.setLogisticsField,
    setLogisticsNumericField: logistics.setLogisticsNumericField,
    onLogisticsNumericBlur: logistics.onLogisticsNumericBlur,
    getLogisticsNumericValue: logistics.getLogisticsNumericValue,
    setLogisticsPage: logistics.setLogisticsPage,
    setLogisticsPageSize: logistics.setLogisticsPageSize,
    setLogisticsSearch: logistics.setLogisticsSearch,
    setLogisticsImportOpen: logistics.setLogisticsImportOpen,
    setLogisticsCellDrafts: logistics.setLogisticsCellDrafts,
    toLiveLogisticsRow: logistics.toLiveLogisticsRow,
    loadLogisticsData: logistics.loadLogisticsData,
    fmtCell: logistics.fmtCell,
    getLogisticsCellKey: logistics.getLogisticsCellKey,
    setLogisticsCellDraftByKey: logistics.setLogisticsCellDraftByKey,
    commitLogisticsCell: logistics.commitLogisticsCell,
    handleLogisticsImportDone: logistics.handleLogisticsImportDone,
  };
}
