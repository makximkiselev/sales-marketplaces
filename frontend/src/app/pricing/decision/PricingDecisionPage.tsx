import { useEffect, useMemo, useState } from "react";
import PricingCatalogFrame from "../_components/PricingCatalogFrame";
import CatalogTreeControls from "../_components/CatalogTreeControls";
import commonStyles from "../_components/PricingPageCommon.module.css";
import { filterWorkingMarketplaceStores, parseStoreTabKey, tabKeyForStore } from "../_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter } from "../_shared/stockFilterState";
import { usePricingCatalogController } from "../_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../_shared/usePricingOverviewData";
import { StrategyOverviewRow, StrategyTable } from "./StrategyTable";
import styles from "./StrategyPage.module.css";

const STRATEGY_CTX_CACHE_KEY = "pricing_strategy_ctx_v6";
const STRATEGY_TREE_SOURCE_STORE_KEY = "pricing_strategy_tree_source_store_id_v6";
const STRATEGY_OVERVIEW_CACHE_PREFIX = "pricing_strategy_overview_v6:";

type SalesPlanMetric = {
  label?: string;
  currency_code?: string;
  planned_revenue_daily?: number | null;
  planned_profit_daily?: number | null;
  planned_profit_pct?: number | null;
  adjusted_planned_revenue_daily?: number | null;
  adjusted_planned_profit_daily?: number | null;
  adjusted_planned_profit_pct?: number | null;
  fact_revenue?: number | null;
  fact_profit?: number | null;
  fact_profit_pct?: number | null;
  operational_revenue?: number | null;
  operational_profit?: number | null;
  operational_profit_pct?: number | null;
};

type StrategyOverviewResp = {
  sales_plan_summary?: {
    overall?: SalesPlanMetric | null;
    by_store?: Record<string, SalesPlanMetric>;
  };
};

function clearStrategyPageCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(STRATEGY_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (!key) continue;
      if (key.startsWith(STRATEGY_OVERVIEW_CACHE_PREFIX)) keysToRemove.push(key);
    }
    keysToRemove.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}

export default function PricingDecisionPage() {
  const {
    loading,
    error,
    setError,
    context,
    stores,
    tab,
    setTab,
    treeSourceStoreId,
    setTreeSourceStoreId,
    search,
    searchDraft,
    setSearchDraft,
    page,
    setPage,
    pageSize,
    setPageSize,
    selectedTreePath,
    setTreeRoots,
    expanded,
    flatTree,
    activeStoreRef,
    activeStoreCurrency,
    moneySign,
    reloadNonce,
    setReloadNonce,
    toggleTree,
    toggleExpand,
    toggleExpandAll,
  } = usePricingCatalogController({
    contextEndpoint: "/api/pricing/strategy/context",
    contextCacheKey: STRATEGY_CTX_CACHE_KEY,
    treeSourceStoreKey: STRATEGY_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 200,
  });
  const [strategyFilter, setStrategyFilter] = useState<
    | "all"
    | "promo2_profitable_boost"
    | "promo2_profitable"
    | "promo1_profitable_boost"
    | "promo1_profitable"
    | "promo2_moderate_boost"
    | "promo2_moderate"
    | "promo1_moderate_boost"
    | "promo1_moderate"
    | "profitable_boost"
    | "profitable"
    | "moderate_boost"
    | "moderate"
    | "overpriced"
  >("all");
  const [salesFilter, setSalesFilter] = useState<"all" | "with_sales" | "without_sales">("all");
  const [sortKey, setSortKey] = useState<
    "sku_plan_revenue" | "sku_plan_qty" | "sku_plan_profit_abs" | "sku_plan_profit_pct" | "fact_sales_revenue" | "fact_sales_qty" | "fact_profit_abs" | "fact_profit_pct" | "profit_completion_pct"
  >("fact_sales_revenue");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [stockFilter, setStockFilter] = useState<"all" | "in_stock" | "out_of_stock">("all");
  const [stockFilterReady, setStockFilterReady] = useState(false);
  const visiblePageStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);

  const { rows, overviewData, visibleStores, totalCount, tableLoading } = usePricingOverviewData<StrategyOverviewRow>({
    enabled: Boolean(context),
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/pricing/strategy/tree",
    overviewEndpoint: "/api/pricing/strategy/overview",
    refreshEndpoint: "/api/pricing/strategy/refresh",
    overviewCachePrefix: STRATEGY_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearStrategyPageCache,
    extraParams: { strategy_filter: strategyFilter, sales_filter: salesFilter, stock_filter: stockFilter, sort_key: sortKey, sort_dir: sortDir },
  });

  useEffect(() => {
    setStockFilter(readGlobalStockFilter());
    setStockFilterReady(true);
  }, []);

  useEffect(() => {
    if (!stockFilterReady) return;
    writeGlobalStockFilter(stockFilter);
  }, [stockFilter, stockFilterReady]);
  const visibleOverviewStores = useMemo(() => filterWorkingMarketplaceStores(visibleStores), [visibleStores]);

  useEffect(() => {
    if (!visiblePageStores.length) return;
    if (tab !== "all") {
      const parsed = parseStoreTabKey(tab);
      if (!parsed || parsed.platform !== "yandex_market") setTab("all");
    }
    if (!visiblePageStores.some((store) => store.store_uid === treeSourceStoreId)) {
      setTreeSourceStoreId(visiblePageStores[0].store_uid);
    }
  }, [tab, treeSourceStoreId, visiblePageStores, setTab, setTreeSourceStoreId]);

  function handleRefreshDone() {
    setPage(1);
    setReloadNonce((n) => n + 1);
  }

  const activeStoreLabel = useMemo(() => {
    if (tab === "all") return "Все товары";
    const parsed = parseStoreTabKey(tab);
    if (!parsed) return "";
    const found = visiblePageStores.find((s) => s.platform === parsed.platform && s.store_id === parsed.store_id);
    return found?.label || tab;
  }, [tab, visiblePageStores]);

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const moneyHeader = (label: string) => (tab === "all" ? `${label}, ₽ / $` : `${label}, ${moneySign}`);
  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const activeStoreUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
  const salesPlanSummary = (overviewData as (StrategyOverviewResp & { ok?: boolean }) | null)?.sales_plan_summary;
  const activeSalesPlan = useMemo(() => {
    if (!salesPlanSummary) return null;
    if (tab === "all") return salesPlanSummary.overall || null;
    return (salesPlanSummary.by_store || {})[activeStoreUid] || null;
  }, [salesPlanSummary, tab, activeStoreUid]);

  function formatPlanMoney(value: number | null | undefined, currencyCode: string | undefined) {
    if (value == null || Number.isNaN(Number(value))) return "—";
    const rounded = Math.round(Number(value)).toLocaleString("ru-RU");
    const symbol = (currencyCode || "RUB").toUpperCase() === "USD" ? "$" : "₽";
    return `${rounded} ${symbol}`;
  }

  function formatPlanPct(value: number | null | undefined) {
    if (value == null || Number.isNaN(Number(value))) return "—";
    return `${Math.round(Number(value) * 100) / 100}%`;
  }

  const salesPlanPanel = activeSalesPlan ? (
    <section className={styles.planPanel}>
      <div className={styles.planPanelHeader}>
        <div>
          <div className={styles.planPanelTitle}>План/Факт продаж</div>
          <div className={styles.planPanelSubtle}>
            {tab === "all" ? "Сводно по всем магазинам, показатели сведены в RUB. Факт = только финализированные продажи, оперативка = текущий HOT-слой." : activeStoreLabel}
          </div>
        </div>
      </div>
      <div className={styles.planTableWrap}>
        <table className={styles.planTable}>
          <thead>
            <tr>
              <th />
              <th>Оборот</th>
              <th>Прибыль, %</th>
              <th>Прибыль, {((activeSalesPlan.currency_code || "RUB").toUpperCase() === "USD" && tab !== "all") ? "$" : "₽"}</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>План</td>
              <td>{formatPlanMoney(activeSalesPlan.planned_revenue_daily, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
              <td>{formatPlanPct(activeSalesPlan.planned_profit_pct)}</td>
              <td>{formatPlanMoney(activeSalesPlan.planned_profit_daily, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
            </tr>
            <tr>
              <td>Скорректированный план</td>
              <td>{formatPlanMoney(activeSalesPlan.adjusted_planned_revenue_daily, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
              <td>{formatPlanPct(activeSalesPlan.adjusted_planned_profit_pct)}</td>
              <td>{formatPlanMoney(activeSalesPlan.adjusted_planned_profit_daily, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
            </tr>
            <tr>
              <td>Факт</td>
              <td>{formatPlanMoney(activeSalesPlan.fact_revenue, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
              <td>{formatPlanPct(activeSalesPlan.fact_profit_pct)}</td>
              <td>{formatPlanMoney(activeSalesPlan.fact_profit, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
            </tr>
            <tr>
              <td>Оперативка</td>
              <td>{formatPlanMoney(activeSalesPlan.operational_revenue, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
              <td>{formatPlanPct(activeSalesPlan.operational_profit_pct)}</td>
              <td>{formatPlanMoney(activeSalesPlan.operational_profit, activeSalesPlan.currency_code || (tab === "all" ? "RUB" : activeStoreCurrency))}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  ) : null;

  const tableConfig = StrategyTable({
    rows,
    visibleStores: visibleOverviewStores,
    tab,
    activeStoreUid,
    activeStoreCurrency,
    moneyHeader,
    page,
    totalPages,
    pageSize,
    totalCount,
    selectedTreePath,
    tableLoading,
    strategyFilter,
    onStrategyFilterChange: setStrategyFilter,
    salesFilter,
    onSalesFilterChange: setSalesFilter,
    sortKey,
    sortDir,
    onSortChange: (key) => {
      setPage(1);
      if (sortKey === key) {
        setSortDir((current) => (current === "desc" ? "asc" : "desc"));
      } else {
        setSortKey(key);
        setSortDir("desc");
      }
    },
    onPageChange: setPage,
    onPageSizeChange: (value) => {
      setPage(1);
      setPageSize(value);
    },
  });
  const tableHeaderControls = (
    <div className={commonStyles.tableControlRow}>
      <div className={commonStyles.tableFilterWrap}>{tableConfig.tableTitleControls}</div>
      <div className={commonStyles.tableSearchWrap}>
        <input
          id="strategy-table-search"
          className={`input ${commonStyles.select}`}
          value={searchDraft}
          onChange={(e) => setSearchDraft(e.target.value)}
          placeholder="Поиск по SKU или наименованию"
        />
      </div>
    </div>
  );

  return (
    <PricingCatalogFrame
      title="Стратегия ценообразования"
      subtitle="Финальный сценарий по каждому товару: промо, привлекательность, буст и итоговая цена отправки."
      summaryPanel={salesPlanPanel}
      tabs={(
        <>
          <button className={`btn inline ${commonStyles.tabBtn} ${tab === "all" ? commonStyles.tabBtnActive : ""}`} onClick={() => setTab("all")}>Все товары</button>
          {visiblePageStores.map((store) => {
            const key = tabKeyForStore(store);
            return (
              <button key={key} className={`btn inline ${commonStyles.tabBtn} ${tab === key ? commonStyles.tabBtnActive : ""}`} onClick={() => setTab(key)}>
                <span>{store.label}</span>
                <span className={commonStyles.tabBadge}>{store.platform_label}</span>
              </button>
            );
          })}
        </>
      )}
      searchValue={searchDraft}
      onSearchChange={setSearchDraft}
      searchPlaceholder="Поиск по SKU или наименованию"
      hideSearchPanel
      error={error}
      treeSelector={
        <CatalogTreeControls
          selectId="strategy-tree-source-store"
          stores={visiblePageStores}
          treeSourceStoreId={treeSourceStoreId}
          onTreeSourceStoreChange={setTreeSourceStoreId}
          showStoreSelector={tab === "all"}
          stockFilter={stockFilter}
          onStockFilterChange={(value) => {
            setPage(1);
            setStockFilter(value);
          }}
        />
      }
      treeMeta="Сводный список товаров"
      flatTree={flatTree}
      selectedTreePath={selectedTreePath}
      expandedSize={expanded.size}
      isExpanded={(path) => expanded.has(path)}
      onToggleExpandAll={toggleExpandAll}
      onToggleExpand={toggleExpand}
      onToggleTree={toggleTree}
      treeLoadingText={treeLoadingText}
      tableTitle=""
      tableTitleControls={tableHeaderControls}
      tableMeta={tableConfig.tableMeta}
      table={tableConfig.table}
      page={tableConfig.page}
      totalPages={tableConfig.totalPages}
      onPageChange={setPage}
      onPrevPage={tableConfig.onPrevPage}
      onNextPage={tableConfig.onNextPage}
      canPrev={tableConfig.canPrev}
      canNext={tableConfig.canNext}
      pageSize={tableConfig.pageSize}
      onPageSizeChange={tableConfig.onPageSizeSelect}
    />
  );
}
