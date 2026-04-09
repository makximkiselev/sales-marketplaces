import { useEffect, useMemo, useState } from "react";
import CatalogTreeControls from "../../pricing/_components/CatalogTreeControls";
import PricingCatalogFrame from "../../pricing/_components/PricingCatalogFrame";
import commonStyles from "../../pricing/_components/PricingPageCommon.module.css";
import summaryStyles from "../elasticity/SalesElasticityPage.module.css";
import { filterWorkingMarketplaceStores, formatPercent, parseStoreTabKey, tabKeyForStore } from "../../pricing/_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter } from "../../pricing/_shared/stockFilterState";
import { usePricingCatalogController } from "../../pricing/_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../../pricing/_shared/usePricingOverviewData";
import { BoostOverviewRow, BoostTable } from "./BoostTable";
import { WorkspaceTabs } from "../../../components/page/WorkspaceKit";

const BOOST_CTX_CACHE_KEY = "sales_boost_ctx_v3";
const BOOST_TREE_SOURCE_STORE_KEY = "sales_boost_tree_source_store_id_v3";
const BOOST_OVERVIEW_CACHE_PREFIX = "sales_boost_overview_v4:";

function clearBoostPageCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(BOOST_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (!key) continue;
      if (key.startsWith(BOOST_OVERVIEW_CACHE_PREFIX)) keysToRemove.push(key);
    }
    keysToRemove.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}

export default function SalesBoostPage() {
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
    reloadNonce,
    page,
    setPage,
    pageSize,
    setPageSize,
    selectedTreePath,
    treeRoots,
    setTreeRoots,
    expanded,
    flatTree,
    activeStoreRef,
    activeStoreCurrency,
    moneySign,
    toggleTree,
    toggleExpand,
    toggleExpandAll,
  } = usePricingCatalogController({
    contextEndpoint: "/api/pricing/boost/context",
    contextCacheKey: BOOST_CTX_CACHE_KEY,
    treeSourceStoreKey: BOOST_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 200,
  });
  const [stockFilter, setStockFilter] = useState<"all" | "in_stock" | "out_of_stock">("all");
  const [stockFilterReady, setStockFilterReady] = useState(false);
  const [windowDays, setWindowDays] = useState<7 | 14 | 30>(7);

  const yandexStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);

  useEffect(() => {
    if (!yandexStores.length) return;
    if (tab === "all") return;
    const parsed = parseStoreTabKey(tab);
    if (parsed?.platform === "yandex_market") return;
    setTab("all");
  }, [tab, yandexStores, setTab]);

  useEffect(() => {
    if (!yandexStores.length) return;
    const valid = yandexStores.some((store) => String(store.store_uid) === String(treeSourceStoreId || ""));
    if (!valid) setTreeSourceStoreId(String(yandexStores[0].store_uid));
  }, [treeSourceStoreId, yandexStores, setTreeSourceStoreId]);

  const { rows, overviewData, visibleStores, totalCount, tableLoading } = usePricingOverviewData<BoostOverviewRow>({
    enabled: Boolean(context) && yandexStores.length > 0,
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/pricing/boost/tree",
    overviewEndpoint: "/api/pricing/boost/overview",
    refreshEndpoint: "/api/pricing/boost/refresh",
    overviewCachePrefix: BOOST_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearBoostPageCache,
    extraParams: { stock_filter: stockFilter, window_days: String(windowDays) },
  });

  useEffect(() => {
    setStockFilter(readGlobalStockFilter());
    setStockFilterReady(true);
  }, []);

  useEffect(() => {
    if (!stockFilterReady) return;
    writeGlobalStockFilter(stockFilter);
  }, [stockFilter, stockFilterReady]);

  const yandexVisibleStores = useMemo(() => filterWorkingMarketplaceStores(visibleStores), [visibleStores]);

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const moneyHeader = (label: string) => (tab === "all" ? `${label}, ₽ / $` : `${label}, ${moneySign}`);
  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const activeStoreUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
  const summary = (overviewData as {
    summary?: {
      sku_with_plan_count?: number;
      sku_with_fact_count?: number;
      avg_effectiveness_pct?: number | null;
      avg_planned_boost_bid_percent?: number | null;
      avg_actual_boost_rate_percent?: number | null;
    };
    anchor_date?: string;
    date_from?: string;
    date_to?: string;
    window_days?: number;
  } | null)?.summary;
  const anchorDate = (overviewData as { anchor_date?: string } | null)?.anchor_date || "";
  const dateFrom = (overviewData as { date_from?: string } | null)?.date_from || "";
  const dateTo = (overviewData as { date_to?: string } | null)?.date_to || "";

  const tableConfig = BoostTable({
    rows,
    visibleStores: yandexVisibleStores,
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
    reportDateLabel: anchorDate,
    onPageChange: setPage,
    onPageSizeChange: (value) => {
      setPage(1);
      setPageSize(value);
    },
  });
  const summaryCards = [
    { label: "Окно", value: `${windowDays} дней` },
    { label: "Последняя доставка", value: anchorDate || "—" },
    { label: "SKU с планом буста", value: String(summary?.sku_with_plan_count ?? "—") },
    { label: "SKU с фактом буста", value: String(summary?.sku_with_fact_count ?? "—") },
    { label: "Среднее срабатывание", value: formatPercent(summary?.avg_effectiveness_pct) },
    { label: "Средняя плановая ставка", value: formatPercent(summary?.avg_planned_boost_bid_percent) },
    { label: "Средняя фактическая ставка", value: formatPercent(summary?.avg_actual_boost_rate_percent) },
  ];
  const summaryPanel = (
    <div className={summaryStyles.summaryStack}>
      <div className={summaryStyles.summaryGrid}>
        {summaryCards.map((card) => (
          <div key={card.label} className={summaryStyles.summaryCard}>
            <div className={summaryStyles.summaryLabel}>{card.label}</div>
            <div className={summaryStyles.summaryValue}>{card.value}</div>
          </div>
        ))}
      </div>
    </div>
  );
  const tableHeaderControls = (
      <div className={commonStyles.tableControlRow}>
        <div className={commonStyles.tableSearchWrap}>
        <input
          id="boost-table-search"
          className={`input input-size-xl ${commonStyles.select}`}
          value={searchDraft}
          onChange={(e) => setSearchDraft(e.target.value)}
          placeholder="Поиск по SKU или наименованию"
        />
        </div>
        <div className={commonStyles.tableFilterWrap}>
          <div className={commonStyles.tableFilterBlock}>
            <label className={commonStyles.fieldLabel}>Окно по доставке</label>
            <div className={commonStyles.chipRow}>
              {[7, 14, 30].map((days) => (
                <button
                  key={days}
                  type="button"
                  className={`btn inline sm ${windowDays === days ? commonStyles.chipActive : ""}`}
                  onClick={() => {
                    setPage(1);
                    setWindowDays(days as 7 | 14 | 30);
                  }}
                >
                  {days} дней
                </button>
              ))}
            </div>
          </div>
          <div className={commonStyles.tableFilterBlock}>
            <label className={commonStyles.fieldLabel}>Период</label>
            <div className={commonStyles.filterHint}>{dateFrom && dateTo ? `${dateFrom} — ${dateTo}` : "—"}</div>
          </div>
        </div>
      </div>
  );

  return (
    <PricingCatalogFrame
      title="Эффективность буста"
      subtitle="Исторический отчет по delivered SKU: план буста из заказа, факт из netting `Буст продаж, оплата за продажи`."
      tabs={(
        <WorkspaceTabs
          items={[
            { id: "all", label: "Все товары" },
            ...yandexStores.map((store) => ({
              id: tabKeyForStore(store),
              label: store.label,
              meta: store.platform_label,
            })),
          ]}
          activeId={tab}
          onChange={setTab}
        />
      )}
      searchValue={searchDraft}
      onSearchChange={setSearchDraft}
      searchPlaceholder="Поиск по SKU или наименованию"
      hideSearchPanel
      error={error}
      summaryPanel={summaryPanel}
      treeSelector={
        <CatalogTreeControls
          selectId="boost-tree-source-store"
          stores={yandexStores}
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
