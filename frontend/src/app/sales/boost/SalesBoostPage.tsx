import { useEffect, useMemo, useState } from "react";
import CatalogTreeControls from "../../pricing/_components/CatalogTreeControls";
import PricingCatalogFrame from "../../pricing/_components/PricingCatalogFrame";
import commonStyles from "../../pricing/_components/PricingPageCommon.module.css";
import { filterWorkingMarketplaceStores, parseStoreTabKey, tabKeyForStore } from "../../pricing/_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter } from "../../pricing/_shared/stockFilterState";
import { usePricingCatalogController } from "../../pricing/_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../../pricing/_shared/usePricingOverviewData";
import { BoostOverviewRow, BoostTable } from "./BoostTable";

const BOOST_CTX_CACHE_KEY = "sales_boost_ctx_v3";
const BOOST_TREE_SOURCE_STORE_KEY = "sales_boost_tree_source_store_id_v3";
const BOOST_OVERVIEW_CACHE_PREFIX = "sales_boost_overview_v3:";

function todayLocalDateInputValue() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

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
    reloadNonce,
    setReloadNonce,
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
  const [reportDate, setReportDate] = useState(todayLocalDateInputValue);

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

  const { rows, visibleStores, totalCount, tableLoading } = usePricingOverviewData<BoostOverviewRow>({
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
    extraParams: { stock_filter: stockFilter, report_date: reportDate },
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

  const activeStoreLabel = useMemo(() => {
    if (tab === "all") return "Все товары";
    const parsed = parseStoreTabKey(tab);
    if (!parsed) return "";
    const found = yandexStores.find((store) => store.platform === parsed.platform && store.store_id === parsed.store_id);
    return found?.label || tab;
  }, [tab, yandexStores]);

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const moneyHeader = (label: string) => (tab === "all" ? `${label}, ₽ / $` : `${label}, ${moneySign}`);
  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const activeStoreUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";

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
    reportDateLabel: reportDate,
    onPageChange: setPage,
    onPageSizeChange: (value) => {
      setPage(1);
      setPageSize(value);
    },
  });
  const tableHeaderControls = (
      <div className={commonStyles.tableControlRow}>
        <div className={commonStyles.tableSearchWrap}>
        <input
          id="boost-table-search"
          className={`input ${commonStyles.select}`}
          value={searchDraft}
          onChange={(e) => setSearchDraft(e.target.value)}
          placeholder="Поиск по SKU или наименованию"
        />
        </div>
        <div className={commonStyles.tableFilterWrap}>
          <div className={commonStyles.tableFilterBlock}>
            <label className={commonStyles.fieldLabel} htmlFor="boost-report-date">Дата отчёта</label>
            <input
              id="boost-report-date"
              type="date"
              className={`input ${commonStyles.select}`}
              value={reportDate}
              onChange={(e) => {
                setPage(1);
                setReportDate(e.target.value || todayLocalDateInputValue());
              }}
            />
          </div>
        </div>
      </div>
  );

  return (
    <PricingCatalogFrame
      title="Эффективность буста"
      subtitle="Дневной отчёт по SKU: какая стратегия буста стояла и какую долю продаж товар получил под бустом."
      tabs={(
        <>
          <button className={`btn inline ${commonStyles.tabBtn} ${tab === "all" ? commonStyles.tabBtnActive : ""}`} onClick={() => setTab("all")}>Все товары</button>
          {yandexStores.map((store) => {
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
