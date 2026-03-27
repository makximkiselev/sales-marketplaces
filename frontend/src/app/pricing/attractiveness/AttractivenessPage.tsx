"use client";

import { useEffect, useMemo, useState } from "react";
import PricingCatalogFrame from "../_components/PricingCatalogFrame";
import CatalogTreeControls from "../_components/CatalogTreeControls";
import commonStyles from "../_components/PricingPageCommon.module.css";
import { filterWorkingMarketplaceStores, parseStoreTabKey, tabKeyForStore } from "../_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter } from "../_shared/stockFilterState";
import { usePricingCatalogController } from "../_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../_shared/usePricingOverviewData";
import { AttractivenessTable } from "./AttractivenessTable";
import { AttractivenessOverviewRow } from "./attractivenessUtils";

const ATTR_CTX_CACHE_KEY = "pricing_attractiveness_ctx_v4";
const ATTR_TREE_SOURCE_STORE_KEY = "pricing_attractiveness_tree_source_store_id_v4";
const ATTR_OVERVIEW_CACHE_PREFIX = "pricing_attractiveness_overview_v4:";

function clearAttractivenessCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(ATTR_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (!key) continue;
      if (key.startsWith(ATTR_OVERVIEW_CACHE_PREFIX)) keysToRemove.push(key);
    }
    keysToRemove.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}

export default function AttractivenessPage() {
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
    contextEndpoint: "/api/pricing/attractiveness/context",
    contextCacheKey: ATTR_CTX_CACHE_KEY,
    treeSourceStoreKey: ATTR_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 200,
  });
  const [statusFilter, setStatusFilter] = useState<"all" | "profitable" | "moderate" | "overpriced">("all");
  const [stockFilter, setStockFilter] = useState<"all" | "in_stock" | "out_of_stock">("all");
  const [stockFilterReady, setStockFilterReady] = useState(false);
  const visiblePageStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);
  const { rows, visibleStores, totalCount, tableLoading } = usePricingOverviewData<AttractivenessOverviewRow>({
    enabled: Boolean(context),
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/pricing/attractiveness/tree",
    overviewEndpoint: "/api/pricing/attractiveness/overview",
    refreshEndpoint: "/api/pricing/attractiveness/refresh",
    overviewCachePrefix: ATTR_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearAttractivenessCache,
    extraParams: { status_filter: statusFilter, stock_filter: stockFilter },
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
  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const isOzonView = false;

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

  const activeStoreLabel = useMemo(() => {
    if (tab === "all") return "Все товары";
    const parsed = parseStoreTabKey(tab);
    if (!parsed) return "";
    const found = visiblePageStores.find((s) => s.platform === parsed.platform && s.store_id === parsed.store_id);
    return found?.label || tab;
  }, [tab, visiblePageStores]);

  const moneyHeader = (label: string) => (tab === "all" ? `${label}, ₽ / $` : `${label}, ${moneySign}`);
  const filteredRows = rows;

  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const activeStoreUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
  const tableConfig = AttractivenessTable({
    rows: filteredRows,
    visibleStores: visibleOverviewStores,
    tab,
    activeStoreUid,
    activeStoreCurrency,
    isOzonView,
    moneyHeader,
    page,
    totalPages,
    pageSize,
    totalCount,
    selectedTreePath,
    tableLoading,
    statusFilter,
    onStatusFilterChange: setStatusFilter,
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
          id="attractiveness-table-search"
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
      title="Привлекательность"
      subtitle="Каталог товаров для расчета выгодных/умеренных/невыгодных цен и конкурентов по площадкам."
      tabs={(
        <>
          <button className={`btn inline ${commonStyles.tabBtn} ${tab === "all" ? commonStyles.tabBtnActive : ""}`} onClick={() => setTab("all")}>Все товары</button>
          {visiblePageStores.map((s) => {
            const key = tabKeyForStore(s);
            return (
              <button key={key} className={`btn inline ${commonStyles.tabBtn} ${tab === key ? commonStyles.tabBtnActive : ""}`} onClick={() => setTab(key)}>
                <span>{s.label}</span>
                <span className={commonStyles.tabBadge}>{s.platform_label}</span>
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
          selectId="attract-tree-source-store"
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
