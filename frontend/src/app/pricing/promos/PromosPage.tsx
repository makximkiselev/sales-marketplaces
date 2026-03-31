import { useEffect, useMemo, useState } from "react";
import PricingCatalogFrame from "../_components/PricingCatalogFrame";
import CatalogTreeControls from "../_components/CatalogTreeControls";
import commonStyles from "../_components/PricingPageCommon.module.css";
import { filterWorkingMarketplaceStores, parseStoreTabKey, tabKeyForStore } from "../_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter } from "../_shared/stockFilterState";
import { usePricingCatalogController } from "../_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../_shared/usePricingOverviewData";
import { PromoColumn, PromoOverviewRow, PromoTable } from "./PromoTable";
import { WorkspaceTabs } from "../../../components/page/WorkspaceKit";

const PROMOS_CTX_CACHE_KEY = "pricing_promos_ctx_v10";
const PROMOS_OVERVIEW_CACHE_PREFIX = "pricing_promos_overview_v10:";
const PROMOS_TREE_SOURCE_STORE_KEY = "pricing_promos_tree_source_store_id_v10";

function clearPromosPageCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(PROMOS_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (!key) continue;
      if (key.startsWith(PROMOS_OVERVIEW_CACHE_PREFIX)) keysToRemove.push(key);
    }
    keysToRemove.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}

export default function PromosPage() {
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
    contextEndpoint: "/api/pricing/promos/context",
    contextCacheKey: PROMOS_CTX_CACHE_KEY,
    treeSourceStoreKey: PROMOS_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 200,
  });
  const [stockFilter, setStockFilter] = useState<"all" | "in_stock" | "out_of_stock">("all");
  const [stockFilterReady, setStockFilterReady] = useState(false);

  const { rows, overviewData, visibleStores, totalCount, tableLoading } = usePricingOverviewData<PromoOverviewRow>({
    enabled: Boolean(context),
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/pricing/promos/tree",
    overviewEndpoint: "/api/pricing/promos/overview",
    refreshEndpoint: "/api/pricing/promos/refresh",
    overviewCachePrefix: PROMOS_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearPromosPageCache,
    extraParams: { stock_filter: stockFilter },
  });

  useEffect(() => {
    setStockFilter(readGlobalStockFilter());
    setStockFilterReady(true);
  }, []);

  useEffect(() => {
    if (!stockFilterReady) return;
    writeGlobalStockFilter(stockFilter);
  }, [stockFilter, stockFilterReady]);

  const visiblePageStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);
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

  const activeStoreLabel = useMemo(() => {
    if (tab === "all") return "Все товары";
    const parsed = parseStoreTabKey(tab);
    if (!parsed) return "";
    const found = visiblePageStores.find((s) => s.platform === parsed.platform && s.store_id === parsed.store_id);
    return found ? `${found.label} (${found.store_id})` : tab;
  }, [tab, visiblePageStores]);

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const moneyHeader = (label: string) => (tab === "all" ? `${label}, ₽ / $` : `${label}, ${moneySign}`);
  const activeStoreUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
  const promoColumns = useMemo<PromoColumn[]>(() => {
    const raw = (overviewData as { promo_columns?: PromoColumn[] } | null)?.promo_columns;
    return Array.isArray(raw) ? raw : [];
  }, [overviewData]);

  const tableConfig = PromoTable({
    rows,
    visibleStores: visibleOverviewStores,
    promoColumns,
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
    onPageChange: setPage,
    onPageSizeChange: (value) => {
      setPage(1);
      setPageSize(value);
    },
  });

  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const tableHeaderControls = (
    <div className={commonStyles.tableControlRow}>
      <div className={commonStyles.tableSearchWrap}>
        <input
          id="promos-table-search"
          className={`input input-size-xl ${commonStyles.select}`}
          value={searchDraft}
          onChange={(e) => setSearchDraft(e.target.value)}
          placeholder="Поиск по SKU или наименованию"
        />
      </div>
    </div>
  );

  return (
    <PricingCatalogFrame
      title="Промо"
      subtitle="Промо-сценарии по товарам, участию в акциях и экономике по каждой акции Яндекс.Маркета."
      tabs={(
        <WorkspaceTabs
          items={[
            { id: "all", label: "Все товары" },
            ...visiblePageStores.map((store) => ({
              id: tabKeyForStore(store),
              label: `${store.label} (${store.store_id})`,
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
      treeSelector={
        <CatalogTreeControls
          selectId="promos-tree-source-store"
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
