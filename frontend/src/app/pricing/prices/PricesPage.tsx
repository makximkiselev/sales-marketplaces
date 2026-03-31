import { useEffect, useMemo, useState } from "react";
import CatalogTreeControls from "../_components/CatalogTreeControls";
import PricingCatalogFrame from "../_components/PricingCatalogFrame";
import { MatrixMultiValue, MatrixNameCell, buildStoreLines, pricingMatrixStyles as matrixStyles } from "../_components/PricingMatrixKit";
import commonStyles from "../_components/PricingPageCommon.module.css";
import { currencySymbol, filterWorkingMarketplaceStores, formatMoney, parseStoreTabKey, tabKeyForStore } from "../_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter } from "../_shared/stockFilterState";
import { usePricingCatalogController } from "../_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../_shared/usePricingOverviewData";
import { WorkspaceTabs } from "../../../components/page/WorkspaceKit";

type PriceMetric = {
  mrc_with_boost_price?: number | null;
  mrc_with_boost_profit_abs?: number | null;
  mrc_with_boost_profit_pct?: number | null;
  rrc_no_ads_price?: number | null;
  rrc_no_ads_profit_abs?: number | null;
  rrc_no_ads_profit_pct?: number | null;
  market_price?: number | null;
  market_profit_abs?: number | null;
  market_profit_pct?: number | null;
  mrc_price?: number | null;
  mrc_profit_abs?: number | null;
  mrc_profit_pct?: number | null;
  target_price?: number | null;
  target_profit_abs?: number | null;
  target_profit_pct?: number | null;
};

type OverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  cogs_price_by_store?: Record<string, number | null>;
  stock_by_store?: Record<string, number | null>;
  market_price_by_store?: Record<string, number | null>;
  rrc_no_ads_price_by_store?: Record<string, number | null>;
  mrc_price_by_store?: Record<string, number | null>;
  mrc_with_boost_price_by_store?: Record<string, number | null>;
  target_price_by_store?: Record<string, number | null>;
  installed_price_by_store?: Record<string, number | null>;
  installed_profit_abs_by_store?: Record<string, number | null>;
  installed_profit_pct_by_store?: Record<string, number | null>;
  price_metrics_by_store?: Record<string, PriceMetric>;
  updated_at: string;
};

const PRICES_CTX_CACHE_KEY = "pricing_prices_ctx_v7";
const PRICES_OVERVIEW_CACHE_PREFIX = "pricing_prices_overview_v7:";
const CATALOG_CTX_CACHE_KEY = "catalog_page_ctx_v1";
const CATALOG_DATA_CACHE_PREFIX = "catalog_page_data_v1:";
const PRICES_TREE_SOURCE_STORE_KEY = "pricing_prices_tree_source_store_id_v3";
function clearPricesPageCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(PRICES_CTX_CACHE_KEY);
    window.localStorage.removeItem(CATALOG_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const k = window.localStorage.key(i);
      if (!k) continue;
      if (k.startsWith(PRICES_OVERVIEW_CACHE_PREFIX) || k.startsWith(CATALOG_DATA_CACHE_PREFIX)) keysToRemove.push(k);
    }
    keysToRemove.forEach((k) => window.localStorage.removeItem(k));
  } catch {
    // noop
  }
}

function formatPercentValue(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value) * 100) / 100}%`;
}

export default function PricesPage() {
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
    contextEndpoint: "/api/pricing/prices/context",
    contextCacheKey: PRICES_CTX_CACHE_KEY,
    treeSourceStoreKey: PRICES_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 50,
  });
  const [stockFilter, setStockFilter] = useState<"all" | "in_stock" | "out_of_stock">("all");
  const [stockFilterReady, setStockFilterReady] = useState(false);

  const { rows, visibleStores, totalCount, tableLoading } = usePricingOverviewData<OverviewRow>({
    enabled: Boolean(context),
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/pricing/prices/tree",
    overviewEndpoint: "/api/pricing/prices/overview",
    refreshEndpoint: "/api/pricing/prices/refresh",
    overviewCachePrefix: PRICES_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearPricesPageCache,
    extraParams: { stock_filter: stockFilter },
  });

  const visiblePageStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);
  const visibleOverviewStores = useMemo(() => filterWorkingMarketplaceStores(visibleStores), [visibleStores]);

  useEffect(() => {
    setStockFilter(readGlobalStockFilter());
    setStockFilterReady(true);
  }, []);

  useEffect(() => {
    if (!stockFilterReady) return;
    writeGlobalStockFilter(stockFilter);
  }, [stockFilter, stockFilterReady]);

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

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const cogsHeader = () => (tab === "all" ? "Себестоимость, ₽ / $" : `Себестоимость, ${currencySymbol(activeStoreCurrency)}`);
  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const dataCellAlignClass = tab === "all" ? matrixStyles.leftCell : matrixStyles.centerCell;
  const renderStoreMoneyMapCell = (row: OverviewRow, source: Record<string, number | null> | undefined, keySuffix: string) => {
    if (tab !== "all") {
      const storeUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
      const value = storeUid ? source?.[storeUid] : null;
      const shown = formatMoney(value);
      return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(activeStoreCurrency)}`}</span>;
    }
    return (
      <MatrixMultiValue
        rows={buildStoreLines(visibleOverviewStores, (store) => {
          const value = source?.[store.store_uid];
          const shown = formatMoney(value);
          const sym = currencySymbol(store.currency_code);
          return shown === "—" ? "—" : `${shown}${sym}`;
        }, `${row.sku}-${keySuffix}`)}
      />
    );
  };

  const renderStorePercentMapCell = (row: OverviewRow, source: Record<string, number | null> | undefined, keySuffix: string) => {
    if (tab !== "all") {
      const storeUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
      return <span>{storeUid ? formatPercentValue(source?.[storeUid]) : "—"}</span>;
    }
    return (
      <MatrixMultiValue
        rows={buildStoreLines(visibleOverviewStores, (store) => formatPercentValue(source?.[store.store_uid]), `${row.sku}-${keySuffix}`)}
      />
    );
  };

  const renderMetricMoneyCell = (row: OverviewRow, pick: (metric: PriceMetric) => number | null | undefined, keySuffix: string) => {
    if (tab !== "all") {
      const storeUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
      const metric = storeUid ? row.price_metrics_by_store?.[storeUid] : undefined;
      const shown = formatMoney(metric ? pick(metric) : null);
      return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(activeStoreCurrency)}`}</span>;
    }
    return (
      <MatrixMultiValue
        rows={buildStoreLines(visibleOverviewStores, (store) => {
          const metric = row.price_metrics_by_store?.[store.store_uid];
          const shown = formatMoney(metric ? pick(metric) : null);
          const sym = currencySymbol(store.currency_code);
          return shown === "—" ? "—" : `${shown}${sym}`;
        }, `${row.sku}-${keySuffix}`)}
      />
    );
  };

  const renderMetricPercentCell = (row: OverviewRow, pick: (metric: PriceMetric) => number | null | undefined, keySuffix: string) => {
    if (tab !== "all") {
      const storeUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
      const metric = storeUid ? row.price_metrics_by_store?.[storeUid] : undefined;
      return <span>{formatPercentValue(metric ? pick(metric) : null)}</span>;
    }
    return (
      <MatrixMultiValue
        rows={buildStoreLines(visibleOverviewStores, (store) => {
          const metric = row.price_metrics_by_store?.[store.store_uid];
          return formatPercentValue(metric ? pick(metric) : null);
        }, `${row.sku}-${keySuffix}`)}
      />
    );
  };

  const renderStockCell = (row: OverviewRow) => {
    const formatStock = (value: number | null | undefined) => {
      if (value == null || Number.isNaN(Number(value))) return "—";
      const num = Number(value);
      return Number.isInteger(num) ? String(num) : String(Math.round(num * 100) / 100);
    };
    if (tab !== "all") {
      const storeUid = activeStoreRef ? `${activeStoreRef.platform}:${activeStoreRef.store_id}` : "";
      return <span>{storeUid ? formatStock(row.stock_by_store?.[storeUid]) : "—"}</span>;
    }
    return (
      <MatrixMultiValue
        rows={buildStoreLines(visibleOverviewStores, (store) => formatStock(row.stock_by_store?.[store.store_uid]), `${row.sku}-stock`)}
      />
    );
  };

  const renderCogsCell = (row: OverviewRow) => renderStoreMoneyMapCell(row, row.cogs_price_by_store, "cogs");
  const renderMrcCell = (row: OverviewRow) => renderStoreMoneyMapCell(row, row.mrc_price_by_store, "mrc");
  const renderMrcWithBoostCell = (row: OverviewRow) => renderStoreMoneyMapCell(row, row.mrc_with_boost_price_by_store, "mrc-with-boost");
  const renderRrcCell = (row: OverviewRow) => renderStoreMoneyMapCell(row, row.target_price_by_store, "rrc");
  const renderInstalledPriceCell = (row: OverviewRow) => renderStoreMoneyMapCell(row, row.installed_price_by_store, "installed-price");
  const renderInstalledProfitCell = (row: OverviewRow) => renderStoreMoneyMapCell(row, row.installed_profit_abs_by_store, "installed-profit-abs");
  const renderInstalledProfitPctCell = (row: OverviewRow) => renderStorePercentMapCell(row, row.installed_profit_pct_by_store, "installed-profit-pct");

  const tableHeaderControls = (
    <div className={commonStyles.tableSearchWrap}>
      <input
        id="prices-table-search"
        className={`input input-size-xl ${commonStyles.select}`}
        value={searchDraft}
        onChange={(e) => setSearchDraft(e.target.value)}
        placeholder="Поиск по SKU или наименованию"
      />
    </div>
  );

  return (
    <PricingCatalogFrame
      title="Цены"
      subtitle="Базовый слой расчёта цены: РРЦ, РРЦ без рекламы, МРЦ и финальная установленная цена из стратегии."
      tabs={(
        <WorkspaceTabs
          items={[
            { id: "all", label: "Все товары" },
            ...visiblePageStores.map((store) => ({
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
      treeSelector={
        <CatalogTreeControls
          selectId="prices-tree-source-store"
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
      tableMeta={`Всего: ${totalCount}`}
      table={(
        <table className={matrixStyles.matrixTable}>
          <colgroup>
            <col style={{ width: 128 }} />
            <col style={{ width: 360 }} />
            <col style={{ width: 108 }} />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
            <col />
          </colgroup>
          <thead>
            <tr>
              <th>SKU</th>
              <th className={matrixStyles.nameHeader}>Наименование товара</th>
              <th className={matrixStyles.stockHeader}>Остаток</th>
              <th>{cogsHeader()}</th>
              <th>МРЦ</th>
              <th>{tab === "all" ? "Заработок МРЦ, ₽ / $" : `Заработок МРЦ, ${moneySign}`}</th>
              <th>Заработок МРЦ, %</th>
              <th>МРЦ + буст</th>
              <th>{tab === "all" ? "Заработок МРЦ + буст, ₽ / $" : `Заработок МРЦ + буст, ${moneySign}`}</th>
              <th>Заработок МРЦ + буст, %</th>
              <th>РРЦ cap</th>
              <th>{tab === "all" ? "Заработок РРЦ, ₽ / $" : `Заработок РРЦ, ${moneySign}`}</th>
              <th>Заработок РРЦ, %</th>
              <th>Финальная цена</th>
              <th>{tab === "all" ? "Заработок, ₽ / $" : `Заработок, ${moneySign}`}</th>
              <th>Заработок, %</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={16} className={matrixStyles.emptyCell}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</td>
              </tr>
            ) : rows.map((row) => (
              <tr key={`prices-${row.sku}`}>
                <td className={matrixStyles.skuCell}>{row.sku}</td>
                <MatrixNameCell name={row.name} path={row.tree_path} />
                <td className={`${matrixStyles.stockCell} ${dataCellAlignClass}`}>{renderStockCell(row)}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderCogsCell(row)}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderMrcCell(row)}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderMetricMoneyCell(row, (metric) => metric.mrc_profit_abs, "mrc-profit-abs")}</td>
                <td className={matrixStyles.centerCell}>{renderMetricPercentCell(row, (metric) => metric.mrc_profit_pct, "mrc-profit-pct")}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderMrcWithBoostCell(row)}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderMetricMoneyCell(row, (metric) => metric.mrc_with_boost_profit_abs, "mrc-with-boost-profit-abs")}</td>
                <td className={matrixStyles.centerCell}>{renderMetricPercentCell(row, (metric) => metric.mrc_with_boost_profit_pct, "mrc-with-boost-profit-pct")}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderRrcCell(row)}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderMetricMoneyCell(row, (metric) => metric.target_profit_abs, "rrc-profit-abs")}</td>
                <td className={matrixStyles.centerCell}>{renderMetricPercentCell(row, (metric) => metric.target_profit_pct, "rrc-profit-pct")}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderInstalledPriceCell(row)}</td>
                <td className={`${matrixStyles.moneyCell} ${dataCellAlignClass}`}>{renderInstalledProfitCell(row)}</td>
                <td className={matrixStyles.centerCell}>{renderInstalledProfitPctCell(row)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      page={page}
      totalPages={totalPages}
      onPageChange={setPage}
      onPrevPage={() => setPage((value) => Math.max(1, value - 1))}
      onNextPage={() => setPage((value) => Math.min(totalPages, value + 1))}
      canPrev={page > 1}
      canNext={page < totalPages}
      pageSize={pageSize}
      onPageSizeChange={(value) => {
        setPage(1);
        setPageSize(value < 0 ? -1 : value);
      }}
      pageSizeOptions={[25, 50, 100, 200, -1]}
    />
  );
}
