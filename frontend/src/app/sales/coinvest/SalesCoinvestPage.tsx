import { useEffect, useMemo, useState } from "react";
import CatalogTreeControls from "../../pricing/_components/CatalogTreeControls";
import PricingCatalogFrame from "../../pricing/_components/PricingCatalogFrame";
import commonStyles from "../../pricing/_components/PricingPageCommon.module.css";
import { MatrixNameCell, pricingMatrixStyles as matrixStyles } from "../../pricing/_components/PricingMatrixKit";
import { filterWorkingMarketplaceStores, parseStoreTabKey, tabKeyForStore } from "../../pricing/_shared/catalogPageShared";
import { readGlobalStockFilter, writeGlobalStockFilter, type StockFilterValue } from "../../pricing/_shared/stockFilterState";
import { usePricingCatalogController } from "../../pricing/_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../../pricing/_shared/usePricingOverviewData";
import styles from "../elasticity/SalesElasticityPage.module.css";
import { apiPostOk } from "../../../lib/api";
import { WorkspaceTabs } from "../../../components/page/WorkspaceKit";

type OverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  installed_price_by_store?: Record<string, number | null>;
  promo_extra_discount_percent_by_store?: Record<string, number | null>;
  effective_coinvest_percent_by_store?: Record<string, number | null>;
  effective_on_display_price_by_store?: Record<string, number | null>;
  total_coinvest_percent_by_store?: Record<string, number | null>;
  total_on_display_price_by_store?: Record<string, number | null>;
  stock_by_store?: Record<string, number | null>;
  by_store?: Array<{
    store_uid: string;
    store_id: string;
    label: string;
    platform_label?: string;
    mentions_count?: number;
    avg_sale_price?: number | null;
    avg_payment_price?: number | null;
    avg_coinvest_percent?: number | null;
  }>;
};

type OverviewSummary = {
  catalog_items_count?: number;
  report_items_count?: number;
  report_coverage_percent?: number | null;
  avg_installed_price?: number | null;
  avg_on_display_price?: number | null;
  avg_coinvest_percent?: number | null;
};

type CoinvestContext = {
  ok: boolean;
  marketplace_stores?: Array<{
    store_uid: string;
    store_id: string;
    platform: string;
    platform_label: string;
    label: string;
    currency_code?: string;
  }>;
  promo_adjustments_by_store?: Record<
    string,
    Array<{
      promo_id: string;
      promo_name: string;
      max_discount_percent?: number | null;
    }>
  >;
  sync_state?: {
    manual_updated_at?: string;
  };
};

const COINVEST_CTX_CACHE_KEY = "sales_coinvest_ctx_v2";
const COINVEST_TREE_SOURCE_STORE_KEY = "sales_coinvest_tree_source_store_id_v2";
const COINVEST_OVERVIEW_CACHE_PREFIX = "sales_coinvest_overview_v2:";

function clearCoinvestPageCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(COINVEST_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (!key) continue;
      if (key.startsWith(COINVEST_OVERVIEW_CACHE_PREFIX)) keysToRemove.push(key);
    }
    keysToRemove.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}

function formatInt(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return String(Math.round(Number(value)));
}

function formatMoney(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value)).toLocaleString("ru-RU")} ₽`;
}

function formatPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  const num = Math.round(Number(value) * 100) / 100;
  return `${num > 0 ? "+" : ""}${num}%`;
}

function compactStoreLabel(value: string) {
  return value.replace(/^Я\.Маркет\s*/i, "").trim();
}

export default function SalesCoinvestPage() {
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
    reloadNonce,
    setReloadNonce,
    toggleTree,
    toggleExpand,
    toggleExpandAll,
  } = usePricingCatalogController({
    contextEndpoint: "/api/sales/coinvest/context",
    contextCacheKey: COINVEST_CTX_CACHE_KEY,
    treeSourceStoreKey: COINVEST_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 200,
  });
  const visiblePageStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);

  const ctx = context as CoinvestContext | null;
  const [stockFilter, setStockFilter] = useState<StockFilterValue>("all");
  const [stockFilterReady, setStockFilterReady] = useState(false);
  const [promoDraftsByStore, setPromoDraftsByStore] = useState<Record<string, Record<string, string>>>({});
  const [promoSaving, setPromoSaving] = useState(false);

  useEffect(() => {
    setStockFilter(readGlobalStockFilter());
    setStockFilterReady(true);
  }, []);

  useEffect(() => {
    if (!stockFilterReady) return;
    writeGlobalStockFilter(stockFilter);
  }, [stockFilter, stockFilterReady]);

  useEffect(() => {
    const source = ctx?.promo_adjustments_by_store;
    if (!source || typeof source !== "object") return;
    const next: Record<string, Record<string, string>> = {};
    Object.entries(source).forEach(([storeUid, items]) => {
      const local: Record<string, string> = {};
      (Array.isArray(items) ? items : []).forEach((item) => {
        const promoId = String(item?.promo_id || "").trim();
        if (!promoId) return;
        local[promoId] = item?.max_discount_percent == null ? "" : String(item.max_discount_percent);
      });
      next[storeUid] = local;
    });
    setPromoDraftsByStore(next);
  }, [ctx]);

  const { rows, totalCount, tableLoading, overviewData } = usePricingOverviewData<OverviewRow>({
    enabled: Boolean(context && stockFilterReady),
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/sales/coinvest/tree",
    overviewEndpoint: "/api/sales/coinvest/overview",
    refreshEndpoint: "/api/sales/coinvest/refresh",
    overviewCachePrefix: COINVEST_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearCoinvestPageCache,
    extraParams: {
      stock_filter: stockFilter,
      period: "month",
    },
  });
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
    if (tab === "all") return "Сводный список товаров";
    const parsed = parseStoreTabKey(tab);
    if (!parsed) return "";
    const found = visiblePageStores.find((s) => s.platform === parsed.platform && s.store_id === parsed.store_id);
    return found?.label || tab;
  }, [tab, visiblePageStores]);
  const activeStoreUid = useMemo(() => {
    const parsed = parseStoreTabKey(tab);
    return parsed ? `${parsed.platform}:${parsed.store_id}` : "";
  }, [tab]);
  const activePromoAdjustments = useMemo(
    () => (activeStoreUid ? ctx?.promo_adjustments_by_store?.[activeStoreUid] || [] : []),
    [activeStoreUid, ctx],
  );

  const summary = (overviewData as { summary?: OverviewSummary } | null)?.summary;
  const summaryCards = [
    { label: "Товаров в каталоге", value: formatInt(summary?.catalog_items_count ?? totalCount) },
    { label: "С витринной ценой", value: formatInt(summary?.report_items_count) },
    { label: "Покрытие выборки", value: formatPercent(summary?.report_coverage_percent) },
    { label: "Средняя установленная цена", value: formatMoney(summary?.avg_installed_price) },
    { label: "Средняя итоговая цена на витрине", value: formatMoney(summary?.avg_on_display_price) },
    { label: "Средний итоговый соинвест", value: formatPercent(summary?.avg_coinvest_percent) },
  ];

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";

  const summaryPanel = (
    <div className={styles.summaryStack}>
      {tab !== "all" && activeStoreUid ? (
        <div className={styles.summaryCard}>
          <div className={styles.summaryLabel}>Дополнительная скидка от акций для {activeStoreLabel}</div>
          {activePromoAdjustments.length === 0 ? (
            <div className={styles.summaryValue}>Нет активных акций</div>
          ) : (
            <div className={styles.storeMetricList}>
              {activePromoAdjustments.map((item) => {
                const promoId = String(item?.promo_id || "").trim();
                return (
                  <div key={promoId} className={styles.storeMetricRow}>
                    <span className={styles.storeMetricName}>{item?.promo_name || promoId}</span>
                    <input
                      className={`input input-size-sm ${styles.dateInput}`}
                      value={promoDraftsByStore[activeStoreUid]?.[promoId] ?? ""}
                      onChange={(e) =>
                        setPromoDraftsByStore((prev) => ({
                          ...prev,
                          [activeStoreUid]: {
                            ...(prev[activeStoreUid] || {}),
                            [promoId]: e.target.value,
                          },
                        }))
                      }
                      placeholder="%"
                    />
                  </div>
                );
              })}
              <div className={styles.storeMetricRow}>
                <button
                  type="button"
                  className="btn"
                  disabled={promoSaving}
                      onClick={async () => {
                        try {
                          setPromoSaving(true);
                          const rowsPayload = activePromoAdjustments.map((item) => ({
                            promo_id: item.promo_id,
                            promo_name: item.promo_name,
                            max_discount_percent: promoDraftsByStore[activeStoreUid]?.[item.promo_id] ?? "",
                          }));
                          await apiPostOk<{ ok: boolean; message?: string }>(
                            "/api/sales/coinvest/promo-adjustments",
                            { store_uid: activeStoreUid, rows: rowsPayload },
                          );
                          clearCoinvestPageCache();
                          setReloadNonce((prev) => prev + 1);
                        } catch (err) {
                      setError(err instanceof Error ? err.message : "Не удалось сохранить скидки акций");
                    } finally {
                      setPromoSaving(false);
                    }
                  }}
                >
                  {promoSaving ? "Сохранение..." : "Сохранить скидки акций"}
                </button>
              </div>
            </div>
          )}
        </div>
      ) : null}
      <div className={styles.summaryGrid}>
        {summaryCards.map((card) => (
          <div key={card.label} className={styles.summaryCard}>
            <div className={styles.summaryLabel}>{card.label}</div>
            <div className={styles.summaryValue}>{card.value}</div>
          </div>
        ))}
      </div>
    </div>
  );

  const renderStoreMapCell = (
    row: OverviewRow,
    source: Record<string, number | null | undefined> | undefined,
    formatter: (value: number | null | undefined) => string,
  ) => {
    const items = (row.by_store || []).filter((item) => String(item.store_uid || "").startsWith("yandex_market:"));
    const parsed = parseStoreTabKey(tab);
    const activeStoreUid = parsed ? `${parsed.platform}:${parsed.store_id}` : "";
    if (tab !== "all") {
      return <span>{formatter(source?.[activeStoreUid])}</span>;
    }
    return (
      <div className={styles.storeMetricList}>
        {items.map((item) => (
          <div key={item.store_uid} className={styles.storeMetricRow}>
            <span className={styles.storeMetricName}>{compactStoreLabel(item.label)}</span>
            <span className={styles.storeMetricValue}>{formatter(source?.[item.store_uid])}</span>
          </div>
        ))}
      </div>
    );
  };

  const tableHeaderControls = (
    <div className={commonStyles.tableControlRow}>
      <div className={commonStyles.tableFilterWrap}>
        <div className={styles.summaryLabel}>Текущая витрина по каталогу. Продажи используются только как fallback, если отчёт Маркета ещё не приехал.</div>
      </div>
      <div className={commonStyles.tableSearchWrap}>
        <input
          id="coinvest-table-search"
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
      title="Соинвест"
      subtitle="Актуальная сводка по витрине: установленная цена, соинвест и цена показа товара."
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
      summaryPanel={summaryPanel}
      searchValue={searchDraft}
      onSearchChange={setSearchDraft}
      searchPlaceholder="Поиск по SKU или наименованию"
      hideSearchPanel
      error={error}
      treeSelector={
        tab === "all" ? (
          <CatalogTreeControls
            selectId="coinvest-tree-source-store"
            stores={visiblePageStores}
            treeSourceStoreId={treeSourceStoreId}
            onTreeSourceStoreChange={setTreeSourceStoreId}
            stockFilter={stockFilter}
            onStockFilterChange={(value) => {
              setPage(1);
              setStockFilter(value);
            }}
          />
        ) : undefined
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
      tableMeta={<span>Всего: {totalCount}</span>}
      table={(
        <table className={matrixStyles.matrixTable}>
          <thead>
            <tr>
              <th>SKU</th>
              <th className={matrixStyles.nameHeader}>Наименование товара</th>
              <th className={styles.metricCol}>Установленная цена</th>
              <th className={styles.metricCol}>Соинвест из отчета</th>
              <th className={styles.metricCol}>Доп. скидка от акций</th>
              <th className={styles.metricCol}>Итоговый соинвест</th>
              <th className={styles.metricCol}>Итоговая цена на витрине</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={7} className={matrixStyles.emptyCell}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</td>
              </tr>
            ) : rows.map((row) => (
              <tr key={row.sku}>
                <td className={matrixStyles.skuCell}>{row.sku}</td>
                <MatrixNameCell name={row.name} path={row.tree_path} />
                <td className={styles.metricCell}>{renderStoreMapCell(row, row.installed_price_by_store, formatMoney)}</td>
                <td className={styles.metricCell}>{renderStoreMapCell(row, row.effective_coinvest_percent_by_store, formatPercent)}</td>
                <td className={styles.metricCell}>{renderStoreMapCell(row, row.promo_extra_discount_percent_by_store, formatPercent)}</td>
                <td className={styles.metricCell}>{renderStoreMapCell(row, row.total_coinvest_percent_by_store, formatPercent)}</td>
                <td className={styles.metricCell}>{renderStoreMapCell(row, row.total_on_display_price_by_store, formatMoney)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      page={page}
      totalPages={totalPages}
      onPageChange={setPage}
      onPrevPage={() => setPage((prev) => Math.max(1, prev - 1))}
      onNextPage={() => setPage((prev) => Math.min(totalPages, prev + 1))}
      canPrev={page > 1}
      canNext={page < totalPages}
      pageSize={pageSize}
      onPageSizeChange={(value) => {
        setPage(1);
        setPageSize(value);
      }}
    />
  );
}
