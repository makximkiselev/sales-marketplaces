"use client";

import { useEffect, useMemo, useState } from "react";
import CatalogTreeControls from "../../pricing/_components/CatalogTreeControls";
import PricingCatalogFrame from "../../pricing/_components/PricingCatalogFrame";
import commonStyles from "../../pricing/_components/PricingPageCommon.module.css";
import { MatrixNameCell, pricingMatrixStyles as matrixStyles } from "../../pricing/_components/PricingMatrixKit";
import { filterWorkingMarketplaceStores, parseStoreTabKey, tabKeyForStore } from "../../pricing/_shared/catalogPageShared";
import { usePricingCatalogController } from "../../pricing/_shared/usePricingCatalogController";
import { usePricingOverviewData } from "../../pricing/_shared/usePricingOverviewData";
import { readGlobalStockFilter, StockFilterValue, writeGlobalStockFilter } from "../../pricing/_shared/stockFilterState";
import styles from "./SalesElasticityPage.module.css";

type ElasticityPeriod = "today" | "yesterday" | "week" | "month" | "quarter" | "custom";

type OverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  stock_by_store?: Record<string, number | null>;
  mentions_count?: number;
  turnover?: number | null;
  avg_sale_price?: number | null;
  avg_payment_price?: number | null;
  avg_coinvest_percent?: number | null;
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
  count_delta_percent?: number | null;
  turnover_delta_percent?: number | null;
  elasticity?: number | null;
};

type OverviewSummary = {
  turnover?: number | null;
  mentions_count?: number;
  avg_sale_price?: number | null;
  avg_payment_price?: number | null;
  avg_coinvest_percent?: number | null;
  count_delta_percent?: number | null;
  turnover_delta_percent?: number | null;
};

type ElasticityOverviewData = {
  summary?: OverviewSummary;
  comparison_enabled?: boolean;
  series?: Array<{
    date: string;
    mentions_count?: number;
    turnover?: number | null;
    avg_sale_price?: number | null;
    avg_payment_price?: number | null;
    avg_coinvest_percent?: number | null;
  }>;
};

type ElasticityContext = {
  ok: boolean;
  marketplace_stores?: Array<{
    store_uid: string;
    store_id: string;
    platform: string;
    platform_label: string;
    label: string;
    currency_code?: string;
  }>;
  sync_state?: {
    done?: boolean;
    full_updated_at?: string;
    full_date_from?: string;
    full_date_to?: string;
    recent_updated_at?: string;
    daily_updated_at?: string;
    manual_updated_at?: string;
    manual_mode?: string;
    last_updated_at?: string;
  };
};

const ELASTICITY_CTX_CACHE_KEY = "sales_elasticity_ctx_v2";
const ELASTICITY_TREE_SOURCE_STORE_KEY = "sales_elasticity_tree_source_store_id_v2";
const ELASTICITY_OVERVIEW_CACHE_PREFIX = "sales_elasticity_overview_v2:";

function clearElasticityPageCache() {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.removeItem(ELASTICITY_CTX_CACHE_KEY);
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (!key) continue;
      if (key.startsWith(ELASTICITY_OVERVIEW_CACHE_PREFIX)) keysToRemove.push(key);
    }
    keysToRemove.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}

function renderPlaceholder() {
  return <span className={styles.placeholder}>—</span>;
}

function formatInt(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return String(Math.round(Number(value)));
}

function formatStock(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  const num = Number(value);
  return Number.isInteger(num) ? String(num) : String(Math.round(num * 100) / 100);
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

function formatElasticity(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return (Math.round(Number(value) * 100) / 100).toLocaleString("ru-RU");
}

function formatShortDate(value: string) {
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
}

function compactStoreLabel(value: string) {
  return value.replace(/^Я\.Маркет\s*/i, "").trim();
}

function paddedRange(min: number, max: number, ratio = 0.12) {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 1 };
  if (max === min) {
    const delta = Math.max(Math.abs(max) * ratio, 1);
    return { min: min - delta, max: max + delta };
  }
  const span = max - min;
  const pad = span * ratio;
  return { min: min - pad, max: max + pad };
}

function elasticityLevel(value: number | null | undefined): { label: string; tone: "weak" | "moderate" | "strong" | "extreme" } | null {
  if (value == null || Number.isNaN(Number(value))) return null;
  const abs = Math.abs(Number(value));
  if (abs <= 1) return { label: "Слабая", tone: "weak" };
  if (abs <= 5) return { label: "Умеренная", tone: "moderate" };
  if (abs <= 10) return { label: "Сильная", tone: "strong" };
  return { label: "Экстремальная", tone: "extreme" };
}

export default function SalesElasticityPage() {
  const [sortKey, setSortKey] = useState<"mentions_count" | "turnover">("turnover");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
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
    contextEndpoint: "/api/sales/elasticity/context",
    contextCacheKey: ELASTICITY_CTX_CACHE_KEY,
    treeSourceStoreKey: ELASTICITY_TREE_SOURCE_STORE_KEY,
    defaultPageSize: 200,
  });
  const visiblePageStores = useMemo(() => filterWorkingMarketplaceStores(stores), [stores]);

  const [period, setPeriod] = useState<ElasticityPeriod>("yesterday");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [stockFilter, setStockFilter] = useState<StockFilterValue>(() => readGlobalStockFilter());
  const customPeriodReady = period !== "custom" || Boolean(dateFrom && dateTo);
  const ctx = context as ElasticityContext | null;

  const { rows, totalCount, tableLoading, overviewData } = usePricingOverviewData<OverviewRow>({
    enabled: Boolean(context && customPeriodReady),
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint: "/api/sales/elasticity/tree",
    overviewEndpoint: "/api/sales/elasticity/overview",
    refreshEndpoint: "/api/sales/elasticity/refresh",
    overviewCachePrefix: ELASTICITY_OVERVIEW_CACHE_PREFIX,
    setError,
    setTreeRoots,
    clearPageCache: clearElasticityPageCache,
    extraParams: {
      period,
      stock_filter: stockFilter,
      ...(period === "custom" && dateFrom && dateTo ? { date_from: dateFrom, date_to: dateTo } : {}),
    },
  });
  useEffect(() => {
    writeGlobalStockFilter(stockFilter);
  }, [stockFilter]);
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

  const summary = (overviewData as (typeof overviewData & ElasticityOverviewData) | null)?.summary;
  const comparisonEnabled = Boolean((overviewData as (typeof overviewData & ElasticityOverviewData) | null)?.comparison_enabled);
  const series = (overviewData as (typeof overviewData & ElasticityOverviewData) | null)?.series || [];
  const summaryCards = [
    { label: "Оборот", value: formatMoney(summary?.turnover) },
    { label: "Кол-во заказов", value: formatInt(summary?.mentions_count) },
    { label: "Средняя цена продажи", value: formatMoney(summary?.avg_sale_price) },
    { label: "Цена с соинвестом", value: formatMoney(summary?.avg_payment_price) },
    { label: "Средний соинвест", value: formatPercent(summary?.avg_coinvest_percent) },
    {
      label: "Динамика",
      value: comparisonEnabled ? null : "—",
      detailTop: `Оборот: ${formatPercent(summary?.turnover_delta_percent)}`,
      detailBottom: `Кол-во: ${formatPercent(summary?.count_delta_percent)}`,
    },
  ];

  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));
  const sortedRows = useMemo(() => {
    const copy = [...rows];
    copy.sort((a, b) => {
      const aVal = Number(a?.[sortKey] || 0);
      const bVal = Number(b?.[sortKey] || 0);
      if (aVal === bVal) return String(a.sku || "").localeCompare(String(b.sku || ""), "ru");
      return sortDir === "asc" ? aVal - bVal : bVal - aVal;
    });
    return copy;
  }, [rows, sortDir, sortKey]);
  const treeLoadingText = flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : "";
  const showGraph = ["week", "month", "quarter", "custom"].includes(period) && series.length > 1;
  const saleSeries = series.map((point) => Number(point.avg_sale_price || 0));
  const coinvestSeries = series.map((point) => Number(point.avg_coinvest_percent || 0));
  const turnoverSeries = series.map((point) => Number(point.turnover || 0));
  const saleRange = showGraph ? paddedRange(Math.min(...saleSeries), Math.max(...saleSeries), 0.16) : { min: 0, max: 1 };
  const coinvestRange = showGraph ? paddedRange(Math.min(...coinvestSeries), Math.max(...coinvestSeries), 0.18) : { min: 0, max: 1 };
  const saleMin = saleRange.min;
  const saleMax = saleRange.max;
  const coinvestMin = coinvestRange.min;
  const coinvestMax = coinvestRange.max;
  const turnoverMax = showGraph ? Math.max(...turnoverSeries, 0) : 0;
  const chartWidth = 760;
  const chartHeight = 252;
  const chartPad = { top: 22, right: 64, bottom: 38, left: 64 };
  const chartInnerWidth = chartWidth - chartPad.left - chartPad.right;
  const chartInnerHeight = chartHeight - chartPad.top - chartPad.bottom;
  const saleY = (value: number) => {
    if (saleMax === saleMin) return chartPad.top + chartInnerHeight / 2;
    const ratio = (value - saleMin) / (saleMax - saleMin);
    return chartPad.top + chartInnerHeight - ratio * chartInnerHeight;
  };
  const coinvestY = (value: number) => {
    if (coinvestMax === coinvestMin) return chartPad.top + chartInnerHeight / 2;
    const ratio = (value - coinvestMin) / (coinvestMax - coinvestMin);
    return chartPad.top + chartInnerHeight - ratio * chartInnerHeight;
  };
  const pointX = (index: number) => {
    if (series.length <= 1) return chartPad.left;
    return chartPad.left + (index / (series.length - 1)) * chartInnerWidth;
  };
  const barStep = series.length > 0 ? chartInnerWidth / series.length : chartInnerWidth;
  const barWidth = Math.max(8, Math.min(20, barStep * 0.54));
  const turnoverY = (value: number) => {
    if (turnoverMax <= 0) return chartPad.top + chartInnerHeight;
    const ratio = value / turnoverMax;
    return chartPad.top + chartInnerHeight - ratio * chartInnerHeight;
  };
  const salePath = showGraph
    ? series.map((point, index) => `${index === 0 ? "M" : "L"} ${pointX(index)} ${saleY(Number(point.avg_sale_price || 0))}`).join(" ")
    : "";
  const coinvestPath = showGraph
    ? series.map((point, index) => `${index === 0 ? "M" : "L"} ${pointX(index)} ${coinvestY(Number(point.avg_coinvest_percent || 0))}`).join(" ")
    : "";
  const xLabelStep = series.length <= 10 ? 1 : series.length <= 16 ? 2 : series.length <= 24 ? 3 : 4;
  const showPointMarkers = series.length <= 18;
  const gridValues = [0.25, 0.5, 0.75];

  const periodTabs = (
      <div className={styles.periodControls}>
      <div className={styles.periodTabs}>
        {[
          ["today", "Сегодня"],
          ["yesterday", "Вчера"],
          ["week", "За неделю"],
          ["month", "30 дней"],
          ["quarter", "Квартал"],
          ["custom", "Период"],
        ].map(([key, label]) => (
          <button
            key={key}
            className={`btn inline ${commonStyles.tabBtn} ${period === key ? commonStyles.tabBtnActive : ""}`}
            onClick={() => setPeriod(key as ElasticityPeriod)}
            type="button"
          >
            {label}
          </button>
        ))}
      </div>
      {period === "custom" ? (
        <div className={styles.dateRange}>
          <input className={`input ${styles.dateInput}`} type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          <span className={styles.dateDivider}>—</span>
          <input className={`input ${styles.dateInput}`} type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
        </div>
      ) : null}
      </div>
  );

  const summaryPanel = (
    <div className={styles.summaryStack}>
      <div className={styles.summaryGrid}>
        {summaryCards.map((card) => (
          <div key={card.label} className={styles.summaryCard}>
            <div className={styles.summaryLabel}>{card.label}</div>
            {card.value !== null ? <div className={styles.summaryValue}>{card.value}</div> : null}
            {card.value === null ? (
              <div className={styles.summaryDual}>
                <span>{card.detailTop}</span>
                <span>{card.detailBottom}</span>
              </div>
            ) : null}
          </div>
        ))}
      </div>
      {showGraph ? (
        <div className={styles.chartCard}>
          <div className={styles.chartHeader}>
            <div className={styles.chartTitle}>Динамика цены и соинвеста</div>
            <div className={styles.chartLegend}>
              <span className={styles.legendItem}><span className={`${styles.legendDot} ${styles.legendDotTurnover}`} />Оборот</span>
              <span className={styles.legendItem}><span className={`${styles.legendDot} ${styles.legendDotSale}`} />Средняя цена продажи</span>
              <span className={styles.legendItem}><span className={`${styles.legendDot} ${styles.legendDotCoinvest}`} />Соинвест</span>
            </div>
          </div>
          <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className={styles.chartSvg} role="img" aria-label="График оборота, цены продажи и соинвеста">
            {gridValues.map((value) => {
              const y = chartPad.top + chartInnerHeight - value * chartInnerHeight;
              return <line key={value} x1={chartPad.left} y1={y} x2={chartWidth - chartPad.right} y2={y} className={styles.chartGrid} />;
            })}
            <line x1={chartPad.left} y1={chartPad.top + chartInnerHeight} x2={chartWidth - chartPad.right} y2={chartPad.top + chartInnerHeight} className={styles.chartAxis} />
            <line x1={chartPad.left} y1={chartPad.top} x2={chartPad.left} y2={chartPad.top + chartInnerHeight} className={styles.chartAxis} />
            <line x1={chartWidth - chartPad.right} y1={chartPad.top} x2={chartWidth - chartPad.right} y2={chartPad.top + chartInnerHeight} className={styles.chartAxis} />
            {series.map((point, index) => {
              const x = pointX(index) - barWidth / 2;
              const y = turnoverY(Number(point.turnover || 0));
              const height = chartPad.top + chartInnerHeight - y;
              return (
                <rect
                  key={`${point.date}-bar`}
                  x={x}
                  y={y}
                  width={barWidth}
                  height={Math.max(2, height)}
                  rx="5"
                  className={styles.chartBar}
                />
              );
            })}
            <path d={salePath} className={styles.chartLineSale} />
            <path d={coinvestPath} className={styles.chartLineCoinvest} />
            {series.map((point, index) => (
              <g key={point.date}>
                {showPointMarkers ? <circle cx={pointX(index)} cy={saleY(Number(point.avg_sale_price || 0))} r="3.5" className={styles.chartPointSale} /> : null}
                {showPointMarkers ? <circle cx={pointX(index)} cy={coinvestY(Number(point.avg_coinvest_percent || 0))} r="3.5" className={styles.chartPointCoinvest} /> : null}
                {index % xLabelStep === 0 || index === series.length - 1 ? (
                  <text x={pointX(index)} y={chartHeight - 10} textAnchor="middle" className={styles.chartLabel}>{formatShortDate(point.date)}</text>
                ) : null}
              </g>
            ))}
            <text x={chartPad.left} y={12} className={styles.chartScaleLabel}>{formatMoney(saleMax)}</text>
            <text x={chartPad.left} y={chartPad.top + chartInnerHeight + 22} className={styles.chartScaleLabel}>{formatMoney(saleMin)}</text>
            <text x={chartWidth / 2} y={12} textAnchor="middle" className={styles.chartScaleLabel}>Оборот до {formatMoney(turnoverMax)}</text>
            <text x={chartWidth - chartPad.right} y={12} textAnchor="end" className={styles.chartScaleLabel}>{formatPercent(coinvestMax)}</text>
            <text x={chartWidth - chartPad.right} y={chartPad.top + chartInnerHeight + 22} textAnchor="end" className={styles.chartScaleLabel}>{formatPercent(coinvestMin)}</text>
          </svg>
        </div>
      ) : null}
    </div>
  );

  const renderStoreMetricCell = (
    row: OverviewRow,
    key: "avg_sale_price" | "avg_coinvest_percent" | "avg_payment_price",
    formatter: (value: number | null | undefined) => string,
  ) => {
    const items = (row.by_store || []).filter((item) => String(item.store_uid || "").startsWith("yandex_market:"));
    if (tab !== "all") {
      return <span>{formatter(row[key])}</span>;
    }
    return (
      <div className={styles.storeMetricList}>
        {items.map((item) => {
          const hasValue = item.mentions_count && item.mentions_count > 0;
          return (
            <div key={item.store_uid} className={styles.storeMetricRow}>
              <span className={styles.storeMetricName}>{compactStoreLabel(item.label)}</span>
              <span className={styles.storeMetricValue}>{hasValue ? formatter(item[key]) : "—"}</span>
            </div>
          );
        })}
      </div>
    );
  };
  const tableHeaderControls = (
    <div className={commonStyles.tableControlRow}>
      <div className={commonStyles.tableFilterWrap}>{periodTabs}</div>
      <div className={commonStyles.tableSearchWrap}>
        <input
          id="elasticity-table-search"
          className={`input ${commonStyles.select}`}
          value={searchDraft}
          onChange={(e) => setSearchDraft(e.target.value)}
          placeholder="Поиск по SKU или наименованию"
        />
      </div>
    </div>
  );

  const toggleSort = (key: "mentions_count" | "turnover") => {
    if (sortKey === key) {
      setSortDir((current) => (current === "desc" ? "asc" : "desc"));
      return;
    }
    setSortKey(key);
    setSortDir("desc");
  };

  const sortMarker = (key: "mentions_count" | "turnover") => {
    if (sortKey !== key) return "↕";
    return sortDir === "desc" ? "↓" : "↑";
  };

  const renderStockCell = (row: OverviewRow) => {
    const items = (row.by_store || []).filter((item) => String(item.store_uid || "").startsWith("yandex_market:"));
    if (tab !== "all") {
      const parsed = parseStoreTabKey(tab);
      const activeStore = visiblePageStores.find((store) => store.platform === parsed?.platform && store.store_id === parsed?.store_id);
      return <span>{formatStock(activeStore ? row.stock_by_store?.[activeStore.store_uid] : undefined)}</span>;
    }
    return (
      <div className={styles.storeMetricList}>
        {items.map((item) => (
          <div key={item.store_uid} className={styles.storeMetricRow}>
            <span className={styles.storeMetricName}>{compactStoreLabel(item.label)}</span>
            <span className={styles.storeMetricValue}>{formatStock(row.stock_by_store?.[item.store_uid])}</span>
          </div>
        ))}
      </div>
    );
  };

  return (
    <PricingCatalogFrame
      title="Эластичность"
      subtitle="Оперативный мониторинг спроса по цене: за день, неделю, 30 дней, квартал и произвольный период."
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
      summaryPanel={summaryPanel}
      searchValue={searchDraft}
      onSearchChange={setSearchDraft}
      searchPlaceholder="Поиск по SKU или наименованию"
      hideSearchPanel
      error={error}
      treeSelector={
        tab === "all" ? (
          <CatalogTreeControls
            selectId="elasticity-tree-source-store"
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
      tableMeta={
        <span>Всего: {totalCount}</span>
      }
      table={(
        <table className={matrixStyles.matrixTable}>
          <thead>
            <tr>
              <th>SKU</th>
              <th className={matrixStyles.nameHeader}>Наименование товара</th>
              <th className={styles.metricCol}>Остаток</th>
              <th className={styles.metricCol}>
                <button type="button" className={styles.sortButton} onClick={() => toggleSort("mentions_count")}>
                  <span>Кол-во</span>
                  <span className={styles.sortMarker}>{sortMarker("mentions_count")}</span>
                </button>
              </th>
              <th className={styles.metricCol}>
                <button type="button" className={styles.sortButton} onClick={() => toggleSort("turnover")}>
                  <span>Оборот</span>
                  <span className={styles.sortMarker}>{sortMarker("turnover")}</span>
                </button>
              </th>
              <th className={styles.wideMetricCol}>Динамика</th>
              <th className={styles.metricCol}>Цена продажи</th>
              <th className={styles.metricCol}>Соинвест</th>
              <th className={styles.metricCol}>Цена с соинвестом</th>
              <th className={styles.metricCol}>Эластичность</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={10} className={matrixStyles.emptyCell}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</td>
              </tr>
            ) : sortedRows.map((row) => (
              <tr key={row.sku}>
                <td className={matrixStyles.skuCell}>{row.sku}</td>
                <MatrixNameCell name={row.name} path={row.tree_path} />
                <td className={styles.metricCell}>{renderStockCell(row)}</td>
                <td className={styles.metricCell}>{formatInt(row.mentions_count)}</td>
                <td className={styles.metricCell}>{formatMoney(row.turnover)}</td>
                <td className={styles.metricCell}>
                  <div className={styles.deltaCell}>
                    <span>Кол-во: {formatPercent(row.count_delta_percent)}</span>
                    <span>Оборот: {formatPercent(row.turnover_delta_percent)}</span>
                  </div>
                </td>
                <td className={styles.metricCell}>{renderStoreMetricCell(row, "avg_sale_price", formatMoney)}</td>
                <td className={styles.metricCell}>{renderStoreMetricCell(row, "avg_coinvest_percent", formatPercent)}</td>
                <td className={styles.metricCell}>{renderStoreMetricCell(row, "avg_payment_price", formatMoney)}</td>
                <td className={styles.metricCell}>
                  {period === "today" || period === "yesterday" ? (
                    renderPlaceholder()
                  ) : (() => {
                    const level = elasticityLevel(row.elasticity);
                    return level ? (
                      <div className={styles.elasticityCell}>
                        <span className={`${styles.elasticityBadge} ${styles[`elasticityBadge_${level.tone}`]}`}>{level.label}</span>
                        <span className={styles.elasticityValue}>{formatElasticity(row.elasticity)}</span>
                      </div>
                    ) : renderPlaceholder();
                  })()}
                </td>
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
