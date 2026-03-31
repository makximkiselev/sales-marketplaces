import type { ReactNode } from "react";
import { MatrixMultiValue, MatrixNameCell, buildStoreLines, pricingMatrixStyles as matrixStyles } from "../_components/PricingMatrixKit";
import commonStyles from "../_components/PricingPageCommon.module.css";
import { currencySymbol, formatMoney, formatPercent, StoreCtx } from "../_shared/catalogPageShared";
import styles from "./StrategyPage.module.css";

type DecisionValue = { label?: string; tone?: string; code?: string };
type StrategyFilterValue =
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
  | "overpriced";

type StrategySortKey =
  | "sku_plan_revenue"
  | "sku_plan_qty"
  | "sku_plan_profit_abs"
  | "sku_plan_profit_pct"
  | "fact_sales_revenue"
  | "fact_sales_qty"
  | "fact_profit_abs"
  | "fact_profit_pct"
  | "profit_completion_pct";

export type StrategyOverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  stock_by_store?: Record<string, number | null>;
  cogs_price_by_store?: Record<string, number | null>;
  decision_by_store?: Record<string, DecisionValue>;
  installed_price_by_store?: Record<string, number | null>;
  mrc_price_by_store?: Record<string, number | null>;
  mrc_with_boost_price_by_store?: Record<string, number | null>;
  rrc_price_by_store?: Record<string, number | null>;
  promo_participation_by_store?: Record<string, boolean>;
  promo_details_by_store?: Record<string, Array<{ promo_id?: string; promo_name?: string; status_label?: string; status_tone?: string; detail?: string }>>;
  attractiveness_status_by_store?: Record<string, string>;
  boost_bid_by_store?: Record<string, number | null>;
  market_boost_bid_by_store?: Record<string, number | null>;
  boost_share_by_store?: Record<string, number | null>;
  planned_unit_profit_abs_by_store?: Record<string, number | null>;
  planned_unit_profit_pct_by_store?: Record<string, number | null>;
  elasticity_by_store?: Record<string, number | null>;
  coinvest_pct_by_store?: Record<string, number | null>;
  avg_check_by_store?: Record<string, number | null>;
  fact_sales_by_store?: Record<string, number | null>;
  fact_sales_revenue_by_store?: Record<string, number | null>;
  sku_sales_plan_qty_by_store?: Record<string, number | null>;
  sku_sales_plan_revenue_by_store?: Record<string, number | null>;
  forecast_sales_by_store?: Record<string, number | null>;
  forecast_profit_abs_by_store?: Record<string, number | null>;
  planned_price_with_coinvest_by_store?: Record<string, number | null>;
  on_display_price_by_store?: Record<string, number | null>;
  minimum_profit_percent_by_store?: Record<string, number | null>;
  experimental_floor_pct_by_store?: Record<string, number | null>;
  sales_delta_pct_by_store?: Record<string, number | null>;
  final_price_by_store?: Record<string, number | null>;
  final_boost_by_store?: Record<string, number | null>;
  final_profit_abs_by_store?: Record<string, number | null>;
  final_profit_pct_by_store?: Record<string, number | null>;
  fact_economy_abs_by_store?: Record<string, number | null>;
  fact_economy_pct_by_store?: Record<string, number | null>;
  economy_delta_pct_by_store?: Record<string, number | null>;
  hypothesis_by_store?: Record<string, string>;
  hypothesis_started_at_by_store?: Record<string, string>;
  hypothesis_expires_at_by_store?: Record<string, string>;
  control_state_by_store?: Record<string, string>;
  control_state_started_at_by_store?: Record<string, string>;
  market_promo_status_by_store?: Record<string, string>;
  market_promo_checked_at_by_store?: Record<string, string>;
  market_promo_message_by_store?: Record<string, string>;
  strategy_code_by_store?: Record<string, string>;
  updated_at: string;
};

type Props = {
  rows: StrategyOverviewRow[];
  visibleStores: StoreCtx[];
  tab: string;
  activeStoreUid: string;
  activeStoreCurrency: string | undefined;
  moneyHeader: (label: string) => string;
  page: number;
  totalPages: number;
  pageSize: number;
  totalCount: number;
  selectedTreePath: string;
  tableLoading: boolean;
  strategyFilter: StrategyFilterValue;
  onStrategyFilterChange: (value: StrategyFilterValue) => void;
  salesFilter: "all" | "with_sales" | "without_sales";
  onSalesFilterChange: (value: "all" | "with_sales" | "without_sales") => void;
  sortKey: StrategySortKey;
  sortDir: "asc" | "desc";
  onSortChange: (value: StrategySortKey) => void;
  onPageChange: (updater: (page: number) => number) => void;
  onPageSizeChange: (value: number) => void;
};

type RenderCtx = {
  tab: string;
  activeStoreUid: string;
  activeStoreCurrency: string | undefined;
  visibleStores: StoreCtx[];
};

function toneClass(tone: string | null | undefined) {
  const normalized = String(tone || "").trim().toLowerCase();
  if (normalized === "positive") return matrixStyles.statusPositive;
  if (normalized === "negative") return matrixStyles.statusNegative;
  return matrixStyles.statusWarning;
}

function statusToneForAttractiveness(value: string | null | undefined) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "выгодная") return matrixStyles.statusPositive;
  if (normalized === "завышенная") return matrixStyles.statusNegative;
  return matrixStyles.statusWarning;
}

function statusToneForPromoMarket(value: string | null | undefined) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "verified") return matrixStyles.statusPositive;
  if (normalized === "pending" || normalized === "warning") return matrixStyles.statusWarning;
  if (normalized === "rejected" || normalized === "error") return matrixStyles.statusNegative;
  return matrixStyles.statusWarning;
}

function promoMarketLabel(value: string | null | undefined) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "verified") return "Подтверждено";
  if (normalized === "pending") return "В обработке";
  if (normalized === "rejected") return "Отклонено";
  if (normalized === "warning") return "Проверить";
  if (normalized === "error") return "Ошибка";
  return "—";
}

function renderMoney(value: number | null | undefined, currencyCode: string | undefined) {
  const shown = formatMoney(value);
  if (shown === "—") return "—";
  return `${shown}${currencySymbol(currencyCode)}`;
}

function renderCount(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return String(Math.round(Number(value)));
}

function renderMoneyWithPct(abs: number | null | undefined, pct: number | null | undefined, currencyCode: string | undefined) {
  const money = renderMoney(abs, currencyCode);
  const pctText = formatPercent(pct);
  if (money === "—" && pctText === "—") return "—";
  if (money === "—") return pctText;
  if (pctText === "—") return money;
  return `${money} (${pctText})`;
}

function renderDecisionContent(value: DecisionValue | undefined) {
  const label = String(value?.label || "").trim();
  if (!label) return "—";
  return <span className={`${matrixStyles.statusPill} ${toneClass(value?.tone)}`}>{label}</span>;
}

function renderSingleOrMulti(
  row: StrategyOverviewRow,
  ctx: RenderCtx,
  keyPrefix: string,
  renderForStore: (storeUid: string, currencyCode: string | undefined) => ReactNode,
) {
  if (ctx.tab !== "all") {
    return renderForStore(ctx.activeStoreUid, ctx.activeStoreCurrency);
  }
  return (
    <MatrixMultiValue
      rows={buildStoreLines(
        ctx.visibleStores,
        (store) => renderForStore(store.store_uid, store.currency_code),
        `${row.sku}-${keyPrefix}`,
      )}
    />
  );
}

function renderStockCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "stock", (storeUid) => renderCount(row.stock_by_store?.[storeUid]));
}

function renderCogsCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "cogs", (storeUid, currencyCode) => renderMoney(row.cogs_price_by_store?.[storeUid], currencyCode));
}

function renderDecisionCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "decision", (storeUid) => renderDecisionContent(row.decision_by_store?.[storeUid]));
}

function renderInstalledPriceCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "installed-price", (storeUid, currencyCode) => renderMoney(row.installed_price_by_store?.[storeUid], currencyCode));
}

function renderStoreMoneyCell(
  row: StrategyOverviewRow,
  ctx: RenderCtx,
  keyPrefix: string,
  source: Record<string, number | null> | undefined,
) {
  return renderSingleOrMulti(row, ctx, keyPrefix, (storeUid, currencyCode) => renderMoney(source?.[storeUid], currencyCode));
}

function renderPromoParticipationCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "promo-participation", (storeUid) => {
    const details = Array.isArray(row.promo_details_by_store?.[storeUid]) ? row.promo_details_by_store?.[storeUid] : [];
    if (!details || details.length === 0) {
      const participates = Boolean(row.promo_participation_by_store?.[storeUid]);
      return (
        <span className={`${matrixStyles.statusPill} ${participates ? matrixStyles.statusPositive : matrixStyles.statusNegative}`}>
          {participates ? "Участвует" : "—"}
        </span>
      );
    }
    return (
      <div className={styles.metricStack}>
        {details.map((detail, index) => {
          const tone = toneClass(detail?.status_tone);
          const name = String(detail?.promo_name || detail?.promo_id || "Промо").trim();
          const status = String(detail?.status_label || "").trim() || "—";
          const meta = String(detail?.detail || "").trim();
          return (
            <div key={`${storeUid}-promo-detail-${index}`} className={styles.promoDetailItem}>
              <span className={styles.promoDetailName}>{name}</span>
              <span className={`${matrixStyles.statusPill} ${tone}`}>{status}</span>
              {meta ? <span className={styles.metricSubtle}>{meta}</span> : null}
            </div>
          );
        })}
      </div>
    );
  });
}

function renderAttractivenessCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "attractiveness", (storeUid) => {
    const value = String(row.attractiveness_status_by_store?.[storeUid] || "").trim();
    if (!value) return "—";
    return <span className={`${matrixStyles.statusPill} ${statusToneForAttractiveness(value)}`}>{value}</span>;
  });
}

function renderBoostCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "boost", (storeUid) => {
    const bid = formatPercent(row.market_boost_bid_by_store?.[storeUid] ?? row.boost_bid_by_store?.[storeUid]);
    const shareRaw = row.boost_share_by_store?.[storeUid];
    const share = shareRaw == null || Number.isNaN(Number(shareRaw)) ? "—" : `${Math.round(Number(shareRaw))}% показов`;
    if (bid === "—" && share === "—") return "—";
    return (
      <div className={styles.metricStack}>
        <span>{bid}</span>
        {share !== "—" ? <span className={styles.metricSubtle}>{share}</span> : null}
      </div>
    );
  });
}

function renderProfitCell(
  row: StrategyOverviewRow,
  ctx: RenderCtx,
  keyPrefix: string,
  absSource: Record<string, number | null> | undefined,
  pctSource: Record<string, number | null> | undefined,
) {
  return renderSingleOrMulti(row, ctx, keyPrefix, (storeUid, currencyCode) =>
    renderMoneyWithPct(absSource?.[storeUid], pctSource?.[storeUid], currencyCode),
  );
}

function renderPercentCell(row: StrategyOverviewRow, ctx: RenderCtx, keyPrefix: string, source: Record<string, number | null> | undefined) {
  return renderSingleOrMulti(row, ctx, keyPrefix, (storeUid) => formatPercent(source?.[storeUid]));
}

function renderAvgCheckCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "avg-check", (storeUid, currencyCode) => renderMoney(row.avg_check_by_store?.[storeUid], currencyCode));
}

function renderSalesCell(row: StrategyOverviewRow, ctx: RenderCtx, keyPrefix: string, source: Record<string, number | null> | undefined) {
  return renderSingleOrMulti(row, ctx, keyPrefix, (storeUid) => renderCount(source?.[storeUid]));
}

function renderPlannedCoinvestCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "planned-price-coinvest", (storeUid, currencyCode) =>
    renderMoney(row.planned_price_with_coinvest_by_store?.[storeUid], currencyCode),
  );
}

function renderStrategyCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "strategy-branch", (storeUid) => {
    const title = String(row.decision_by_store?.[storeUid]?.label || "").trim() || "—";
    return (
      <div className={styles.strategyBranch}>
        <span className={`${matrixStyles.statusPill} ${matrixStyles.statusWarning}`}>{title}</span>
      </div>
    );
  });
}

function renderSalesMoneyCell(row: StrategyOverviewRow, ctx: RenderCtx, keyPrefix: string, source: Record<string, number | null> | undefined) {
  return renderSingleOrMulti(row, ctx, keyPrefix, (storeUid, currencyCode) => renderMoney(source?.[storeUid], currencyCode));
}

function sortMetricValue(row: StrategyOverviewRow, ctx: RenderCtx, key: StrategySortKey) {
  const storeUids = ctx.tab === "all" ? ctx.visibleStores.map((store) => store.store_uid) : [ctx.activeStoreUid];
  return storeUids.reduce((sum, storeUid) => {
    const qtyPlan = Number(row.sku_sales_plan_qty_by_store?.[storeUid] || 0);
    const revenuePlan = Number(row.sku_sales_plan_revenue_by_store?.[storeUid] || 0);
    const unitProfitAbs = Number(row.planned_unit_profit_abs_by_store?.[storeUid] || 0);
    const unitProfitPct = Number(row.planned_unit_profit_pct_by_store?.[storeUid] || 0);
    const factRevenue = Number(row.fact_sales_revenue_by_store?.[storeUid] || 0);
    const factQty = Number(row.fact_sales_by_store?.[storeUid] || 0);
    const factProfitAbs = Number(row.fact_economy_abs_by_store?.[storeUid] || 0);
    const factProfitPct = Number(row.fact_economy_pct_by_store?.[storeUid] || 0);
    const planProfitAbs = qtyPlan * unitProfitAbs;
    if (key === "sku_plan_revenue") return sum + revenuePlan;
    if (key === "sku_plan_qty") return sum + qtyPlan;
    if (key === "sku_plan_profit_abs") return sum + planProfitAbs;
    if (key === "sku_plan_profit_pct") return sum + unitProfitPct;
    if (key === "fact_sales_revenue") return sum + factRevenue;
    if (key === "fact_sales_qty") return sum + factQty;
    if (key === "fact_profit_abs") return sum + factProfitAbs;
    if (key === "fact_profit_pct") return sum + factProfitPct;
    if (key === "profit_completion_pct") return sum + (planProfitAbs ? (factProfitAbs / planProfitAbs) * 100 : 0);
    return sum;
  }, 0);
}

function renderPlanProfitCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "sku-plan-profit", (storeUid, currencyCode) => {
    const qty = Number(row.sku_sales_plan_qty_by_store?.[storeUid] || 0);
    const unitAbs = Number(row.planned_unit_profit_abs_by_store?.[storeUid] || 0);
    return renderMoneyWithPct(qty * unitAbs || null, row.planned_unit_profit_pct_by_store?.[storeUid], currencyCode);
  });
}

function renderFactProfitCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "fact-profit-tail", (storeUid, currencyCode) =>
    renderMoneyWithPct(row.fact_economy_abs_by_store?.[storeUid], row.fact_economy_pct_by_store?.[storeUid], currencyCode),
  );
}

function renderProfitCompletionCell(row: StrategyOverviewRow, ctx: RenderCtx) {
  return renderSingleOrMulti(row, ctx, "profit-completion", (storeUid) => {
    const qty = Number(row.sku_sales_plan_qty_by_store?.[storeUid] || 0);
    const unitAbs = Number(row.planned_unit_profit_abs_by_store?.[storeUid] || 0);
    const planProfitAbs = qty * unitAbs;
    const factProfitAbs = Number(row.fact_economy_abs_by_store?.[storeUid] || 0);
    if (!planProfitAbs) return "—";
    return formatPercent((factProfitAbs / planProfitAbs) * 100);
  });
}

function renderSortHeader(label: string, key: StrategySortKey, activeKey: StrategySortKey, sortDir: "asc" | "desc", onSortChange: (value: StrategySortKey) => void) {
  const marker = activeKey === key ? (sortDir === "desc" ? "↓" : "↑") : "↕";
  return (
    <button type="button" className={styles.sortButton} onClick={() => onSortChange(key)}>
      <span>{label}</span>
      <span className={styles.sortMarker}>{marker}</span>
    </button>
  );
}

export function StrategyTable(props: Props) {
  const {
    rows,
    visibleStores,
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
    onStrategyFilterChange,
    salesFilter,
    onSalesFilterChange,
    sortKey,
    sortDir,
    onSortChange,
    onPageChange,
    onPageSizeChange,
  } = props;

  const renderCtx = { tab, activeStoreUid, activeStoreCurrency, visibleStores };
  const filteredRows = rows.filter((row) => {
    if (strategyFilter === "all") return true;
    const storeUids = tab === "all" ? visibleStores.map((store) => store.store_uid) : [activeStoreUid];
    return storeUids.some((storeUid) => {
      const decision = row.decision_by_store?.[storeUid];
      return String(decision?.code || "").trim() === strategyFilter;
    });
  });
  const sortedRows = [...filteredRows].sort((a, b) => {
    const aVal = sortMetricValue(a, renderCtx, sortKey);
    const bVal = sortMetricValue(b, renderCtx, sortKey);
    return sortDir === "asc" ? aVal - bVal : bVal - aVal;
  });

  return {
    tableTitleControls: (
      <div className={styles.filterWrap}>
        <span className={commonStyles.fieldLabel}>Решение</span>
        <select
          className={`input input-size-md ${commonStyles.select} ${styles.filterSelect}`}
          value={strategyFilter}
          onChange={(e) => {
            onPageChange(() => 1);
            onStrategyFilterChange((e.target.value as StrategyFilterValue) || "all");
          }}
        >
          <option value="all">Все</option>
          <option value="promo2_profitable_boost">2 промо + выгодно + буст</option>
          <option value="promo2_profitable">2 промо + выгодно</option>
          <option value="promo1_profitable_boost">1 промо + выгодно + буст</option>
          <option value="promo1_profitable">1 промо + выгодно</option>
          <option value="promo2_moderate_boost">2 промо + умеренно + буст</option>
          <option value="promo2_moderate">2 промо + умеренно</option>
          <option value="promo1_moderate_boost">1 промо + умеренно + буст</option>
          <option value="promo1_moderate">1 промо + умеренно</option>
          <option value="profitable_boost">Выгодная цена + буст</option>
          <option value="profitable">Выгодно</option>
          <option value="moderate_boost">Умеренная цена + буст</option>
          <option value="moderate">Умеренно</option>
          <option value="overpriced">Невыгодная цена</option>
        </select>
        <span className={commonStyles.fieldLabel}>Продажи</span>
        <select
          className={`input input-size-md ${commonStyles.select} ${styles.filterSelect}`}
          value={salesFilter}
          onChange={(e) => {
            onPageChange(() => 1);
            onSalesFilterChange((e.target.value as "all" | "with_sales" | "without_sales") || "all");
          }}
        >
          <option value="all">Все</option>
          <option value="with_sales">С продажами</option>
          <option value="without_sales">Без продаж</option>
        </select>
      </div>
    ),
    tableMeta: <span>{tableLoading ? "Обновление..." : `Всего: ${totalCount}`}{selectedTreePath ? ` • Фильтр: ${selectedTreePath}` : ""}</span>,
    table: (
        <table className={matrixStyles.matrixTable}>
          <thead>
            <tr>
              <th rowSpan={2}>SKU</th>
              <th rowSpan={2} className={matrixStyles.nameHeader}>Наименование товара</th>
              <th colSpan={8}>Продажи</th>
              <th colSpan={4}>Стратегия</th>
              <th colSpan={9}>Цены</th>
            </tr>
            <tr>
              <th>{renderSortHeader(moneyHeader("План продаж"), "sku_plan_revenue", sortKey, sortDir, onSortChange)}</th>
              <th>{renderSortHeader("План продаж, шт", "sku_plan_qty", sortKey, sortDir, onSortChange)}</th>
              <th>{renderSortHeader(moneyHeader("Плановая прибыль"), "sku_plan_profit_abs", sortKey, sortDir, onSortChange)}</th>
              <th>{renderSortHeader(moneyHeader("Факт продаж"), "fact_sales_revenue", sortKey, sortDir, onSortChange)}</th>
              <th>{renderSortHeader("Факт продаж, шт", "fact_sales_qty", sortKey, sortDir, onSortChange)}</th>
              <th>{renderSortHeader(moneyHeader("Фактическая прибыль"), "fact_profit_abs", sortKey, sortDir, onSortChange)}</th>
              <th>{renderSortHeader("Выполнение, %", "profit_completion_pct", sortKey, sortDir, onSortChange)}</th>
              <th>Решение</th>
              <th>Промо</th>
              <th>Привлекательность</th>
              <th>Буст</th>
              <th>Остаток</th>
              <th>Себестоимость</th>
              <th>{moneyHeader("МРЦ")}</th>
              <th>{moneyHeader("МРЦ + буст")}</th>
              <th>{moneyHeader("РРЦ cap")}</th>
              <th>{moneyHeader("Финальная цена")}</th>
              <th>{moneyHeader("Цена на витрине")}</th>
              <th>Соинвест, %</th>
              <th>{moneyHeader("Прибыль на SKU")}</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.length === 0 ? (
              <tr>
                <td colSpan={23} className={matrixStyles.emptyCell}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</td>
              </tr>
            ) : sortedRows.map((row) => (
              <tr key={`strategy-${row.sku}`}>
                <td className={matrixStyles.skuCell}>{row.sku}</td>
                <MatrixNameCell name={row.name} path={row.tree_path} />
                <td className={matrixStyles.moneyCell}>{renderSalesMoneyCell(row, renderCtx, "sku-plan-revenue", row.sku_sales_plan_revenue_by_store)}</td>
                <td className={matrixStyles.centerCell}>{renderSalesCell(row, renderCtx, "sku-plan-qty", row.sku_sales_plan_qty_by_store)}</td>
                <td className={matrixStyles.moneyCell}>{renderPlanProfitCell(row, renderCtx)}</td>
                <td className={matrixStyles.moneyCell}>{renderSalesMoneyCell(row, renderCtx, "fact-sales-revenue", row.fact_sales_revenue_by_store)}</td>
                <td className={matrixStyles.centerCell}>{renderSalesCell(row, renderCtx, "fact-sales-qty-tail", row.fact_sales_by_store)}</td>
                <td className={matrixStyles.moneyCell}>{renderFactProfitCell(row, renderCtx)}</td>
                <td className={matrixStyles.centerCell}>{renderProfitCompletionCell(row, renderCtx)}</td>
                <td className={matrixStyles.leftCell}>{renderStrategyCell(row, renderCtx)}</td>
                <td className={matrixStyles.centerCell}>{renderPromoParticipationCell(row, renderCtx)}</td>
                <td className={matrixStyles.centerCell}>{renderAttractivenessCell(row, renderCtx)}</td>
                <td className={matrixStyles.centerCell}>{renderBoostCell(row, renderCtx)}</td>
                <td className={matrixStyles.stockCell}>{renderStockCell(row, renderCtx)}</td>
                <td className={matrixStyles.moneyCell}>{renderCogsCell(row, renderCtx)}</td>
                <td className={matrixStyles.moneyCell}>{renderStoreMoneyCell(row, renderCtx, "mrc", row.mrc_price_by_store)}</td>
                <td className={matrixStyles.moneyCell}>{renderStoreMoneyCell(row, renderCtx, "mrc-with-boost", row.mrc_with_boost_price_by_store)}</td>
                <td className={matrixStyles.moneyCell}>{renderStoreMoneyCell(row, renderCtx, "rrc-cap", row.rrc_price_by_store)}</td>
                <td className={matrixStyles.moneyCell}>{renderInstalledPriceCell(row, renderCtx)}</td>
                <td className={matrixStyles.moneyCell}>{renderStoreMoneyCell(row, renderCtx, "on-display", row.on_display_price_by_store)}</td>
                <td className={matrixStyles.centerCell}>{renderPercentCell(row, renderCtx, "coinvest", row.coinvest_pct_by_store)}</td>
                <td className={matrixStyles.moneyCell}>{renderProfitCell(row, renderCtx, "planned-profit", row.planned_unit_profit_abs_by_store, row.planned_unit_profit_pct_by_store)}</td>
              </tr>
            ))}
        </tbody>
      </table>
    ),
    canPrev: page > 1,
    canNext: page < totalPages,
    onPrevPage: () => onPageChange((current) => Math.max(1, current - 1)),
    onNextPage: () => onPageChange((current) => Math.min(totalPages, current + 1)),
    onPageSizeSelect: onPageSizeChange,
    totalPages,
    pageSize,
    page,
  };
}
