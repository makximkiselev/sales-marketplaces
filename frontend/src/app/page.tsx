import { type ReactNode, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { apiGetOk } from "../lib/api";
import { PageFrame } from "../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceToolbar } from "../components/page/WorkspaceKit";
import layoutStyles from "./_shared/AppPageLayout.module.css";
import styles from "./DashboardPage.module.css";

type StoreCtx = {
  store_uid: string;
  store_id: string;
  platform: string;
  platform_label: string;
  label: string;
  currency_code?: string;
};

type ContextResp = {
  ok: boolean;
  marketplace_stores?: StoreCtx[];
};

type DataFlowItem = {
  code: string;
  label: string;
  description?: string;
  date_from?: string;
  date_to?: string;
  loaded_at?: string;
};

type DataFlowResp = {
  ok: boolean;
  flows?: DataFlowItem[];
};

type OrderRow = {
  store_uid?: string;
  sale_price?: number | null;
  sale_price_with_coinvest?: number | null;
  profit?: number | null;
  item_name?: string;
  sku?: string;
  item_status?: string;
};

type OrdersResp = {
  ok: boolean;
  rows?: OrderRow[];
  total_count?: number;
  date_from?: string;
  date_to?: string;
  loaded_at?: string;
  kpis?: {
    orders_count?: number;
    avg_coinvest_pct?: number;
    additional_ads?: number;
    operational_errors?: number;
  };
};

type ProblemOrdersResp = {
  ok: boolean;
  rows?: Array<{ sku?: string; item_name?: string; item_status?: string }>;
  total_count?: number;
  date_from?: string;
  date_to?: string;
  loaded_at?: string;
};

type TrackingDay = {
  date: string;
  revenue?: number | null;
  revenue_plan_amount?: number | null;
  profit_amount?: number | null;
  profit_plan_amount?: number | null;
  profit_pct?: number | null;
  coinvest_pct?: number | null;
  returns_pct?: number | null;
  ads_amount?: number | null;
  operational_errors?: number | null;
};

type TrackingMonth = {
  month_key: string;
  month_label: string;
  is_active: boolean;
  revenue?: number | null;
  revenue_plan_amount?: number | null;
  profit_amount?: number | null;
  profit_plan_amount?: number | null;
  profit_pct?: number | null;
  returns_pct?: number | null;
  ads_amount?: number | null;
  operational_errors?: number | null;
  days?: TrackingDay[];
};

type TrackingYear = {
  year: number;
  months: TrackingMonth[];
};

type TrackingResp = {
  ok: boolean;
  years?: TrackingYear[];
  active_month_key?: string;
  loaded_at?: string;
  kpis?: {
    revenue?: number | null;
    profit?: number | null;
    profit_pct?: number | null;
    avg_coinvest_pct?: number | null;
    days?: number | null;
  };
};

type RetrospectivePeriod = {
  period_label: string;
  revenue?: number | null;
  profit_amount?: number | null;
};

type RetrospectiveRow = {
  key: string;
  label: string;
  sku?: string;
  item_name?: string;
  category_path?: string;
  revenue?: number | null;
  profit_amount?: number | null;
  profit_pct?: number | null;
  periods?: RetrospectivePeriod[];
};

type RetrospectiveResp = {
  ok: boolean;
  rows?: RetrospectiveRow[];
  total_count?: number;
  category_groups?: CategoryAggregate[];
};

type ChartRange = "7d" | "30d";
type AnalysisView = "sku" | "category" | "status";

type CategoryAggregate = {
  label: string;
  value: number;
  profit: number;
  marginPct: number | null;
  brandCount: number;
  brands: Array<{ label: string; value: number; profit: number; marginPct: number | null }>;
};

type DashboardBundle = {
  tracking: TrackingResp;
  orders: OrdersResp;
  problems: ProblemOrdersResp;
  dataFlow: DataFlowResp;
  sku: RetrospectiveResp;
  category: RetrospectiveResp;
  today: OrdersResp;
  yesterday: OrdersResp;
  todayProblems: ProblemOrdersResp;
  yesterdayProblems: ProblemOrdersResp;
  previousOrders: OrdersResp;
  previousProblems: ProblemOrdersResp;
};

type StoreComparison = {
  storeId: string;
  label: string;
  platformLabel: string;
  revenue: number;
  profit: number;
  orders: number;
  marginPct: number | null;
  revenueDeltaPct: number | null;
  profitDeltaPct: number | null;
  ordersDeltaPct: number | null;
};

type DashboardSummaryResp = {
  ok: boolean;
  context: ContextResp;
  bundle: DashboardBundle;
  storeComparison: StoreComparison[];
};

type DashboardPeriod = "today" | "yesterday";

const PERIOD_OPTIONS: Array<{ value: DashboardPeriod; label: string }> = [
  { value: "today", label: "Сегодня" },
  { value: "yesterday", label: "Вчера" },
];

function moneySign(currencyCode: string | undefined | null) {
  return String(currencyCode || "").trim().toUpperCase() === "USD" ? "$" : "₽";
}

function formatMoney(value: number | null | undefined, currencyCode?: string | null) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value)).toLocaleString("ru-RU")} ${moneySign(currencyCode)}`;
}

function formatCompactMoney(value: number | null | undefined, currencyCode?: string | null) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  const numeric = Number(value);
  const abs = Math.abs(numeric);
  const symbol = moneySign(currencyCode);
  if (abs >= 1_000_000) return `${(numeric / 1_000_000).toLocaleString("ru-RU", { maximumFractionDigits: 1 })} млн ${symbol}`;
  if (abs >= 1_000) return `${(numeric / 1_000).toLocaleString("ru-RU", { maximumFractionDigits: 0 })}k ${symbol}`;
  return `${Math.round(numeric).toLocaleString("ru-RU")} ${symbol}`;
}

function formatNumber(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return Math.round(Number(value)).toLocaleString("ru-RU");
}

function formatPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${(Math.round(Number(value) * 100) / 100).toLocaleString("ru-RU")}%`;
}

function formatShortDate(value: string | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const parsed = new Date(`${raw}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
}

function formatLongDate(value: string | Date | undefined) {
  if (!value) return "—";
  const parsed = value instanceof Date ? value : new Date(`${String(value).trim()}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
}

function formatDateTime(value: string | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function sumBy<T>(rows: T[], getter: (item: T) => number | null | undefined) {
  return rows.reduce((acc, row) => acc + Number(getter(row) || 0), 0);
}

function localDateOnly(base = new Date()) {
  return new Date(base.getFullYear(), base.getMonth(), base.getDate());
}

function shiftDate(base: Date, days: number) {
  const copy = new Date(base);
  copy.setDate(copy.getDate() + days);
  return copy;
}

function toIsoDate(base: Date) {
  const year = base.getFullYear();
  const month = String(base.getMonth() + 1).padStart(2, "0");
  const day = String(base.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getPeriodSpanDays(period: DashboardPeriod) {
  return 1;
}

function splitCategoryPath(value: string | undefined) {
  return String(value || "")
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean);
}

function buildCategoryAggregates(rows: RetrospectiveRow[]) {
  const categoryMap = new Map<string, { revenue: number; profit: number; brands: Map<string, { revenue: number; profit: number }> }>();
  for (const row of rows || []) {
    const parts = splitCategoryPath(row.category_path || row.label);
    const category = parts[0] || "Не определено";
    const brand = parts.length >= 2 ? parts[parts.length >= 3 ? parts.length - 2 : 1] || "Без бренда" : "Без бренда";
    const revenue = Number(row.revenue || 0);
    const profit = Number(row.profit_amount || 0);
    const categoryBucket = categoryMap.get(category) || { revenue: 0, profit: 0, brands: new Map() };
    categoryBucket.revenue += revenue;
    categoryBucket.profit += profit;
    const brandBucket = categoryBucket.brands.get(brand) || { revenue: 0, profit: 0 };
    brandBucket.revenue += revenue;
    brandBucket.profit += profit;
    categoryBucket.brands.set(brand, brandBucket);
    categoryMap.set(category, categoryBucket);
  }
  return Array.from(categoryMap.entries())
    .map(([label, bucket]) => {
      const brands = Array.from(bucket.brands.entries())
        .map(([brandLabel, brandBucket]) => ({
          label: brandLabel,
          value: brandBucket.revenue,
          profit: brandBucket.profit,
          marginPct: brandBucket.revenue > 0 ? (brandBucket.profit / brandBucket.revenue) * 100 : null,
        }))
        .sort((a, b) => b.value - a.value);
      return {
        label,
        value: bucket.revenue,
        profit: bucket.profit,
        marginPct: bucket.revenue > 0 ? (bucket.profit / bucket.revenue) * 100 : null,
        brandCount: brands.length,
        brands,
      };
    })
    .sort((a, b) => b.value - a.value);
}

function getCurrentPeriodRange(period: DashboardPeriod) {
  const end = localDateOnly();
  const span = getPeriodSpanDays(period);
  const start = shiftDate(end, -(span - 1));
  return { start: toIsoDate(start), end: toIsoDate(end), span };
}

function getPreviousPeriodRange(period: DashboardPeriod) {
  const current = getCurrentPeriodRange(period);
  const currentStart = localDateOnly(new Date(`${current.start}T00:00:00`));
  const previousEnd = shiftDate(currentStart, -1);
  const previousStart = shiftDate(previousEnd, -(current.span - 1));
  return { start: toIsoDate(previousStart), end: toIsoDate(previousEnd), span: current.span };
}

function Sparkline({
  values,
  tone = "cyan",
}: {
  values: number[];
  tone?: "cyan" | "green" | "amber";
}) {
  const points = values.length
    ? values.map((value, index) => {
        const max = Math.max(...values);
        const min = Math.min(...values);
        const range = max - min || 1;
        const x = values.length === 1 ? 60 : (index / (values.length - 1)) * 120;
        const y = 32 - ((value - min) / range) * 32;
        return `${x},${y}`;
      }).join(" ")
    : "";
  if (!points) return <div className={styles.sparklineEmpty}>Нет данных</div>;
  return (
    <svg viewBox="0 0 120 32" className={`${styles.sparkline} ${styles[`sparkline${tone[0].toUpperCase()}${tone.slice(1)}`]}`}>
      <polyline points={points} />
    </svg>
  );
}

function TrendChart({
  days,
  currencyCode,
  title = "Оборот и прибыль по дням",
  hint = "Выбранный диапазон из обзора продаж.",
  emptyText = "Нет дневных данных для выбранного диапазона.",
  controls,
}: {
  days: TrackingDay[];
  currencyCode?: string | null;
  title?: string;
  hint?: string;
  emptyText?: string;
  controls?: ReactNode;
}) {
  if (days.length === 0) {
    return (
      <div className={styles.chartCard}>
        <div className={styles.chartHead}>
          <div>
            <div className={styles.panelTitle}>{title}</div>
            <div className={styles.panelHint}>{hint}</div>
          </div>
          {controls}
        </div>
        <div className={styles.placeholderCard}>{emptyText}</div>
      </div>
    );
  }
  const chartWidth = 760;
  const chartHeight = 228;
  const pad = { top: 18, right: 18, bottom: 32, left: 48 };
  const innerWidth = chartWidth - pad.left - pad.right;
  const innerHeight = chartHeight - pad.top - pad.bottom;
  const revenueValues = days.map((day) => Number(day.revenue || 0));
  const profitValues = days.map((day) => Number(day.profit_amount || 0));
  const coinvestValues = days.map((day) => Number(day.coinvest_pct || 0));
  const maxValue = Math.max(1, ...revenueValues, ...profitValues);
  const maxCoinvest = Math.max(1, ...coinvestValues, 20);
  const bandWidth = innerWidth / Math.max(days.length, 1);
  const barGap = Math.max(4, bandWidth * 0.08);
  const groupWidth = Math.max(14, bandWidth - barGap);
  const singleBarWidth = Math.max(6, (groupWidth - barGap) / 2);
  const barHeight = (value: number) => Math.max(0, (value / maxValue) * innerHeight);
  const coinvestY = (value: number) => pad.top + innerHeight - (Math.max(0, value) / maxCoinvest) * innerHeight;
  const gridValues = [0, 0.25, 0.5, 0.75, 1];
  const coinvestPath = days
    .map((day, index) => {
      const x = pad.left + index * bandWidth + bandWidth / 2;
      const y = coinvestY(Number(day.coinvest_pct || 0));
      return `${index === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");
  const showBarValues = bandWidth >= 22;

  return (
    <div className={styles.chartCard}>
      <div className={styles.chartHead}>
        <div>
          <div className={styles.panelTitle}>{title}</div>
          <div className={styles.panelHint}>{hint}</div>
        </div>
        <div className={styles.chartTools}>
          <div className={styles.chartLegend}>
            <span><i className={styles.legendRevenue} /> Оборот</span>
            <span><i className={styles.legendProfit} /> Прибыль</span>
            <span><i className={styles.legendCoinvest} /> Соинвест</span>
          </div>
          {controls}
        </div>
      </div>
      <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className={styles.chartSvg} role="img" aria-label="Динамика оборота и прибыли по дням">
        <defs>
          <linearGradient id="chartRevenueGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#64d6ff" />
            <stop offset="100%" stopColor="#2f79d6" />
          </linearGradient>
          <linearGradient id="chartProfitGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#63e3a8" />
            <stop offset="100%" stopColor="#1f8d63" />
          </linearGradient>
        </defs>
        {gridValues.map((tick) => {
          const y = pad.top + innerHeight - tick * innerHeight;
          return <line key={tick} x1={pad.left} y1={y} x2={chartWidth - pad.right} y2={y} className={styles.chartGrid} />;
        })}
        <path d={coinvestPath} className={styles.chartCoinvestLine} />
        {days.map((day, index) => (
          <g key={`${day.date}-${index}`}>
            {(() => {
              const revenue = Number(day.revenue || 0);
              const profit = Number(day.profit_amount || 0);
              const coinvest = Number(day.coinvest_pct || 0);
              const groupX = pad.left + index * bandWidth + (bandWidth - groupWidth) / 2;
              const revenueH = barHeight(revenue);
              const profitH = barHeight(profit);
              return (
                <>
                  <rect
                    x={groupX}
                    y={pad.top + innerHeight - revenueH}
                    width={singleBarWidth}
                    height={revenueH}
                    rx="5"
                    className={styles.chartBarRevenue}
                  />
                  {showBarValues && revenueH > 16 ? (
                    <text
                      x={groupX + singleBarWidth / 2}
                      y={pad.top + innerHeight - revenueH - 6}
                      textAnchor="middle"
                      className={styles.chartValueLabel}
                    >
                      {formatCompactMoney(revenue, currencyCode)}
                    </text>
                  ) : null}
                  <rect
                    x={groupX + singleBarWidth + barGap}
                    y={pad.top + innerHeight - profitH}
                    width={singleBarWidth}
                    height={profitH}
                    rx="5"
                    className={styles.chartBarProfit}
                  />
                  {showBarValues && profitH > 16 ? (
                    <text
                      x={groupX + singleBarWidth + barGap + singleBarWidth / 2}
                      y={pad.top + innerHeight - profitH - 6}
                      textAnchor="middle"
                      className={styles.chartValueLabelAlt}
                    >
                      {formatCompactMoney(profit, currencyCode)}
                    </text>
                  ) : null}
                  <circle cx={pad.left + index * bandWidth + bandWidth / 2} cy={coinvestY(coinvest)} r="3.2" className={styles.chartCoinvestPoint} />
                </>
              );
            })()}
            {index % Math.max(1, Math.ceil(days.length / 6)) === 0 ? (
              <text x={pad.left + index * bandWidth + bandWidth / 2} y={chartHeight - 10} textAnchor="middle" className={styles.chartLabel}>
                {formatShortDate(day.date)}
              </text>
            ) : null}
          </g>
        ))}
        <text x={pad.left} y={14} className={styles.chartScaleLabel}>{formatMoney(maxValue, currencyCode)}</text>
        <text x={pad.left} y={chartHeight - 10} className={styles.chartScaleLabel}>{formatMoney(0, currencyCode)}</text>
        <text x={chartWidth - pad.right} y={14} textAnchor="end" className={styles.chartScaleLabel}>{formatPercent(maxCoinvest)}</text>
      </svg>
    </div>
  );
}

function CategoryDrilldownCard({
  categories,
  currencyCode,
  selectedCategory,
  onSelectCategory,
  actionTo,
}: {
  categories: CategoryAggregate[];
  currencyCode?: string | null;
  selectedCategory: string;
  onSelectCategory: (value: string) => void;
  actionTo?: string;
}) {
  const rows = categories.slice(0, 4);
  const selected = rows.find((row) => row.label === selectedCategory) || rows[0] || null;
  const maxValue = Math.max(1, ...rows.map((row) => row.value));
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelHead}>
        <div>
          <div className={styles.panelTitle}>Топ категорий</div>
          <div className={styles.panelHint}>Сначала категория, внутри нее бренды. Без SKU-листьев в верхнем списке.</div>
        </div>
        {actionTo ? <Link className={styles.panelAction} to={actionTo}>Открыть категории</Link> : null}
      </div>
      <div className={styles.categorySplit}>
        <div className={styles.rankingList}>
          {rows.map((row) => (
            <button
              key={row.label}
              type="button"
              className={`${styles.categoryRowButton} ${selected?.label === row.label ? styles.categoryRowButtonActive : ""}`}
              onClick={() => onSelectCategory(row.label)}
            >
              <div className={styles.rankingRowHead}>
                <div className={styles.rankingLabel}>{row.label}</div>
                <div className={styles.rankingValue}>{formatMoney(row.value, currencyCode)}</div>
              </div>
              <div className={styles.rankingBarTrack}>
                <div className={styles.rankingBarFill} style={{ width: `${(row.value / maxValue) * 100}%` }} />
              </div>
              <div className={styles.rankingDetail}>
                Прибыль: {formatMoney(row.profit, currencyCode)} · Маржа: {formatPercent(row.marginPct)} · Брендов: {formatNumber(row.brandCount)}
              </div>
            </button>
          ))}
        </div>
        <div className={styles.brandPane}>
          <div className={styles.brandPaneTitle}>{selected ? `Бренды: ${selected.label}` : "Бренды категории"}</div>
          <div className={styles.brandPaneHint}>{selected ? "Внутренний срез выбранной категории по обороту." : "Категория не выбрана."}</div>
          {selected?.brands?.length ? (
            <div className={styles.rankingList}>
              {selected.brands.slice(0, 4).map((brand) => (
                <div key={`${selected.label}-${brand.label}`} className={styles.rankingRow}>
                  <div className={styles.rankingRowHead}>
                    <div className={styles.rankingLabel}>{brand.label}</div>
                    <div className={styles.rankingValue}>{formatMoney(brand.value, currencyCode)}</div>
                  </div>
                  <div className={styles.rankingDetail}>
                    Прибыль: {formatMoney(brand.profit, currencyCode)} · Маржа: {formatPercent(brand.marginPct)}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className={styles.placeholderCard}>По выбранной категории бренды пока не выделены.</div>
          )}
        </div>
      </div>
    </div>
  );
}

function RankingCard({
  title,
  hint,
  rows,
  currencyCode,
  actionTo,
  actionLabel,
}: {
  title: string;
  hint: string;
  rows: Array<{ label: string; value: number; detail?: string }>;
  currencyCode?: string | null;
  actionTo?: string;
  actionLabel?: string;
}) {
  const maxValue = Math.max(1, ...rows.map((row) => row.value));
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelHead}>
        <div>
          <div className={styles.panelTitle}>{title}</div>
          <div className={styles.panelHint}>{hint}</div>
        </div>
        {actionTo && actionLabel ? <Link className={styles.panelAction} to={actionTo}>{actionLabel}</Link> : null}
      </div>
      <div className={styles.rankingList}>
        {rows.map((row) => (
          <div key={row.label} className={styles.rankingRow}>
            <div className={styles.rankingRowHead}>
              <div className={styles.rankingLabel}>{row.label}</div>
              <div className={styles.rankingValue}>{formatMoney(row.value, currencyCode)}</div>
            </div>
            <div className={styles.rankingBarTrack}>
              <div className={styles.rankingBarFill} style={{ width: `${(row.value / maxValue) * 100}%` }} />
            </div>
            {row.detail ? <div className={styles.rankingDetail}>{row.detail}</div> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

function StatusBreakdownCard({
  rows,
  total,
}: {
  rows: Array<{ label: string; count: number }>;
  total: number;
}) {
  const maxCount = Math.max(1, ...rows.map((row) => row.count));
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelTitle}>Статусы проблемных заказов</div>
      <div className={styles.panelHint}>Какие статусы сейчас дают основной проблемный хвост.</div>
      <div className={styles.rankingList}>
        {rows.map((row) => (
          <div key={row.label} className={styles.rankingRow}>
            <div className={styles.rankingRowHead}>
              <div className={styles.rankingLabel}>{row.label}</div>
              <div className={styles.rankingValue}>{formatNumber(row.count)}</div>
            </div>
            <div className={styles.rankingBarTrack}>
              <div className={styles.statusBarFill} style={{ width: `${(row.count / maxCount) * 100}%` }} />
            </div>
            <div className={styles.rankingDetail}>
              {total > 0 ? formatPercent((row.count / total) * 100) : "—"} от всех проблемных заказов
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function InsightCard({
  title,
  value,
  detail,
  tone = "neutral",
}: {
  title: string;
  value: string;
  detail: string;
  tone?: "neutral" | "positive" | "warn";
}) {
  return (
    <div className={`${styles.insightCard} ${styles[`insight${tone[0].toUpperCase()}${tone.slice(1)}`]}`}>
      <div className={styles.insightTitle}>{title}</div>
      <div className={styles.insightValue}>{value}</div>
      <div className={styles.insightDetail}>{detail}</div>
    </div>
  );
}

function compareDelta(current: number, previous: number) {
  if (!previous) return null;
  return ((current - previous) / previous) * 100;
}

function overviewDateRange(period: DashboardPeriod) {
  if (period === "today") {
    const date = toIsoDate(localDateOnly());
    return { dateFrom: date, dateTo: date, grain: "day" as const };
  }
  if (period === "yesterday") {
    const date = toIsoDate(shiftDate(localDateOnly(), -1));
    return { dateFrom: date, dateTo: date, grain: "day" as const };
  }
  const current = getCurrentPeriodRange(period);
  return {
    dateFrom: current.start,
    dateTo: current.end,
    grain: current.span <= 31 ? "day" as const : "month" as const,
  };
}

function buildOverviewLink(
  tab: "orders" | "problems" | "sku" | "category",
  options: {
    storeId: string;
    period: DashboardPeriod;
    dateMode?: "created" | "delivery";
  },
) {
  const params = new URLSearchParams();
  params.set("tab", tab);
  params.set("storeId", options.storeId);
  if (tab === "orders" || tab === "problems") {
    params.set("period", options.period);
  } else {
    const range = overviewDateRange(options.period);
    params.set("dateMode", options.dateMode || "created");
    params.set("grain", range.grain);
    params.set("date_from", range.dateFrom);
    params.set("date_to", range.dateTo);
  }
  return `/sales/overview?${params.toString()}`;
}

export default function Page() {
  const [context, setContext] = useState<ContextResp | null>(null);
  const [bundle, setBundle] = useState<DashboardBundle | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [period, setPeriod] = useState<DashboardPeriod>("today");
  const [storeId, setStoreId] = useState("all");
  const [chartRange, setChartRange] = useState<ChartRange>("7d");
  const [analysisView, setAnalysisView] = useState<AnalysisView>("sku");
  const [selectedCategoryLabel, setSelectedCategoryLabel] = useState("");

  useEffect(() => {
    let active = true;
    async function loadDashboard() {
      setLoading(true);
      setError("");
      try {
        const summary = await apiGetOk<DashboardSummaryResp>(
          `/api/sales/overview/dashboard-summary?store_id=${encodeURIComponent(storeId)}&period=${encodeURIComponent(period)}`,
        );
        if (!active) return;
        setContext(summary.context);
        setBundle(summary.bundle);
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (active) setLoading(false);
      }
    }
    void loadDashboard();
    return () => {
      active = false;
    };
  }, [period, storeId]);

  const stores = useMemo(() => context?.marketplace_stores || [], [context]);
  const selectedStore = useMemo(() => stores.find((store) => String(store.store_id) === String(storeId)) || null, [stores, storeId]);
  const currencyCode = selectedStore?.currency_code || stores[0]?.currency_code || "RUB";
  const selectedDayLabel = period === "yesterday" ? "Вчера" : "Сегодня";
  const selectedDayDate = period === "yesterday" ? shiftDate(localDateOnly(), -1) : localDateOnly();
  const selectedOverviewStoreId = storeId === "all" ? "all" : storeId;
  const allTrendDays = useMemo(
    () =>
      (bundle?.tracking?.years || [])
        .flatMap((year) => year.months || [])
        .flatMap((month) => month.days || [])
        .filter((day) => day.date)
        .sort((a, b) => String(a.date).localeCompare(String(b.date))),
    [bundle?.tracking?.years],
  );

  const topSku = (bundle?.sku?.rows || []).slice(0, 4).map((row) => ({
    label: row.label || row.item_name || row.sku || "SKU",
    value: Number(row.revenue || 0),
    detail: `Прибыль: ${formatMoney(row.profit_amount, currencyCode)} · Маржа: ${formatPercent(row.profit_pct)}`,
  }));
  const categoryAggregates = useMemo(() => {
    const backendGroups = bundle?.category?.category_groups || [];
    return backendGroups.length ? backendGroups : buildCategoryAggregates(bundle?.category?.rows || []);
  }, [bundle?.category?.category_groups, bundle?.category?.rows]);
  const problematicStatuses = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of bundle?.problems?.rows || []) {
      const key = String(row.item_status || "Не определено").trim() || "Не определено";
      map.set(key, (map.get(key) || 0) + 1);
    }
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]).slice(0, 4);
  }, [bundle?.problems?.rows]);
  const problemStatusRows = problematicStatuses.map(([label, count]) => ({ label, count }));
  const todayRevenue = sumBy(bundle?.today?.rows || [], (row) => row.sale_price);
  const yesterdayRevenue = sumBy(bundle?.yesterday?.rows || [], (row) => row.sale_price);
  const todayProfit = sumBy(bundle?.today?.rows || [], (row) => row.profit);
  const yesterdayProfit = sumBy(bundle?.yesterday?.rows || [], (row) => row.profit);
  const previousRevenue = sumBy(bundle?.previousOrders?.rows || [], (row) => row.sale_price);
  const previousProfit = sumBy(bundle?.previousOrders?.rows || [], (row) => row.profit);
  const todayOrdersCount = Number(bundle?.today?.kpis?.orders_count || (bundle?.today?.rows || []).length);
  const yesterdayOrdersCount = Number(bundle?.yesterday?.kpis?.orders_count || (bundle?.yesterday?.rows || []).length);
  const previousOrdersCount = Number(bundle?.previousOrders?.kpis?.orders_count || (bundle?.previousOrders?.rows || []).length);
  const todayProblemsCount = Number(bundle?.todayProblems?.total_count || 0);
  const yesterdayProblemsCount = Number(bundle?.yesterdayProblems?.total_count || 0);
  const previousProblemsCount = Number(bundle?.previousProblems?.total_count || 0);
  const todayOrdersDeltaPct = compareDelta(todayOrdersCount, yesterdayOrdersCount);
  const chartSpan = chartRange === "30d" ? 30 : 7;
  const chartRangeWindow = useMemo(() => {
    const end = localDateOnly();
    const start = shiftDate(end, -(chartSpan - 1));
    return { start: toIsoDate(start), end: toIsoDate(end) };
  }, [chartSpan]);
  const trendDays = useMemo(() => {
    return allTrendDays.filter((day) => day.date >= chartRangeWindow.start && day.date <= chartRangeWindow.end);
  }, [allTrendDays, chartRangeWindow.end, chartRangeWindow.start]);
  useEffect(() => {
    if (!categoryAggregates.length) {
      setSelectedCategoryLabel("");
      return;
    }
    if (!categoryAggregates.some((item) => item.label === selectedCategoryLabel)) {
      setSelectedCategoryLabel(categoryAggregates[0].label);
    }
  }, [categoryAggregates, selectedCategoryLabel]);
  const revenueSpark = trendDays.map((day) => Number(day.revenue || 0));
  const profitSpark = trendDays.map((day) => Number(day.profit_amount || 0));
  const marginSpark = trendDays.map((day) => Number(day.profit_pct || 0));
  const chartRangeLabel = `${formatLongDate(chartRangeWindow.start)} - ${formatLongDate(chartRangeWindow.end)}`;
  const selectedDayRevenue = period === "yesterday" ? yesterdayRevenue : todayRevenue;
  const selectedDayProfit = period === "yesterday" ? yesterdayProfit : todayProfit;
  const selectedDayOrdersCount = period === "yesterday" ? yesterdayOrdersCount : todayOrdersCount;
  const selectedDayProblemsCount = period === "yesterday" ? yesterdayProblemsCount : todayProblemsCount;
  const selectedDayOrdersLoadedAt = period === "yesterday" ? bundle?.yesterday?.loaded_at : bundle?.today?.loaded_at;
  const selectedDayMarginPct = selectedDayRevenue > 0 ? (selectedDayProfit / selectedDayRevenue) * 100 : null;
  const selectedRevenueDeltaPct = period === "yesterday" ? compareDelta(yesterdayRevenue, todayRevenue) : compareDelta(todayRevenue, yesterdayRevenue);
  const selectedProfitDeltaPct = period === "yesterday" ? compareDelta(yesterdayProfit, todayProfit) : compareDelta(todayProfit, yesterdayProfit);
  const selectedOrdersDeltaPct = period === "yesterday" ? compareDelta(yesterdayOrdersCount, todayOrdersCount) : todayOrdersDeltaPct;
  const selectedProblemsDeltaPct = period === "yesterday" ? compareDelta(yesterdayProblemsCount, todayProblemsCount) : compareDelta(todayProblemsCount, yesterdayProblemsCount);
  const averageDayRevenue = trendDays.length ? trendDays.reduce((acc, day) => acc + Number(day.revenue || 0), 0) / trendDays.length : null;
  const averageDayProfit = trendDays.length ? trendDays.reduce((acc, day) => acc + Number(day.profit_amount || 0), 0) / trendDays.length : null;
  const isSelectedDayEmpty = selectedDayOrdersCount === 0 && selectedDayRevenue === 0 && selectedDayProblemsCount === 0;
  const chartTitle = "Динамика оборота и прибыли";
  const chartHint = `Последние ${chartSpan} дней. Сейчас: ${chartRangeLabel}.`;
  const chartEmptyText = `За последние ${chartSpan} дней в графике пока нет дневных точек.`;

  return (
    <PageFrame title="Сводка" subtitle="Оперативный слой по сегодняшнему и вчерашнему дню без глубоких периодных срезов.">
      <div className={layoutStyles.shell}>
        <WorkspaceSurface className={`${layoutStyles.heroSurface} ${styles.heroSurface}`}>
          <WorkspaceHeader
            title="Оперативная сводка"
            subtitle="Готовые дневные данные: оборот, прибыль, заказы и риски."
            meta={(
              <div className={layoutStyles.heroMeta}>
                <span className={layoutStyles.metaChip}>{selectedStore ? selectedStore.label : "Все магазины"}</span>
                <span className={layoutStyles.metaChip}>{PERIOD_OPTIONS.find((item) => item.value === period)?.label}</span>
              </div>
            )}
          />
          <WorkspaceToolbar className={`${layoutStyles.toolbar} ${styles.heroToolbar}`}>
            <div className={`${layoutStyles.toolbarGroup} ${styles.toolbarFilters}`}>
              <select className="input input-size-lg" value={storeId} onChange={(e) => setStoreId(e.target.value)}>
                <option value="all">Все магазины</option>
                {stores.map((store) => (
                  <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                ))}
              </select>
              <select className="input input-size-md" value={period} onChange={(e) => setPeriod(e.target.value as DashboardPeriod)}>
                {PERIOD_OPTIONS.map((item) => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </div>
            <div className={`${layoutStyles.toolbarGroup} ${styles.toolbarActions}`}>
              <Link className="btn ghost" to={buildOverviewLink("orders", { storeId: selectedOverviewStoreId, period })}>Обзор</Link>
              <Link className={`btn ghost ${styles.monitoringLink}`} to="/settings/monitoring">Мониторинг</Link>
            </div>
          </WorkspaceToolbar>
        </WorkspaceSurface>

        {error ? <div className="status error">{error}</div> : null}
        {loading ? <div className="status">Собираем сводку по продажам...</div> : null}

        {!loading && !error ? (
          <>
            <section className={styles.kpiGrid}>
              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Оборот</div>
                    <div className={styles.kpiValue}>{formatMoney(selectedDayRevenue, currencyCode)}</div>
                  </div>
                  <Sparkline values={revenueSpark} tone="cyan" />
                </div>
                <div className={styles.kpiMeta}>
                  <span>{selectedDayLabel} · {formatLongDate(selectedDayDate)}</span>
                  <span className={selectedRevenueDeltaPct != null && selectedRevenueDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    vs день {formatPercent(selectedRevenueDeltaPct)}
                  </span>
                </div>
              </div>

              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Прибыль</div>
                    <div className={styles.kpiValue}>{formatMoney(selectedDayProfit, currencyCode)}</div>
                  </div>
                  <Sparkline values={profitSpark} tone="green" />
                </div>
                <div className={styles.kpiMeta}>
                  <span>Маржа: {formatPercent(selectedDayMarginPct)}</span>
                  <span className={selectedProfitDeltaPct != null && selectedProfitDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    vs день {formatPercent(selectedProfitDeltaPct)}
                  </span>
                </div>
              </div>

              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Заказы дня</div>
                    <div className={styles.kpiValue}>{formatNumber(selectedDayOrdersCount)}</div>
                  </div>
                  <Sparkline values={marginSpark} tone="amber" />
                </div>
                <div className={styles.kpiMeta}>
                  <span>Средний соинвест: {formatPercent(period === "yesterday" ? bundle?.yesterday?.kpis?.avg_coinvest_pct : bundle?.today?.kpis?.avg_coinvest_pct)}</span>
                  <span className={selectedOrdersDeltaPct != null && selectedOrdersDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    vs день {formatPercent(selectedOrdersDeltaPct)}
                  </span>
                </div>
              </div>

              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Проблемные заказы</div>
                    <div className={styles.kpiValue}>{formatNumber(selectedDayProblemsCount)}</div>
                  </div>
                  <div className={styles.kpiBadge}>Риски</div>
                </div>
                <div className={styles.kpiMeta}>
                  <span className={selectedProblemsDeltaPct != null && selectedProblemsDeltaPct <= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    vs день {formatPercent(selectedProblemsDeltaPct)}
                  </span>
                  <span>{selectedDayOrdersLoadedAt ? `Обновлено: ${formatDateTime(selectedDayOrdersLoadedAt)}` : "Дневной срез"}</span>
                </div>
              </div>
            </section>

            <section className={styles.dashboardGrid}>
              <TrendChart
                days={trendDays}
                currencyCode={currencyCode}
                title={chartTitle}
                hint={chartHint}
                emptyText={chartEmptyText}
                controls={(
                  <div className={styles.rangeTabs}>
                    <button
                      type="button"
                      className={`${styles.rangeTab} ${chartRange === "7d" ? styles.rangeTabActive : ""}`}
                      onClick={() => setChartRange("7d")}
                    >
                      7 дней
                    </button>
                    <button
                      type="button"
                      className={`${styles.rangeTab} ${chartRange === "30d" ? styles.rangeTabActive : ""}`}
                      onClick={() => setChartRange("30d")}
                    >
                      30 дней
                    </button>
                  </div>
                )}
              />

              <div className={styles.sideStack}>
                <div className={styles.panelCard}>
                <div className={styles.panelTitle}>Ритм последних дней</div>
                <div className={styles.panelHint}>{chartRangeLabel}</div>
                  <div className={styles.tempoMiniGrid}>
                    <div className={styles.tempoMiniCard}>
                      <span>Средний оборот</span>
                      <strong>{formatMoney(averageDayRevenue, currencyCode)}</strong>
                    </div>
                    <div className={styles.tempoMiniCard}>
                      <span>Средняя прибыль</span>
                      <strong>{formatMoney(averageDayProfit, currencyCode)}</strong>
                    </div>
                    <div className={styles.tempoMiniCard}>
                      <span>Активных дней</span>
                      <strong>{formatNumber(trendDays.length)}</strong>
                    </div>
                  </div>
                </div>

                <div className={styles.insightGrid}>
                  {isSelectedDayEmpty ? (
                    <InsightCard
                      title="Статус дня"
                      value="День еще пустой"
                      detail="Новых заказов за выбранный день пока нет, поэтому сводка показывает нулевой дневной срез без подмены месячными данными."
                      tone="neutral"
                    />
                  ) : null}
                  <InsightCard
                    title="Средний день"
                    value={formatMoney(averageDayRevenue, currencyCode)}
                    detail={`Активных дней: ${formatNumber(trendDays.length)}`}
                    tone="positive"
                  />
                  <InsightCard
                    title="Топ проблема"
                    value={problematicStatuses[0] ? problematicStatuses[0][0] : "Нет сигнала"}
                    detail={problematicStatuses[0] ? `${formatNumber(problematicStatuses[0][1])} заказов` : "Проблемные заказы не обнаружены"}
                    tone="warn"
                  />
                  <InsightCard
                    title="Лидер SKU"
                    value={topSku[0]?.label || "Нет данных"}
                    detail={topSku[0] ? formatMoney(topSku[0].value, currencyCode) : "Срез пока пуст"}
                    tone="neutral"
                  />
                </div>
              </div>
            </section>

            <section className={styles.analysisSection}>
              <div className={styles.panelHead}>
                <div>
                  <div className={styles.panelTitle}>Быстрые срезы</div>
                  <div className={styles.panelHint}>Один операционный блок без длинного хвоста из трех карточек подряд.</div>
                </div>
                <div className={styles.analysisTabs}>
                  <button
                    type="button"
                    className={`${styles.analysisTab} ${analysisView === "sku" ? styles.analysisTabActive : ""}`}
                    onClick={() => setAnalysisView("sku")}
                  >
                    SKU
                  </button>
                  <button
                    type="button"
                    className={`${styles.analysisTab} ${analysisView === "category" ? styles.analysisTabActive : ""}`}
                    onClick={() => setAnalysisView("category")}
                  >
                    Категории
                  </button>
                  <button
                    type="button"
                    className={`${styles.analysisTab} ${analysisView === "status" ? styles.analysisTabActive : ""}`}
                    onClick={() => setAnalysisView("status")}
                  >
                    Риски
                  </button>
                </div>
              </div>
              <div className={styles.analysisViewport}>
                {analysisView === "sku" ? (
                  <RankingCard
                    title="Топ SKU"
                    hint="Лидеры выбранного операционного дня по обороту."
                    rows={topSku}
                    currencyCode={currencyCode}
                    actionTo={buildOverviewLink("sku", { storeId: selectedOverviewStoreId, period })}
                    actionLabel="Открыть товары"
                  />
                ) : null}
                {analysisView === "category" ? (
                  <CategoryDrilldownCard
                    categories={categoryAggregates}
                    currencyCode={currencyCode}
                    selectedCategory={selectedCategoryLabel}
                    onSelectCategory={setSelectedCategoryLabel}
                    actionTo={buildOverviewLink("category", { storeId: selectedOverviewStoreId, period })}
                  />
                ) : null}
                {analysisView === "status" ? (
                  <StatusBreakdownCard rows={problemStatusRows} total={Number(bundle?.problems?.total_count || 0)} />
                ) : null}
              </div>
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
