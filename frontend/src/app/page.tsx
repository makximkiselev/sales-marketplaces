import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { apiGetOk } from "../lib/api";
import { PageFrame } from "../components/page/PageKit";
import { SectionBlock } from "../components/page/SectionKit";
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
  sale_price?: number | null;
  profit?: number | null;
  item_name?: string;
  sku?: string;
  item_status?: string;
};

type OrdersResp = {
  ok: boolean;
  rows?: OrderRow[];
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
  loaded_at?: string;
};

type TrackingDay = {
  date: string;
  revenue?: number | null;
  revenue_plan_amount?: number | null;
  profit_amount?: number | null;
  profit_plan_amount?: number | null;
  profit_pct?: number | null;
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
};

type StoreComparison = {
  storeId: string;
  label: string;
  platformLabel: string;
  revenue: number;
  profit: number;
  orders: number;
  marginPct: number | null;
};

type DashboardPeriod = "week" | "month" | "quarter";

const PERIOD_OPTIONS: Array<{ value: DashboardPeriod; label: string }> = [
  { value: "week", label: "7 дней" },
  { value: "month", label: "30 дней" },
  { value: "quarter", label: "90 дней" },
];

function moneySign(currencyCode: string | undefined | null) {
  return String(currencyCode || "").trim().toUpperCase() === "USD" ? "$" : "₽";
}

function formatMoney(value: number | null | undefined, currencyCode?: string | null) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value)).toLocaleString("ru-RU")} ${moneySign(currencyCode)}`;
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

function getActiveMonth(tracking: TrackingResp | null) {
  const years = tracking?.years || [];
  const activeKey = String(tracking?.active_month_key || "").trim();
  for (const year of years) {
    for (const month of year.months || []) {
      if (activeKey && month.month_key === activeKey) return month;
      if (month.is_active) return month;
    }
  }
  return years[0]?.months?.[0] || null;
}

function buildPolyline(values: number[], width: number, height: number) {
  if (!values.length) return "";
  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = max - min || 1;
  return values.map((value, index) => {
    const x = values.length === 1 ? width / 2 : (index / (values.length - 1)) * width;
    const y = height - ((value - min) / range) * height;
    return `${x},${y}`;
  }).join(" ");
}

function Sparkline({
  values,
  tone = "cyan",
}: {
  values: number[];
  tone?: "cyan" | "green" | "amber";
}) {
  const points = buildPolyline(values, 120, 32);
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
}: {
  days: TrackingDay[];
  currencyCode?: string | null;
}) {
  const chartWidth = 760;
  const chartHeight = 280;
  const pad = { top: 24, right: 20, bottom: 34, left: 52 };
  const innerWidth = chartWidth - pad.left - pad.right;
  const innerHeight = chartHeight - pad.top - pad.bottom;
  const revenueValues = days.map((day) => Number(day.revenue || 0));
  const profitValues = days.map((day) => Number(day.profit_amount || 0));
  const maxValue = Math.max(1, ...revenueValues, ...profitValues);
  const minProfit = Math.min(0, ...profitValues);
  const range = maxValue - minProfit || 1;

  const pointX = (index: number) => (days.length <= 1 ? pad.left : pad.left + (index / (days.length - 1)) * innerWidth);
  const pointY = (value: number) => pad.top + innerHeight - ((value - minProfit) / range) * innerHeight;

  const revenuePath = days.map((day, index) => `${index === 0 ? "M" : "L"} ${pointX(index)} ${pointY(Number(day.revenue || 0))}`).join(" ");
  const profitPath = days.map((day, index) => `${index === 0 ? "M" : "L"} ${pointX(index)} ${pointY(Number(day.profit_amount || 0))}`).join(" ");
  const gridValues = [0, 0.25, 0.5, 0.75, 1];

  return (
    <div className={styles.chartCard}>
      <div className={styles.chartHead}>
        <div>
          <div className={styles.panelTitle}>Оборот и прибыль по дням</div>
          <div className={styles.panelHint}>Текущий активный месяц из обзора продаж.</div>
        </div>
        <div className={styles.chartLegend}>
          <span><i className={styles.legendRevenue} /> Оборот</span>
          <span><i className={styles.legendProfit} /> Прибыль</span>
        </div>
      </div>
      <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className={styles.chartSvg} role="img" aria-label="Динамика оборота и прибыли по дням">
        {gridValues.map((tick) => {
          const y = pad.top + innerHeight - tick * innerHeight;
          return <line key={tick} x1={pad.left} y1={y} x2={chartWidth - pad.right} y2={y} className={styles.chartGrid} />;
        })}
        <path d={revenuePath} className={styles.chartRevenue} />
        <path d={profitPath} className={styles.chartProfit} />
        {days.map((day, index) => (
          <g key={`${day.date}-${index}`}>
            <circle cx={pointX(index)} cy={pointY(Number(day.revenue || 0))} r="3.2" className={styles.chartPointRevenue} />
            <circle cx={pointX(index)} cy={pointY(Number(day.profit_amount || 0))} r="3.2" className={styles.chartPointProfit} />
            {index % Math.max(1, Math.ceil(days.length / 6)) === 0 ? (
              <text x={pointX(index)} y={chartHeight - 10} textAnchor="middle" className={styles.chartLabel}>
                {formatShortDate(day.date)}
              </text>
            ) : null}
          </g>
        ))}
        <text x={pad.left} y={14} className={styles.chartScaleLabel}>{formatMoney(maxValue, currencyCode)}</text>
        <text x={pad.left} y={chartHeight - 10} className={styles.chartScaleLabel}>{formatMoney(minProfit, currencyCode)}</text>
      </svg>
    </div>
  );
}

function RankingCard({
  title,
  hint,
  rows,
  currencyCode,
}: {
  title: string;
  hint: string;
  rows: Array<{ label: string; value: number; detail?: string }>;
  currencyCode?: string | null;
}) {
  const maxValue = Math.max(1, ...rows.map((row) => row.value));
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelTitle}>{title}</div>
      <div className={styles.panelHint}>{hint}</div>
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

function StoreComparisonCard({ rows, currencyCode }: { rows: StoreComparison[]; currencyCode?: string | null }) {
  const maxRevenue = Math.max(1, ...rows.map((row) => row.revenue));
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelTitle}>Сравнение магазинов</div>
      <div className={styles.panelHint}>Сводка по обороту, прибыли и марже за выбранный период.</div>
      <div className={styles.storeList}>
        {rows.map((row) => (
          <div key={row.storeId} className={styles.storeRow}>
            <div className={styles.storeRowHead}>
              <div>
                <div className={styles.storeLabel}>{row.label}</div>
                <div className={styles.storeMeta}>{row.platformLabel} · {formatNumber(row.orders)} заказов</div>
              </div>
              <div className={styles.storeRevenue}>{formatMoney(row.revenue, currencyCode)}</div>
            </div>
            <div className={styles.rankingBarTrack}>
              <div className={styles.storeBarFill} style={{ width: `${(row.revenue / maxRevenue) * 100}%` }} />
            </div>
            <div className={styles.storeStats}>
              <span>Прибыль: {formatMoney(row.profit, currencyCode)}</span>
              <span>Маржа: {formatPercent(row.marginPct)}</span>
            </div>
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

function WatchlistCard({
  title,
  hint,
  rows,
  currencyCode,
}: {
  title: string;
  hint: string;
  rows: Array<{ label: string; revenue: number; profit: number; marginPct: number | null }>;
  currencyCode?: string | null;
}) {
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelTitle}>{title}</div>
      <div className={styles.panelHint}>{hint}</div>
      <div className={styles.watchList}>
        {rows.map((row) => (
          <div key={row.label} className={styles.watchRow}>
            <div className={styles.watchRowHead}>
              <div className={styles.rankingLabel}>{row.label}</div>
              <div className={styles.watchMargin}>{formatPercent(row.marginPct)}</div>
            </div>
            <div className={styles.storeStats}>
              <span>Оборот: {formatMoney(row.revenue, currencyCode)}</span>
              <span>Прибыль: {formatMoney(row.profit, currencyCode)}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function DataFlowCard({ flows }: { flows: DataFlowItem[] }) {
  return (
    <div className={styles.rankingCard}>
      <div className={styles.panelTitle}>Слой данных</div>
      <div className={styles.panelHint}>Какие потоки сейчас питают продажи и насколько они свежие.</div>
      <div className={styles.flowList}>
        {flows.length === 0 ? (
          <div className={styles.placeholderCard}>Нет активных потоков данных</div>
        ) : (
          flows.map((flow) => (
            <div key={flow.code} className={styles.flowRow}>
              <div className={styles.flowRowHead}>
                <div>
                  <div className={styles.flowLabel}>{flow.label}</div>
                  {flow.description ? <div className={styles.flowHint}>{flow.description}</div> : null}
                </div>
                <span className={styles.flowCode}>{flow.code}</span>
              </div>
              <div className={styles.flowMeta}>
                <span>Период: {flow.date_from ? formatShortDate(flow.date_from) : "—"} - {flow.date_to ? formatShortDate(flow.date_to) : "—"}</span>
                <span>Загрузка: {formatDateTime(flow.loaded_at)}</span>
              </div>
            </div>
          ))
        )}
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

export default function Page() {
  const [context, setContext] = useState<ContextResp | null>(null);
  const [bundle, setBundle] = useState<DashboardBundle | null>(null);
  const [storeComparison, setStoreComparison] = useState<StoreComparison[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [period, setPeriod] = useState<DashboardPeriod>("month");
  const [storeId, setStoreId] = useState("all");

  useEffect(() => {
    let active = true;
    apiGetOk<ContextResp>("/api/sales/overview/context")
      .then((data) => {
        if (!active) return;
        setContext(data);
      })
      .catch((e) => {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!context) return;
    let active = true;
    const storesCtx = context.marketplace_stores || [];
    async function loadDashboard() {
      setLoading(true);
      setError("");
      try {
        const storeQuery = storeId === "all" ? "" : `store_id=${encodeURIComponent(storeId)}&`;
        const [tracking, orders, problems, dataFlow, sku, category, today, yesterday, todayProblems, yesterdayProblems] = await Promise.all([
          apiGetOk<TrackingResp>(`/api/sales/overview/tracking?store_id=${encodeURIComponent(storeId === "all" ? "all" : storeId)}&date_mode=created`),
          apiGetOk<OrdersResp>(`/api/sales/overview/united-orders?${storeQuery}period=${encodeURIComponent(period)}&page=1&page_size=200`),
          apiGetOk<ProblemOrdersResp>(`/api/sales/overview/problem-orders?${storeQuery}period=${encodeURIComponent(period)}&page=1&page_size=50`),
          apiGetOk<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(storeId === "all" ? "" : storeId)}`),
          apiGetOk<RetrospectiveResp>(`/api/sales/overview/retrospective?${storeQuery}group_by=sku&grain=month&date_mode=created&limit=6`),
          apiGetOk<RetrospectiveResp>(`/api/sales/overview/retrospective?${storeQuery}group_by=category&grain=month&date_mode=created&limit=6`),
          apiGetOk<OrdersResp>(`/api/sales/overview/united-orders?${storeQuery}period=today&page=1&page_size=200`),
          apiGetOk<OrdersResp>(`/api/sales/overview/united-orders?${storeQuery}period=yesterday&page=1&page_size=200`),
          apiGetOk<ProblemOrdersResp>(`/api/sales/overview/problem-orders?${storeQuery}period=today&page=1&page_size=50`),
          apiGetOk<ProblemOrdersResp>(`/api/sales/overview/problem-orders?${storeQuery}period=yesterday&page=1&page_size=50`),
        ]);

        const comparison = await Promise.all(
          storesCtx.map(async (store) => {
            const response = await apiGetOk<OrdersResp>(
              `/api/sales/overview/united-orders?store_id=${encodeURIComponent(store.store_id)}&period=${encodeURIComponent(period)}&page=1&page_size=200`,
            );
            const rows = response.rows || [];
            const revenue = sumBy(rows, (row) => row.sale_price);
            const profit = sumBy(rows, (row) => row.profit);
            return {
              storeId: store.store_id,
              label: store.label,
              platformLabel: store.platform_label,
              revenue,
              profit,
              orders: Number(response.kpis?.orders_count || rows.length),
              marginPct: revenue > 0 ? (profit / revenue) * 100 : null,
            } satisfies StoreComparison;
          }),
        );

        if (!active) return;
        setBundle({ tracking, orders, problems, dataFlow, sku, category, today, yesterday, todayProblems, yesterdayProblems });
        setStoreComparison(comparison.sort((a, b) => b.revenue - a.revenue));
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
  }, [context, period, storeId]);

  const stores = useMemo(() => context?.marketplace_stores || [], [context]);
  const selectedStore = useMemo(() => stores.find((store) => String(store.store_id) === String(storeId)) || null, [stores, storeId]);
  const currencyCode = selectedStore?.currency_code || stores[0]?.currency_code || "RUB";
  const activeMonth = useMemo(() => getActiveMonth(bundle?.tracking || null), [bundle?.tracking]);
  const trendDays = useMemo(() => (activeMonth?.days || []).filter((day) => day.date), [activeMonth]);

  const revenueTotal = sumBy(bundle?.orders?.rows || [], (row) => row.sale_price);
  const profitTotal = sumBy(bundle?.orders?.rows || [], (row) => row.profit);
  const marginPct = revenueTotal > 0 ? (profitTotal / revenueTotal) * 100 : null;
  const planRevenue = Number(activeMonth?.revenue_plan_amount || 0);
  const planProfit = Number(activeMonth?.profit_plan_amount || 0);
  const revenueDelta = planRevenue > 0 ? revenueTotal - planRevenue : null;
  const profitDelta = planProfit > 0 ? profitTotal - planProfit : null;
  const topSku = (bundle?.sku?.rows || []).slice(0, 5).map((row) => ({
    label: row.label || row.item_name || row.sku || "SKU",
    value: Number(row.revenue || 0),
    detail: `Прибыль: ${formatMoney(row.profit_amount, currencyCode)} · Маржа: ${formatPercent(row.profit_pct)}`,
  }));
  const topCategories = (bundle?.category?.rows || []).slice(0, 5).map((row) => ({
    label: row.label || row.category_path || "Категория",
    value: Number(row.revenue || 0),
    detail: `Прибыль: ${formatMoney(row.profit_amount, currencyCode)} · Маржа: ${formatPercent(row.profit_pct)}`,
  }));
  const problematicStatuses = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of bundle?.problems?.rows || []) {
      const key = String(row.item_status || "Не определено").trim() || "Не определено";
      map.set(key, (map.get(key) || 0) + 1);
    }
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]).slice(0, 3);
  }, [bundle?.problems?.rows]);
  const problemStatusRows = problematicStatuses.map(([label, count]) => ({ label, count }));
  const revenueSpark = trendDays.map((day) => Number(day.revenue || 0));
  const profitSpark = trendDays.map((day) => Number(day.profit_amount || 0));
  const marginSpark = trendDays.map((day) => Number(day.profit_pct || 0));
  const todayRevenue = sumBy(bundle?.today?.rows || [], (row) => row.sale_price);
  const yesterdayRevenue = sumBy(bundle?.yesterday?.rows || [], (row) => row.sale_price);
  const todayProfit = sumBy(bundle?.today?.rows || [], (row) => row.profit);
  const yesterdayProfit = sumBy(bundle?.yesterday?.rows || [], (row) => row.profit);
  const todayOrdersCount = Number(bundle?.today?.kpis?.orders_count || (bundle?.today?.rows || []).length);
  const yesterdayOrdersCount = Number(bundle?.yesterday?.kpis?.orders_count || (bundle?.yesterday?.rows || []).length);
  const todayProblemsCount = Number(bundle?.todayProblems?.total_count || 0);
  const yesterdayProblemsCount = Number(bundle?.yesterdayProblems?.total_count || 0);
  const todayRevenueDeltaPct = compareDelta(todayRevenue, yesterdayRevenue);
  const todayProfitDeltaPct = compareDelta(todayProfit, yesterdayProfit);
  const todayOrdersDeltaPct = compareDelta(todayOrdersCount, yesterdayOrdersCount);
  const todayProblemsDeltaPct = compareDelta(todayProblemsCount, yesterdayProblemsCount);
  const activeDaysCount = Math.max(1, Number(bundle?.tracking?.kpis?.days || trendDays.length || 1));
  const averageDailyRevenue = activeDaysCount > 0 ? Number(bundle?.tracking?.kpis?.revenue || 0) / activeDaysCount : 0;
  const todayVsAveragePct = compareDelta(todayRevenue, averageDailyRevenue);
  const todayMarginPct = todayRevenue > 0 ? (todayProfit / todayRevenue) * 100 : null;
  const yesterdayMarginPct = yesterdayRevenue > 0 ? (yesterdayProfit / yesterdayRevenue) * 100 : null;
  const todayMarginDeltaPct = todayMarginPct != null && yesterdayMarginPct != null ? todayMarginPct - yesterdayMarginPct : null;
  const periodProblemShare = Number(bundle?.orders?.kpis?.orders_count || 0) > 0
    ? (Number(bundle?.problems?.total_count || 0) / Math.max(1, Number(bundle?.orders?.kpis?.orders_count || 0))) * 100
    : null;
  const todayProblemShare = todayOrdersCount > 0 ? (todayProblemsCount / todayOrdersCount) * 100 : null;
  const weakestSku = (bundle?.sku?.rows || [])
    .filter((row) => Number(row.revenue || 0) > 0)
    .sort((a, b) => Number(a.profit_pct ?? 999999) - Number(b.profit_pct ?? 999999))
    .slice(0, 4)
    .map((row) => ({
      label: row.label || row.item_name || row.sku || "SKU",
      revenue: Number(row.revenue || 0),
      profit: Number(row.profit_amount || 0),
      marginPct: row.profit_pct ?? null,
    }));
  const weakestCategories = (bundle?.category?.rows || [])
    .filter((row) => Number(row.revenue || 0) > 0)
    .sort((a, b) => Number(a.profit_pct ?? 999999) - Number(b.profit_pct ?? 999999))
    .slice(0, 4)
    .map((row) => ({
      label: row.label || row.category_path || "Категория",
      revenue: Number(row.revenue || 0),
      profit: Number(row.profit_amount || 0),
      marginPct: row.profit_pct ?? null,
    }));
  const flowRows = bundle?.dataFlow?.flows || [];
  const freshestLoadAt = [
    bundle?.orders?.loaded_at,
    bundle?.problems?.loaded_at,
    bundle?.tracking?.loaded_at,
    ...flowRows.map((flow) => flow.loaded_at),
  ]
    .filter(Boolean)
    .sort()
    .at(-1);

  return (
    <PageFrame title="Сводка" subtitle="Финальный dashboard по продажам, эффективности и зонам риска.">
      <div className={layoutStyles.shell}>
        <WorkspaceSurface className={`${layoutStyles.heroSurface} ${styles.heroSurface}`}>
          <WorkspaceHeader
            title="Executive dashboard"
            subtitle="Сводка собирает ключевые сигналы из обзора продаж: оборот, прибыль, маржу, проблемные заказы, лидеров роста и отстающие зоны."
            meta={(
              <div className={layoutStyles.heroMeta}>
                <span className={layoutStyles.metaChip}>{selectedStore ? selectedStore.label : "Все магазины"}</span>
                <span className={layoutStyles.metaChip}>{PERIOD_OPTIONS.find((item) => item.value === period)?.label}</span>
                <span className={layoutStyles.metaChip}>{bundle?.orders?.loaded_at ? `Обновлено: ${formatDateTime(bundle.orders.loaded_at)}` : "Ожидание данных"}</span>
              </div>
            )}
          />
          <WorkspaceToolbar className={layoutStyles.toolbar}>
            <div className={layoutStyles.toolbarGroup}>
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
            <div className={layoutStyles.toolbarGroup}>
              <Link className="btn ghost" to="/sales/overview">Открыть обзор</Link>
              <Link className="btn ghost" to="/settings/monitoring">Мониторинг</Link>
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
                    <div className={styles.kpiValue}>{formatMoney(revenueTotal, currencyCode)}</div>
                  </div>
                  <Sparkline values={revenueSpark} tone="cyan" />
                </div>
                <div className={styles.kpiMeta}>
                  <span>План: {formatMoney(planRevenue, currencyCode)}</span>
                  <span className={revenueDelta != null && revenueDelta >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    Δ {formatMoney(revenueDelta, currencyCode)}
                  </span>
                </div>
              </div>

              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Прибыль</div>
                    <div className={styles.kpiValue}>{formatMoney(profitTotal, currencyCode)}</div>
                  </div>
                  <Sparkline values={profitSpark} tone="green" />
                </div>
                <div className={styles.kpiMeta}>
                  <span>План: {formatMoney(planProfit, currencyCode)}</span>
                  <span className={profitDelta != null && profitDelta >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    Δ {formatMoney(profitDelta, currencyCode)}
                  </span>
                </div>
              </div>

              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Маржа</div>
                    <div className={styles.kpiValue}>{formatPercent(marginPct)}</div>
                  </div>
                  <Sparkline values={marginSpark} tone="amber" />
                </div>
                <div className={styles.kpiMeta}>
                  <span>Средний соинвест: {formatPercent(bundle?.orders?.kpis?.avg_coinvest_pct)}</span>
                  <span>Реклама: {formatMoney(bundle?.orders?.kpis?.additional_ads, currencyCode)}</span>
                </div>
              </div>

              <div className={styles.kpiCard}>
                <div className={styles.kpiHead}>
                  <div>
                    <div className={styles.kpiLabel}>Проблемные заказы</div>
                    <div className={styles.kpiValue}>{formatNumber(bundle?.problems?.total_count)}</div>
                  </div>
                  <div className={styles.kpiBadge}>Риски</div>
                </div>
                <div className={styles.kpiMeta}>
                  <span>Ошибки: {formatMoney(bundle?.orders?.kpis?.operational_errors, currencyCode)}</span>
                  <span>{bundle?.problems?.loaded_at ? `Обновлено: ${formatDateTime(bundle.problems.loaded_at)}` : "Требует контроля"}</span>
                </div>
              </div>
            </section>

            <section className={styles.dailyGrid}>
              <div className={styles.dailyCard}>
                <div className={styles.dailyTitle}>Сегодня</div>
                <div className={styles.dailyMetrics}>
                  <div className={styles.dailyMetric}>
                    <span>Оборот</span>
                    <strong>{formatMoney(todayRevenue, currencyCode)}</strong>
                    <em className={todayRevenueDeltaPct != null && todayRevenueDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                      vs вчера {formatPercent(todayRevenueDeltaPct)}
                    </em>
                  </div>
                  <div className={styles.dailyMetric}>
                    <span>Прибыль</span>
                    <strong>{formatMoney(todayProfit, currencyCode)}</strong>
                    <em className={todayProfitDeltaPct != null && todayProfitDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                      vs вчера {formatPercent(todayProfitDeltaPct)}
                    </em>
                  </div>
                  <div className={styles.dailyMetric}>
                    <span>Заказы</span>
                    <strong>{formatNumber(todayOrdersCount)}</strong>
                    <em className={todayOrdersDeltaPct != null && todayOrdersDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                      vs вчера {formatPercent(todayOrdersDeltaPct)}
                    </em>
                  </div>
                  <div className={styles.dailyMetric}>
                    <span>Проблемные</span>
                    <strong>{formatNumber(todayProblemsCount)}</strong>
                    <em className={todayProblemsDeltaPct != null && todayProblemsDeltaPct <= 0 ? styles.deltaPositive : styles.deltaNegative}>
                      vs вчера {formatPercent(todayProblemsDeltaPct)}
                    </em>
                  </div>
                </div>
              </div>

              <div className={styles.dailyCard}>
                <div className={styles.dailyTitle}>Вчера</div>
                <div className={styles.dailyMetrics}>
                  <div className={styles.dailyMetric}>
                    <span>Оборот</span>
                    <strong>{formatMoney(yesterdayRevenue, currencyCode)}</strong>
                    <em>{bundle?.yesterday?.loaded_at ? `Срез: ${formatDateTime(bundle.yesterday.loaded_at)}` : "Дневной срез"}</em>
                  </div>
                  <div className={styles.dailyMetric}>
                    <span>Прибыль</span>
                    <strong>{formatMoney(yesterdayProfit, currencyCode)}</strong>
                    <em>Маржа: {formatPercent(yesterdayRevenue > 0 ? (yesterdayProfit / yesterdayRevenue) * 100 : null)}</em>
                  </div>
                  <div className={styles.dailyMetric}>
                    <span>Заказы</span>
                    <strong>{formatNumber(yesterdayOrdersCount)}</strong>
                    <em>Средний соинвест: {formatPercent(bundle?.yesterday?.kpis?.avg_coinvest_pct)}</em>
                  </div>
                  <div className={styles.dailyMetric}>
                    <span>Проблемные</span>
                    <strong>{formatNumber(yesterdayProblemsCount)}</strong>
                    <em>{bundle?.yesterdayProblems?.loaded_at ? `Обновлено: ${formatDateTime(bundle.yesterdayProblems.loaded_at)}` : "Контроль качества"}</em>
                  </div>
                </div>
              </div>
            </section>

            <section className={styles.tempoGrid}>
              <div className={styles.tempoCard}>
                <div className={styles.tempoLabel}>Темп дня</div>
                <div className={styles.tempoValue}>{formatMoney(todayRevenue, currencyCode)}</div>
                <div className={styles.tempoMeta}>
                  <span>Средний активный день: {formatMoney(averageDailyRevenue, currencyCode)}</span>
                  <span className={todayVsAveragePct != null && todayVsAveragePct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    {todayVsAveragePct == null ? "—" : `${todayVsAveragePct >= 0 ? "+" : ""}${formatPercent(todayVsAveragePct)}`} к среднему дню
                  </span>
                </div>
              </div>
              <div className={styles.tempoCard}>
                <div className={styles.tempoLabel}>Маржа дня</div>
                <div className={styles.tempoValue}>{formatPercent(todayMarginPct)}</div>
                <div className={styles.tempoMeta}>
                  <span>Вчера: {formatPercent(yesterdayMarginPct)}</span>
                  <span className={todayMarginDeltaPct != null && todayMarginDeltaPct >= 0 ? styles.deltaPositive : styles.deltaNegative}>
                    Δ {formatPercent(todayMarginDeltaPct)}
                  </span>
                </div>
              </div>
              <div className={styles.tempoCard}>
                <div className={styles.tempoLabel}>Доля проблем</div>
                <div className={styles.tempoValue}>{formatPercent(todayProblemShare)}</div>
                <div className={styles.tempoMeta}>
                  <span>За сегодня: {formatNumber(todayProblemsCount)} / {formatNumber(todayOrdersCount)}</span>
                  <span>За период: {formatPercent(periodProblemShare)}</span>
                </div>
              </div>
              <div className={styles.tempoCard}>
                <div className={styles.tempoLabel}>Свежесть данных</div>
                <div className={styles.tempoValue}>{formatNumber(flowRows.length)}</div>
                <div className={styles.tempoMeta}>
                  <span>Активных потоков: {formatNumber(flowRows.length)}</span>
                  <span>{freshestLoadAt ? `Последняя загрузка: ${formatDateTime(freshestLoadAt)}` : "Нет отметки о загрузке"}</span>
                </div>
              </div>
            </section>

            <section className={styles.dashboardGrid}>
              <TrendChart days={trendDays} currencyCode={currencyCode} />

              <div className={styles.sideStack}>
                <div className={styles.panelCard}>
                  <div className={styles.panelTitle}>Выполнение плана</div>
                  <div className={styles.panelHint}>{activeMonth?.month_label || "Текущий месяц"}</div>
                  <div className={styles.progressGroup}>
                    <div className={styles.progressRow}>
                      <div className={styles.progressHead}>
                        <span>Оборот</span>
                        <strong>{planRevenue > 0 ? formatPercent((revenueTotal / planRevenue) * 100) : "—"}</strong>
                      </div>
                      <div className={styles.progressTrack}>
                        <div className={styles.progressFillRevenue} style={{ width: `${Math.max(6, Math.min(100, planRevenue > 0 ? (revenueTotal / planRevenue) * 100 : 0))}%` }} />
                      </div>
                    </div>
                    <div className={styles.progressRow}>
                      <div className={styles.progressHead}>
                        <span>Прибыль</span>
                        <strong>{planProfit > 0 ? formatPercent((profitTotal / planProfit) * 100) : "—"}</strong>
                      </div>
                      <div className={styles.progressTrack}>
                        <div className={styles.progressFillProfit} style={{ width: `${Math.max(6, Math.min(100, planProfit > 0 ? (profitTotal / planProfit) * 100 : 0))}%` }} />
                      </div>
                    </div>
                  </div>
                </div>

                <div className={styles.insightGrid}>
                  <InsightCard
                    title="Средний день"
                    value={formatMoney(bundle?.tracking?.kpis?.revenue, currencyCode)}
                    detail={`Активных дней: ${formatNumber(bundle?.tracking?.kpis?.days)}`}
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

            <section className={styles.analysisGrid}>
              <RankingCard
                title="Топ SKU"
                hint="Лидеры по обороту за выбранный период."
                rows={topSku}
                currencyCode={currencyCode}
              />
              <RankingCard
                title="Топ категорий"
                hint="Категории, которые сейчас несут основной оборот."
                rows={topCategories}
                currencyCode={currencyCode}
              />
              <StoreComparisonCard rows={storeComparison.slice(0, 6)} currencyCode={currencyCode} />
            </section>

            <section className={styles.analysisGrid}>
              <StatusBreakdownCard rows={problemStatusRows} total={Number(bundle?.problems?.total_count || 0)} />
              <WatchlistCard
                title="SKU под давлением"
                hint="Позиции с оборотом, но слабой маржей. Это первые кандидаты на пересмотр цены или экономики."
                rows={weakestSku}
                currencyCode={currencyCode}
              />
              <WatchlistCard
                title="Категории риска"
                hint="Категории с самым слабым процентом прибыли внутри текущего среза."
                rows={weakestCategories}
                currencyCode={currencyCode}
              />
            </section>

            <section className={styles.analysisGridSingle}>
              <DataFlowCard flows={flowRows} />
            </section>

            <SectionBlock title="Куда идти дальше">
              <div className={styles.actionGrid}>
                <Link className={styles.actionCard} to="/sales/overview">
                  <div className={styles.actionTitle}>Обзор продаж</div>
                  <div className={styles.actionText}>Провалиться в заказы, проблемные позиции, SKU и категории.</div>
                </Link>
                <Link className={styles.actionCard} to="/pricing/decision">
                  <div className={styles.actionTitle}>Ценовая стратегия</div>
                  <div className={styles.actionText}>Проверить, где просадка по марже требует смены тактики.</div>
                </Link>
                <Link className={styles.actionCard} to="/settings/monitoring">
                  <div className={styles.actionTitle}>Мониторинг</div>
                  <div className={styles.actionText}>Убедиться, что экспорт и обновления не ломают слой продаж.</div>
                </Link>
                <Link className={styles.actionCard} to="/catalog">
                  <div className={styles.actionTitle}>Каталог</div>
                  <div className={styles.actionText}>Проверить SKU и категории, которые тянут результат вверх или вниз.</div>
                </Link>
              </div>
            </SectionBlock>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
