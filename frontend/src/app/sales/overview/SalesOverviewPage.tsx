import { useEffect, useMemo, useState } from "react";
import { API_BASE } from "../../../lib/api";
import { readFreshPageSnapshot, writePageSnapshot } from "../../_shared/pageCache";
import styles from "./SalesOverviewPage.module.css";
import { SalesOverviewDesktop } from "./SalesOverviewDesktop";
import { SalesOverviewMobile } from "./SalesOverviewMobile";

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

type OrdersKpis = {
  additional_ads?: number;
  operational_errors?: number;
  orders_count?: number;
  avg_coinvest_pct?: number;
};

type OrderRow = {
  order_created_at?: string;
  order_created_date?: string;
  delivery_date?: string;
  order_id?: string;
  item_status?: string;
  sku?: string;
  item_name?: string;
  currency_code?: string | null;
  fx_usd_rub_rate?: number | null;
  sale_price?: number | null;
  sale_price_native?: number | null;
  sale_price_with_coinvest?: number | null;
  sale_price_with_coinvest_native?: number | null;
  cogs_price?: number | null;
  cogs_price_native?: number | null;
  commission?: number | null;
  commission_native?: number | null;
  acquiring?: number | null;
  acquiring_native?: number | null;
  delivery?: number | null;
  delivery_native?: number | null;
  ads?: number | null;
  ads_native?: number | null;
  tax?: number | null;
  tax_native?: number | null;
  profit?: number | null;
  profit_native?: number | null;
  strategy_snapshot_at?: string;
  strategy_installed_price?: number | null;
  strategy_installed_price_native?: number | null;
  strategy_decision_label?: string;
  strategy_attractiveness_status?: string;
  strategy_promo_count?: number;
  strategy_coinvest_pct?: number | null;
  strategy_boost_bid_percent?: number | null;
  strategy_market_boost_bid_percent?: number | null;
  actual_market_boost_bid_percent?: number | null;
  ads_rate_percent?: number | null;
  ads_source?: string;
};

type OrdersResp = {
  ok: boolean;
  rows?: OrderRow[];
  total_count?: number;
  page?: number;
  page_size?: number;
  available_statuses?: string[];
  min_date?: string;
  max_date?: string;
  date_from?: string;
  date_to?: string;
  loaded_at?: string;
  kpis?: OrdersKpis;
};

type ProblemOrdersResp = {
  ok: boolean;
  rows?: OrderRow[];
  total_count?: number;
  page?: number;
  page_size?: number;
  date_from?: string;
  date_to?: string;
  loaded_at?: string;
  kpis?: {
    problem_orders_count?: number;
  };
};

type TrackingDay = {
  date: string;
  revenue?: number | null;
  revenue_plan_amount?: number | null;
  profit_amount?: number | null;
  profit_plan_amount?: number | null;
  profit_pct?: number | null;
  coinvest_amount?: number | null;
  returns_pct?: number | null;
  ads_amount?: number | null;
  operational_errors?: number | null;
  delivery_time_days?: number | null;
};

type TrackingMonth = {
  month_key: string;
  month: number;
  month_label: string;
  is_active: boolean;
  revenue?: number | null;
  revenue_plan_amount?: number | null;
  revenue_plan_pct?: number | null;
  profit_amount?: number | null;
  profit_plan_amount?: number | null;
  profit_plan_pct?: number | null;
  profit_pct?: number | null;
  coinvest_amount?: number | null;
  returns_pct?: number | null;
  ads_amount?: number | null;
  operational_errors?: number | null;
  delivery_time_days?: number | null;
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
    days?: number | null;
    avg_coinvest_pct?: number | null;
  };
};

type RetrospectivePeriod = {
  period_key: string;
  period_label: string;
  revenue?: number | null;
  profit_amount?: number | null;
  profit_pct?: number | null;
  coinvest_amount?: number | null;
  coinvest_pct?: number | null;
  ads_amount?: number | null;
  returns_pct?: number | null;
  order_count_total?: number | null;
};

type RetrospectiveRow = {
  key: string;
  label: string;
  sku?: string;
  item_name?: string;
  category_path?: string;
  category_parent_path?: string;
  category_level?: string;
  revenue?: number | null;
  profit_amount?: number | null;
  profit_pct?: number | null;
  coinvest_amount?: number | null;
  coinvest_pct?: number | null;
  ads_amount?: number | null;
  returns_pct?: number | null;
  order_count_total?: number | null;
  periods?: RetrospectivePeriod[];
};

type RetrospectiveResp = {
  ok: boolean;
  rows?: RetrospectiveRow[];
  total_count?: number;
  group_by?: string;
  grain?: string;
  date_mode?: string;
};

type TabKey = "tracking" | "orders" | "problems" | "sku" | "category";
type OrdersPeriod = "today" | "yesterday" | "week" | "month" | "quarter";
type DateMode = "created" | "delivery";
type RetrospectiveGrain = "month" | "day";
type CategoryLevel = "level1" | "level2" | "level3";

type OverviewCacheEntry = {
  tracking?: TrackingResp | null;
  orders?: OrdersResp | null;
  problemOrders?: ProblemOrdersResp | null;
  skuRetrospective?: RetrospectiveResp | null;
  categoryRetrospective?: RetrospectiveResp | null;
  dataFlow?: DataFlowResp | null;
};

const ORDERS_PERIOD_OPTIONS: Array<{ value: OrdersPeriod; label: string }> = [
  { value: "today", label: "Сегодня" },
  { value: "yesterday", label: "Вчера" },
  { value: "week", label: "7 дней" },
  { value: "month", label: "30 дней" },
  { value: "quarter", label: "90 дней" },
];

const OVERVIEW_CONTEXT_CACHE_KEY = "page_sales_overview_context_v1";

function getInitialSearchParams() {
  if (typeof window === "undefined") return new URLSearchParams();
  return new URLSearchParams(window.location.search);
}

function latestTimestamp(values: Array<string | undefined>) {
  return values.filter(Boolean).sort().at(-1) || "";
}

function moneySign(currencyCode: string | null | undefined) {
  return String(currencyCode || "").trim().toUpperCase() === "USD" ? "$" : "₽";
}

function formatMoney(value: number | null | undefined, currencyCode?: string | null) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value)).toLocaleString("ru-RU")} ${moneySign(currencyCode)}`;
}

function formatPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${(Math.round(Number(value) * 100) / 100).toLocaleString("ru-RU")}%`;
}

function formatNumber(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return Math.round(Number(value)).toLocaleString("ru-RU");
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

function formatDate(value: string | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const parsed = new Date(`${raw}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
}

function formatDelta(actual: number | null | undefined, plan: number | null | undefined, currencyCode?: string | null) {
  if (actual == null || plan == null || Number.isNaN(Number(actual)) || Number.isNaN(Number(plan))) return "—";
  const delta = Number(actual) - Number(plan);
  const sign = delta > 0 ? "+" : "";
  return `${sign}${Math.round(delta).toLocaleString("ru-RU")} ${moneySign(currencyCode)}`;
}

function percentOfBase(value: number | null | undefined, base: number | null | undefined) {
  if (value == null || base == null || Number.isNaN(Number(value)) || Number.isNaN(Number(base))) return "—";
  const baseNum = Number(base);
  if (!baseNum) return "—";
  return formatPercent((Number(value) / baseNum) * 100);
}

function statusTone(status: string | undefined) {
  const norm = String(status || "").trim().toLowerCase();
  if (!norm) return "neutral";
  if (norm.includes("достав")) return "positive";
  if (norm.includes("возврат")) return "warn";
  if (norm.includes("отгруж")) return "info";
  return "neutral";
}

function decisionTone(value: string | undefined) {
  const norm = String(value || "").trim().toLowerCase();
  if (!norm) return "neutral";
  if (norm.includes("2 промо")) return "positive";
  if (norm.includes("1 промо")) return "info";
  if (norm.includes("буст")) return "warn";
  if (norm.includes("выгод")) return "positive";
  if (norm.includes("умерен")) return "warn";
  return "neutral";
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

function mergeOrdersResponses(responses: OrdersResp[], page: number, pageSize: number): OrdersResp {
  const rows = responses.flatMap((response) => response.rows || []);
  const statuses = Array.from(new Set(responses.flatMap((response) => response.available_statuses || []))).sort((a, b) => a.localeCompare(b, "ru"));
  const sortedRows = [...rows].sort((a, b) => String(b.order_created_at || b.order_created_date || "").localeCompare(String(a.order_created_at || a.order_created_date || "")));
  const start = Math.max(0, (page - 1) * pageSize);
  const pageRows = sortedRows.slice(start, start + pageSize);
  const totalRevenue = sortedRows.reduce((acc, row) => acc + Number(row.sale_price || 0), 0);
  const coinvestAmount = sortedRows.reduce((acc, row) => {
    const revenue = Number(row.sale_price || 0);
    const buyerPrice = Number(row.sale_price_with_coinvest ?? row.sale_price ?? 0);
    if (revenue <= 0) return acc;
    return acc + Math.max(0, revenue - buyerPrice);
  }, 0);
  return {
    ok: true,
    rows: pageRows,
    total_count: sortedRows.length,
    page,
    page_size: pageSize,
    available_statuses: statuses,
    min_date: responses.map((response) => response.min_date).filter(Boolean).sort().at(0),
    max_date: responses.map((response) => response.max_date).filter(Boolean).sort().at(-1),
    date_from: responses.map((response) => response.date_from).filter(Boolean).sort().at(0),
    date_to: responses.map((response) => response.date_to).filter(Boolean).sort().at(-1),
    loaded_at: latestTimestamp(responses.map((response) => response.loaded_at)),
    kpis: {
      orders_count: sortedRows.length,
      avg_coinvest_pct: totalRevenue > 0 ? Number(((coinvestAmount / totalRevenue) * 100).toFixed(2)) : 0,
      additional_ads: responses.reduce((acc, response) => acc + Number(response.kpis?.additional_ads || 0), 0),
      operational_errors: responses.reduce((acc, response) => acc + Number(response.kpis?.operational_errors || 0), 0),
    },
  };
}

function mergeProblemResponses(responses: ProblemOrdersResp[], page: number, pageSize: number): ProblemOrdersResp {
  const rows = responses.flatMap((response) => response.rows || []);
  const sortedRows = [...rows].sort((a, b) => String(b.order_created_at || b.order_created_date || "").localeCompare(String(a.order_created_at || a.order_created_date || "")));
  const start = Math.max(0, (page - 1) * pageSize);
  return {
    ok: true,
    rows: sortedRows.slice(start, start + pageSize),
    total_count: sortedRows.length,
    page,
    page_size: pageSize,
    date_from: responses.map((response) => response.date_from).filter(Boolean).sort().at(0),
    date_to: responses.map((response) => response.date_to).filter(Boolean).sort().at(-1),
    loaded_at: latestTimestamp(responses.map((response) => response.loaded_at)),
    kpis: {
      problem_orders_count: sortedRows.length,
    },
  };
}

function mergeRetrospectiveResponses(responses: RetrospectiveResp[]): RetrospectiveResp {
  const grouped = new Map<string, RetrospectiveRow>();
  for (const response of responses) {
    for (const row of response.rows || []) {
      const key = String(row.key || row.label || row.sku || row.category_path || "").trim();
      if (!key) continue;
      const current = grouped.get(key);
      if (!current) {
        grouped.set(key, {
          ...row,
          revenue: Number(row.revenue || 0),
          profit_amount: Number(row.profit_amount || 0),
          coinvest_amount: Number(row.coinvest_amount || 0),
          ads_amount: Number(row.ads_amount || 0),
          order_count_total: Number(row.order_count_total || 0),
          periods: [...(row.periods || [])],
        });
        continue;
      }
      const periodMap = new Map<string, RetrospectivePeriod>();
      for (const period of current.periods || []) periodMap.set(period.period_key, { ...period });
      for (const period of row.periods || []) {
        const existing = periodMap.get(period.period_key);
        if (!existing) {
          periodMap.set(period.period_key, { ...period });
          continue;
        }
        const revenue = Number(existing.revenue || 0) + Number(period.revenue || 0);
        const profit = Number(existing.profit_amount || 0) + Number(period.profit_amount || 0);
        periodMap.set(period.period_key, {
          ...existing,
          revenue,
          profit_amount: profit,
          profit_pct: revenue > 0 ? Number(((profit / revenue) * 100).toFixed(2)) : null,
          coinvest_amount: Number(existing.coinvest_amount || 0) + Number(period.coinvest_amount || 0),
          ads_amount: Number(existing.ads_amount || 0) + Number(period.ads_amount || 0),
          order_count_total: Number(existing.order_count_total || 0) + Number(period.order_count_total || 0),
          returns_pct: period.returns_pct ?? existing.returns_pct,
        });
      }
      const nextRevenue = Number(current.revenue || 0) + Number(row.revenue || 0);
      const nextProfit = Number(current.profit_amount || 0) + Number(row.profit_amount || 0);
      grouped.set(key, {
        ...current,
        revenue: nextRevenue,
        profit_amount: nextProfit,
        profit_pct: nextRevenue > 0 ? Number(((nextProfit / nextRevenue) * 100).toFixed(2)) : null,
        coinvest_amount: Number(current.coinvest_amount || 0) + Number(row.coinvest_amount || 0),
        ads_amount: Number(current.ads_amount || 0) + Number(row.ads_amount || 0),
        order_count_total: Number(current.order_count_total || 0) + Number(row.order_count_total || 0),
        periods: Array.from(periodMap.values()).sort((a, b) => String(b.period_key).localeCompare(String(a.period_key))),
      });
    }
  }
  const rows = Array.from(grouped.values()).sort((a, b) => Number(b.revenue || 0) - Number(a.revenue || 0));
  return { ok: true, rows, total_count: rows.length };
}

function mergeDataFlowResponses(responses: DataFlowResp[]): DataFlowResp {
  const grouped = new Map<string, DataFlowItem>();
  for (const response of responses) {
    for (const flow of response.flows || []) {
      const current = grouped.get(flow.code);
      if (!current || String(flow.loaded_at || "") > String(current.loaded_at || "")) {
        grouped.set(flow.code, flow);
      }
    }
  }
  return { ok: true, flows: Array.from(grouped.values()) };
}

function SummaryCard({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className={styles.summaryCard}>
      <div className={styles.summaryLabel}>{label}</div>
      <div className={styles.summaryValue}>{value}</div>
      {detail ? <div className={styles.summaryDetail}>{detail}</div> : null}
    </div>
  );
}

export default function SalesOverviewPage() {
  const initialParams = getInitialSearchParams();
  const [isMobile, setIsMobile] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [context, setContext] = useState<ContextResp | null>(null);
  const [tab, setTab] = useState<TabKey>((() => {
    const value = initialParams.get("tab");
    return value === "tracking" || value === "orders" || value === "problems" || value === "sku" || value === "category" ? value : "orders";
  })());
  const [storeId, setStoreId] = useState(initialParams.get("storeId") || "");
  const [dateMode, setDateMode] = useState<DateMode>(initialParams.get("dateMode") === "delivery" ? "delivery" : "created");
  const [period, setPeriod] = useState<OrdersPeriod>((() => {
    const value = initialParams.get("period");
    return value === "today" || value === "yesterday" || value === "week" || value === "month" || value === "quarter" ? value : "today";
  })());
  const [itemStatus, setItemStatus] = useState(initialParams.get("itemStatus") || "");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [grain, setGrain] = useState<RetrospectiveGrain>(initialParams.get("grain") === "day" ? "day" : "month");
  const [categoryLevel, setCategoryLevel] = useState<CategoryLevel>((() => {
    const value = initialParams.get("categoryLevel");
    return value === "level1" || value === "level2" || value === "level3" ? value : "level2";
  })());
  const [trackingStoreId, setTrackingStoreId] = useState(initialParams.get("trackingStoreId") || "");
  const [customDateFrom] = useState(initialParams.get("date_from") || "");
  const [customDateTo] = useState(initialParams.get("date_to") || "");
  const [tracking, setTracking] = useState<TrackingResp | null>(null);
  const [orders, setOrders] = useState<OrdersResp | null>(null);
  const [problemOrders, setProblemOrders] = useState<ProblemOrdersResp | null>(null);
  const [skuRetrospective, setSkuRetrospective] = useState<RetrospectiveResp | null>(null);
  const [categoryRetrospective, setCategoryRetrospective] = useState<RetrospectiveResp | null>(null);
  const [dataFlow, setDataFlow] = useState<DataFlowResp | null>(null);
  const [expandedMonthKey, setExpandedMonthKey] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function loadContext() {
      setLoading(true);
      setError("");
      try {
        const data = await fetchJson<ContextResp>("/api/sales/overview/context");
        if (cancelled) return;
        setContext(data);
        writePageSnapshot(OVERVIEW_CONTEXT_CACHE_KEY, data);
        const firstStore = String(data.marketplace_stores?.[0]?.store_id || "").trim();
        setStoreId((prev) => prev || firstStore);
        setTrackingStoreId((prev) => prev || firstStore || "all");
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Не удалось загрузить контекст");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    const cachedContext = readFreshPageSnapshot<ContextResp>(OVERVIEW_CONTEXT_CACHE_KEY, 10 * 60 * 1000);
    if (cachedContext?.ok) {
      setContext(cachedContext);
      const firstStore = String(cachedContext.marketplace_stores?.[0]?.store_id || "").trim();
      setStoreId((prev) => prev || firstStore);
      setTrackingStoreId((prev) => prev || firstStore || "all");
      setLoading(false);
    }
    void loadContext();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!storeId || (storeId === "all" && !context)) return;
    let cancelled = false;
    async function loadOverview() {
      setLoading(true);
      setError("");
      try {
        const rangeQuery = customDateFrom || customDateTo
          ? `&date_from=${encodeURIComponent(customDateFrom)}&date_to=${encodeURIComponent(customDateTo)}`
          : "";
        const nextState: OverviewCacheEntry = {
          tracking,
          orders,
          problemOrders,
          skuRetrospective,
          categoryRetrospective,
          dataFlow,
        };

        if (tab === "tracking") {
          const [trackingData, flowData] = await Promise.all([
            fetchJson<TrackingResp>(`/api/sales/overview/tracking?store_id=${encodeURIComponent(trackingStoreId)}&date_mode=${encodeURIComponent(dateMode)}`),
            fetchJson<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(trackingStoreId)}`),
          ]);
          nextState.tracking = trackingData;
          nextState.dataFlow = flowData;
        } else if (tab === "orders") {
          const fetchScopedOrders = async (sid: string) => fetchJson<OrdersResp>(
            `/api/sales/overview/united-orders?store_id=${encodeURIComponent(sid)}&period=${encodeURIComponent(period)}&item_status=${encodeURIComponent(itemStatus)}&page=${page}&page_size=${pageSize}`,
          );
          const [ordersData, flowData] = await Promise.all([
            storeId === "all"
              ? Promise.all((context?.marketplace_stores || []).map((store) => fetchScopedOrders(store.store_id))).then((responses) => mergeOrdersResponses(responses, page, pageSize))
              : fetchScopedOrders(storeId),
            fetchJson<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(storeId)}`),
          ]);
          nextState.orders = ordersData;
          nextState.dataFlow = flowData;
        } else if (tab === "problems") {
          const fetchScopedProblems = async (sid: string) => fetchJson<ProblemOrdersResp>(
            `/api/sales/overview/problem-orders?store_id=${encodeURIComponent(sid)}&period=${encodeURIComponent(period)}&page=${page}&page_size=${pageSize}`,
          );
          const [problemsData, flowData] = await Promise.all([
            storeId === "all"
              ? Promise.all((context?.marketplace_stores || []).map((store) => fetchScopedProblems(store.store_id))).then((responses) => mergeProblemResponses(responses, page, pageSize))
              : fetchScopedProblems(storeId),
            fetchJson<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(storeId)}`),
          ]);
          nextState.problemOrders = problemsData;
          nextState.dataFlow = flowData;
        } else if (tab === "sku") {
          const fetchScopedSku = async (sid: string) => fetchJson<RetrospectiveResp>(
            `/api/sales/overview/retrospective?store_id=${encodeURIComponent(sid)}&group_by=sku&grain=${encodeURIComponent(grain)}&date_mode=${encodeURIComponent(dateMode)}${rangeQuery}&limit=120`,
          );
          const [skuData, flowData] = await Promise.all([
            storeId === "all"
              ? Promise.all((context?.marketplace_stores || []).map((store) => fetchScopedSku(store.store_id))).then((responses) => mergeRetrospectiveResponses(responses))
              : fetchScopedSku(storeId),
            fetchJson<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(storeId)}`),
          ]);
          nextState.skuRetrospective = skuData;
          nextState.dataFlow = flowData;
        } else if (tab === "category") {
          const fetchScopedCategory = async (sid: string) => fetchJson<RetrospectiveResp>(
            `/api/sales/overview/retrospective?store_id=${encodeURIComponent(sid)}&group_by=category&category_level=${encodeURIComponent(categoryLevel)}&grain=${encodeURIComponent(grain)}&date_mode=${encodeURIComponent(dateMode)}${rangeQuery}&limit=120`,
          );
          const [categoryData, flowData] = await Promise.all([
            storeId === "all"
              ? Promise.all((context?.marketplace_stores || []).map((store) => fetchScopedCategory(store.store_id))).then((responses) => mergeRetrospectiveResponses(responses))
              : fetchScopedCategory(storeId),
            fetchJson<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(storeId)}`),
          ]);
          nextState.categoryRetrospective = categoryData;
          nextState.dataFlow = flowData;
        }

        if (cancelled) return;
        setTracking(nextState.tracking ?? null);
        setOrders(nextState.orders ?? null);
        setProblemOrders(nextState.problemOrders ?? null);
        setDataFlow(nextState.dataFlow ?? null);
        setSkuRetrospective(nextState.skuRetrospective ?? null);
        setCategoryRetrospective(nextState.categoryRetrospective ?? null);
        if (nextState.tracking?.active_month_key) {
          setExpandedMonthKey((prev) => prev || String(nextState.tracking?.active_month_key || ""));
        }
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Не удалось загрузить обзор продаж");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void loadOverview();
    return () => {
      cancelled = true;
    };
  }, [context, storeId, trackingStoreId, tab, dateMode, period, itemStatus, page, pageSize, grain, categoryLevel, customDateFrom, customDateTo]);

  useEffect(() => {
    setExpandedMonthKey(String(tracking?.active_month_key || ""));
  }, [tracking?.active_month_key, dateMode, trackingStoreId]);

  useEffect(() => {
    setPage(1);
  }, [tab]);

  const stores = useMemo<StoreCtx[]>(
    () => [{ store_uid: "all", store_id: "all", platform: "multi", platform_label: "Все магазины", label: "Все магазины", currency_code: "RUB" }, ...(context?.marketplace_stores || [])],
    [context],
  );
  const trackingStores = useMemo<StoreCtx[]>(
    () => [{ store_uid: "all", store_id: "all", platform: "yandex_market", platform_label: "Яндекс Маркет", label: "Все магазины", currency_code: "RUB" }, ...((context?.marketplace_stores || []))],
    [context],
  );
  const availableStatuses = useMemo(() => orders?.available_statuses || [], [orders]);
  const activeStore = useMemo(() => stores.find((store) => String(store.store_id) === String(storeId)) || null, [stores, storeId]);
  const activeStoreCurrencyCode = String(activeStore?.currency_code || "RUB").trim().toUpperCase() || "RUB";
  const activeTrackingStore = useMemo(
    () => trackingStores.find((store) => String(store.store_id) === String(trackingStoreId)) || null,
    [trackingStores, trackingStoreId],
  );
  const activeTrackingCurrencyCode = String(activeTrackingStore?.currency_code || "RUB").trim().toUpperCase() || "RUB";
  const trackingYears = useMemo(() => tracking?.years || [], [tracking]);
  const orderRows = useMemo(() => orders?.rows || [], [orders]);
  const problemRows = useMemo(() => problemOrders?.rows || [], [problemOrders]);
  const flowRows = useMemo(() => dataFlow?.flows || [], [dataFlow]);
  const skuRows = useMemo(() => skuRetrospective?.rows || [], [skuRetrospective]);
  const categoryRows = useMemo(() => categoryRetrospective?.rows || [], [categoryRetrospective]);
  const totalCount = Number(orders?.total_count || 0);
  const totalPages = pageSize < 1 ? 1 : Math.max(1, Math.ceil(totalCount / pageSize));

  useEffect(() => {
    if (typeof window === "undefined") return;
    const media = window.matchMedia("(max-width: 960px)");
    const sync = () => setIsMobile(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined" || !storeId) return;
    const params = new URLSearchParams();
    params.set("tab", tab);
    params.set("storeId", storeId);
    if (trackingStoreId) params.set("trackingStoreId", trackingStoreId);
    params.set("dateMode", dateMode);
    params.set("period", period);
    params.set("grain", grain);
    params.set("categoryLevel", categoryLevel);
    if (itemStatus) params.set("itemStatus", itemStatus);
    if (customDateFrom) params.set("date_from", customDateFrom);
    if (customDateTo) params.set("date_to", customDateTo);
    window.history.replaceState({}, "", `${window.location.pathname}?${params.toString()}`);
  }, [categoryLevel, customDateFrom, customDateTo, dateMode, grain, itemStatus, period, storeId, tab, trackingStoreId]);

  const summaryCards = useMemo(() => {
    if (tab === "tracking") {
      return [
        { label: "Оборот", value: formatMoney(tracking?.kpis?.revenue, "RUB"), detail: tracking?.loaded_at ? `Обновлено: ${formatDateTime(tracking.loaded_at)}` : undefined },
        { label: "Прибыль", value: formatMoney(tracking?.kpis?.profit, "RUB"), detail: `Рентабельность: ${formatPercent(tracking?.kpis?.profit_pct)}` },
        { label: "Средний соинвест", value: formatPercent(tracking?.kpis?.avg_coinvest_pct), detail: `Активных дней: ${formatNumber(tracking?.kpis?.days)}` },
      ];
    }
    if (tab === "sku") {
      const revenue = skuRows.reduce((acc, row) => acc + Number(row.revenue || 0), 0);
      const profit = skuRows.reduce((acc, row) => acc + Number(row.profit_amount || 0), 0);
      return [
        { label: "SKU в срезе", value: formatNumber(skuRetrospective?.total_count), detail: `Период: ${grain === "month" ? "по месяцам" : "по дням"}` },
        { label: "Оборот", value: formatMoney(revenue, "RUB"), detail: customDateFrom && customDateTo ? `${formatDate(customDateFrom)} - ${formatDate(customDateTo)}` : `Дата: ${dateMode === "created" ? "по заказу" : "по доставке"}` },
        { label: "Прибыль", value: formatMoney(profit, "RUB"), detail: `Рентабельность: ${revenue > 0 ? formatPercent((profit / revenue) * 100) : "—"}` },
      ];
    }
    if (tab === "category") {
      const revenue = categoryRows.reduce((acc, row) => acc + Number(row.revenue || 0), 0);
      const profit = categoryRows.reduce((acc, row) => acc + Number(row.profit_amount || 0), 0);
      return [
        { label: "Категорий в срезе", value: formatNumber(categoryRetrospective?.total_count), detail: `Период: ${grain === "month" ? "по месяцам" : "по дням"}` },
        { label: "Уровень", value: categoryLevel === "level1" ? "Уровень 1" : categoryLevel === "level2" ? "Уровень 2" : "Уровень 3", detail: `Период: ${grain === "month" ? "по месяцам" : "по дням"}` },
        { label: "Оборот", value: formatMoney(revenue, "RUB"), detail: customDateFrom && customDateTo ? `${formatDate(customDateFrom)} - ${formatDate(customDateTo)}` : `Дата: ${dateMode === "created" ? "по заказу" : "по доставке"}` },
        { label: "Прибыль", value: formatMoney(profit, "RUB"), detail: `Рентабельность: ${revenue > 0 ? formatPercent((profit / revenue) * 100) : "—"}` },
      ];
    }
    if (tab === "problems") {
      return [
        { label: "Проблемных заказов", value: formatNumber(problemOrders?.total_count), detail: problemOrders?.loaded_at ? `Обновлено: ${formatDateTime(problemOrders.loaded_at)}` : undefined },
        { label: "Период", value: problemOrders?.date_from && problemOrders?.date_to ? `${formatDate(problemOrders.date_from)} - ${formatDate(problemOrders.date_to)}` : "—" },
        { label: "Причина", value: "Нет себестоимости", detail: "Доставленные заказы без COGS исключены из чистой аналитики" },
      ];
    }
    return [
      { label: "Заказы", value: formatNumber(orders?.kpis?.orders_count), detail: orders?.loaded_at ? `Обновлено: ${formatDateTime(orders.loaded_at)}` : undefined },
      { label: "Средний соинвест", value: formatPercent(orders?.kpis?.avg_coinvest_pct), detail: orders?.date_from && orders?.date_to ? `${formatDate(orders.date_from)} - ${formatDate(orders.date_to)}` : undefined },
      { label: "Доп. реклама", value: formatMoney(orders?.kpis?.additional_ads, "RUB"), detail: `Ошибки: ${formatMoney(orders?.kpis?.operational_errors, "RUB")}` },
    ];
  }, [categoryLevel, categoryRetrospective?.total_count, categoryRows, customDateFrom, customDateTo, dateMode, grain, orders, problemOrders, skuRetrospective?.total_count, skuRows, tab, tracking]);

  const vm = {
    stylesRef: styles,
    loading,
    error,
    stores,
    trackingStores,
    availableStatuses,
    activeStore,
    activeTrackingStore,
    activeStoreCurrencyCode,
    activeTrackingCurrencyCode,
    trackingYears,
    orderRows,
    problemRows,
    flowRows,
    skuRows,
    categoryRows,
    totalCount,
    totalPages,
    summaryCards,
    tab,
    setTab,
    storeId,
    setStoreId,
    dateMode,
    setDateMode,
    period,
    setPeriod,
    itemStatus,
    setItemStatus,
    page,
    setPage,
    pageSize,
    setPageSize,
    grain,
    setGrain,
    categoryLevel,
    setCategoryLevel,
    trackingStoreId,
    setTrackingStoreId,
    expandedMonthKey,
    setExpandedMonthKey,
    tracking,
    orders,
    problemOrders,
    skuRetrospective,
    categoryRetrospective,
    formatMoney,
    formatPercent,
    formatNumber,
    formatDateTime,
    formatDate,
    formatDelta,
    percentOfBase,
    statusTone,
    ORDERS_PERIOD_OPTIONS,
  };

  return isMobile ? <SalesOverviewMobile vm={vm} /> : <SalesOverviewDesktop vm={vm} />;
}
