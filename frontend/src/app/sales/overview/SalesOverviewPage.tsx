import { useEffect, useMemo, useState } from "react";
import { API_BASE } from "../../../lib/api";
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
  sale_price?: number | null;
  sale_price_with_coinvest?: number | null;
  cogs_price?: number | null;
  commission?: number | null;
  acquiring?: number | null;
  delivery?: number | null;
  ads?: number | null;
  tax?: number | null;
  profit?: number | null;
  strategy_snapshot_at?: string;
  strategy_installed_price?: number | null;
  strategy_decision_label?: string;
  strategy_attractiveness_status?: string;
  strategy_promo_count?: number;
  strategy_coinvest_pct?: number | null;
  strategy_boost_bid_percent?: number | null;
  strategy_market_boost_bid_percent?: number | null;
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
  revenue?: number | null;
  profit_amount?: number | null;
  profit_pct?: number | null;
  coinvest_amount?: number | null;
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

const ORDERS_PERIOD_OPTIONS: Array<{ value: OrdersPeriod; label: string }> = [
  { value: "today", label: "Сегодня" },
  { value: "yesterday", label: "Вчера" },
  { value: "week", label: "7 дней" },
  { value: "month", label: "30 дней" },
  { value: "quarter", label: "90 дней" },
];

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
  const [isMobile, setIsMobile] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [context, setContext] = useState<ContextResp | null>(null);
  const [tab, setTab] = useState<TabKey>("orders");
  const [storeId, setStoreId] = useState("");
  const [dateMode, setDateMode] = useState<DateMode>("created");
  const [period, setPeriod] = useState<OrdersPeriod>("month");
  const [itemStatus, setItemStatus] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [grain, setGrain] = useState<RetrospectiveGrain>("month");
  const [trackingStoreId, setTrackingStoreId] = useState("all");
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
        const firstStore = String(data.marketplace_stores?.[0]?.store_id || "").trim();
        setStoreId((prev) => prev || firstStore);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Не удалось загрузить контекст");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void loadContext();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!storeId) return;
    let cancelled = false;
    async function loadOverview() {
      setLoading(true);
      setError("");
      try {
        const [trackingData, ordersData, problemOrdersData, dataFlowData, skuData, categoryData] = await Promise.all([
          fetchJson<TrackingResp>(`/api/sales/overview/tracking?store_id=${encodeURIComponent(tab === "tracking" ? trackingStoreId : storeId)}&date_mode=${encodeURIComponent(dateMode)}`),
          fetchJson<OrdersResp>(
            `/api/sales/overview/united-orders?store_id=${encodeURIComponent(storeId)}&period=${encodeURIComponent(period)}&item_status=${encodeURIComponent(itemStatus)}&page=${page}&page_size=${pageSize}`,
          ),
          fetchJson<ProblemOrdersResp>(
            `/api/sales/overview/problem-orders?store_id=${encodeURIComponent(storeId)}&period=${encodeURIComponent(period)}&page=${page}&page_size=${pageSize}`,
          ),
          fetchJson<DataFlowResp>(`/api/sales/overview/data-flow?store_id=${encodeURIComponent(storeId)}`),
          fetchJson<RetrospectiveResp>(
            `/api/sales/overview/retrospective?store_id=${encodeURIComponent(storeId)}&group_by=sku&grain=${encodeURIComponent(grain)}&date_mode=${encodeURIComponent(dateMode)}&limit=120`,
          ),
          fetchJson<RetrospectiveResp>(
            `/api/sales/overview/retrospective?store_id=${encodeURIComponent(storeId)}&group_by=category&grain=${encodeURIComponent(grain)}&date_mode=${encodeURIComponent(dateMode)}&limit=120`,
          ),
        ]);
        if (cancelled) return;
        setTracking(trackingData);
        setOrders(ordersData);
        setProblemOrders(problemOrdersData);
        setDataFlow(dataFlowData);
        setSkuRetrospective(skuData);
        setCategoryRetrospective(categoryData);
        setExpandedMonthKey((prev) => prev || String(trackingData.active_month_key || ""));
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
  }, [storeId, trackingStoreId, tab, dateMode, period, itemStatus, page, pageSize, grain]);

  const stores = useMemo(() => context?.marketplace_stores || [], [context]);
  const trackingStores = useMemo<StoreCtx[]>(
    () => [{ store_uid: "all", store_id: "all", platform: "yandex_market", platform_label: "Яндекс Маркет", label: "Все магазины", currency_code: "RUB" }, ...stores],
    [stores],
  );
  const availableStatuses = useMemo(() => orders?.available_statuses || [], [orders]);
  const activeStore = useMemo(() => stores.find((store) => String(store.store_id) === String(storeId)) || null, [stores, storeId]);
  const activeTrackingStore = useMemo(
    () => trackingStores.find((store) => String(store.store_id) === String(trackingStoreId)) || null,
    [trackingStores, trackingStoreId],
  );
  const activeStoreCurrencyCode = String(activeStore?.currency_code || "RUB").trim().toUpperCase() || "RUB";
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

  const summaryCards = useMemo(() => {
    if (tab === "tracking") {
      return [
        { label: "Оборот", value: formatMoney(tracking?.kpis?.revenue, activeTrackingCurrencyCode), detail: tracking?.loaded_at ? `Обновлено: ${formatDateTime(tracking.loaded_at)}` : undefined },
        { label: "Прибыль", value: formatMoney(tracking?.kpis?.profit, activeTrackingCurrencyCode), detail: `Рентабельность: ${formatPercent(tracking?.kpis?.profit_pct)}` },
        { label: "Средний соинвест", value: formatPercent(tracking?.kpis?.avg_coinvest_pct), detail: `Активных дней: ${formatNumber(tracking?.kpis?.days)}` },
      ];
    }
    if (tab === "sku") {
      const revenue = skuRows.reduce((acc, row) => acc + Number(row.revenue || 0), 0);
      const profit = skuRows.reduce((acc, row) => acc + Number(row.profit_amount || 0), 0);
      return [
        { label: "SKU в срезе", value: formatNumber(skuRetrospective?.total_count), detail: `Период: ${grain === "month" ? "по месяцам" : "по дням"}` },
        { label: "Оборот", value: formatMoney(revenue, activeStoreCurrencyCode), detail: `Дата: ${dateMode === "created" ? "по заказу" : "по доставке"}` },
        { label: "Прибыль", value: formatMoney(profit, activeStoreCurrencyCode), detail: `Рентабельность: ${revenue > 0 ? formatPercent((profit / revenue) * 100) : "—"}` },
      ];
    }
    if (tab === "category") {
      const revenue = categoryRows.reduce((acc, row) => acc + Number(row.revenue || 0), 0);
      const profit = categoryRows.reduce((acc, row) => acc + Number(row.profit_amount || 0), 0);
      return [
        { label: "Категорий в срезе", value: formatNumber(categoryRetrospective?.total_count), detail: `Период: ${grain === "month" ? "по месяцам" : "по дням"}` },
        { label: "Оборот", value: formatMoney(revenue, activeStoreCurrencyCode), detail: `Дата: ${dateMode === "created" ? "по заказу" : "по доставке"}` },
        { label: "Прибыль", value: formatMoney(profit, activeStoreCurrencyCode), detail: `Рентабельность: ${revenue > 0 ? formatPercent((profit / revenue) * 100) : "—"}` },
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
      { label: "Доп. реклама", value: formatMoney(orders?.kpis?.additional_ads, activeStoreCurrencyCode), detail: `Ошибки: ${formatMoney(orders?.kpis?.operational_errors, activeStoreCurrencyCode)}` },
    ];
  }, [activeStoreCurrencyCode, activeTrackingCurrencyCode, categoryRetrospective?.total_count, categoryRows, dateMode, grain, orders, problemOrders, skuRetrospective?.total_count, skuRows, tab, tracking]);

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
