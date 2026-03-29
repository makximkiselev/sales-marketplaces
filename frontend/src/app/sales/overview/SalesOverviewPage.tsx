import { Fragment, useEffect, useMemo, useState } from "react";
import { PageFrame, PageSectionTitle } from "../../../components/page/PageKit";
import { API_BASE } from "../../../lib/api";
import styles from "./SalesOverviewPage.module.css";

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

  return (
    <PageFrame
      className={styles.pageFrame}
      innerClassName={styles.pageFrameInner}
      title="Обзор продаж"
      subtitle="Единый слой 'по заказам': история загружается один раз, текущий месяц пополняется инкрементально, а в течение дня заказы донасыщаются оперативной экономикой."
      toolbarLeft={(
        <div className={styles.toolbar}>
          <select className={`input ${styles.dateInput}`} value={storeId} onChange={(e) => setStoreId(e.target.value)}>
            {stores.map((store) => (
              <option key={store.store_uid} value={store.store_id}>{store.label}</option>
            ))}
          </select>
          <div className={styles.segmented}>
            <button className={`btn inline ${tab === "orders" ? styles.segmentedActive : ""}`} onClick={() => setTab("orders")}>По заказам</button>
            <button className={`btn inline ${tab === "problems" ? styles.segmentedActive : ""}`} onClick={() => setTab("problems")}>Проблемные</button>
            <button className={`btn inline ${tab === "tracking" ? styles.segmentedActive : ""}`} onClick={() => setTab("tracking")}>Трекинг</button>
            <button className={`btn inline ${tab === "sku" ? styles.segmentedActive : ""}`} onClick={() => setTab("sku")}>Товары</button>
            <button className={`btn inline ${tab === "category" ? styles.segmentedActive : ""}`} onClick={() => setTab("category")}>Категории</button>
          </div>
        </div>
      )}
      toolbarRight={(
        tab === "tracking" || tab === "sku" || tab === "category" ? (
          <div className={styles.toolbar}>
          <select className={`input ${styles.dateInput}`} value={dateMode} onChange={(e) => setDateMode(e.target.value as DateMode)}>
            <option value="created">По дате заказа</option>
            <option value="delivery">По дате доставки</option>
          </select>
          {tab === "sku" || tab === "category" ? (
            <select className={`input ${styles.dateInput}`} value={grain} onChange={(e) => setGrain(e.target.value as RetrospectiveGrain)}>
              <option value="month">По месяцам</option>
              <option value="day">По дням</option>
            </select>
          ) : null}
          </div>
        ) : (
          <div className={styles.toolbar}>
            <select
              className={`input ${styles.dateInput}`}
              value={period}
              onChange={(e) => {
                setPage(1);
                setPeriod(e.target.value as OrdersPeriod);
              }}
            >
              {ORDERS_PERIOD_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
            <select
              className={`input ${styles.dateInput}`}
              value={itemStatus}
              onChange={(e) => {
                setPage(1);
                setItemStatus(e.target.value);
              }}
            >
              <option value="">Все статусы</option>
              {availableStatuses.map((status) => (
                <option key={status} value={status}>{status}</option>
              ))}
            </select>
          </div>
        )
      )}
    >
      <div className={styles.summaryGrid}>
        {summaryCards.map((card) => (
          <SummaryCard key={card.label} label={card.label} value={card.value} detail={card.detail} />
        ))}
      </div>

      {!loading && !error && flowRows.length > 0 ? (
        <div className={styles.summaryGrid}>
          {flowRows.map((flow) => (
            <SummaryCard
              key={flow.code}
              label={flow.label}
              value={flow.date_from && flow.date_to ? `${formatDate(flow.date_from)} - ${formatDate(flow.date_to)}` : "—"}
              detail={flow.loaded_at ? `${flow.description || ""} Обновлено: ${formatDateTime(flow.loaded_at)}`.trim() : flow.description}
            />
          ))}
        </div>
      ) : null}

      {tab === "tracking" ? (
        <div className={styles.toolbar}>
          <div className={styles.segmented}>
            {trackingStores.map((store) => (
              <button
                key={store.store_uid}
                className={`btn inline ${trackingStoreId === store.store_id ? styles.segmentedActive : ""}`}
                onClick={() => setTrackingStoreId(store.store_id)}
              >
                {store.label}
              </button>
            ))}
          </div>
        </div>
      ) : null}
      {tab === "tracking"
        ? (activeTrackingStore ? <div className={styles.pageInfo}>Магазин: {activeTrackingStore.label}</div> : null)
        : (activeStore ? <div className={styles.pageInfo}>Магазин: {activeStore.label}</div> : null)}
      {loading ? <div className={styles.empty}>Загрузка...</div> : null}
      {error ? <div className={styles.errorBox}>{error}</div> : null}

      {!loading && !error && tab === "orders" ? (
        <section>
          <PageSectionTitle title="Заказы" meta={`Всего: ${totalCount}`} />
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Дата</th>
                  <th>Заказ</th>
                  <th>SKU</th>
                  <th className={styles.nameCell}>Наименование</th>
                  <th>Статус</th>
                  <th>Продажа</th>
                  <th>С соинвестом</th>
                  <th>Цена стратегии</th>
                  <th>Отклонение</th>
                  <th>Реклама</th>
                  <th>Себестоимость</th>
                  <th>Комиссия</th>
                  <th>Эквайринг</th>
                  <th>Логистика</th>
                  <th>Налог</th>
                  <th>Расходы</th>
                  <th>Прибыль</th>
                </tr>
              </thead>
              <tbody>
                {orderRows.length === 0 ? (
                  <tr>
                    <td colSpan={17} className={styles.empty}>Нет заказов для выбранных параметров</td>
                  </tr>
                ) : orderRows.map((row) => {
                  const totalCosts =
                    Number(row.commission || 0) +
                    Number(row.acquiring || 0) +
                    Number(row.delivery || 0) +
                    Number(row.tax || 0) +
                    Number(row.ads || 0);
                  return (
                    <tr key={`${row.order_id || ""}-${row.sku || ""}`}>
                      <td>{formatDateTime(row.order_created_at)}</td>
                      <td>{row.order_id || "—"}</td>
                      <td>{row.sku || "—"}</td>
                      <td className={styles.nameCell}>{row.item_name || "—"}</td>
                      <td>
                        <span className={`${styles.statusBadge} ${styles[`tone_${statusTone(row.item_status)}`]}`}>{row.item_status || "—"}</span>
                      </td>
                      <td>{formatMoney(row.sale_price, activeStoreCurrencyCode)}</td>
                      <td>{formatMoney(row.sale_price_with_coinvest, activeStoreCurrencyCode)}</td>
                      <td>
                        <div>{formatMoney(row.strategy_installed_price, activeStoreCurrencyCode)}</div>
                        <div className={styles.subtleText}>{formatDateTime(row.strategy_snapshot_at)}</div>
                      </td>
                      <td>{formatDelta(row.sale_price, row.strategy_installed_price, activeStoreCurrencyCode)}</td>
                      <td>
                        <div>{formatMoney(row.ads, activeStoreCurrencyCode)}</div>
                        <div className={styles.subtleText}>
                          План: {formatPercent(row.strategy_boost_bid_percent)} / Факт: {formatPercent(row.strategy_market_boost_bid_percent)}
                        </div>
                      </td>
                      <td>{formatMoney(row.cogs_price, activeStoreCurrencyCode)}</td>
                      <td>
                        <div>{formatMoney(row.commission, activeStoreCurrencyCode)}</div>
                        <div className={styles.subtleText}>{percentOfBase(row.commission, row.sale_price)}</div>
                      </td>
                      <td>
                        <div>{formatMoney(row.acquiring, activeStoreCurrencyCode)}</div>
                        <div className={styles.subtleText}>{percentOfBase(row.acquiring, row.sale_price)}</div>
                      </td>
                      <td>{formatMoney(row.delivery, activeStoreCurrencyCode)}</td>
                      <td>
                        <div>{formatMoney(row.tax, activeStoreCurrencyCode)}</div>
                        <div className={styles.subtleText}>{percentOfBase(row.tax, row.sale_price)}</div>
                      </td>
                      <td>{formatMoney(totalCosts, activeStoreCurrencyCode)}</td>
                      <td>
                        <div>{formatMoney(row.profit, activeStoreCurrencyCode)}</div>
                        <div className={styles.subtleText}>{percentOfBase(row.profit, row.sale_price)}</div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div className={styles.toolbar}>
            <div className={styles.pager}>
              <button className="btn inline" disabled={page <= 1} onClick={() => setPage((prev) => Math.max(1, prev - 1))}>Назад</button>
              <span className={styles.pageInfo}>{page} / {totalPages}</span>
              <button className="btn inline" disabled={page >= totalPages} onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}>Дальше</button>
            </div>
            <label className={styles.pageSize}>
              На странице
              <select
                className={`input ${styles.dateInput}`}
                value={pageSize}
                onChange={(e) => {
                  setPage(1);
                  setPageSize(Number(e.target.value) || 50);
                }}
              >
                {[25, 50, 100, 200].map((value) => (
                  <option key={value} value={value}>{value}</option>
                ))}
              </select>
            </label>
          </div>
        </section>
      ) : null}

      {!loading && !error && tab === "problems" ? (
        <section>
          <PageSectionTitle title="Проблемные заказы" meta={`Всего: ${formatNumber(problemOrders?.total_count)}`} />
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Дата</th>
                  <th>Доставка</th>
                  <th>Заказ</th>
                  <th>SKU</th>
                  <th className={styles.nameCell}>Наименование</th>
                  <th>Статус</th>
                  <th>Продажа</th>
                  <th>Себестоимость</th>
                  <th>Причина</th>
                </tr>
              </thead>
              <tbody>
                {problemRows.length === 0 ? (
                  <tr>
                    <td colSpan={9} className={styles.empty}>Нет проблемных заказов</td>
                  </tr>
                ) : problemRows.map((row) => (
                  <tr key={`${row.order_id || ""}-${row.sku || ""}`}>
                    <td>{formatDateTime(row.order_created_at)}</td>
                    <td>{formatDate(row.delivery_date)}</td>
                    <td>{row.order_id || "—"}</td>
                    <td>{row.sku || "—"}</td>
                    <td className={styles.nameCell}>{row.item_name || "—"}</td>
                    <td><span className={`${styles.statusBadge} ${styles.tone_warn}`}>{row.item_status || "—"}</span></td>
                    <td>{formatMoney(row.sale_price, activeStoreCurrencyCode)}</td>
                    <td>{formatMoney(row.cogs_price, activeStoreCurrencyCode)}</td>
                    <td className={styles.nameCell}>Доставлен, но нет себестоимости. Заказ исключён из чистой аналитики.</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {!loading && !error && tab === "tracking" ? (
        <section>
          <PageSectionTitle title="Трекинг" meta={tracking?.loaded_at ? `Обновлено: ${formatDateTime(tracking.loaded_at)}` : ""} />
          {trackingYears.length === 0 ? (
            <div className={styles.empty}>Нет данных для трекинга</div>
          ) : trackingYears.map((year) => (
            <div key={year.year} className={styles.trackingYearSection}>
              <div className={styles.trackingYearTitle}>{year.year}</div>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th className={styles.nameCell}>Период</th>
                      <th>Оборот</th>
                      <th>Прибыль</th>
                      <th>Маржинальность</th>
                      <th>Соинвест</th>
                      <th>Возвраты</th>
                      <th>Реклама</th>
                      <th>Ошибки</th>
                      <th>Доставка</th>
                    </tr>
                  </thead>
                  <tbody>
                    {year.months.map((month) => {
                      const open = expandedMonthKey === month.month_key;
                      return (
                        <Fragment key={month.month_key}>
                          <tr
                            className={`${styles.trackingMonthRow} ${open ? styles.trackingMonthRowActive : ""}`}
                            onClick={() => setExpandedMonthKey((prev) => prev === month.month_key ? "" : month.month_key)}
                          >
                            <td className={styles.trackingMonthCell}>
                              <span className={styles.trackingChevron}>{open ? "▾" : "▸"}</span>
                              {month.month_label}
                            </td>
                            <td>
                              <div>{formatMoney(month.revenue, activeTrackingCurrencyCode)}</div>
                              {month.revenue_plan_amount != null ? <div className={styles.trackingPlanText}>План: {formatMoney(month.revenue_plan_amount, activeTrackingCurrencyCode)}</div> : null}
                            </td>
                            <td>
                              <div>{formatMoney(month.profit_amount, activeTrackingCurrencyCode)}</div>
                              {month.profit_plan_amount != null ? <div className={styles.trackingPlanText}>План: {formatMoney(month.profit_plan_amount, activeTrackingCurrencyCode)}</div> : null}
                            </td>
                            <td>{formatPercent(month.profit_pct)}</td>
                            <td>{formatPercent(month.revenue && month.coinvest_amount ? (month.coinvest_amount / month.revenue) * 100 : 0)}</td>
                            <td>{formatPercent(month.returns_pct)}</td>
                            <td>{formatMoney(month.ads_amount, activeTrackingCurrencyCode)}</td>
                            <td>{formatMoney(month.operational_errors, activeTrackingCurrencyCode)}</td>
                            <td>{formatNumber(month.delivery_time_days)}</td>
                          </tr>
                          <tr className={styles.trackingDaysHostRow}>
                            <td colSpan={9} className={styles.trackingDaysHostCell}>
                              <div className={`${styles.trackingDaysWrap} ${open ? styles.trackingDaysWrapOpen : ""}`}>
                                <table className={`${styles.table} ${styles.trackingDaysTable}`}>
                                  <thead>
                                    <tr>
                                      <th className={styles.nameCell}>День</th>
                                      <th>Оборот</th>
                                      <th>Прибыль</th>
                                      <th>Маржинальность</th>
                                      <th>Соинвест</th>
                                      <th>Возвраты</th>
                                      <th>Реклама</th>
                                      <th>Ошибки</th>
                                      <th>Доставка</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {(month.days || []).map((day) => (
                                      <tr key={`${month.month_key}-${day.date}`}>
                                        <td className={styles.nameCell}>{formatDate(day.date)}</td>
                                        <td>
                                          <div>{formatMoney(day.revenue, activeTrackingCurrencyCode)}</div>
                                          {day.revenue_plan_amount != null ? <div className={styles.trackingPlanText}>План: {formatMoney(day.revenue_plan_amount, activeTrackingCurrencyCode)}</div> : null}
                                        </td>
                                        <td>
                                          <div>{formatMoney(day.profit_amount, activeTrackingCurrencyCode)}</div>
                                          {day.profit_plan_amount != null ? <div className={styles.trackingPlanText}>План: {formatMoney(day.profit_plan_amount, activeTrackingCurrencyCode)}</div> : null}
                                        </td>
                                        <td>{formatPercent(day.profit_pct)}</td>
                                        <td>{formatPercent(day.revenue && day.coinvest_amount ? (day.coinvest_amount / day.revenue) * 100 : 0)}</td>
                                        <td>{formatPercent(day.returns_pct)}</td>
                                        <td>{formatMoney(day.ads_amount, activeTrackingCurrencyCode)}</td>
                                        <td>{formatMoney(day.operational_errors, activeTrackingCurrencyCode)}</td>
                                        <td>{formatNumber(day.delivery_time_days)}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            </td>
                          </tr>
                        </Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </section>
      ) : null}

      {!loading && !error && tab === "sku" ? (
        <section>
          <PageSectionTitle title="Товары во времени" meta={`Рядов: ${formatNumber(skuRetrospective?.total_count)}`} />
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th className={styles.nameCell}>SKU / товар</th>
                  <th>Категория</th>
                  <th>Оборот</th>
                  <th>Прибыль</th>
                  <th>Маржинальность</th>
                  <th>Соинвест</th>
                  <th>Возвраты</th>
                  <th>Периоды</th>
                </tr>
              </thead>
              <tbody>
                {skuRows.length === 0 ? (
                  <tr><td colSpan={8} className={styles.empty}>Нет данных по товарам</td></tr>
                ) : skuRows.map((row) => (
                  <tr key={row.key}>
                    <td className={styles.nameCell}>
                      <div>{row.sku || "—"}</div>
                      <div className={styles.subtleText}>{row.item_name || row.label || "—"}</div>
                    </td>
                    <td className={styles.nameCell}>{row.category_path || "—"}</td>
                    <td>{formatMoney(row.revenue, activeStoreCurrencyCode)}</td>
                    <td>{formatMoney(row.profit_amount, activeStoreCurrencyCode)}</td>
                    <td>{formatPercent(row.profit_pct)}</td>
                    <td>{formatMoney(row.coinvest_amount, activeStoreCurrencyCode)}</td>
                    <td>{formatPercent(row.returns_pct)}</td>
                    <td className={styles.nameCell}>
                      {(row.periods || []).slice(0, 4).map((period) => (
                        <div key={`${row.key}-${period.period_key}`} className={styles.subtleText}>
                          {period.period_label}: {formatMoney(period.revenue, activeStoreCurrencyCode)} / {formatMoney(period.profit_amount, activeStoreCurrencyCode)}
                        </div>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {!loading && !error && tab === "category" ? (
        <section>
          <PageSectionTitle title="Категории во времени" meta={`Рядов: ${formatNumber(categoryRetrospective?.total_count)}`} />
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th className={styles.nameCell}>Категория</th>
                  <th>Оборот</th>
                  <th>Прибыль</th>
                  <th>Маржинальность</th>
                  <th>Соинвест</th>
                  <th>Возвраты</th>
                  <th>Периоды</th>
                </tr>
              </thead>
              <tbody>
                {categoryRows.length === 0 ? (
                  <tr><td colSpan={7} className={styles.empty}>Нет данных по категориям</td></tr>
                ) : categoryRows.map((row) => (
                  <tr key={row.key}>
                    <td className={styles.nameCell}>{row.label || row.category_path || "—"}</td>
                    <td>{formatMoney(row.revenue, activeStoreCurrencyCode)}</td>
                    <td>{formatMoney(row.profit_amount, activeStoreCurrencyCode)}</td>
                    <td>{formatPercent(row.profit_pct)}</td>
                    <td>{formatMoney(row.coinvest_amount, activeStoreCurrencyCode)}</td>
                    <td>{formatPercent(row.returns_pct)}</td>
                    <td className={styles.nameCell}>
                      {(row.periods || []).slice(0, 4).map((period) => (
                        <div key={`${row.key}-${period.period_key}`} className={styles.subtleText}>
                          {period.period_label}: {formatMoney(period.revenue, activeStoreCurrencyCode)} / {formatMoney(period.profit_amount, activeStoreCurrencyCode)}
                        </div>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </PageFrame>
  );
}
