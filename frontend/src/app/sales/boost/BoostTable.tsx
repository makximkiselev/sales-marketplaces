"use client";

import { MatrixMultiValue, MatrixNameCell, buildStoreLines, pricingMatrixStyles as matrixStyles } from "../../pricing/_components/PricingMatrixKit";
import { currencySymbol, formatMoney, formatPercent, StoreCtx } from "../../pricing/_shared/catalogPageShared";

export type BoostOverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  selected_decision_by_store?: Record<string, string>;
  selected_price_by_store?: Record<string, number | null>;
  coinvest_pct_by_store?: Record<string, number | null>;
  mrc_price_by_store?: Record<string, number | null>;
  mrc_with_boost_price_by_store?: Record<string, number | null>;
  rrc_price_by_store?: Record<string, number | null>;
  on_display_price_by_store?: Record<string, number | null>;
  internal_boost_by_store?: Record<string, number | null>;
  market_boost_by_store?: Record<string, number | null>;
  expected_boost_share_by_store?: Record<string, number | null>;
  orders_count_by_store?: Record<string, number>;
  revenue_by_store?: Record<string, number | null>;
  profit_by_store?: Record<string, number | null>;
  ads_by_store?: Record<string, number | null>;
  boosted_orders_count_by_store?: Record<string, number>;
  boosted_revenue_by_store?: Record<string, number | null>;
  boosted_ads_by_store?: Record<string, number | null>;
  boost_revenue_share_by_store?: Record<string, number | null>;
  boost_orders_share_by_store?: Record<string, number | null>;
  last_order_at_by_store?: Record<string, string>;
  updated_at: string;
};

type Props = {
  rows: BoostOverviewRow[];
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
  onPageChange: (updater: (page: number) => number) => void;
  onPageSizeChange: (value: number) => void;
  reportDateLabel?: string;
};

type RenderCtx = {
  tab: string;
  activeStoreUid: string;
  activeStoreCurrency: string | undefined;
  visibleStores: StoreCtx[];
};

function renderMoney(value: number | null | undefined, currencyCode: string | undefined) {
  const shown = formatMoney(value);
  if (shown === "—") return "—";
  return `${shown}${currencySymbol(currencyCode)}`;
}

function renderNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return "—";
  return Number(value || 0).toLocaleString("ru-RU");
}

function renderPercentValue(value: number | null | undefined) {
  return formatPercent(value);
}

function renderStoreValue<RowValue>(
  row: BoostOverviewRow,
  ctx: RenderCtx,
  getter: (storeUid: string) => RowValue,
  render: (value: RowValue, currencyCode: string | undefined) => React.ReactNode,
  suffix: string,
) {
  if (ctx.tab !== "all") {
    return render(getter(ctx.activeStoreUid), ctx.activeStoreCurrency);
  }
  return (
    <MatrixMultiValue
      rows={buildStoreLines(
        ctx.visibleStores,
        (store) => render(getter(store.store_uid), store.currency_code),
        `${row.sku}-${suffix}`,
      )}
    />
  );
}

export function BoostTable(props: Props) {
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
    onPageChange,
    onPageSizeChange,
    reportDateLabel,
  } = props;

  const renderCtx = { tab, activeStoreUid, activeStoreCurrency, visibleStores };

  return {
    tableMeta: (
      <span>
        {tableLoading ? "Обновление..." : `Всего: ${totalCount}`}
        {reportDateLabel ? ` • Дата: ${reportDateLabel}` : ""}
        {selectedTreePath ? ` • Фильтр: ${selectedTreePath}` : ""}
      </span>
    ),
    table: (
        <table className={matrixStyles.matrixTable}>
        <thead>
          <tr>
            <th>SKU</th>
            <th>Наименование товара</th>
            <th>Решение</th>
            <th>МРЦ</th>
            <th>МРЦ + буст</th>
            <th>РРЦ cap</th>
            <th>Финальная цена</th>
            <th>Цена на витрине</th>
            <th>Соинвест</th>
            <th>Буст в цене</th>
            <th>Буст в Маркете</th>
            <th>Доля показов</th>
            <th>Заказы</th>
            <th>{moneyHeader("Выручка")}</th>
            <th>{moneyHeader("Прибыль")}</th>
            <th>Заказы с бустом</th>
            <th>{moneyHeader("Выручка с бустом")}</th>
            <th>Доля буста</th>
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={18} className={matrixStyles.emptyCell}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</td>
            </tr>
          ) : rows.map((row) => (
            <tr key={`boost-${row.sku}`}>
              <td className={matrixStyles.skuCell}>{row.sku}</td>
              <MatrixNameCell name={row.name} path={row.tree_path} />
              <td className={matrixStyles.leftCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.selected_decision_by_store?.[storeUid],
                  (value) => <span>{String(value || "").trim() || "—"}</span>,
                  "selected-decision",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.mrc_price_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "mrc-price",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.mrc_with_boost_price_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "mrc-with-boost-price",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.rrc_price_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "rrc-price",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.selected_price_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "selected-price",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.on_display_price_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "on-display-price",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.coinvest_pct_by_store?.[storeUid],
                  (value) => <span>{renderPercentValue(value as number | null | undefined)}</span>,
                  "coinvest-pct",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.internal_boost_by_store?.[storeUid],
                  (value) => <span>{formatPercent(value as number | null | undefined)}</span>,
                  "internal-boost",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.market_boost_by_store?.[storeUid],
                  (value) => <span>{formatPercent(value as number | null | undefined)}</span>,
                  "market-boost",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.expected_boost_share_by_store?.[storeUid],
                  (value) => <span>{formatPercent(value as number | null | undefined)}</span>,
                  "expected-boost-share",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.orders_count_by_store?.[storeUid],
                  (value) => <span>{renderNumber(value as number | null | undefined)}</span>,
                  "orders-count",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.revenue_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "revenue",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.profit_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "profit",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.boosted_orders_count_by_store?.[storeUid],
                  (value) => <span>{renderNumber(value as number | null | undefined)}</span>,
                  "boosted-orders-count",
                )}
              </td>
              <td className={matrixStyles.moneyCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.boosted_revenue_by_store?.[storeUid],
                  (value, currencyCode) => <span>{renderMoney(value as number | null | undefined, currencyCode)}</span>,
                  "boosted-revenue",
                )}
              </td>
              <td className={matrixStyles.centerCell}>
                {renderStoreValue(
                  row,
                  renderCtx,
                  (storeUid) => row.boost_revenue_share_by_store?.[storeUid],
                  (value) => <span>{formatPercent(value as number | null | undefined)}</span>,
                  "boost-revenue-share",
                )}
              </td>
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
