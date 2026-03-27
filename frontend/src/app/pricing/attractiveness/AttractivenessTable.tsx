"use client";

import type { ReactNode } from "react";
import { MatrixMultiValue, MatrixNameCell, buildStoreLines, pricingMatrixStyles as matrixStyles } from "../_components/PricingMatrixKit";
import commonStyles from "../_components/PricingPageCommon.module.css";
import styles from "./AttractivenessPage.module.css";
import { StoreCtx, currencySymbol, formatMoney } from "../_shared/catalogPageShared";
import { AttractivenessOverviewRow } from "./attractivenessUtils";

type Props = {
  rows: AttractivenessOverviewRow[];
  visibleStores: StoreCtx[];
  tab: string;
  activeStoreUid: string;
  activeStoreCurrency: string | undefined;
    isOzonView: boolean;
  moneyHeader: (label: string) => string;
  page: number;
  totalPages: number;
  pageSize: number;
  totalCount: number;
  selectedTreePath: string;
  tableLoading: boolean;
  statusFilter: "all" | "profitable" | "moderate" | "overpriced";
  onStatusFilterChange: (value: "all" | "profitable" | "moderate" | "overpriced") => void;
  onPageChange: (updater: (page: number) => number) => void;
  onPageSizeChange: (value: number) => void;
};

type RenderCtx = {
  tab: string;
  activeStoreUid: string;
  activeStoreCurrency: string | undefined;
  visibleStores: StoreCtx[];
};

type ScenarioKey = "rrc_with_boost" | "rrc_no_ads" | "mrc_with_boost" | "mrc";
type ScenarioPriceKey = "rrc_price" | "rrc_no_ads_price" | "mrc_with_boost_price" | "mrc_price";

function renderMoney(value: number | null | undefined, currencyCode: string | undefined) {
  const shown = formatMoney(value);
  if (shown === "—") return "—";
  return `${shown}${currencySymbol(currencyCode)}`;
}

function formatStock(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  const num = Number(value);
  return Number.isInteger(num) ? String(num) : String(Math.round(num * 100) / 100);
}

function renderStoreMatrix(
  row: AttractivenessOverviewRow,
  ctx: RenderCtx,
  keyPrefix: string,
  map: (store: StoreCtx) => ReactNode,
) {
  return <MatrixMultiValue rows={buildStoreLines(ctx.visibleStores, map, `${row.sku}-${keyPrefix}`)} />;
}

function getScenarioSet(row: AttractivenessOverviewRow, storeUid: string) {
  return row.iteration_scenarios_by_store?.[storeUid] || {};
}

function getScenarioPayload(row: AttractivenessOverviewRow, storeUid: string, scenarioKey: ScenarioKey) {
  const payload = getScenarioSet(row, storeUid)?.[scenarioKey];
  return payload && typeof payload === "object" ? payload : {};
}

function getScenarioPrice(row: AttractivenessOverviewRow, storeUid: string, priceKey: ScenarioPriceKey) {
  return getScenarioSet(row, storeUid)?.[priceKey] ?? null;
}

function getSelectedPrice(row: AttractivenessOverviewRow, storeUid: string) {
  return getScenarioSet(row, storeUid)?.selected_price ?? row.installed_price_by_store?.[storeUid] ?? null;
}

function getSelectedDecision(row: AttractivenessOverviewRow, storeUid: string) {
  return String(getScenarioSet(row, storeUid)?.selected_decision_label || "").trim();
}

function getSelectedCoinvest(row: AttractivenessOverviewRow, storeUid: string) {
  return getScenarioSet(row, storeUid)?.selected_coinvest_pct ?? null;
}

function renderStatusBadge(label: string | null | undefined) {
  const normalized = String(label || "").trim() || "—";
  const className =
    normalized === "Выгодная"
      ? styles.statusGood
      : normalized === "Умеренная"
        ? styles.statusMid
        : styles.statusBad;
  return <span className={`${styles.statusBadge} ${className}`}>{normalized}</span>;
}

export function AttractivenessTable(props: Props) {
  const {
    rows,
    visibleStores,
    tab,
    activeStoreUid,
    activeStoreCurrency,
    isOzonView: _isOzonView,
    moneyHeader,
    page,
    totalPages,
    pageSize,
    totalCount,
    selectedTreePath,
    tableLoading,
    statusFilter,
    onStatusFilterChange,
    onPageChange,
    onPageSizeChange,
  } = props;

  const renderCtx = {
    tab,
    activeStoreUid,
    activeStoreCurrency,
    visibleStores,
  };
  const totalColumns = 10;

  const renderStockCell = (row: AttractivenessOverviewRow) => {
    if (tab !== "all") return <span>{formatStock(row.stock_by_store?.[activeStoreUid])}</span>;
    return renderStoreMatrix(row, renderCtx, "stock", (store) => formatStock(row.stock_by_store?.[store.store_uid]));
  };

  const renderCogsCell = (row: AttractivenessOverviewRow) => {
    if (tab !== "all") return <span>{renderMoney(row.cogs_price_by_store?.[activeStoreUid], activeStoreCurrency)}</span>;
    return renderStoreMatrix(row, renderCtx, "cogs", (store) => renderMoney(row.cogs_price_by_store?.[store.store_uid], store.currency_code));
  };

  const renderSelectedPriceCell = (row: AttractivenessOverviewRow) => {
    if (tab !== "all") return <span>{renderMoney(getSelectedPrice(row, activeStoreUid), activeStoreCurrency)}</span>;
    return renderStoreMatrix(row, renderCtx, "selected-price", (store) => renderMoney(getSelectedPrice(row, store.store_uid), store.currency_code));
  };

  const renderDecisionCell = (row: AttractivenessOverviewRow) => {
    if (tab !== "all") return <span>{getSelectedDecision(row, activeStoreUid) || "—"}</span>;
    return renderStoreMatrix(row, renderCtx, "selected-decision", (store) => getSelectedDecision(row, store.store_uid) || "—");
  };

  const renderSelectedCoinvestCell = (row: AttractivenessOverviewRow) => {
    if (tab !== "all") {
      const raw = getSelectedCoinvest(row, activeStoreUid);
      return <span>{raw == null || Number.isNaN(Number(raw)) ? "—" : `${Math.round(Number(raw) * 100) / 100}%`}</span>;
    }
    return renderStoreMatrix(row, renderCtx, "selected-coinvest", (store) => {
      const raw = getSelectedCoinvest(row, store.store_uid);
      return raw == null || Number.isNaN(Number(raw)) ? "—" : `${Math.round(Number(raw) * 100) / 100}%`;
    });
  };

  const renderScenarioPriceCell = (row: AttractivenessOverviewRow, scenarioKey: ScenarioPriceKey) => {
    if (tab !== "all") return <span>{renderMoney(getScenarioPrice(row, activeStoreUid, scenarioKey), activeStoreCurrency)}</span>;
    return renderStoreMatrix(row, renderCtx, `scenario-price-${scenarioKey}`, (store) => renderMoney(getScenarioPrice(row, store.store_uid, scenarioKey), store.currency_code));
  };

  const renderScenarioStatusCell = (row: AttractivenessOverviewRow, scenarioKey: ScenarioKey) => {
    if (tab !== "all") return renderStatusBadge(getScenarioPayload(row, activeStoreUid, scenarioKey).status_label as string | undefined);
    return renderStoreMatrix(row, renderCtx, `scenario-status-${scenarioKey}`, (store) => renderStatusBadge(getScenarioPayload(row, store.store_uid, scenarioKey).status_label as string | undefined));
  };

  return {
    tableTitleControls: (
      <div className={styles.statusFilterWrap}>
        <select
          className={`input ${commonStyles.select} ${styles.statusSelect}`}
          value={statusFilter}
          onChange={(e) => {
            onPageChange(() => 1);
            onStatusFilterChange((e.target.value as "all" | "profitable" | "moderate" | "overpriced") || "all");
          }}
        >
          <option value="all">Все</option>
          <option value="profitable">Выгодная цена</option>
          <option value="moderate">Умеренная цена</option>
          <option value="overpriced">Завышенная цена</option>
        </select>
      </div>
    ),
    tableMeta: <span>{tableLoading ? "Обновление..." : `Всего: ${totalCount}`}{selectedTreePath ? ` • Фильтр: ${selectedTreePath}` : ""}</span>,
    table: (
      <table className={matrixStyles.matrixTable}>
        <thead>
          <tr>
            <th>SKU</th>
            <th className={matrixStyles.nameHeader}>Наименование товара</th>
            <th className={matrixStyles.stockHeader}>Остаток</th>
            <th>Себестоимость</th>
            <th>{moneyHeader("Финальная цена")}</th>
            <th>Решение</th>
            <th>{moneyHeader("МРЦ")}</th>
            <th>Статус</th>
            <th>{moneyHeader("МРЦ + буст")}</th>
            <th>Соинвест</th>
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={totalColumns} className={matrixStyles.emptyCell}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</td>
            </tr>
          ) : rows.map((row) => (
            <tr key={`attract-${row.sku}`}>
              <td className={matrixStyles.skuCell}>{row.sku}</td>
              <MatrixNameCell name={row.name} path={row.tree_path} />
              <td className={`${matrixStyles.stockCell} ${matrixStyles.centerCell}`}>{renderStockCell(row)}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderCogsCell(row)}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderSelectedPriceCell(row)}</td>
              <td className={matrixStyles.leftCell}>{renderDecisionCell(row)}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderScenarioPriceCell(row, "mrc_price")}</td>
              <td className={matrixStyles.centerCell}>{renderScenarioStatusCell(row, "mrc")}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderScenarioPriceCell(row, "mrc_with_boost_price")}</td>
              <td className={matrixStyles.centerCell}>{renderSelectedCoinvestCell(row)}</td>
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
