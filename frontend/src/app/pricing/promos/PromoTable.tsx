"use client";

import type { ReactNode } from "react";
import { MatrixMultiValue, MatrixNameCell, buildStoreLines, pricingMatrixStyles as matrixStyles } from "../_components/PricingMatrixKit";
import { currencySymbol, formatMoney, StoreCtx } from "../_shared/catalogPageShared";

export type PromoColumn = {
  promo_id: string;
  promo_name: string;
};

type ScenarioPromoDetail = {
  promo_id?: string;
  promo_name?: string;
  status_label?: string;
  status_tone?: string;
  threshold_price?: number | null;
  detail?: string;
};

type ScenarioPromoPayload = {
  price?: number | null;
  boost_pct?: number | null;
  promo_count?: number | null;
  market_promo_status?: string;
  market_promo_message?: string;
  promo_details?: ScenarioPromoDetail[];
};

type ScenarioSet = {
  selected_price?: number | null;
  selected_boost_pct?: number | null;
  selected_coinvest_pct?: number | null;
  selected_decision_label?: string;
  selected_iteration_code?: string;
  rrc_price?: number | null;
  rrc_no_ads_price?: number | null;
  mrc_with_boost_price?: number | null;
  mrc_price?: number | null;
  rrc_with_boost?: ScenarioPromoPayload;
  rrc_no_ads?: ScenarioPromoPayload;
  mrc_with_boost?: ScenarioPromoPayload;
  mrc?: ScenarioPromoPayload;
};

export type PromoOverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  cogs_price_by_store?: Record<string, number | null>;
  stock_by_store?: Record<string, number | null>;
  installed_price_by_store?: Record<string, number | null>;
  iteration_scenarios_by_store?: Record<string, ScenarioSet>;
  updated_at: string;
};

type Props = {
  rows: PromoOverviewRow[];
  visibleStores: StoreCtx[];
  promoColumns: PromoColumn[];
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
};

type PromoTableConfig = {
  tableMeta: ReactNode;
  table: ReactNode;
  canPrev: boolean;
  canNext: boolean;
  onPrevPage: () => void;
  onNextPage: () => void;
  onPageSizeSelect: (value: number) => void;
  totalPages: number;
  pageSize: number;
  page: number;
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

function getScenarioSet(row: PromoOverviewRow, storeUid: string): ScenarioSet {
  return row.iteration_scenarios_by_store?.[storeUid] || {};
}

function getScenarioPayload(row: PromoOverviewRow, storeUid: string, scenarioKey: ScenarioKey): ScenarioPromoPayload {
  const scenarioSet = getScenarioSet(row, storeUid);
  const payload = scenarioSet[scenarioKey];
  return payload && typeof payload === "object" ? payload : {};
}

function getScenarioPrice(row: PromoOverviewRow, storeUid: string, scenarioKey: ScenarioPriceKey): number | null | undefined {
  const scenarioSet = getScenarioSet(row, storeUid);
  return scenarioSet[scenarioKey];
}

function getSelectedPrice(row: PromoOverviewRow, storeUid: string) {
  const scenarioSet = getScenarioSet(row, storeUid);
  return scenarioSet.selected_price ?? row.installed_price_by_store?.[storeUid] ?? null;
}

function getSelectedDecision(row: PromoOverviewRow, storeUid: string) {
  return String(getScenarioSet(row, storeUid).selected_decision_label || "").trim();
}

function getSelectedCoinvest(row: PromoOverviewRow, storeUid: string) {
  return getScenarioSet(row, storeUid).selected_coinvest_pct ?? null;
}

function promoStatusClass(statusTone: string | null | undefined) {
  const tone = String(statusTone || "").trim().toLowerCase();
  if (tone === "positive") return matrixStyles.statusPositive;
  if (tone === "negative") return matrixStyles.statusNegative;
  return matrixStyles.statusWarning;
}

function promoParticipates(detail: ScenarioPromoDetail) {
  return String(detail.status_tone || "").trim().toLowerCase() === "positive";
}

function renderPromoNames(payload: ScenarioPromoPayload | undefined) {
  const details = Array.isArray(payload?.promo_details) ? payload?.promo_details || [] : [];
  if (!details.length) return <span>—</span>;
  return (
    <div className={matrixStyles.multiValueCell}>
      {details.map((detail, index) => {
        const participates = promoParticipates(detail);
        return (
          <div key={`${detail.promo_id || detail.promo_name || "promo"}-${index}`} className={matrixStyles.multiLine}>
            <span className={`${matrixStyles.statusPill} ${participates ? matrixStyles.statusPositive : matrixStyles.statusNegative}`}>
              {participates ? "✓" : "✕"}
            </span>
            <div style={{ minWidth: 0, flex: 1 }}>
              <div>{detail.promo_name || "—"}</div>
              <div className={matrixStyles.mutedText}>{detail.status_label || (participates ? "Участвует" : "Не участвует")}</div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function renderPromoThresholds(payload: ScenarioPromoPayload | undefined, currencyCode: string | undefined) {
  const details = Array.isArray(payload?.promo_details) ? payload?.promo_details || [] : [];
  if (!details.length) return <span>—</span>;
  return (
    <div className={matrixStyles.multiValueCell}>
      {details.map((detail, index) => (
        <div key={`${detail.promo_id || detail.promo_name || "threshold"}-${index}`} className={matrixStyles.multiLine}>
          <span>{renderMoney(detail.threshold_price, currencyCode)}</span>
        </div>
      ))}
    </div>
  );
}

function renderStoreMatrix(
  row: PromoOverviewRow,
  ctx: RenderCtx,
  keyPrefix: string,
  map: (store: StoreCtx) => ReactNode,
) {
  return <MatrixMultiValue rows={buildStoreLines(ctx.visibleStores, map, `${row.sku}-${keyPrefix}`)} />;
}

export function PromoTable(props: Props): PromoTableConfig {
  const {
    rows,
    visibleStores,
    promoColumns: _promoColumns,
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
  } = props;

  const renderCtx = { tab, activeStoreUid, activeStoreCurrency, visibleStores };
  const totalColumns = 11;

  const renderStockCell = (row: PromoOverviewRow) => {
    if (tab !== "all") return <span>{formatStock(row.stock_by_store?.[activeStoreUid])}</span>;
    return renderStoreMatrix(row, renderCtx, "stock", (store) => formatStock(row.stock_by_store?.[store.store_uid]));
  };

  const renderCogsCell = (row: PromoOverviewRow) => {
    if (tab !== "all") return <span>{renderMoney(row.cogs_price_by_store?.[activeStoreUid], activeStoreCurrency)}</span>;
    return renderStoreMatrix(row, renderCtx, "cogs", (store) => renderMoney(row.cogs_price_by_store?.[store.store_uid], store.currency_code));
  };

  const renderSelectedPriceCell = (row: PromoOverviewRow) => {
    if (tab !== "all") return <span>{renderMoney(getSelectedPrice(row, activeStoreUid), activeStoreCurrency)}</span>;
    return renderStoreMatrix(row, renderCtx, "selected-price", (store) => renderMoney(getSelectedPrice(row, store.store_uid), store.currency_code));
  };

  const renderDecisionCell = (row: PromoOverviewRow) => {
    if (tab !== "all") return <span>{getSelectedDecision(row, activeStoreUid) || "—"}</span>;
    return renderStoreMatrix(row, renderCtx, "selected-decision", (store) => getSelectedDecision(row, store.store_uid) || "—");
  };

  const renderCoinvestCell = (row: PromoOverviewRow) => {
    if (tab !== "all") {
      const raw = getSelectedCoinvest(row, activeStoreUid);
      return <span>{raw == null || Number.isNaN(Number(raw)) ? "—" : `${Math.round(Number(raw) * 100) / 100}%`}</span>;
    }
    return renderStoreMatrix(row, renderCtx, "selected-coinvest", (store) => {
      const raw = getSelectedCoinvest(row, store.store_uid);
      return raw == null || Number.isNaN(Number(raw)) ? "—" : `${Math.round(Number(raw) * 100) / 100}%`;
    });
  };

  const renderScenarioPriceCell = (row: PromoOverviewRow, scenarioKey: ScenarioPriceKey) => {
    if (tab !== "all") return <span>{renderMoney(getScenarioPrice(row, activeStoreUid, scenarioKey), activeStoreCurrency)}</span>;
    return renderStoreMatrix(row, renderCtx, `scenario-price-${scenarioKey}`, (store) => renderMoney(getScenarioPrice(row, store.store_uid, scenarioKey), store.currency_code));
  };

  const renderScenarioPromosCell = (row: PromoOverviewRow, scenarioKey: ScenarioKey) => {
    if (tab !== "all") return renderPromoNames(getScenarioPayload(row, activeStoreUid, scenarioKey));
    return renderStoreMatrix(row, renderCtx, `scenario-promos-${scenarioKey}`, (store) => renderPromoNames(getScenarioPayload(row, store.store_uid, scenarioKey)));
  };

  const renderScenarioThresholdsCell = (row: PromoOverviewRow, scenarioKey: ScenarioKey) => {
    if (tab !== "all") return renderPromoThresholds(getScenarioPayload(row, activeStoreUid, scenarioKey), activeStoreCurrency);
    return renderStoreMatrix(row, renderCtx, `scenario-thresholds-${scenarioKey}`, (store) => renderPromoThresholds(getScenarioPayload(row, store.store_uid, scenarioKey), store.currency_code));
  };

  return {
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
            <th>Промо</th>
            <th>Пороги</th>
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
            <tr key={`promo-${row.sku}`}>
              <td className={matrixStyles.skuCell}>{row.sku}</td>
              <MatrixNameCell name={row.name} path={row.tree_path} />
              <td className={`${matrixStyles.stockCell} ${matrixStyles.centerCell}`}>{renderStockCell(row)}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderCogsCell(row)}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderSelectedPriceCell(row)}</td>
              <td className={matrixStyles.leftCell}>{renderDecisionCell(row)}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderScenarioPriceCell(row, "mrc_price")}</td>
              <td className={matrixStyles.leftCell}>{renderScenarioPromosCell(row, "mrc")}</td>
              <td className={matrixStyles.leftCell}>{renderScenarioThresholdsCell(row, "mrc")}</td>
              <td className={`${matrixStyles.moneyCell} ${matrixStyles.centerCell}`}>{renderScenarioPriceCell(row, "mrc_with_boost_price")}</td>
              <td className={matrixStyles.centerCell}>{renderCoinvestCell(row)}</td>
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
