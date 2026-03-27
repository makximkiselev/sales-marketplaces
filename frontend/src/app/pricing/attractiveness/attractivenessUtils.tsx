import { ReactNode } from "react";
import { MatrixMultiValue, buildStoreLines } from "../_components/PricingMatrixKit";
import { currencySymbol, formatMoney, formatPercent, StoreCtx } from "../_shared/catalogPageShared";

export type AttractMetric = {
  attractiveness_set_price?: number | null;
  attractiveness_set_profit_abs?: number | null;
  attractiveness_set_profit_pct?: number | null;
  attractiveness_overpriced_price?: number | null;
  attractiveness_overpriced_profit_abs?: number | null;
  attractiveness_overpriced_profit_pct?: number | null;
  attractiveness_moderate_price?: number | null;
  attractiveness_moderate_profit_abs?: number | null;
  attractiveness_moderate_profit_pct?: number | null;
  attractiveness_profitable_price?: number | null;
  attractiveness_profitable_profit_abs?: number | null;
  attractiveness_profitable_profit_pct?: number | null;
  ozon_competitor_price?: number | null;
  ozon_competitor_profit_abs?: number | null;
  ozon_competitor_profit_pct?: number | null;
  external_competitor_price?: number | null;
  external_competitor_profit_abs?: number | null;
  external_competitor_profit_pct?: number | null;
  attractiveness_chosen_price?: number | null;
  attractiveness_chosen_profit_abs?: number | null;
  attractiveness_chosen_profit_pct?: number | null;
  attractiveness_chosen_boost_bid_percent?: number | null;
};

export type AttractivenessOverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  cogs_price_by_store?: Record<string, number | null>;
  mrc_price_by_store?: Record<string, number | null>;
  installed_price_by_store?: Record<string, number | null>;
  installed_profit_abs_by_store?: Record<string, number | null>;
  installed_profit_pct_by_store?: Record<string, number | null>;
  chosen_boost_bid_by_store?: Record<string, number | null>;
  stock_by_store?: Record<string, number | null>;
  status_by_store?: Record<string, string>;
  base_price_by_store?: Record<string, number | null>;
  price_metrics_by_store?: Record<string, {
    target_price?: number | null;
    rrc_no_ads_price?: number | null;
    target_profit_abs?: number | null;
    target_profit_pct?: number | null;
    mrc_price?: number | null;
    mrc_profit_abs?: number | null;
    mrc_profit_pct?: number | null;
  }>;
  attractiveness_by_store?: Record<string, AttractMetric>;
  iteration_scenarios_by_store?: Record<
    string,
    {
      selected_price?: number | null;
      selected_boost_pct?: number | null;
      selected_coinvest_pct?: number | null;
      selected_decision_label?: string;
      selected_iteration_code?: string;
      rrc_price?: number | null;
      rrc_no_ads_price?: number | null;
      mrc_with_boost_price?: number | null;
      mrc_price?: number | null;
      rrc_with_boost?: {
        price?: number | null;
        status_code?: string;
        status_label?: string;
        boost_pct?: number | null;
        coinvest_pct?: number | null;
        on_display_price?: number | null;
      };
      rrc_no_ads?: {
        price?: number | null;
        status_code?: string;
        status_label?: string;
        boost_pct?: number | null;
        coinvest_pct?: number | null;
        on_display_price?: number | null;
      };
      mrc_with_boost?: {
        price?: number | null;
        status_code?: string;
        status_label?: string;
        boost_pct?: number | null;
        coinvest_pct?: number | null;
        on_display_price?: number | null;
      };
      mrc?: {
        price?: number | null;
        status_code?: string;
        status_label?: string;
        boost_pct?: number | null;
        coinvest_pct?: number | null;
        on_display_price?: number | null;
      };
    }
  >;
  updated_at: string;
};

export function resolveAttractivenessStatus(
  metric: AttractMetric | undefined,
  platformCode: string = "yandex_market",
): "Выгодная" | "Умеренная" | "Завышенная" {
  if (!metric) return "Выгодная";
  const hasAnyPriceData = [
    metric.attractiveness_chosen_price,
    metric.attractiveness_set_price,
    metric.attractiveness_overpriced_price,
    metric.attractiveness_moderate_price,
    metric.attractiveness_profitable_price,
    metric.ozon_competitor_price,
    metric.external_competitor_price,
  ].some((value) => Number.isFinite(Number(value)));
  if (!hasAnyPriceData) return "Выгодная";

  const chosen = Number(metric.attractiveness_chosen_price);
  if (!Number.isFinite(chosen)) return "Выгодная";

  const profitable = Number(metric.attractiveness_profitable_price);
  const moderate = Number(metric.attractiveness_moderate_price);
  const nonProfitable = Number(metric.attractiveness_overpriced_price);
  const hasProf = Number.isFinite(profitable);
  const hasMod = Number.isFinite(moderate);
  const hasNonProf = Number.isFinite(nonProfitable);

  if (!hasProf && !hasMod && !hasNonProf) return "Выгодная";
  if (platformCode === "yandex_market") {
    if (hasProf && chosen <= profitable) return "Выгодная";
    if (hasMod) return chosen <= moderate ? "Умеренная" : "Завышенная";
    return "Умеренная";
  }
  if (hasProf && chosen <= profitable) return "Выгодная";
  if (hasMod && chosen <= moderate) return "Умеренная";
  return "Завышенная";
}

type RenderCtx = {
  tab: string;
  activeStoreUid: string;
  activeStoreCurrency: string | undefined;
  visibleStores: StoreCtx[];
};

export function renderStoreMoneyMatrix(
  row: AttractivenessOverviewRow,
  visibleStores: StoreCtx[],
  keyPrefix: string,
  pick: (store: StoreCtx) => number | null | undefined,
): ReactNode {
  return (
    <MatrixMultiValue
      rows={buildStoreLines(visibleStores, (store) => {
        const shown = formatMoney(pick(store));
        return shown === "—" ? "—" : `${shown}${currencySymbol(store.currency_code)}`;
      }, keyPrefix)}
    />
  );
}

export function renderStorePercentMatrix(
  visibleStores: StoreCtx[],
  keyPrefix: string,
  pick: (store: StoreCtx) => number | null | undefined,
): ReactNode {
  return (
    <MatrixMultiValue
      rows={buildStoreLines(visibleStores, (store) => formatPercent(pick(store)), keyPrefix)}
    />
  );
}

export function renderAttrMoneyCell(
  row: AttractivenessOverviewRow,
  field: keyof AttractMetric,
  ctx: RenderCtx,
): ReactNode {
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.attractiveness_by_store?.[ctx.activeStoreUid] : undefined;
    const value = metric?.[field] as number | null | undefined;
    const shown = formatMoney(value);
    return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(ctx.activeStoreCurrency)}`}</span>;
  }
  return renderStoreMoneyMatrix(row, ctx.visibleStores, `${row.sku}-attr-${String(field)}`, (store) => {
    const metric = row.attractiveness_by_store?.[store.store_uid];
    return metric?.[field] as number | null | undefined;
  });
}

export function renderAttrPercentCell(
  row: AttractivenessOverviewRow,
  field: keyof AttractMetric,
  ctx: RenderCtx,
): ReactNode {
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.attractiveness_by_store?.[ctx.activeStoreUid] : undefined;
    return <span>{formatPercent(metric?.[field] as number | null | undefined)}</span>;
  }
  return renderStorePercentMatrix(ctx.visibleStores, `${row.sku}-attr-pct-${String(field)}`, (store) => {
    const metric = row.attractiveness_by_store?.[store.store_uid];
    return metric?.[field] as number | null | undefined;
  });
}

export function renderMetricMoneyCell(
  row: AttractivenessOverviewRow,
  pick: (metric: NonNullable<AttractivenessOverviewRow["price_metrics_by_store"]>[string]) => number | null | undefined,
  ctx: RenderCtx,
): ReactNode {
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.price_metrics_by_store?.[ctx.activeStoreUid] : undefined;
    const value = metric ? pick(metric) : null;
    const shown = formatMoney(value);
    return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(ctx.activeStoreCurrency)}`}</span>;
  }
  return renderStoreMoneyMatrix(row, ctx.visibleStores, `${row.sku}-metric-money`, (store) => {
    const metric = row.price_metrics_by_store?.[store.store_uid];
    return metric ? pick(metric) : null;
  });
}

export function renderMetricPercentCell(
  row: AttractivenessOverviewRow,
  pick: (metric: NonNullable<AttractivenessOverviewRow["price_metrics_by_store"]>[string]) => number | null | undefined,
  ctx: RenderCtx,
): ReactNode {
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.price_metrics_by_store?.[ctx.activeStoreUid] : undefined;
    return <span>{formatPercent(metric ? pick(metric) : null)}</span>;
  }
  return renderStorePercentMatrix(ctx.visibleStores, `${row.sku}-metric-pct`, (store) => {
    const metric = row.price_metrics_by_store?.[store.store_uid];
    return metric ? pick(metric) : null;
  });
}

export function renderMetricProfitCell(
  row: AttractivenessOverviewRow,
  pickAbs: (metric: NonNullable<AttractivenessOverviewRow["price_metrics_by_store"]>[string]) => number | null | undefined,
  pickPct: (metric: NonNullable<AttractivenessOverviewRow["price_metrics_by_store"]>[string]) => number | null | undefined,
  ctx: RenderCtx,
): ReactNode {
  const formatCombined = (abs: number | null | undefined, pct: number | null | undefined, currencyCode: string | undefined) => {
    const absShown = formatMoney(abs);
    const pctShown = formatPercent(pct);
    if (absShown === "—" && pctShown === "—") return "—";
    if (absShown === "—") return pctShown;
    const money = `${absShown}${currencySymbol(currencyCode)}`;
    return pctShown === "—" ? money : `${money} (${pctShown})`;
  };
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.price_metrics_by_store?.[ctx.activeStoreUid] : undefined;
    return <span>{formatCombined(metric ? pickAbs(metric) : null, metric ? pickPct(metric) : null, ctx.activeStoreCurrency)}</span>;
  }
  return (
    <MatrixMultiValue
      rows={buildStoreLines(ctx.visibleStores, (store) => {
        const metric = row.price_metrics_by_store?.[store.store_uid];
        return formatCombined(metric ? pickAbs(metric) : null, metric ? pickPct(metric) : null, store.currency_code);
      }, `${row.sku}-metric-profit`)}
    />
  );
}

export function renderRrcCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  if (ctx.tab !== "all") {
    const value = ctx.activeStoreUid ? row.base_price_by_store?.[ctx.activeStoreUid] : null;
    const shown = formatMoney(value);
    return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(ctx.activeStoreCurrency)}`}</span>;
  }
  return renderStoreMoneyMatrix(row, ctx.visibleStores, `${row.sku}-base-price`, (store) => row.base_price_by_store?.[store.store_uid]);
}

export function renderMrcCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  if (ctx.tab !== "all") {
    const value = ctx.activeStoreUid ? row.mrc_price_by_store?.[ctx.activeStoreUid] : null;
    const shown = formatMoney(value);
    return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(ctx.activeStoreCurrency)}`}</span>;
  }
  return renderStoreMoneyMatrix(row, ctx.visibleStores, `${row.sku}-mrc-price`, (store) => row.mrc_price_by_store?.[store.store_uid]);
}

export function renderStockCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  const formatStock = (value: number | null | undefined) => {
    if (value == null || Number.isNaN(Number(value))) return "—";
    const num = Number(value);
    return Number.isInteger(num) ? String(num) : String(Math.round(num * 100) / 100);
  };
  if (ctx.tab !== "all") {
    return <span>{formatStock(row.stock_by_store?.[ctx.activeStoreUid])}</span>;
  }
  return (
    <MatrixMultiValue
      rows={buildStoreLines(ctx.visibleStores, (store) => formatStock(row.stock_by_store?.[store.store_uid]), `${row.sku}-attr-stock`)}
    />
  );
}

export function renderCogsCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  if (ctx.tab !== "all") {
    const value = ctx.activeStoreUid ? row.cogs_price_by_store?.[ctx.activeStoreUid] : null;
    const shown = formatMoney(value);
    return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(ctx.activeStoreCurrency)}`}</span>;
  }
  return renderStoreMoneyMatrix(row, ctx.visibleStores, `${row.sku}-attr-cogs`, (store) => row.cogs_price_by_store?.[store.store_uid]);
}

export function renderInstalledPriceCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  if (ctx.tab !== "all") {
    const value = ctx.activeStoreUid ? row.installed_price_by_store?.[ctx.activeStoreUid] : null;
    const shown = formatMoney(value);
    return <span>{shown === "—" ? "—" : `${shown}${currencySymbol(ctx.activeStoreCurrency)}`}</span>;
  }
  return (
    <div style={{ display: "flex", justifyContent: "center" }}>
      {renderStoreMoneyMatrix(row, ctx.visibleStores, `${row.sku}-installed-price`, (store) => row.installed_price_by_store?.[store.store_uid])}
    </div>
  );
}

export function renderInstalledProfitCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  const formatCombined = (abs: number | null | undefined, pct: number | null | undefined, currencyCode: string | undefined) => {
    const absShown = formatMoney(abs);
    const pctShown = formatPercent(pct);
    if (absShown === "—" && pctShown === "—") return "—";
    if (absShown === "—") return pctShown;
    const money = `${absShown}${currencySymbol(currencyCode)}`;
    return pctShown === "—" ? money : `${money} (${pctShown})`;
  };
  if (ctx.tab !== "all") {
    return (
      <span>
        {formatCombined(
          ctx.activeStoreUid ? row.installed_profit_abs_by_store?.[ctx.activeStoreUid] : null,
          ctx.activeStoreUid ? row.installed_profit_pct_by_store?.[ctx.activeStoreUid] : null,
          ctx.activeStoreCurrency,
        )}
      </span>
    );
  }
  return (
    <div style={{ display: "flex", justifyContent: "center" }}>
      <MatrixMultiValue
        rows={buildStoreLines(ctx.visibleStores, (store) => formatCombined(
          row.installed_profit_abs_by_store?.[store.store_uid],
          row.installed_profit_pct_by_store?.[store.store_uid],
          store.currency_code,
        ), `${row.sku}-installed-profit`)}
      />
    </div>
  );
}

export function renderChosenBoostCell(row: AttractivenessOverviewRow, ctx: RenderCtx): ReactNode {
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.attractiveness_by_store?.[ctx.activeStoreUid] : undefined;
    const value = (metric?.attractiveness_chosen_boost_bid_percent ?? (ctx.activeStoreUid ? row.chosen_boost_bid_by_store?.[ctx.activeStoreUid] : null)) as number | null | undefined;
    return <span>{formatPercent(value)}</span>;
  }
  return (
    <MatrixMultiValue
      rows={buildStoreLines(
        ctx.visibleStores,
        (store) => {
          const metric = row.attractiveness_by_store?.[store.store_uid];
          const value = metric?.attractiveness_chosen_boost_bid_percent ?? row.chosen_boost_bid_by_store?.[store.store_uid];
          return formatPercent(value);
        },
        `${row.sku}-chosen-boost`,
      )}
    />
  );
}

export function renderAttrProfitCell(
  row: AttractivenessOverviewRow,
  absField: keyof AttractMetric,
  pctField: keyof AttractMetric,
  ctx: RenderCtx,
): ReactNode {
  const formatCombined = (abs: number | null | undefined, pct: number | null | undefined, currencyCode: string | undefined) => {
    const absShown = formatMoney(abs);
    const pctShown = formatPercent(pct);
    if (absShown === "—" && pctShown === "—") return "—";
    if (absShown === "—") return pctShown;
    const money = `${absShown}${currencySymbol(currencyCode)}`;
    return pctShown === "—" ? money : `${money} (${pctShown})`;
  };
  if (ctx.tab !== "all") {
    const metric = ctx.activeStoreUid ? row.attractiveness_by_store?.[ctx.activeStoreUid] : undefined;
    return (
      <span>
        {formatCombined(metric?.[absField] as number | null | undefined, metric?.[pctField] as number | null | undefined, ctx.activeStoreCurrency)}
      </span>
    );
  }
  return (
    <MatrixMultiValue
      rows={buildStoreLines(ctx.visibleStores, (store) => {
        const metric = row.attractiveness_by_store?.[store.store_uid];
        return formatCombined(
          metric?.[absField] as number | null | undefined,
          metric?.[pctField] as number | null | undefined,
          store.currency_code,
        );
      }, `${row.sku}-attr-profit-${String(absField)}-${String(pctField)}`)}
    />
  );
}
