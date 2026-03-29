import type {
  EditableFieldKey,
  IntegrationsResponse,
  LogisticsEditableFieldKey,
  LogisticsRow,
  LogisticsStoreSettingsApi,
  PlatformItem,
  PricingCategoryRow,
  PricingCategoryTreeApiRow,
  PricingStoreSettingsApi,
  PricingTableColumn,
  StoreItem,
} from "./types";

export function buildPlatformsFromIntegrations(data: IntegrationsResponse): PlatformItem[] {
  const ymAccounts = Array.isArray(data.yandex_market?.accounts) ? data.yandex_market.accounts : [];
  const ymStores: StoreItem[] = [];
  for (const account of ymAccounts) {
    const businessId = String(account?.business_id || "").trim();
    const shops = Array.isArray(account?.shops) ? account.shops : [];
    for (const shop of shops) {
      const campaignId = String(shop?.campaign_id || "").trim();
      if (!campaignId) continue;
      const campaignName = String(shop?.campaign_name || "").trim() || `Магазин ${campaignId}`;
      ymStores.push({
        id: campaignId,
        name: businessId ? `${campaignName} (${businessId})` : campaignName,
        currencyCode: String(shop?.currency_code || "RUB").toUpperCase() === "USD" ? "USD" : "RUB",
        fulfillmentModel: normalizeFulfillmentModel(shop?.fulfillment_model),
      });
    }
  }

  const ozAccounts = Array.isArray(data.ozon?.accounts) ? data.ozon.accounts : [];
  const ozStores = ozAccounts
    .map((acc) => {
      const clientId = String(acc?.client_id || "").trim();
      if (!clientId) return null;
      const sellerId = String(acc?.seller_id || "").trim();
      const sellerName = String(acc?.seller_name || "").trim() || `Ozon кабинет ${clientId}`;
      const currencyCode: "RUB" | "USD" = String(acc?.currency_code || "RUB").toUpperCase() === "USD" ? "USD" : "RUB";
      return {
        id: clientId,
        name: sellerId ? `${sellerName} (${sellerId})` : sellerName,
        currencyCode,
        fulfillmentModel: normalizeFulfillmentModel(acc?.fulfillment_model),
      };
    })
    .filter((v): v is StoreItem => v !== null);

  return [
    { id: "yandex_market", label: "Яндекс.Маркет", stores: ymStores },
    { id: "ozon", label: "Ozon", stores: ozStores },
  ];
}

export function normalizeFulfillmentModel(value: unknown): "FBO" | "FBS" | "DBS" | "EXPRESS" {
  const v = String(value || "FBO").toUpperCase();
  return v === "FBS" || v === "DBS" || v === "EXPRESS" ? v : "FBO";
}

export function buildCategoryRows(rows: PricingCategoryTreeApiRow[]): PricingCategoryRow[] {
  const looksLikeProductOrBrandLeaf = (value: string) => {
    const text = String(value || "").trim();
    if (!text) return false;
    if (/\d/.test(text)) return true;
    if (/[(),+]/.test(text)) return true;
    if (text.length > 24) return true;
    if (!text.includes(" ") && /[A-Za-z]/.test(text)) return true;
    return false;
  };
  const built = rows.map((r) => ({
    key: String(r.leaf_path || [r.category, r.subcategory_1, r.subcategory_2, r.subcategory_3].filter(Boolean).join(" / ")),
    leafPath: String(r.leaf_path || "").trim(),
    category: String(r.category || "").trim() || "Без категории",
    subcategoryLevels: [r.subcategory_1, r.subcategory_2, r.subcategory_3].map((v) => String(v || "").trim()).filter(Boolean),
    itemsCount: Number(r.items_count || 0),
    values: {
      commission_percent: r.commission_percent == null ? null : Number(r.commission_percent),
      acquiring_percent: r.acquiring_percent == null ? null : Number(r.acquiring_percent),
      logistics_rub: r.logistics_rub == null ? null : Number(r.logistics_rub),
      ads_percent: r.ads_percent == null ? null : Number(r.ads_percent),
      returns_percent: r.returns_percent == null ? null : Number(r.returns_percent),
      tax_percent: r.tax_percent == null ? null : Number(r.tax_percent),
      other_expenses_rub: r.other_expenses_rub == null ? null : Number(r.other_expenses_rub),
      other_expenses_percent: r.other_expenses_percent == null ? null : Number(r.other_expenses_percent),
      cogs_rub: r.cogs_rub == null ? null : Number(r.cogs_rub),
      target_profit_rub: r.target_profit_rub == null ? null : Number(r.target_profit_rub),
      target_profit_percent: r.target_profit_percent == null ? null : Number(r.target_profit_percent),
      target_margin_rub: r.target_margin_rub == null ? null : Number(r.target_margin_rub),
      target_margin_percent: r.target_margin_percent == null ? null : Number(r.target_margin_percent),
    },
  }));
  const subtreeCounts = new Map<string, number>();
  for (const row of built) {
    const path = row.leafPath || row.key;
    const segments = String(path).split(" / ").map((part) => part.trim()).filter(Boolean);
    for (let size = 1; size <= segments.length; size += 1) {
      const prefix = segments.slice(0, size).join(" / ");
      subtreeCounts.set(prefix, Number(subtreeCounts.get(prefix) || 0) + Number(row.itemsCount || 0));
    }
  }
  const byLeaf = new Map(built.map((row) => [row.leafPath || row.key, row] as const));
  const fields: EditableFieldKey[] = [
    "commission_percent",
    "acquiring_percent",
    "logistics_rub",
    "ads_percent",
    "returns_percent",
    "tax_percent",
    "other_expenses_rub",
    "other_expenses_percent",
    "cogs_rub",
    "target_profit_rub",
    "target_profit_percent",
    "target_margin_rub",
    "target_margin_percent",
  ];
  return built.map((row) => {
    const parts = [row.category, ...row.subcategoryLevels].filter(Boolean);
    const nextValues = { ...row.values };
    for (const field of fields) {
      if (nextValues[field] != null) continue;
      for (let size = parts.length - 1; size >= 1; size -= 1) {
        const parent = byLeaf.get(parts.slice(0, size).join(" / "));
        if (parent && parent.values[field] != null) {
          nextValues[field] = parent.values[field];
          break;
        }
      }
    }
    return {
      ...row,
      itemsCount: Number(subtreeCounts.get(row.leafPath || row.key) || row.itemsCount || 0),
      values: nextValues,
    };
  }).filter((row, _, allRows) => {
    if (row.subcategoryLevels.length < 3) return true;
    const parts = [row.category, ...row.subcategoryLevels].filter(Boolean);
    const parentLeaf = parts.slice(0, -1).join(" / ");
    const hasParent = allRows.some((candidate) => (candidate.leafPath || candidate.key) === parentLeaf);
    if (!hasParent) return true;
    return !looksLikeProductOrBrandLeaf(row.subcategoryLevels[row.subcategoryLevels.length - 1] || "");
  });
}

export function getUsedSubcategoryDepth(categoryRows: PricingCategoryRow[]) {
  let maxDepth = 0;
  for (const row of categoryRows) {
    if (row.subcategoryLevels.length > maxDepth) maxDepth = row.subcategoryLevels.length;
  }
  return Math.min(3, maxDepth);
}

export function buildPricingTableColumns(opts: {
  isProfitMode: boolean;
  usedSubcategoryDepth: number;
  earningUnit: "rub" | "percent";
  moneySign: string;
}): PricingTableColumn[] {
  const { isProfitMode, usedSubcategoryDepth, earningUnit, moneySign } = opts;
  const cols: PricingTableColumn[] = [
    { id: "category", label: "Категория", kind: "text" },
    ...Array.from({ length: usedSubcategoryDepth }, (_, i) => ({ id: `subcategory_${i + 1}`, label: `Подкатегория ${i + 1}`, kind: "text" as const, subIndex: i })),
  ];
  const common = [
    { id: "commission_percent", label: "Комиссия, %", kind: "input" as const, field: "commission_percent" as const },
    { id: "acquiring_percent", label: "Эквайринг, %", kind: "input" as const, field: "acquiring_percent" as const },
    { id: "ads_percent", label: "Реклама, %", kind: "input" as const, field: "ads_percent" as const },
    { id: "tax_percent", label: "Налог, %", kind: "input" as const, field: "tax_percent" as const },
    { id: "other_expenses_rub", label: `Прочие расходы, ${moneySign}`, kind: "input" as const, field: "other_expenses_rub" as const },
    { id: "other_expenses_percent", label: "Прочие расходы, %", kind: "input" as const, field: "other_expenses_percent" as const },
    { id: "returns_percent", label: "Браки и возвраты, %", kind: "input" as const, field: "returns_percent" as const },
  ];
  cols.push(...common);
  cols.push({
    id: isProfitMode ? "target_profit" : "target_margin",
    label: isProfitMode
      ? (earningUnit === "percent" ? "Целевая прибыль, %" : `Целевая прибыль, ${moneySign}`)
      : (earningUnit === "percent" ? "Целевая маржа, %" : `Целевая маржа, ${moneySign}`),
    kind: "input",
    field: isProfitMode
      ? (earningUnit === "percent" ? "target_profit_percent" : "target_profit_rub")
      : (earningUnit === "percent" ? "target_margin_percent" : "target_margin_rub"),
  });
  return cols;
}

export function formatNum(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "";
  return String(v);
}

export function defaultFieldValue(field: EditableFieldKey, values: {
  targetDrr: string;
  targetMargin: string;
  targetMarginRub: string;
  targetProfit: string;
  targetProfitPercent: string;
}) {
  if (field === "ads_percent") return values.targetDrr;
  if (field === "target_margin_percent") return values.targetMargin;
  if (field === "target_margin_rub") return values.targetMarginRub;
  if (field === "target_profit_rub") return values.targetProfit;
  if (field === "target_profit_percent") return values.targetProfitPercent;
  return "";
}

export function getCellKey(leafPath: string, field: EditableFieldKey) {
  return `${leafPath}::${field}`;
}

export function fmtCell(v: number | string | null | undefined): string {
  if (v == null || v === "") return "—";
  if (typeof v === "number" && Number.isNaN(v)) return "—";
  return String(v);
}

export function fmtInputNum(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "";
  return String(v);
}

export function numFromAny(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const n = Number(String(v).replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

export function computeHandlingValueForCalc(settings: LogisticsStoreSettingsApi): number | null {
  const mode = String(settings.handling_mode || "fixed").toLowerCase();
  if (mode === "percent") return numFromAny(settings.handling_min_amount);
  return numFromAny(settings.handling_fixed_amount);
}

export function computeHandlingLabelLive(settings: LogisticsStoreSettingsApi, moneySign: string): string {
  const mode = String(settings.handling_mode || "fixed").toLowerCase();
  const fmt = (v: number | null) => (v == null ? "—" : String(v).replace(".", ","));
  if (mode === "percent") {
    const p = numFromAny(settings.handling_percent);
    const min = numFromAny(settings.handling_min_amount);
    const max = numFromAny(settings.handling_max_amount);
    const base = min ?? 0;
    return `${fmt(base)}${moneySign} + ${fmt(p)}% от цены (мин ${fmt(min)}${moneySign}, макс ${fmt(max)}${moneySign})`;
  }
  const fixed = numFromAny(settings.handling_fixed_amount);
  return fixed == null ? "—" : `${String(fixed).replace(".", ",")}${moneySign}`;
}

export function toLiveLogisticsRow(row: LogisticsRow, settings: LogisticsStoreSettingsApi, moneySign: string, activePlatform: string): LogisticsRow {
  const deliveryPerKg = numFromAny(settings.delivery_cost_per_kg);
  const returnProcessing = numFromAny(settings.return_processing_cost);
  const disposal = numFromAny(settings.disposal_cost);
  const handlingValue = computeHandlingValueForCalc(settings);
  const handlingLabel = computeHandlingLabelLive(settings, moneySign);
  const handlingMode = String(settings.handling_mode || "fixed").toLowerCase();
  const maxWeight = numFromAny(row.max_weight_kg);
  let deliveryToClient: number | null = null;
  let logisticsTotal: number | null = null;
  if (maxWeight != null && deliveryPerKg != null) deliveryToClient = deliveryPerKg * maxWeight;
  else deliveryToClient = 0;
  if (handlingMode !== "percent") deliveryToClient += handlingValue ?? 0;
  logisticsTotal = deliveryToClient + (returnProcessing ?? 0) + (disposal ?? 0);
  return { ...row, cost_per_kg: deliveryPerKg, handling_cost_display: handlingLabel, return_processing_cost: returnProcessing, disposal_cost: disposal, delivery_to_client_cost: deliveryToClient, logistics_cost_display: logisticsTotal } as LogisticsRow;
}

export function getLogisticsCellKey(sku: string, field: LogisticsEditableFieldKey) {
  return `${sku}::${field}`;
}
