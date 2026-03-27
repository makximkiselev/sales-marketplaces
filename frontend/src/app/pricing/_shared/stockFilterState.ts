export type StockFilterValue = "all" | "in_stock" | "out_of_stock";

export const GLOBAL_STOCK_FILTER_KEY = "pricing_global_stock_filter_v1";

export function readGlobalStockFilter(): StockFilterValue {
  if (typeof window === "undefined") return "all";
  try {
    const raw = window.localStorage.getItem(GLOBAL_STOCK_FILTER_KEY);
    if (raw === "in_stock" || raw === "out_of_stock" || raw === "all") return raw;
  } catch {
    // noop
  }
  return "all";
}

export function writeGlobalStockFilter(value: StockFilterValue) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(GLOBAL_STOCK_FILTER_KEY, value);
  } catch {
    // noop
  }
}
