export type StoreCtx = {
  store_uid: string;
  store_id: string;
  platform: string;
  platform_label: string;
  label: string;
  currency_code?: string;
};

export type TreeNode = {
  name: string;
  children?: TreeNode[];
};

export type FlatTreeNode = {
  path: string;
  name: string;
  depth: number;
  hasChildren: boolean;
};

export type ContextResp = {
  ok: boolean;
  marketplace_stores: Array<StoreCtx & { business_id?: string; account_id?: string; seller_id?: string }>;
};

export type TreeResp = {
  ok: boolean;
  roots: TreeNode[];
};

export function safeReadJson<T>(key: string): T | null {
  try {
    if (typeof window === "undefined") return null;
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export function safeWriteJson(key: string, value: unknown) {
  try {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // noop
  }
}

export function safeReadString(key: string): string {
  try {
    if (typeof window === "undefined") return "";
    return window.localStorage.getItem(key) || "";
  } catch {
    return "";
  }
}

export function safeWriteString(key: string, value: string) {
  try {
    if (typeof window === "undefined") return;
    if (value) window.localStorage.setItem(key, value);
    else window.localStorage.removeItem(key);
  } catch {
    // noop
  }
}

export function flattenTree(nodes: TreeNode[], expanded: Set<string>, depth = 0, parent = ""): FlatTreeNode[] {
  const out: FlatTreeNode[] = [];
  for (const node of nodes || []) {
    const path = parent ? `${parent}/${node.name}` : node.name;
    const children = Array.isArray(node.children) ? node.children : [];
    const hasChildren = children.length > 0;
    out.push({ path, name: node.name, depth, hasChildren });
    if (hasChildren && expanded.has(path)) out.push(...flattenTree(children, expanded, depth + 1, path));
  }
  return out;
}

export function ensureUndefinedRoot<Row extends { tree_path: string[] }>(roots: TreeNode[], rows: Row[]): TreeNode[] {
  const hasUndefinedRows = rows.some((r) => Array.isArray(r.tree_path) && r.tree_path[0] === "Не определено");
  if (!hasUndefinedRows) return roots;
  if (roots.some((r) => r.name === "Не определено")) return roots;
  return [{ name: "Не определено", children: [] }, ...roots];
}

export function tabKeyForStore(s: StoreCtx) {
  return `${s.platform}:${s.store_id}`;
}

export function parseStoreTabKey(tab: string): { platform: string; store_id: string } | null {
  if (!tab || tab === "all") return null;
  const i = tab.indexOf(":");
  if (i <= 0) return null;
  return { platform: tab.slice(0, i), store_id: tab.slice(i + 1) };
}

export function currencySymbol(code: string | undefined): string {
  return String(code || "RUB").toUpperCase() === "USD" ? "$" : "₽";
}

export function filterWorkingMarketplaceStores<T extends StoreCtx>(stores: T[]): T[] {
  return (stores || []).filter((store) => String(store.platform || "") === "yandex_market");
}

export function formatMoney(value: number | null | undefined): string {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return String(Math.round(Number(value)));
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value) * 100) / 100}%`;
}
