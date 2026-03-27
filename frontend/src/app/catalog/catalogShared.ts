import type { ContextResp as BaseContextResp, StoreCtx, TreeNode, TreeResp as BaseTreeResp } from "../_shared/catalogState";

export type { StoreCtx, TreeNode };

export type CatalogContextResp = BaseContextResp & {
  external_tree_source_types: Array<{ id: string; label: string }>;
  external_sources: Array<{ id: string; type: string; label: string }>;
};

export type CatalogOverviewRow = {
  sku: string;
  name: string;
  tree_path: string[];
  placements: Record<string, boolean>;
  updated_at: string;
};

export type CatalogTreeResp = BaseTreeResp & {
  source?: unknown;
};

export type CatalogOverviewResp = {
  ok: boolean;
  rows: CatalogOverviewRow[];
  stores: StoreCtx[];
  total_count: number;
  page: number;
  page_size: number;
  tree_source?: unknown;
};

export type CatalogItem = {
  sku_primary?: string;
  sku?: string;
  title?: string;
  name?: string;
  category?: string;
  category_path?: string;
  source_flags?: Record<string, boolean>;
};

export type RunStatus = {
  status?: string;
  imported?: number;
  total?: number;
  failed?: number;
  selected_sources?: string[];
};

export type CatalogSummaryTreeNode = {
  id: number;
  name: string;
  children?: CatalogSummaryTreeNode[];
};

export type FlatSummaryNode = {
  id: number;
  name: string;
  depth: number;
  path: string;
  hasChildren: boolean;
};

export function sourceShortLabel(sourceId: string, labels: Record<string, string>): string {
  if (sourceId.startsWith("yandex_market:")) return "Яндекс.Маркет";
  if (sourceId.startsWith("ozon:")) return "OZON";
  if (sourceId.startsWith("wb:") || sourceId.startsWith("wildberries")) return "WB";
  if (sourceId.startsWith("gsheets:")) return "Google таблица";
  return labels[sourceId] ? labels[sourceId].slice(0, 6) : sourceId.slice(0, 6);
}

export function sortSummaryTree(nodes: CatalogSummaryTreeNode[] = [], depth = 0): CatalogSummaryTreeNode[] {
  return [...nodes]
    .sort((a, b) => {
      const cmp = String(a.name || "").localeCompare(String(b.name || ""), "ru", { sensitivity: "base" });
      return depth === 0 ? cmp : -cmp;
    })
    .map((node) => ({ ...node, children: sortSummaryTree(node.children || [], depth + 1) }));
}

export function flattenVisibleSummaryTree(
  nodes: CatalogSummaryTreeNode[] = [],
  expanded: Set<string>,
  depth = 0,
  parentPath = "",
): FlatSummaryNode[] {
  const out: FlatSummaryNode[] = [];
  for (const node of nodes) {
    const path = parentPath ? `${parentPath}/${node.name}` : node.name;
    const children = node.children || [];
    const hasChildren = children.length > 0;
    out.push({ id: node.id, name: node.name, depth, path, hasChildren });
    if (!hasChildren || expanded.has(path)) {
      out.push(...flattenVisibleSummaryTree(children, expanded, depth + 1, path));
    }
  }
  return out;
}
