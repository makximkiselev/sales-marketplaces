import { useMemo } from "react";
import type { TreeNode } from "../../app/_shared/catalogState";
import styles from "./CatalogBrowser.module.css";

type NodeMeta = {
  secondary?: string;
  badge?: string;
  badgeTone?: "muted" | "success";
};

type Props = {
  title: string;
  subtitle: string;
  roots: TreeNode[];
  selectedPath: string;
  expandedPaths: string[];
  query: string;
  onQueryChange: (value: string) => void;
  onToggleExpand: (path: string) => void;
  onToggleExpandAll: () => void;
  onSelectPath: (path: string) => void;
  getNodeMeta?: (path: string, hasChildren: boolean) => NodeMeta;
  emptyText?: string;
};

function filterTree(nodes: TreeNode[], query: string): TreeNode[] {
  const normalized = query.trim().toLowerCase();
  if (!normalized) return nodes;
  const walk = (items: TreeNode[]): TreeNode[] =>
    items.flatMap((node) => {
      const children = Array.isArray(node.children) ? walk(node.children) : [];
      const selfMatch = String(node.name || "").toLowerCase().includes(normalized);
      if (!selfMatch && !children.length) return [];
      return [{ ...node, children }];
    });
  return walk(nodes);
}

function collectExpandablePaths(nodes: TreeNode[], parent = ""): string[] {
  const out: string[] = [];
  for (const node of nodes || []) {
    const path = parent ? `${parent} / ${node.name}` : node.name;
    const children = Array.isArray(node.children) ? node.children : [];
    if (children.length) {
      out.push(path, ...collectExpandablePaths(children, path));
    }
  }
  return out;
}

function flattenBrowserTree(nodes: TreeNode[], expanded: Set<string>, depth = 0, parent = "") {
  const out: Array<{ path: string; name: string; depth: number; hasChildren: boolean }> = [];
  for (const node of nodes || []) {
    const path = parent ? `${parent} / ${node.name}` : node.name;
    const children = Array.isArray(node.children) ? node.children : [];
    const hasChildren = children.length > 0;
    out.push({ path, name: node.name, depth, hasChildren });
    if (hasChildren && expanded.has(path)) {
      out.push(...flattenBrowserTree(children, expanded, depth + 1, path));
    }
  }
  return out;
}

export function CatalogBrowser({
  title,
  subtitle,
  roots,
  selectedPath,
  expandedPaths,
  query,
  onQueryChange,
  onToggleExpand,
  onToggleExpandAll,
  onSelectPath,
  getNodeMeta,
  emptyText = "Нет данных для каталога",
}: Props) {
  const filteredRoots = useMemo(() => filterTree(roots, query), [roots, query]);
  const effectiveExpanded = useMemo(
    () => new Set(query.trim() ? collectExpandablePaths(filteredRoots) : expandedPaths),
    [filteredRoots, expandedPaths, query],
  );
  const flatTree = useMemo(() => flattenBrowserTree(filteredRoots, effectiveExpanded), [filteredRoots, effectiveExpanded]);

  return (
    <div className={styles.browserShell}>
      <div className={styles.browserHead}>
        <div className={styles.browserTitle}>{title}</div>
        <div className={styles.browserSubtitle}>{subtitle}</div>
      </div>

      <div className={styles.browserControls}>
        <input
          className={`input ${styles.browserSearch}`}
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          placeholder="Поиск по категории или ветке"
        />
        <button type="button" className={`btn ghost ${styles.browserAction}`} onClick={onToggleExpandAll}>
          {expandedPaths.length ? "Свернуть все" : "Развернуть все"}
        </button>
      </div>

      <div className={styles.browserList}>
        {flatTree.length === 0 ? (
          <div className={styles.browserEmpty}>{emptyText}</div>
        ) : (
          flatTree.map((node) => {
            const meta = getNodeMeta?.(node.path, node.hasChildren);
            const selected = selectedPath === node.path;
            return (
              <div key={node.path} className={styles.browserRow} style={{ paddingLeft: `${node.depth * 14}px` }}>
                <button
                  type="button"
                  className={`${styles.browserExpand} ${!node.hasChildren ? styles.browserExpandGhost : ""}`}
                  onClick={() => {
                    if (node.hasChildren) onToggleExpand(node.path);
                    else onSelectPath(node.path);
                  }}
                  aria-label={effectiveExpanded.has(node.path) ? "Свернуть категорию" : "Раскрыть категорию"}
                >
                  {node.hasChildren ? (effectiveExpanded.has(node.path) ? "−" : "+") : "•"}
                </button>
                <button
                  type="button"
                  className={`${styles.browserNode} ${selected ? styles.browserNodeActive : ""}`}
                  onClick={() => onSelectPath(node.path)}
                >
                  <span className={styles.browserLabel}>{node.name}</span>
                  {(meta?.secondary || meta?.badge) ? (
                    <span className={styles.browserMetaRow}>
                      <span className={styles.browserMeta}>{meta?.secondary || ""}</span>
                      {meta?.badge ? (
                        <span
                          className={`${styles.browserBadge} ${meta.badgeTone === "success" ? styles.browserBadgeSuccess : styles.browserBadgeMuted}`}
                        >
                          {meta.badge}
                        </span>
                      ) : null}
                    </span>
                  ) : null}
                </button>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
