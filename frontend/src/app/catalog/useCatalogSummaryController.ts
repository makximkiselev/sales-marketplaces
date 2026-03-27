"use client";

import { useEffect, useMemo, useState } from "react";
import { API_BASE } from "../../lib/api";
import {
  CatalogItem,
  CatalogSummaryTreeNode,
  FlatSummaryNode,
  RunStatus,
  flattenVisibleSummaryTree,
  sortSummaryTree,
} from "./catalogShared";

type Props = {
  initialItems: CatalogItem[];
  initialTotalCount: number;
  initialRun: RunStatus | null;
  initialTree: CatalogSummaryTreeNode[];
  initialSelectedSources: string[];
};

export function useCatalogSummaryController(props: Props) {
  const { initialItems, initialRun, initialSelectedSources, initialTotalCount, initialTree } = props;
  const [selectedPath, setSelectedPath] = useState("");
  const [activeTab, setActiveTab] = useState<string>("all");
  const [search, setSearch] = useState("");
  const [items, setItems] = useState<CatalogItem[]>(initialItems || []);
  const [totalCount, setTotalCount] = useState<number>(initialTotalCount || 0);
  const [run, setRun] = useState<RunStatus | null>(initialRun);
  const [tree, setTree] = useState<CatalogSummaryTreeNode[]>(initialTree || []);
  const [selectedSources, setSelectedSources] = useState<string[]>(initialSelectedSources || []);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set());

  const sortedTree = useMemo(() => sortSummaryTree(tree), [tree]);
  const allExpandablePaths = useMemo(() => {
    const out: string[] = [];
    function walk(nodes: CatalogSummaryTreeNode[], parent = "") {
      for (const node of nodes) {
        const path = parent ? `${parent}/${node.name}` : node.name;
        if ((node.children || []).length > 0) out.push(path);
        walk(node.children || [], path);
      }
    }
    walk(sortedTree);
    return out;
  }, [sortedTree]);
  const flatTree: FlatSummaryNode[] = useMemo(() => flattenVisibleSummaryTree(sortedTree, expandedPaths), [sortedTree, expandedPaths]);
  const totalPages = Math.max(1, Math.ceil((totalCount || 0) / pageSize));

  const visibleSourceColumns = useMemo(
    () => (activeTab === "all" ? selectedSources : selectedSources.filter((sourceId) => sourceId === activeTab)),
    [activeTab, selectedSources],
  );

  async function loadPage(targetPage: number, categoryPath: string, tabId: string, searchTerm: string) {
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({
        page: String(targetPage),
        page_size: String(pageSize),
      });
      if (categoryPath) params.set("category_prefix", categoryPath);
      if (tabId && tabId !== "all") params.set("source_id", tabId);
      if (searchTerm) params.set("search", searchTerm);
      const [itemsRes, statusRes, treeRes] = await Promise.all([
        fetch(`${API_BASE}/api/catalog/items?${params.toString()}`, { cache: "no-store" }),
        fetch(`${API_BASE}/api/catalog/status`, { cache: "no-store" }),
        fetch(`${API_BASE}/api/catalog/tree`, { cache: "no-store" }),
      ]);
      const itemsData = await itemsRes.json();
      const statusData = await statusRes.json();
      const treeData = await treeRes.json();
      if (!itemsRes.ok || !itemsData.ok) throw new Error(itemsData.message || "Не удалось загрузить товары каталога");
      if (!statusRes.ok || !statusData.ok) throw new Error(statusData.message || "Не удалось загрузить статус каталога");
      if (!treeRes.ok || !treeData.ok) throw new Error(treeData.message || "Не удалось загрузить дерево каталога");
      setItems(itemsData.items || []);
      setTotalCount(Number(itemsData.total_count || 0));
      setRun(statusData.last_run || null);
      setTree(treeData.tree?.roots || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    const tid = setTimeout(() => {
      void loadPage(page, selectedPath, activeTab, search.trim());
    }, 180);
    return () => clearTimeout(tid);
  }, [page, selectedPath, activeTab, search]);

  useEffect(() => {
    async function loadMeta() {
      try {
        const [treeRes, configRes] = await Promise.all([
          fetch(`${API_BASE}/api/catalog/tree`, { cache: "no-store" }),
          fetch(`${API_BASE}/api/imports/catalog/config`, { cache: "no-store" }),
        ]);
        const treeData = await treeRes.json();
        const cfgData = await configRes.json();
        if (treeRes.ok && treeData.ok) setTree(treeData.tree?.roots || []);
        if (configRes.ok && cfgData.ok) setSelectedSources(cfgData.item?.selected_sources || []);
      } catch {
        // noop
      }
    }
    void loadMeta();
  }, []);

  function togglePath(path: string) {
    setPage(1);
    setSelectedPath((prev) => (prev === path ? "" : path));
  }

  function switchTab(tabId: string) {
    setPage(1);
    setActiveTab(tabId);
  }

  function toggleExpand(path: string) {
    setExpandedPaths((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  }

  function toggleExpandAll() {
    setExpandedPaths((prev) => (prev.size > 0 ? new Set() : new Set(allExpandablePaths)));
  }

  return {
    selectedPath,
    activeTab,
    search,
    setSearch,
    items,
    totalCount,
    run,
    selectedSources,
    page,
    setPage,
    pageSize,
    loading,
    error,
    flatTree,
    totalPages,
    visibleSourceColumns,
    expandedPaths,
    togglePath,
    switchTab,
    toggleExpand,
    toggleExpandAll,
  };
}
