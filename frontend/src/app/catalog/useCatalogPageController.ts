"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGetOk, apiGetParams } from "../../lib/api";
import {
  ensureUndefinedRoot,
  flattenTree,
  parseStoreTabKey,
  safeReadJson,
  safeReadString,
  safeWriteJson,
  safeWriteString,
  tabKeyForStore,
} from "../_shared/catalogState";
import type {
  CatalogContextResp,
  CatalogOverviewResp,
  CatalogOverviewRow,
  CatalogTreeResp,
  StoreCtx,
} from "./catalogShared";

const CATALOG_CTX_CACHE_KEY = "catalog_page_ctx_v1";
const CATALOG_DATA_CACHE_PREFIX = "catalog_page_data_v1:";
const CATALOG_TREE_SOURCE_STORE_KEY = "catalog_tree_source_store_id_v1";

export function useCatalogPageController() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [context, setContext] = useState<CatalogContextResp | null>(null);
  const [treeMode, setTreeMode] = useState<"marketplaces" | "external">("marketplaces");
  const [tab, setTab] = useState<string>("all");
  const [treeSourceStoreId, setTreeSourceStoreId] = useState(() => safeReadString(CATALOG_TREE_SOURCE_STORE_KEY));
  const [externalSourceType, setExternalSourceType] = useState("tables");
  const [externalSourceId, setExternalSourceId] = useState("");
  const [search, setSearch] = useState("");
  const [searchDraft, setSearchDraft] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [selectedTreePath, setSelectedTreePath] = useState("");
  const [treeRoots, setTreeRoots] = useState<CatalogTreeResp["roots"]>([]);
  const [rows, setRows] = useState<CatalogOverviewRow[]>([]);
  const [visibleStores, setVisibleStores] = useState<StoreCtx[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [tableLoading, setTableLoading] = useState(false);

  const stores = context?.marketplace_stores || [];
  const flatTree = useMemo(() => flattenTree(treeRoots, expanded), [treeRoots, expanded]);
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const allExpandable = useMemo(() => flatTree.filter((n) => n.hasChildren).map((n) => n.path), [flatTree]);

  useEffect(() => {
    let done = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const cachedCtx = safeReadJson<CatalogContextResp>(CATALOG_CTX_CACHE_KEY);
        if (!done && cachedCtx?.ok && Array.isArray(cachedCtx.marketplace_stores)) {
          setContext(cachedCtx);
          if (!treeSourceStoreId && cachedCtx.marketplace_stores.length) {
            setTreeSourceStoreId(String(cachedCtx.marketplace_stores[0].store_id || ""));
          }
          if (!externalSourceId && cachedCtx.external_sources?.length) {
            setExternalSourceId(String(cachedCtx.external_sources[0].id || ""));
          }
          setLoading(false);
          return;
        }

        const data = await apiGetOk<CatalogContextResp>("/api/catalog/products/context");
        if (done) return;
        setContext(data);
        safeWriteJson(CATALOG_CTX_CACHE_KEY, data);

        const existing = String(treeSourceStoreId || "").trim();
        const validExisting = existing && data.marketplace_stores.some((s) => String(s.store_id || "") === existing);
        if (!validExisting && data.marketplace_stores.length) {
          const next = String(data.marketplace_stores[0].store_id || "");
          setTreeSourceStoreId(next);
          safeWriteString(CATALOG_TREE_SOURCE_STORE_KEY, next);
        }
        if (!externalSourceId && data.external_sources?.length) {
          setExternalSourceId(String(data.external_sources[0].id || ""));
        }
      } catch (e) {
        if (!done) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!done) setLoading(false);
      }
    })();
    return () => {
      done = true;
    };
  }, []);

  useEffect(() => {
    safeWriteString(CATALOG_TREE_SOURCE_STORE_KEY, String(treeSourceStoreId || "").trim());
  }, [treeSourceStoreId]);

  useEffect(() => {
    const t = setTimeout(() => {
      setPage(1);
      setSearch(searchDraft.trim());
    }, 220);
    return () => clearTimeout(t);
  }, [searchDraft]);

  useEffect(() => {
    setPage(1);
    setSelectedTreePath("");
    setExpanded(new Set());
  }, [tab, treeMode, treeSourceStoreId, externalSourceId, externalSourceType]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!context) return;
      setTableLoading(true);
      setError("");
      try {
        const params: Record<string, string> = {};
        if (tab === "all") {
          params.scope = "all";
        } else {
          const parsed = parseStoreTabKey(tab);
          if (parsed) {
            params.scope = "store";
            params.platform = parsed.platform;
            params.store_id = parsed.store_id;
          }
        }
        params.tree_mode = treeMode;
        if (treeMode === "marketplaces" && tab === "all" && treeSourceStoreId) {
          params.tree_source_store_id = treeSourceStoreId;
        }
        if (selectedTreePath) params.category_path = selectedTreePath;
        if (search) params.search = search;
        params.page = String(page);
        params.page_size = String(pageSize);

        const dataCacheKey = `${CATALOG_DATA_CACHE_PREFIX}${JSON.stringify(params)}`;
        const cachedData = safeReadJson<{ tree: CatalogTreeResp; overview: CatalogOverviewResp }>(dataCacheKey);
        if (cachedData?.tree?.ok && cachedData?.overview?.ok) {
          const nextRows = Array.isArray(cachedData.overview.rows) ? cachedData.overview.rows : [];
          setRows(nextRows);
          setVisibleStores(Array.isArray(cachedData.overview.stores) ? cachedData.overview.stores : []);
          setTotalCount(Number(cachedData.overview.total_count || 0));
          setTreeRoots(ensureUndefinedRoot(Array.isArray(cachedData.tree.roots) ? cachedData.tree.roots : [], nextRows));
          setTableLoading(false);
          return;
        }

        const [treeData, overviewData] = await Promise.all([
          apiGetParams<CatalogTreeResp>("/api/catalog/products/tree", params),
          apiGetParams<CatalogOverviewResp>("/api/catalog/products/overview", params),
        ]);
        if (cancelled) return;
        const nextRows = Array.isArray(overviewData.rows) ? overviewData.rows : [];
        setRows(nextRows);
        setVisibleStores(Array.isArray(overviewData.stores) ? overviewData.stores : []);
        setTotalCount(Number(overviewData.total_count || 0));
        setTreeRoots(ensureUndefinedRoot(Array.isArray(treeData.roots) ? treeData.roots : [], nextRows));
        safeWriteJson(dataCacheKey, { tree: treeData, overview: overviewData });
      } catch (e) {
        if (!cancelled) {
          setRows([]);
          setVisibleStores([]);
          setTreeRoots([]);
          setTotalCount(0);
          setError(e instanceof Error ? e.message : String(e));
        }
      } finally {
        if (!cancelled) setTableLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [context, tab, treeMode, treeSourceStoreId, externalSourceType, externalSourceId, selectedTreePath, search, page, pageSize]);

  const activeStoreLabel = useMemo(() => {
    if (tab === "all") return "Все товары";
    const parsed = parseStoreTabKey(tab);
    if (!parsed) return "";
    const found = stores.find((s) => s.platform === parsed.platform && s.store_id === parsed.store_id);
    return found?.label || tab;
  }, [stores, tab]);

  function toggleTree(path: string) {
    setSelectedTreePath((prev) => (prev === path ? "" : path));
    setPage(1);
  }

  function toggleExpand(path: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  }

  function toggleExpandAll() {
    setExpanded((prev) => (prev.size ? new Set() : new Set(allExpandable)));
  }

  return {
    loading,
    error,
    context,
    stores,
    treeMode,
    setTreeMode,
    tab,
    setTab,
    treeSourceStoreId,
    setTreeSourceStoreId,
    externalSourceType,
    setExternalSourceType,
    externalSourceId,
    setExternalSourceId,
    searchDraft,
    setSearchDraft,
    page,
    setPage,
    pageSize,
    setPageSize,
    selectedTreePath,
    flatTree,
    rows,
    visibleStores,
    totalCount,
    totalPages,
    tableLoading,
    activeStoreLabel,
    expandedSize: expanded.size,
    isExpanded: (path: string) => expanded.has(path),
    onToggleTree: toggleTree,
    onToggleExpand: toggleExpand,
    onToggleExpandAll: toggleExpandAll,
    tabItems: [
      { id: "all", label: "Все товары" },
      ...stores.map((s) => ({ id: tabKeyForStore(s), label: s.label, badge: s.platform_label })),
    ],
  };
}
