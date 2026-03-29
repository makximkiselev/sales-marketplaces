import { useEffect, useMemo, useState } from "react";
import { fetchPricingCatalogContext } from "./api";
import {
  ContextResp,
  currencySymbol,
  flattenTree,
  parseStoreTabKey,
  safeReadJson,
  safeReadString,
  safeWriteJson,
  safeWriteString,
  TreeNode,
} from "./catalogPageShared";

function resolveTreeSourceStoreValue(stores: Array<{ store_uid?: string; store_id?: string }>, rawValue: string): string {
  const value = String(rawValue || "").trim();
  if (!value) return "";
  const byUid = stores.find((store) => String(store.store_uid || "").trim() === value);
  if (byUid) return String(byUid.store_uid || "").trim();
  const byId = stores.find((store) => String(store.store_id || "").trim() === value);
  return byId ? String(byId.store_uid || byId.store_id || "").trim() : "";
}

export function usePricingCatalogController(opts: {
  contextEndpoint: string;
  contextCacheKey: string;
  treeSourceStoreKey: string;
  defaultPageSize: number;
}) {
  const { contextEndpoint, contextCacheKey, treeSourceStoreKey, defaultPageSize } = opts;
  const pageSizeStorageKey = `${contextCacheKey}__page_size_v1`;
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [context, setContext] = useState<ContextResp | null>(null);
  const [tab, setTab] = useState<string>("all");
  const [treeSourceStoreId, setTreeSourceStoreId] = useState(() => safeReadString(treeSourceStoreKey));
  const [search, setSearch] = useState("");
  const [searchDraft, setSearchDraft] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(() => {
    const raw = safeReadString(pageSizeStorageKey);
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed !== 0 ? parsed : defaultPageSize;
  });
  const [selectedTreePath, setSelectedTreePath] = useState("");
  const [treeRoots, setTreeRoots] = useState<TreeNode[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [reloadNonce, setReloadNonce] = useState(0);

  const stores = context?.marketplace_stores || [];
  const activeStoreRef = useMemo(() => parseStoreTabKey(tab), [tab]);
  const activeStoreCurrency = useMemo(() => {
    if (!activeStoreRef) return "RUB";
    const suid = `${activeStoreRef.platform}:${activeStoreRef.store_id}`;
    const found = stores.find((s) => s.store_uid === suid);
    return String(found?.currency_code || "RUB").toUpperCase() === "USD" ? "USD" : "RUB";
  }, [activeStoreRef, stores]);
  const moneySign = currencySymbol(activeStoreCurrency);
  const flatTree = useMemo(() => flattenTree(treeRoots, expanded), [treeRoots, expanded]);

  useEffect(() => {
    let done = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const cachedCtx = safeReadJson<ContextResp>(contextCacheKey);
        if (!done && cachedCtx?.ok && Array.isArray(cachedCtx.marketplace_stores)) {
          setContext(cachedCtx);
          const normalized = resolveTreeSourceStoreValue(cachedCtx.marketplace_stores, treeSourceStoreId);
          if (normalized && normalized !== treeSourceStoreId) {
            setTreeSourceStoreId(normalized);
            safeWriteString(treeSourceStoreKey, normalized);
          } else if (!normalized && cachedCtx.marketplace_stores?.length) {
            const next = String(cachedCtx.marketplace_stores[0].store_uid || cachedCtx.marketplace_stores[0].store_id || "");
            setTreeSourceStoreId(next);
            safeWriteString(treeSourceStoreKey, next);
          }
          setLoading(false);
          return;
        }
        const ctx = await fetchPricingCatalogContext(contextEndpoint);
        if (done) return;
        setContext(ctx);
        safeWriteJson(contextCacheKey, ctx);
        const existing = resolveTreeSourceStoreValue(ctx.marketplace_stores || [], treeSourceStoreId);
        const validExisting = Boolean(existing);
        if (!validExisting && ctx.marketplace_stores?.length) {
          const next = String(ctx.marketplace_stores[0].store_uid || ctx.marketplace_stores[0].store_id || "");
          setTreeSourceStoreId(next);
          safeWriteString(treeSourceStoreKey, next);
        } else if (existing && existing !== treeSourceStoreId) {
          setTreeSourceStoreId(existing);
          safeWriteString(treeSourceStoreKey, existing);
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
  }, [reloadNonce, contextEndpoint, contextCacheKey, treeSourceStoreKey]);

  useEffect(() => {
    safeWriteString(treeSourceStoreKey, String(treeSourceStoreId || "").trim());
  }, [treeSourceStoreId, treeSourceStoreKey]);

  useEffect(() => {
    safeWriteString(pageSizeStorageKey, String(pageSize || defaultPageSize));
  }, [pageSize, defaultPageSize, pageSizeStorageKey]);

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
  }, [tab, treeSourceStoreId, pageSize]);

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

  const allExpandable = useMemo(() => flatTree.filter((n) => n.hasChildren).map((n) => n.path), [flatTree]);
  function toggleExpandAll() {
    setExpanded((prev) => (prev.size ? new Set() : new Set(allExpandable)));
  }

  return {
    loading,
    setLoading,
    error,
    setError,
    context,
    setContext,
    stores,
    tab,
    setTab,
    treeSourceStoreId,
    setTreeSourceStoreId,
    search,
    setSearch,
    searchDraft,
    setSearchDraft,
    page,
    setPage,
    pageSize,
    setPageSize,
    selectedTreePath,
    setSelectedTreePath,
    treeRoots,
    setTreeRoots,
    expanded,
    setExpanded,
    flatTree,
    activeStoreRef,
    activeStoreCurrency,
    moneySign,
    reloadNonce,
    setReloadNonce,
    toggleTree,
    toggleExpand,
    toggleExpandAll,
  };
}
