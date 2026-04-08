import { useEffect, useMemo, useState } from "react";
import { apiGetParams, apiPostOk } from "../../../lib/api";
import { ensureUndefinedRoot, parseStoreTabKey, safeReadJson, safeWriteJson, StoreCtx, TreeResp } from "./catalogPageShared";

type OverviewRespBase<Row> = {
  ok: boolean;
  rows: Row[];
  stores: StoreCtx[];
  total_count: number;
  page: number;
  page_size: number;
};

type OverviewCachePayload<Row> = {
  overview: OverviewRespBase<Row>;
};

const inflightOverviewRequests = new Map<string, Promise<OverviewCachePayload<unknown>>>();
const inflightTreeRequests = new Map<string, Promise<TreeResp>>();

export function usePricingOverviewData<Row extends { tree_path: string[] }>(params: {
  enabled: boolean;
  tab: string;
  treeSourceStoreId: string;
  selectedTreePath: string;
  search: string;
  page: number;
  pageSize: number;
  reloadNonce: number;
  treeEndpoint: string;
  overviewEndpoint: string;
  refreshEndpoint: string;
  setError: (value: string) => void;
  setTreeRoots: (roots: TreeResp["roots"]) => void;
  overviewCachePrefix?: string;
  clearPageCache?: () => void;
  extraParams?: Record<string, string>;
}) {
  const {
    enabled,
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    treeEndpoint,
    overviewEndpoint,
    refreshEndpoint,
    setError,
    setTreeRoots,
    overviewCachePrefix,
    clearPageCache,
    extraParams,
  } = params;

  const [rows, setRows] = useState<Row[]>([]);
  const [visibleStores, setVisibleStores] = useState<StoreCtx[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [overviewData, setOverviewData] = useState<OverviewRespBase<Row> | null>(null);
  const [tableLoading, setTableLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshSignal, setRefreshSignal] = useState(0);
  const extraParamsKey = useMemo(() => JSON.stringify(extraParams || {}), [extraParams]);
  const treeCachePrefix = useMemo(() => (overviewCachePrefix ? `${overviewCachePrefix}__tree_v1:` : ""), [overviewCachePrefix]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!enabled) return;
      try {
        const treeQuery: Record<string, string> = { tree_mode: "marketplaces" };
        if (tab === "all") {
          treeQuery.scope = "all";
        } else {
          const parsed = parseStoreTabKey(tab);
          if (parsed) {
            treeQuery.scope = "store";
            treeQuery.platform = parsed.platform;
            treeQuery.store_id = parsed.store_id;
          }
        }
        if (tab === "all" && treeSourceStoreId) treeQuery.tree_source_store_id = treeSourceStoreId;
        const treeCacheKey = treeCachePrefix ? `${treeCachePrefix}${JSON.stringify(treeQuery)}` : "";
        const cachedTree = treeCacheKey ? safeReadJson<TreeResp>(treeCacheKey) : null;
        if (cachedTree?.ok) {
          if (!cancelled) setTreeRoots(Array.isArray(cachedTree.roots) ? cachedTree.roots : []);
          return;
        }

        const loadKey = `${treeEndpoint}::${JSON.stringify(treeQuery)}`;
        let inflight = inflightTreeRequests.get(loadKey);
        if (!inflight) {
          inflight = apiGetParams<TreeResp>(treeEndpoint, treeQuery);
          inflightTreeRequests.set(loadKey, inflight);
        }
        const treeData = await inflight;
        inflightTreeRequests.delete(loadKey);
        if (cancelled) return;
        setTreeRoots(Array.isArray(treeData.roots) ? treeData.roots : []);
        if (treeCacheKey) safeWriteJson(treeCacheKey, treeData);
      } catch (e) {
        if (!cancelled) {
          setTreeRoots([]);
          setError(e instanceof Error ? e.message : String(e));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [
    enabled,
    tab,
    treeSourceStoreId,
    reloadNonce,
    refreshSignal,
    treeEndpoint,
    setError,
    setTreeRoots,
    treeCachePrefix,
  ]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    (async () => {
      if (!enabled) return;
      setTableLoading(true);
      setError("");
      try {
        const query: Record<string, string> = { ...(extraParamsKey ? (JSON.parse(extraParamsKey) as Record<string, string>) : {}) };
        if (tab === "all") {
          query.scope = "all";
        } else {
          const parsed = parseStoreTabKey(tab);
          if (parsed) {
            query.scope = "store";
            query.platform = parsed.platform;
            query.store_id = parsed.store_id;
          }
        }
        query.tree_mode = "marketplaces";
        if (tab === "all" && treeSourceStoreId) query.tree_source_store_id = treeSourceStoreId;
        if (selectedTreePath) query.category_path = selectedTreePath;
        if (search) query.search = search;
        query.page = String(page);
        query.page_size = String(pageSize > 0 ? pageSize : 100000);
        const cacheKey = overviewCachePrefix ? `${overviewCachePrefix}${JSON.stringify(query)}` : "";
        const cachedData = cacheKey ? safeReadJson<OverviewRespBase<Row> | OverviewCachePayload<Row>>(cacheKey) : null;
        const cachedOverview = cachedData && "overview" in cachedData ? cachedData.overview : cachedData;
        if (cachedOverview?.ok) {
          const nextRows = Array.isArray(cachedOverview.rows) ? cachedOverview.rows : [];
          setOverviewData(cachedOverview);
          setRows(nextRows);
          setVisibleStores(Array.isArray(cachedOverview.stores) ? cachedOverview.stores : []);
          setTotalCount(Number(cachedOverview.total_count || 0));
          setTableLoading(false);
          return;
        }

        const loadKey = `${overviewEndpoint}::${cacheKey || JSON.stringify(query)}`;
        let inflight = inflightOverviewRequests.get(loadKey) as Promise<OverviewCachePayload<Row>> | undefined;
        if (!inflight) {
          inflight = (async () => {
            const overviewData = await apiGetParams<OverviewRespBase<Row>>(overviewEndpoint, query, { signal: controller.signal });
            return { overview: overviewData };
          })();
          inflightOverviewRequests.set(loadKey, inflight as Promise<OverviewCachePayload<unknown>>);
        }
        const loaded = await inflight;
        inflightOverviewRequests.delete(loadKey);
        if (cancelled) return;

        const nextRows = Array.isArray(loaded.overview.rows) ? loaded.overview.rows : [];
        setOverviewData(loaded.overview);
        setRows(nextRows);
        setVisibleStores(Array.isArray(loaded.overview.stores) ? loaded.overview.stores : []);
        setTotalCount(Number(loaded.overview.total_count || 0));
        if (cacheKey) safeWriteJson(cacheKey, loaded.overview);
      } catch (e) {
        if ((e instanceof Error && e.name === "AbortError") || cancelled) return;
        if (!cancelled) {
          setRows([]);
          setOverviewData(null);
          setVisibleStores([]);
          setTotalCount(0);
          setError(e instanceof Error ? e.message : String(e));
        }
      } finally {
        if (!cancelled) setTableLoading(false);
      }
    })();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [
    enabled,
    tab,
    treeSourceStoreId,
    selectedTreePath,
    search,
    page,
    pageSize,
    reloadNonce,
    refreshSignal,
    overviewEndpoint,
    setError,
    overviewCachePrefix,
    extraParamsKey,
  ]);

  async function handleRefresh() {
    try {
      setRefreshing(true);
      setError("");
      clearPageCache?.();
      await apiPostOk<{ ok: boolean; message?: string }>(refreshEndpoint);
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setRefreshing(false);
    }
  }

  return {
    rows,
    overviewData,
    visibleStores,
    totalCount,
    tableLoading,
    refreshing,
    handleRefresh,
  };
}
