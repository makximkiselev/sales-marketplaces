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
  tree: TreeResp;
  overview: OverviewRespBase<Row>;
};

const inflightOverviewRequests = new Map<string, Promise<OverviewCachePayload<unknown>>>();

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

  useEffect(() => {
    let cancelled = false;
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
        const treeQuery: Record<string, string> = {
          scope: query.scope,
          tree_mode: query.tree_mode,
        };
        if (query.platform) treeQuery.platform = query.platform;
        if (query.store_id) treeQuery.store_id = query.store_id;
        if (query.tree_source_store_id) treeQuery.tree_source_store_id = query.tree_source_store_id;

        const cacheKey = overviewCachePrefix ? `${overviewCachePrefix}${JSON.stringify(query)}` : "";
        const cachedData = cacheKey ? safeReadJson<OverviewCachePayload<Row>>(cacheKey) : null;
        if (cachedData?.tree?.ok && cachedData?.overview?.ok) {
          const nextRows = Array.isArray(cachedData.overview.rows) ? cachedData.overview.rows : [];
          setOverviewData(cachedData.overview);
          setRows(nextRows);
          setVisibleStores(Array.isArray(cachedData.overview.stores) ? cachedData.overview.stores : []);
          setTotalCount(Number(cachedData.overview.total_count || 0));
          setTreeRoots(ensureUndefinedRoot(Array.isArray(cachedData.tree.roots) ? cachedData.tree.roots : [], nextRows));
          setTableLoading(false);
          return;
        }

        const loadKey = `${treeEndpoint}::${overviewEndpoint}::${cacheKey || JSON.stringify(query)}`;
        let inflight = inflightOverviewRequests.get(loadKey) as Promise<OverviewCachePayload<Row>> | undefined;
        if (!inflight) {
          inflight = (async () => {
            const [treeData, overviewData] = await Promise.all([
              apiGetParams<TreeResp>(treeEndpoint, treeQuery),
              apiGetParams<OverviewRespBase<Row>>(overviewEndpoint, query),
            ]);
            return { tree: treeData, overview: overviewData };
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
        setTreeRoots(ensureUndefinedRoot(Array.isArray(loaded.tree.roots) ? loaded.tree.roots : [], nextRows));
        if (cacheKey) safeWriteJson(cacheKey, loaded);
      } catch (e) {
        if (!cancelled) {
          setRows([]);
          setOverviewData(null);
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
    treeEndpoint,
    overviewEndpoint,
    setError,
    setTreeRoots,
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
