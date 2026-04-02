import { useEffect, useState } from "react";
import { ErrorBox } from "../../../components/ErrorBox";
import {
  fetchPricingMonitoring,
  fetchPricingMonitoringExports,
  runPricingMonitoringExport,
  runPricingMonitoringAll,
  runPricingMonitoringJob,
  savePricingMonitoringExportConfig,
  savePricingMonitoringJob,
} from "../../pricing/settings/api";
import { MonitoringExportSection } from "../../pricing/settings/components/MonitoringExportSection";
import { MonitoringSection } from "../../pricing/settings/components/MonitoringSection";
import type {
  CogsSource,
  MonitoringExportRunKindResult,
  MonitoringExportRunStoreResult,
  MonitoringExportStoreApi,
  RefreshMonitoringRowApi,
  RefreshMonitoringRunAllApi,
  RefreshMonitoringStoreApi,
} from "../../pricing/settings/types";
import { showAppToast } from "../../../components/ui/toastBus";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageFrame } from "../../_shared/WorkspacePageFrame";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";
import { readPageSnapshot, writePageSnapshot } from "../../_shared/pageCache";

const MONITORING_IMPORT_CACHE_KEY = "page_monitoring_import_v1";
const MONITORING_EXPORT_CACHE_KEY = "page_monitoring_export_v1";

export default function MonitoringPage() {
  const [tab, setTab] = useState<"import" | "export">("import");
  const [rows, setRows] = useState<RefreshMonitoringRowApi[]>([]);
  const [exportRows, setExportRows] = useState<MonitoringExportStoreApi[]>([]);
  const [platformStores, setPlatformStores] = useState<Record<string, RefreshMonitoringStoreApi[]>>({});
  const [runAllState, setRunAllState] = useState<RefreshMonitoringRunAllApi>({});
  const [loading, setLoading] = useState(true);
  const [exportLoading, setExportLoading] = useState(true);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [savingMap, setSavingMap] = useState<Record<string, boolean>>({});
  const [runningMap, setRunningMap] = useState<Record<string, boolean>>({});
  const [runAllLoading, setRunAllLoading] = useState(false);
  const [exportRunningMap, setExportRunningMap] = useState<Record<string, boolean>>({});
  const [exportStatusMap, setExportStatusMap] = useState<Record<string, MonitoringExportRunKindResult>>({});
  const [exportLastSummary, setExportLastSummary] = useState<string>("");

  async function loadData(silent = false) {
    if (!silent) setLoading(true);
    if (!silent) setError("");
    try {
      const data = await fetchPricingMonitoring();
      setRows(Array.isArray(data.rows) ? data.rows : []);
      setPlatformStores(data.platform_stores && typeof data.platform_stores === "object" ? data.platform_stores : {});
      setRunAllState(data.run_all && typeof data.run_all === "object" ? data.run_all : {});
      writePageSnapshot(MONITORING_IMPORT_CACHE_KEY, data);
    } catch (e) {
      setRows([]);
      setPlatformStores({});
      setRunAllState({});
      if (!silent) setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (!silent) setLoading(false);
    }
  }

  async function loadExportData(silent = false) {
    if (!silent) setExportLoading(true);
    if (!silent) setExportError("");
    try {
      const data = await fetchPricingMonitoringExports();
      setExportRows(Array.isArray(data.rows) ? data.rows : []);
      writePageSnapshot(MONITORING_EXPORT_CACHE_KEY, data);
    } catch (e) {
      setExportRows([]);
      if (!silent) setExportError(e instanceof Error ? e.message : String(e));
    } finally {
      if (!silent) setExportLoading(false);
    }
  }

  useEffect(() => {
    const cachedImport = readPageSnapshot<{
      rows?: RefreshMonitoringRowApi[];
      platform_stores?: Record<string, RefreshMonitoringStoreApi[]>;
      run_all?: RefreshMonitoringRunAllApi;
    }>(MONITORING_IMPORT_CACHE_KEY);
    const cachedExport = readPageSnapshot<{ rows?: MonitoringExportStoreApi[] }>(MONITORING_EXPORT_CACHE_KEY);
    if (cachedImport) {
      setRows(Array.isArray(cachedImport.rows) ? cachedImport.rows : []);
      setPlatformStores(cachedImport.platform_stores && typeof cachedImport.platform_stores === "object" ? cachedImport.platform_stores : {});
      setRunAllState(cachedImport.run_all && typeof cachedImport.run_all === "object" ? cachedImport.run_all : {});
      setLoading(false);
    }
    if (cachedExport) {
      setExportRows(Array.isArray(cachedExport.rows) ? cachedExport.rows : []);
      setExportLoading(false);
    }
    void loadData();
    void loadExportData();
  }, []);

  useEffect(() => {
    const activeStatuses = new Set(["running", "queued"]);
    const hasRunningRow = rows.some((row) => activeStatuses.has(String(row.last_status || "").trim().toLowerCase()));
    const hasRunningManual =
      Object.values(runningMap).some(Boolean) ||
      runAllLoading ||
      activeStatuses.has(String(runAllState.last_status || "").trim().toLowerCase());
    if (!hasRunningRow && !hasRunningManual) return;
    const timer = window.setInterval(() => {
      void loadData(true);
    }, 2000);
    return () => window.clearInterval(timer);
  }, [rows, runningMap, runAllLoading, runAllState]);

  async function onSaveJob(
    jobCode: string,
    values: {
      enabled: boolean;
      schedule_kind: string;
      interval_minutes?: number | null;
      time_of_day?: string | null;
      date_from?: string | null;
      date_to?: string | null;
      stores?: string[];
    },
  ) {
    const code = String(jobCode || "").trim();
    if (!code) return;
    setSavingMap((prev) => ({ ...prev, [code]: true }));
    setError("");
    try {
      await savePricingMonitoringJob({ job_code: code, ...values });
      showAppToast({ message: "Данные сохранены" });
      await loadData(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSavingMap((prev) => ({ ...prev, [code]: false }));
    }
  }

  async function onSaveExportConfig(storeUid: string, exportKind: "prices" | "ads", config: CogsSource) {
    const key = `${exportKind}:${storeUid}`;
    setSavingMap((prev) => ({ ...prev, [key]: true }));
    setExportError("");
    try {
      await savePricingMonitoringExportConfig({
        store_uid: storeUid,
        export_kind: exportKind,
        type: config.type,
        sourceId: config.sourceId,
        sourceName: config.sourceName,
        skuColumn: config.skuColumn,
        valueColumn: config.valueColumn,
      });
      showAppToast({ message: "Данные сохранены" });
      await loadExportData(true);
    } catch (e) {
      setExportError(e instanceof Error ? e.message : String(e));
      throw e;
    } finally {
      setSavingMap((prev) => ({ ...prev, [key]: false }));
    }
  }

  async function onRunExport(storeUid?: string) {
    const key = storeUid ? `store:${storeUid}` : "all";
    setExportRunningMap((prev) => ({ ...prev, [key]: true }));
    setExportError("");
    try {
      const response = await runPricingMonitoringExport(storeUid);
      const result = response.result;
      const nextStatusMap: Record<string, MonitoringExportRunKindResult> = {};
      const applyStore = (item: MonitoringExportRunStoreResult) => {
        const suid = String(item.store_uid || "").trim();
        for (const kindResult of Array.isArray(item.results) ? item.results : []) {
          const kind = String(kindResult.kind || "").trim();
          if (!suid || !kind) continue;
          nextStatusMap[`${suid}:${kind}`] = kindResult;
        }
      };
      if (result && "stores" in result && Array.isArray(result.stores)) {
        for (const item of result.stores) applyStore(item);
        setExportLastSummary(`Обновлено магазинов: ${result.stores.length}`);
      } else if (result && "store_uid" in result) {
        applyStore(result);
        setExportLastSummary(`Последний запуск: ${result.store_name || result.store_uid}`);
      } else {
        setExportLastSummary("");
      }
      setExportStatusMap((prev) => ({ ...prev, ...nextStatusMap }));
      if (!response.ok) {
        setExportError(response.message || "Экспорт завершился с ошибками");
      }
      showAppToast({ message: storeUid ? "Экспорт магазина выполнен" : "Экспорт выполнен" });
      await loadExportData(true);
    } catch (e) {
      setExportError(e instanceof Error ? e.message : String(e));
    } finally {
      setExportRunningMap((prev) => ({ ...prev, [key]: false }));
    }
  }

  async function onRunJob(jobCode: string) {
    const code = String(jobCode || "").trim();
    if (!code) return;
    setRunningMap((prev) => ({ ...prev, [code]: true }));
    setError("");
    try {
      await runPricingMonitoringJob(code);
      showAppToast({ message: "Обновление запущено" });
      await loadData(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunningMap((prev) => ({ ...prev, [code]: false }));
    }
  }

  async function onRunAll() {
    setRunAllLoading(true);
    setError("");
    try {
      await runPricingMonitoringAll();
      showAppToast({ message: "Обновление запущено" });
      await loadData(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunAllLoading(false);
    }
  }

  return (
    <WorkspacePageFrame>
      <div className={layoutStyles.shell}>
        <WorkspacePageHero
          title="Мониторинг"
          subtitle="Операционный центр для расписаний обновления, ручных запусков и контроля экспортных задач."
          tabs={{
            items: [
              { id: "import", label: "Импорт данных" },
              { id: "export", label: "Экспорт данных" },
            ],
            activeId: tab,
            onChange: setTab,
          }}
          meta={(
            <div className={layoutStyles.heroMeta}>
              <span className={layoutStyles.metaChip}>{tab === "import" ? "Импорт" : "Экспорт"}</span>
              <span className={layoutStyles.metaChip}>{tab === "import" ? `${rows.length} задач` : `${exportRows.length} экспортов`}</span>
            </div>
          )}
          toolbar={<div className={layoutStyles.toolbarGroup}><span className={layoutStyles.metaChip}>{tab === "import" ? "Расписания и ручные прогоны" : "Статусы экспортов и конфигурации"}</span></div>}
        />
        {tab === "import" ? <>{error ? <ErrorBox message={error} /> : null}</> : null}
        {tab === "export" ? <>{exportError ? <ErrorBox message={exportError} /> : null}</> : null}
        {tab === "import" ? (
          <MonitoringSection
            loading={loading}
            error={error}
            rows={rows}
            platformStores={platformStores}
            runAllState={runAllState}
            savingMap={savingMap}
            runningMap={runningMap}
            runAllLoading={runAllLoading}
            onRunAll={onRunAll}
            onRunJob={onRunJob}
            onSaveJob={onSaveJob}
          />
        ) : (
          <MonitoringExportSection
            loading={exportLoading}
            error={exportError}
            rows={exportRows}
            platformStores={platformStores}
            savingMap={savingMap}
            runningMap={exportRunningMap}
            statusMap={exportStatusMap}
            summary={exportLastSummary}
            onSaveConfig={onSaveExportConfig}
            onRunExport={onRunExport}
          />
        )}
      </div>
    </WorkspacePageFrame>
  );
}
