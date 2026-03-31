import { useEffect, useState } from "react";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { RefreshMonitoringRowApi, RefreshMonitoringRunAllApi, RefreshMonitoringStoreApi } from "../types";

type SaveValues = {
  enabled: boolean;
  schedule_kind: string;
  interval_minutes?: number | null;
  time_of_day?: string | null;
  date_from?: string | null;
  date_to?: string | null;
  stores?: string[];
};

type Props = {
  loading: boolean;
  error: string;
  rows: RefreshMonitoringRowApi[];
  platformStores: Record<string, RefreshMonitoringStoreApi[]>;
  runAllState: RefreshMonitoringRunAllApi;
  savingMap: Record<string, boolean>;
  runningMap: Record<string, boolean>;
  runAllLoading: boolean;
  onRunAll: () => Promise<void>;
  onRunJob: (jobCode: string) => Promise<void>;
  onSaveJob: (jobCode: string, values: SaveValues) => Promise<void>;
};

function formatTs(value?: string | null) {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function statusLabel(row: RefreshMonitoringRowApi) {
  const raw = String(row.last_status || "").trim().toLowerCase();
  if (!raw) return "Ожидание";
  if (raw === "queued") return "В очереди";
  if (raw === "running") return "Идет";
  if (raw === "success") return "OK";
  if (raw === "error") return "Ошибка";
  return row.last_status || "—";
}

function statusClass(status: string | null | undefined) {
  const raw = String(status || "").trim().toLowerCase();
  if (raw === "success") return styles.monitoringStatusSuccess;
  if (raw === "running") return styles.monitoringStatusRunning;
  if (raw === "queued") return styles.monitoringStatusIdle;
  if (raw === "error") return styles.monitoringStatusError;
  return styles.monitoringStatusIdle;
}

function progressValue(value: number | null | undefined) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function storeStatusLabel(
  row: RefreshMonitoringRowApi,
  storeUid: string,
) {
  const item = (row.store_statuses || []).find((entry) => String(entry.store_uid || "").trim() === storeUid);
  const status = String(item?.status || "").trim().toLowerCase();
  if (!status || status === "idle") return "—";
  if (status === "success") return "OK";
  if (status === "running") return "Идет";
  if (status === "error") return "Ошибка";
  return status;
}

function renderScheduleSelect(
  row: RefreshMonitoringRowApi,
  saving: boolean,
  onSaveJob: Props["onSaveJob"],
) {
  const code = row.job_code;
  const isDaily = String(row.schedule_kind || "").toLowerCase() === "daily";
  const enabled = Boolean(Number(row.enabled || 0));
  const currentSelect = enabled
    ? isDaily
      ? `daily:${row.time_of_day || "00:00"}`
      : String(row.interval_minutes || 60)
    : "off";
  return (
    <select
      className={`input ${styles.monitoringSelect}`}
      value={currentSelect}
      disabled={saving}
      onChange={(e) => {
        const value = e.target.value;
        if (value === "off") {
          void onSaveJob(code, {
            enabled: false,
            schedule_kind: isDaily ? "daily" : "interval",
            interval_minutes: row.interval_minutes ?? null,
            time_of_day: row.time_of_day ?? null,
            stores: row.selected_store_uids || [],
          });
          return;
        }
        if (value.startsWith("daily:")) {
          void onSaveJob(code, {
            enabled: true,
            schedule_kind: "daily",
            time_of_day: value.split(":", 2)[1] || "00:00",
            interval_minutes: null,
            stores: row.selected_store_uids || [],
          });
          return;
        }
        void onSaveJob(code, {
          enabled: true,
          schedule_kind: "interval",
          interval_minutes: Number(value),
          time_of_day: null,
          stores: row.selected_store_uids || [],
        });
      }}
    >
      {isDaily ? (
        <>
          <option value={`daily:${row.time_of_day || "00:00"}`}>Ежедневно {row.time_of_day || "00:00"}</option>
          <option value="off">Выкл</option>
        </>
      ) : (
        <>
          <option value="30">Каждые 30 мин</option>
          <option value="60">Каждый час</option>
          <option value="120">Каждые 2 часа</option>
          <option value="180">Каждые 3 часа</option>
          <option value="off">Выкл</option>
        </>
      )}
    </select>
  );
}

function supportsDateRange(row: RefreshMonitoringRowApi) {
  return ["sales_reports_hourly_refresh", "shelfs_statistics_refresh", "shows_boost_refresh"].includes(String(row.job_code || "").trim());
}

function todayYmd() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeDateRange(dateFrom: string, dateTo: string) {
  let nextFrom = String(dateFrom || "").trim();
  let nextTo = String(dateTo || "").trim();
  if (nextFrom && !nextTo) {
    nextTo = todayYmd();
  }
  if (nextFrom && nextTo && nextTo < nextFrom) {
    nextTo = nextFrom;
  }
  return { date_from: nextFrom, date_to: nextTo };
}

type DateDraft = {
  date_from: string;
  date_to: string;
  dirty_from?: boolean;
  dirty_to?: boolean;
};

export function MonitoringSection({
  loading,
  error,
  rows,
  platformStores,
  runAllState,
  savingMap,
  runningMap,
  runAllLoading,
  onRunAll,
  onRunJob,
  onSaveJob,
}: Props) {
  const apiRows = rows.filter((row) => String(row.kind || "").trim() === "api");
  const otherRows = rows.filter((row) => String(row.kind || "").trim() !== "api");
  const [dateDrafts, setDateDrafts] = useState<Record<string, DateDraft>>({});

  useEffect(() => {
    setDateDrafts((prev) => {
      const next = { ...prev };
      for (const row of rows) {
        const code = String(row.job_code || "").trim();
        if (!code || !supportsDateRange(row)) continue;
        const current = next[code];
        const serverFrom = String(row.date_from || "");
        const serverTo = String(row.date_to || "");
        if (!current) {
          next[code] = {
            date_from: serverFrom,
            date_to: serverTo,
            dirty_from: false,
            dirty_to: false,
          };
          continue;
        }
        next[code] = {
          ...current,
          date_from: current.dirty_from ? current.date_from : serverFrom,
          date_to: current.dirty_to ? current.date_to : serverTo,
        };
      }
      return next;
    });
  }, [rows]);

  function updateDateDraft(jobCode: string, field: "date_from" | "date_to", value: string) {
    setDateDrafts((prev) => {
      const current = prev[jobCode] || { date_from: "", date_to: "", dirty_from: false, dirty_to: false };
      return {
        ...prev,
        [jobCode]: {
          ...current,
          [field]: value,
          [field === "date_from" ? "dirty_from" : "dirty_to"]: true,
        },
      };
    });
  }

  async function commitDateDraft(row: RefreshMonitoringRowApi, field: "date_from" | "date_to") {
    const code = String(row.job_code || "").trim();
    const current = dateDrafts[code];
    if (!current) return;
    const normalized = normalizeDateRange(current.date_from, current.date_to);
    const savedFrom = String(row.date_from || "");
    const savedTo = String(row.date_to || "");
    if (normalized.date_from === savedFrom && normalized.date_to === savedTo) {
      setDateDrafts((prev) => ({
        ...prev,
        [code]: {
          ...(prev[code] || current),
          date_from: normalized.date_from,
          date_to: normalized.date_to,
          dirty_from: false,
          dirty_to: false,
        },
      }));
      return;
    }
    await onSaveJob(code, {
      enabled: Boolean(Number(row.enabled || 0)),
      schedule_kind: row.schedule_kind,
      interval_minutes: row.interval_minutes ?? null,
      time_of_day: row.time_of_day ?? null,
      date_from: normalized.date_from || null,
      date_to: normalized.date_to || null,
      stores: row.selected_store_uids || [],
    });
    setDateDrafts((prev) => ({
      ...prev,
      [code]: {
        ...(prev[code] || current),
        date_from: normalized.date_from,
        date_to: normalized.date_to,
        dirty_from: false,
        dirty_to: false,
      },
    }));
  }

  const platformEntries = Object.entries(platformStores).sort(([a], [b]) => {
    const order = ["yandex_market", "ozon", "google_sheets", "pricing"];
    const ai = order.indexOf(a);
    const bi = order.indexOf(b);
    return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi) || a.localeCompare(b, "ru");
  });

  return (
    <SectionBlock>
      {loading ? <div className="status">Загрузка мониторинга...</div> : null}
      {!loading && error ? <div className="status error">{error}</div> : null}
      {!loading && !error ? (
        <>
          <div className={styles.monitoringHeader}>
            <div className={styles.monitoringHeaderText}>
              Единый центр управления обновлениями данных, API-методов и внутренних пересчётов.
            </div>
            <div className={styles.monitoringRunAll}>
              <button type="button" className="btn primary" onClick={() => void onRunAll()} disabled={runAllLoading || String(runAllState.last_status || "").trim().toLowerCase() === "running"}>
                {String(runAllState.last_status || "").trim().toLowerCase() === "running"
                  ? `Обновление ${progressValue(runAllState.progress_percent)}%`
                  : runAllLoading
                    ? "Запуск..."
                    : "Обновить всё"}
              </button>
              {String(runAllState.last_status || "").trim() ? (
                <div className={styles.monitoringRunAllMeta}>
                  <span className={`${styles.monitoringJobStatus} ${statusClass(runAllState.last_status)}`}>
                    <span className={styles.monitoringStatusDot} />
                    {statusLabel({ last_status: runAllState.last_status } as RefreshMonitoringRowApi)}
                  </span>
                  {runAllState.current_stage ? <span className={styles.monitoringRunAllStage}>{runAllState.current_stage}</span> : null}
                  {String(runAllState.last_status || "").trim().toLowerCase() === "running" ? (
                    <div className={styles.monitoringProgress}>
                      <div
                        className={styles.monitoringProgressBar}
                        style={{ "--monitoring-progress": `${progressValue(runAllState.progress_percent)}%` } as React.CSSProperties}
                      />
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          </div>

          {apiRows.length ? (
            <div className={styles.pricingTableWrap}>
              <div className={styles.monitoringGroupTitle}>API-методы по площадкам</div>
              <table className={`${styles.pricingTable} ${styles.monitoringTable}`}>
                <thead>
                  <tr>
                    <th rowSpan={2}>Метод</th>
                    <th rowSpan={2}>Частота</th>
                    {platformEntries.map(([platformKey, stores]) => (
                      <th key={platformKey} colSpan={stores.length}>
                        <span className={styles.monitoringPlatformTag}>
                          {stores[0]?.platform_label || platformKey}
                        </span>
                      </th>
                    ))}
                    <th rowSpan={2}>Диапазон</th>
                    <th rowSpan={2}>Последний запуск</th>
                    <th rowSpan={2}>Статус</th>
                    <th rowSpan={2}>Действие</th>
                  </tr>
                  <tr>
                    {platformEntries.flatMap(([platformKey, stores]) =>
                      stores.map((store) => (
                        <th key={`${platformKey}:${store.store_uid}`}>
                          <div className={styles.monitoringStoreHead}>
                            <span>{store.store_name}</span>
                          </div>
                        </th>
                      )),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {apiRows.map((row) => {
                    const code = row.job_code;
                    return (
                      <tr key={code}>
                        <td className={styles.colText}>{row.title}</td>
                        <td>{renderScheduleSelect(row, Boolean(savingMap[code]), onSaveJob)}</td>
                        {platformEntries.flatMap(([platformKey, stores]) =>
                          stores.map((store) => {
                            if (platformKey !== String(row.platform || "").trim()) {
                              return <td key={`${code}:${store.store_uid}`} className={styles.monitoringStoreCellMuted}>—</td>;
                            }
                            const storeState = (row.store_statuses || []).find((entry) => String(entry.store_uid || "").trim() === store.store_uid);
                            return (
                              <td key={`${code}:${store.store_uid}`} className={styles.monitoringStoreCell}>
                                <span
                                  title={storeState?.message || storeStatusLabel(row, store.store_uid)}
                                  className={`${styles.monitoringStoreStatus} ${statusClass(storeState?.status)}`}
                                >
                                  <span className={styles.monitoringStatusDot} />
                                  {storeStatusLabel(row, store.store_uid)}
                                  {storeState?.status === "running" ? ` ${progressValue((storeState as { progress_percent?: number }).progress_percent)}%` : ""}
                                </span>
                                {storeState?.status === "running" ? (
                                  <div className={styles.monitoringProgress}>
                                    <div
                                      className={styles.monitoringProgressBar}
                                      style={{ "--monitoring-progress": `${progressValue((storeState as { progress_percent?: number }).progress_percent)}%` } as React.CSSProperties}
                                    />
                                  </div>
                                ) : null}
                              </td>
                            );
                          }),
                        )}
                        <td>
                          {supportsDateRange(row) ? (
                            <div className={styles.monitoringDateRange}>
                              <label className={styles.monitoringDateField}>
                                <span>с</span>
                                <input
                                  type="date"
                                  className={`input ${styles.monitoringSelect}`}
                                  value={dateDrafts[code]?.date_from ?? String(row.date_from || "")}
                                  disabled={Boolean(savingMap[code])}
                                  onChange={(e) => updateDateDraft(code, "date_from", e.target.value)}
                                  onBlur={() => void commitDateDraft(row, "date_from")}
                                  onKeyDown={(e) => {
                                    if (e.key === "Enter") {
                                      e.currentTarget.blur();
                                    }
                                  }}
                                />
                              </label>
                              <label className={styles.monitoringDateField}>
                                <span>по</span>
                                <input
                                  type="date"
                                  className={`input ${styles.monitoringSelect}`}
                                  value={dateDrafts[code]?.date_to ?? String(row.date_to || "")}
                                  min={(dateDrafts[code]?.date_from ?? String(row.date_from || "")).trim() || undefined}
                                  disabled={Boolean(savingMap[code])}
                                  onChange={(e) => updateDateDraft(code, "date_to", e.target.value)}
                                  onBlur={() => void commitDateDraft(row, "date_to")}
                                  onKeyDown={(e) => {
                                    if (e.key === "Enter") {
                                      e.currentTarget.blur();
                                    }
                                  }}
                                />
                              </label>
                            </div>
                          ) : (
                            "—"
                          )}
                        </td>
                        <td>{formatTs(row.last_finished_at || row.last_started_at)}</td>
                        <td>
                          <span
                            title={row.last_message || statusLabel(row)}
                            className={`${styles.monitoringJobStatus} ${statusClass(row.last_status)}`}
                          >
                            <span className={styles.monitoringStatusDot} />
                            {statusLabel(row)}
                            {String(row.last_status || "").trim().toLowerCase() === "running" ? ` ${progressValue(row.progress_percent)}%` : ""}
                          </span>
                          {String(row.last_status || "").trim().toLowerCase() === "running" ? (
                            <div className={styles.monitoringProgress}>
                              <div
                                className={styles.monitoringProgressBar}
                                style={{ "--monitoring-progress": `${progressValue(row.progress_percent)}%` } as React.CSSProperties}
                              />
                            </div>
                          ) : null}
                        </td>
                        <td>
                          <button
                            type="button"
                            className="btn inline"
                            disabled={Boolean(runningMap[code]) || ["running", "queued"].includes(String(row.last_status || "").trim().toLowerCase())}
                            onClick={() => void onRunJob(code)}
                          >
                            {String(row.last_status || "").trim().toLowerCase() === "queued"
                              ? "В очереди"
                              : runningMap[code]
                                ? "Запуск..."
                                : "Обновить"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}

          {otherRows.length ? (
            <div className={styles.pricingTableWrap}>
              <div className={styles.monitoringGroupTitle}>Google и внутренние пересчеты</div>
              <table className={`${styles.pricingTable} ${styles.monitoringTable}`}>
                <thead>
                  <tr>
                    <th>Метод</th>
                    <th>Тип</th>
                    <th>Частота обновлений</th>
                    <th>Последний запуск</th>
                    <th>Статус запуска</th>
                    <th>Действие</th>
                  </tr>
                </thead>
                <tbody>
                  {otherRows.map((row) => {
                    const code = row.job_code;
                    return (
                      <tr key={code}>
                        <td className={styles.colText}>{row.title}</td>
                        <td>{String(row.kind || "").trim() === "google" ? "Google" : "Система"}</td>
                        <td>{renderScheduleSelect(row, Boolean(savingMap[code]), onSaveJob)}</td>
                        <td>{formatTs(row.last_finished_at || row.last_started_at)}</td>
                        <td>
                          <span
                            title={row.last_message || statusLabel(row)}
                            className={`${styles.monitoringJobStatus} ${statusClass(row.last_status)}`}
                          >
                            <span className={styles.monitoringStatusDot} />
                            {statusLabel(row)}
                          </span>
                        </td>
                        <td>
                          <button
                            type="button"
                            className="btn inline"
                            disabled={Boolean(runningMap[code]) || ["running", "queued"].includes(String(row.last_status || "").trim().toLowerCase())}
                            onClick={() => void onRunJob(code)}
                          >
                            {String(row.last_status || "").trim().toLowerCase() === "queued"
                              ? "В очереди"
                              : runningMap[code]
                                ? "Запуск..."
                                : "Обновить"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </>
      ) : null}
    </SectionBlock>
  );
}
