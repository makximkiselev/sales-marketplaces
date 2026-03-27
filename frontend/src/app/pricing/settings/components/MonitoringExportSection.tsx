"use client";

import { useMemo, useState } from "react";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import { CogsSourceModal } from "./CogsSourceModal";
import type {
  CogsSource,
  MonitoringExportConfigApi,
  MonitoringExportRunKindResult,
  MonitoringExportStoreApi,
  RefreshMonitoringStoreApi,
} from "../types";

type Props = {
  loading: boolean;
  error: string;
  rows: MonitoringExportStoreApi[];
  platformStores: Record<string, RefreshMonitoringStoreApi[]>;
  savingMap: Record<string, boolean>;
  runningMap: Record<string, boolean>;
  statusMap: Record<string, MonitoringExportRunKindResult>;
  summary: string;
  onSaveConfig: (storeUid: string, exportKind: "prices" | "ads", config: CogsSource) => Promise<void>;
  onRunExport: (storeUid?: string) => Promise<void>;
};

type ModalState = {
  storeUid: string;
  exportKind: "prices" | "ads";
  title: string;
  current: CogsSource | null;
};

function toModalConfig(config: MonitoringExportConfigApi | null | undefined): CogsSource | null {
  if (!config) return null;
  const sourceId = String(config.sourceId || "").trim();
  const skuColumn = String(config.skuColumn || "").trim();
  const valueColumn = String(config.valueColumn || "").trim();
  if (!sourceId && !skuColumn && !valueColumn) return null;
  return {
    type: (String(config.type || "").trim() || "table") as "table" | "system",
    sourceId,
    sourceName: String(config.sourceName || "").trim(),
    skuColumn,
    valueColumn,
  };
}

function configLabel(config: MonitoringExportConfigApi | null | undefined) {
  const sourceName = String(config?.sourceName || "").trim();
  return sourceName || "Не настроено";
}

function configMeta(config: MonitoringExportConfigApi | null | undefined) {
  const sku = String(config?.skuColumn || "").trim();
  const value = String(config?.valueColumn || "").trim();
  return {
    sku: sku || "Не выбран",
    value: value || "Не выбран",
  };
}

function isConfigured(config: MonitoringExportConfigApi | null | undefined) {
  return Boolean(String(config?.sourceId || "").trim() && String(config?.skuColumn || "").trim() && String(config?.valueColumn || "").trim());
}

function statusLabel(item: MonitoringExportRunKindResult | undefined) {
  if (!item) return "";
  const status = String(item.status || "").trim().toLowerCase();
  if (status === "success") {
    const updated = Number(item.updated_cells || 0);
    const matched = Number(item.matched_rows || 0);
    if (updated > 0) return `Записано ${updated}`;
    if (matched > 0) return "Совпадения есть, без изменений";
    return "Совпадений не найдено";
  }
  if (status === "skipped") return String(item.message || "Пропущено");
  return String(item.message || "Ошибка");
}

function statusClass(item: MonitoringExportRunKindResult | undefined, stylesObj: Record<string, string>) {
  if (!item) return stylesObj.monitoringConfigStateEmpty;
  const status = String(item.status || "").trim().toLowerCase();
  if (status === "success") return stylesObj.monitoringConfigStateReady;
  if (status === "skipped") return stylesObj.monitoringStatusIdle;
  return stylesObj.monitoringStatusError;
}

export function MonitoringExportSection({
  loading,
  error,
  rows,
  platformStores,
  savingMap,
  runningMap,
  statusMap,
  summary,
  onSaveConfig,
  onRunExport,
}: Props) {
  const [modal, setModal] = useState<ModalState | null>(null);

  const rowsByStore = useMemo(() => {
    const map = new Map<string, MonitoringExportStoreApi>();
    for (const row of rows) {
      map.set(String(row.store_uid || "").trim(), row);
    }
    return map;
  }, [rows]);

  const platformEntries = useMemo(() => {
    return Object.entries(platformStores).filter(([, stores]) => Array.isArray(stores) && stores.length);
  }, [platformStores]);

  const exportRows: Array<{ key: "prices" | "ads"; label: string }> = [
    { key: "prices", label: "Экспорт цен" },
    { key: "ads", label: "Экспорт рекламных расходов" },
  ];
  const marketRows: Array<{ key: "market_prices" | "market_promos" | "market_boosts"; label: string }> = [
    { key: "market_prices", label: "Обновление цен на площадках" },
    { key: "market_promos", label: "Обновление участия в промо" },
    { key: "market_boosts", label: "Обновление рекламных кампаний" },
  ];

  return (
    <SectionBlock>
      {loading ? <div className="status">Загрузка настроек экспорта...</div> : null}
      {!loading && error ? <div className="status error">{error}</div> : null}
      {!loading && !error ? (
        <>
          <div className={styles.monitoringHeader}>
            <div className={styles.monitoringHeaderText}>
              Настройка выгрузки данных по магазинам в Google Sheets. В экспорт уходит финальная цена стратегии, а запись выполняется
              только для источников с включённым `mode_export`. Выгрузка на площадки использует финальные решения стратегии и включается
              через `export_enabled` у магазина.
            </div>
            <div className={styles.monitoringExportHeaderActions}>
              {summary ? <div className={styles.monitoringExportSummary}>{summary}</div> : null}
              <button
                type="button"
                className="btn inline primary"
                disabled={Boolean(runningMap.all)}
                onClick={() => {
                  void onRunExport();
                }}
              >
                {runningMap.all ? "Обновление..." : "Обновить"}
              </button>
            </div>
          </div>
          <div className="table-wrap">
            <table className={`table ${styles.monitoringTable} ${styles.monitoringExportTable}`}>
              <colgroup>
                <col className={styles.monitoringExportMethodCol} />
                {platformEntries.flatMap(([, stores]) =>
                  stores.map((store) => (
                    <col key={`col:${store.store_uid}`} className={styles.monitoringExportStoreCol} />
                  )),
                )}
              </colgroup>
              <thead>
                <tr>
                  <th>Метод</th>
                  {platformEntries.map(([platform, stores]) => (
                    <th key={platform} colSpan={stores.length}>
                      API-методы по площадке
                    </th>
                  ))}
                </tr>
                <tr>
                  <th />
                  {platformEntries.flatMap(([platform, stores]) =>
                    stores.map((store) => (
                      <th key={`${platform}:${store.store_uid}`}>
                        <div className={styles.monitoringStoreHead}>
                          <strong>{store.store_name || store.store_id}</strong>
                          <span className={styles.monitoringPlatformTag}>{store.platform_label}</span>
                          <button
                            type="button"
                            className={`btn inline ${styles.monitoringExportRunButton}`}
                            disabled={Boolean(runningMap[`store:${store.store_uid}`])}
                            onClick={() => {
                              void onRunExport(store.store_uid);
                            }}
                          >
                            {runningMap[`store:${store.store_uid}`] ? "Обновление..." : "Обновить"}
                          </button>
                        </div>
                      </th>
                    )),
                  )}
                </tr>
              </thead>
              <tbody>
                {exportRows.map((exportRow) => (
                  <tr key={exportRow.key}>
                    <td className={styles.monitoringExportMethodCell}>
                      <div className={styles.monitoringExportMethodName}>{exportRow.label}</div>
                    </td>
                    {platformEntries.flatMap(([, stores]) =>
                      stores.map((store) => {
                        const storeUid = String(store.store_uid || "").trim();
                        const item = rowsByStore.get(storeUid);
                        const config =
                          exportRow.key === "prices"
                            ? item?.export_prices
                            : item?.export_ads;
                        const currentStatus = statusMap[`${storeUid}:${exportRow.key}`];
                        const modalConfig = toModalConfig(config);
                        const meta = configMeta(config);
                        const saveKey = `${exportRow.key}:${storeUid}`;
                        return (
                          <td key={`${exportRow.key}:${storeUid}`} className={styles.monitoringStoreCell}>
                            <div
                              className={`${styles.monitoringExportCard} ${styles.cogsSourceRow} ${
                                savingMap[saveKey] ? styles.cogsSourceRowDisabled : ""
                              }`}
                            >
                              <div className={styles.monitoringExportCardTop}>
                                <span
                                  className={`${styles.monitoringConfigState} ${
                                    isConfigured(config) ? styles.monitoringConfigStateReady : styles.monitoringConfigStateEmpty
                                  }`}
                                >
                                  {isConfigured(config) ? "Настроено" : "Не настроено"}
                                </span>
                                <button
                                  type="button"
                                  className={`btn inline ${styles.cogsSelectButton}`}
                                  disabled={Boolean(savingMap[saveKey])}
                                  onClick={() =>
                                    setModal({
                                      storeUid,
                                      exportKind: exportRow.key,
                                      title: exportRow.key === "prices" ? "Источник экспорта цен" : "Источник экспорта рекламных расходов",
                                      current: modalConfig,
                                    })
                                  }
                                >
                                  {modalConfig ? "Изменить" : "Настроить"}
                                </button>
                              </div>
                              <div className={styles.monitoringExportSourceWrap}>
                                <div className={modalConfig ? styles.monitoringExportSourceSet : styles.monitoringExportSource}>
                                  {configLabel(config)}
                                </div>
                              </div>
                              <div className={styles.monitoringExportMetaGrid}>
                                <div className={styles.monitoringExportMetaItem}>
                                  <span className={styles.monitoringExportMetaLabel}>SKU</span>
                                  <strong className={styles.monitoringExportMetaValue}>{meta.sku}</strong>
                                </div>
                                <div className={styles.monitoringExportMetaItem}>
                                  <span className={styles.monitoringExportMetaLabel}>Значение</span>
                                  <strong className={styles.monitoringExportMetaValue}>{meta.value}</strong>
                                </div>
                              </div>
                              {currentStatus ? (
                                <div className={styles.monitoringExportStatusRow}>
                                  <span className={`${styles.monitoringConfigState} ${statusClass(currentStatus, styles)}`}>
                                    {statusLabel(currentStatus)}
                                  </span>
                                </div>
                              ) : null}
                            </div>
                          </td>
                        );
                      }),
                    )}
                  </tr>
                ))}
                {marketRows.map((exportRow) => (
                  <tr key={exportRow.key}>
                    <td className={styles.monitoringExportMethodCell}>
                      <div className={styles.monitoringExportMethodName}>{exportRow.label}</div>
                    </td>
                    {platformEntries.flatMap(([, stores]) =>
                      stores.map((store) => {
                        const storeUid = String(store.store_uid || "").trim();
                        const currentStatus = statusMap[`${storeUid}:${exportRow.key}`];
                        return (
                          <td key={`${exportRow.key}:${storeUid}`} className={styles.monitoringStoreCell}>
                            <div className={`${styles.monitoringExportCard} ${styles.cogsSourceRow}`}>
                              <div className={styles.monitoringExportCardTop}>
                                <span className={`${styles.monitoringConfigState} ${styles.monitoringStatusIdle}`}>API</span>
                              </div>
                              <div className={styles.monitoringExportSourceWrap}>
                                <div className={styles.monitoringExportSourceSet}>Маркет API</div>
                              </div>
                              <div className={styles.monitoringExportMetaGrid}>
                                <div className={styles.monitoringExportMetaItem}>
                                  <span className={styles.monitoringExportMetaLabel}>Источник</span>
                                  <strong className={styles.monitoringExportMetaValue}>Стратегия / Промо</strong>
                                </div>
                                <div className={styles.monitoringExportMetaItem}>
                                  <span className={styles.monitoringExportMetaLabel}>Условие</span>
                                  <strong className={styles.monitoringExportMetaValue}>`export_enabled`</strong>
                                </div>
                              </div>
                              {currentStatus ? (
                                <div className={styles.monitoringExportStatusRow}>
                                  <span className={`${styles.monitoringConfigState} ${statusClass(currentStatus, styles)}`}>
                                    {statusLabel(currentStatus)}
                                  </span>
                                </div>
                              ) : null}
                            </div>
                          </td>
                        );
                      }),
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {modal ? (
            <CogsSourceModal
              current={modal.current}
              title={modal.title}
              skuColumnLabel="Столбец с артикулом (SKU)"
              valueColumnLabel={modal.exportKind === "prices" ? "Столбец с ценой" : "Столбец с рекламными расходами"}
              onClose={() => setModal(null)}
              onSave={(config) => {
                void onSaveConfig(modal.storeUid, modal.exportKind, config).finally(() => setModal(null));
              }}
            />
          ) : null}
        </>
      ) : null}
    </SectionBlock>
  );
}
