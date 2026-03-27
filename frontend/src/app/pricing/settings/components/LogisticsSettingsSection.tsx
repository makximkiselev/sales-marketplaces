"use client";

import { ControlField } from "../../../../components/page/ControlKit";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { LogisticsEditableFieldKey, LogisticsRow } from "../types";

type Props = {
  moneySign: string;
  loading: boolean;
  error: string;
  logisticsError: string;
  logisticsLoading: boolean;
  logisticsRows: LogisticsRow[];
  logisticsSearch: string;
  logisticsPage: number;
  logisticsPageSize: number;
  logisticsTotal: number;
  logisticsPageSizeOptions: number[];
  activePlatform: string;
  activeStoreId: string;
  logisticsCellDrafts: Record<string, string>;
  logisticsCellSaving: Record<string, boolean>;
  setLogisticsPage: React.Dispatch<React.SetStateAction<number>>;
  setLogisticsPageSize: React.Dispatch<React.SetStateAction<number>>;
  setLogisticsSearch: React.Dispatch<React.SetStateAction<string>>;
  setLogisticsImportOpen: React.Dispatch<React.SetStateAction<boolean>>;
  toLiveLogisticsRow: (row: LogisticsRow) => LogisticsRow;
  fmtCell: (value: number | string | null | undefined) => string;
  getLogisticsCellKey: (sku: string, field: LogisticsEditableFieldKey) => string;
  setLogisticsCellDraftByKey: (cellKey: string, rawValue: string) => void;
  commitLogisticsCell: (row: LogisticsRow, field: LogisticsEditableFieldKey) => void;
  setLogisticsCellDrafts: React.Dispatch<React.SetStateAction<Record<string, string>>>;
};

export function LogisticsSettingsSection({
  moneySign,
  loading,
  error,
  logisticsError,
  logisticsLoading,
  logisticsRows,
  logisticsSearch,
  logisticsPage,
  logisticsPageSize,
  logisticsTotal,
  logisticsPageSizeOptions,
  activePlatform,
  activeStoreId,
  logisticsCellDrafts,
  logisticsCellSaving,
  setLogisticsPage,
  setLogisticsPageSize,
  setLogisticsSearch,
  setLogisticsImportOpen,
  toLiveLogisticsRow,
  fmtCell,
  getLogisticsCellKey,
  setLogisticsCellDraftByKey,
  commitLogisticsCell,
  setLogisticsCellDrafts,
}: Props) {
  const totalPages = Math.max(1, Math.ceil(logisticsTotal / Math.max(1, logisticsPageSize)));

  return (
    <SectionBlock>
        {loading ? <div className="status">Загрузка контекста...</div> : null}
        {!loading && error ? <div className="status error">{error}</div> : null}
        {!loading && !error ? (
          <>
            <div className={styles.logisticsListControls}>
              <ControlField label="Поиск" className={styles.logisticsSearchField}>
                <div className={styles.inputWithSuffix}>
                  <input
                    className={`input ${styles.settingInput}`}
                    value={logisticsSearch}
                    onChange={(e) => {
                      setLogisticsSearch(e.target.value);
                      setLogisticsPage(1);
                    }}
                    placeholder="Поиск по SKU или наименованию"
                  />
                </div>
              </ControlField>
              <ControlField label="На странице" className={styles.logisticsPageSizeBox}>
                <select
                  className={`input ${styles.logisticsPageSizeSelect}`}
                  value={String(logisticsPageSize)}
                  onChange={(e) => {
                    setLogisticsPageSize(Number(e.target.value));
                    setLogisticsPage(1);
                  }}
                >
                  {logisticsPageSizeOptions.map((n) => <option key={n} value={n}>{n}</option>)}
                </select>
              </ControlField>
              <div className={styles.logisticsImportBox}>
                <button
                  type="button"
                  className={`btn inline ${styles.importButton}`}
                  onClick={() => setLogisticsImportOpen(true)}
                  disabled={!activeStoreId || (activePlatform !== "yandex_market" && activePlatform !== "ozon")}
                >
                  Импорт
                </button>
              </div>
            </div>

            {logisticsError ? <div className="status error">{logisticsError}</div> : null}
            {activePlatform !== "yandex_market" && activePlatform !== "ozon" ? (
              <div className={styles.emptyState}>Логистика поддерживается только для Яндекс.Маркета и Ozon.</div>
            ) : logisticsLoading ? (
              <div className="status">Загрузка таблицы логистики...</div>
            ) : (
              <>
                <div className={styles.pricingTableWrap}>
                  <table className={`${styles.pricingTable} ${styles.logisticsTable}`}>
                    <thead>
                      <tr>
                        <th>SKU</th>
                        <th>Наименование товара</th>
                        <th>Стоимость логистики</th>
                        <th>Ширина, см</th>
                        <th>Длина, см</th>
                        <th>Высота, см</th>
                        <th>Вес, кг</th>
                        <th>Вес объемный, кг</th>
                        <th>Максимальный вес, кг</th>
                        <th>Стоимость за кг, {moneySign}</th>
                        <th>Стоимость обработки</th>
                        <th>Стоимость доставки до клиента, {moneySign}</th>
                        <th>Стоимость обработки возврата, {moneySign}</th>
                        <th>Стоимость утилизации, {moneySign}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {logisticsRows.map((rawRow) => {
                        const row = toLiveLogisticsRow(rawRow);
                        return (
                          <tr key={row.sku}>
                            <td className={styles.colText}>{row.sku}</td>
                            <td className={styles.colText}>{row.name || "—"}</td>
                            <td>
                              <div className={styles.logisticsComposite}>
                                <div>{fmtCell(row.logistics_cost_display)}</div>
                                <div className={styles.logisticsCompositeMeta}>Обработка: {fmtCell(row.handling_cost_display)}</div>
                              </div>
                            </td>
                            {(["width_cm", "length_cm", "height_cm", "weight_kg"] as LogisticsEditableFieldKey[]).map((field) => {
                              const value = row[field];
                              const cellKey = getLogisticsCellKey(row.sku, field);
                              const draft = logisticsCellDrafts[cellKey];
                              return (
                                <td key={field}>
                                  <div className={styles.cellInputWrap}>
                                    <input
                                      className={`input ${styles.cellInput}`}
                                      value={draft ?? (value == null ? "" : String(value))}
                                      onChange={(e) => setLogisticsCellDraftByKey(cellKey, e.target.value)}
                                      onBlur={() => commitLogisticsCell(row, field)}
                                      onKeyDown={(e) => {
                                        if (e.key === "Enter") e.currentTarget.blur();
                                        if (e.key === "Escape") {
                                          setLogisticsCellDrafts((prev) => {
                                            const next = { ...prev };
                                            delete next[cellKey];
                                            return next;
                                          });
                                        }
                                      }}
                                      inputMode="decimal"
                                      placeholder="Введите"
                                    />
                                    {logisticsCellSaving[cellKey] ? <span className={styles.cellSavingDot} /> : null}
                                  </div>
                                </td>
                              );
                            })}
                            <td>{fmtCell(row.volumetric_weight_kg)}</td>
                            <td>{fmtCell(row.max_weight_kg)}</td>
                            <td>{fmtCell(row.cost_per_kg)}</td>
                            <td>{fmtCell(row.handling_cost_display)}</td>
                            <td>{fmtCell(row.delivery_to_client_cost)}</td>
                            <td>{fmtCell(row.return_processing_cost)}</td>
                            <td>{fmtCell(row.disposal_cost)}</td>
                          </tr>
                        );
                      })}
                      {!logisticsRows.length ? (
                        <tr>
                          <td colSpan={14}>Нет товаров в raw-слое выбранного магазина.</td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>

                <div className={styles.logisticsPager}>
                  <div className={styles.inlineInfo}>Всего: {logisticsTotal}. Страница {logisticsPage} из {totalPages}</div>
                  <div className={styles.platformTabs}>
                    <button type="button" className={`btn inline ${styles.tabButton}`} onClick={() => setLogisticsPage((p) => Math.max(1, p - 1))} disabled={logisticsPage <= 1}>Назад</button>
                    <button type="button" className={`btn inline ${styles.tabButton}`} onClick={() => setLogisticsPage((p) => p + 1)} disabled={logisticsPage >= totalPages}>Вперед</button>
                  </div>
                </div>
              </>
            )}
          </>
        ) : null}
    </SectionBlock>
  );
}
