import { useEffect, useMemo, useState } from "react";
import { CatalogBrowser } from "../../../../components/page/CatalogBrowser";
import { ControlField } from "../../../../components/page/ControlKit";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { LogisticsEditableFieldKey, LogisticsRow } from "../types";
import type { TreeNode } from "../../../_shared/catalogState";

function collectTreePaths(nodes: TreeNode[], parent = ""): string[] {
  const out: string[] = [];
  for (const node of nodes || []) {
    const path = parent ? `${parent} / ${node.name}` : node.name;
    const children = Array.isArray(node.children) ? node.children : [];
    if (children.length) {
      out.push(path, ...collectTreePaths(children, path));
    }
  }
  return out;
}

type Props = {
  moneySign: string;
  loading: boolean;
  error: string;
  logisticsError: string;
  logisticsLoading: boolean;
  logisticsRows: LogisticsRow[];
  logisticsTreeRoots: TreeNode[];
  logisticsTreePath: string;
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
  setLogisticsTreePath: React.Dispatch<React.SetStateAction<string>>;
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
  logisticsTreeRoots,
  logisticsTreePath,
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
  setLogisticsTreePath,
  setLogisticsImportOpen,
  toLiveLogisticsRow,
  fmtCell,
  getLogisticsCellKey,
  setLogisticsCellDraftByKey,
  commitLogisticsCell,
  setLogisticsCellDrafts,
}: Props) {
  const totalPages = Math.max(1, Math.ceil(logisticsTotal / Math.max(1, logisticsPageSize)));
  const [selectedSku, setSelectedSku] = useState("");
  const selectedRow = useMemo(() => {
    if (!logisticsRows.length) return null;
    return logisticsRows.find((row) => row.sku === selectedSku) ?? logisticsRows[0];
  }, [logisticsRows, selectedSku]);
  const [treeQuery, setTreeQuery] = useState("");
  const [expandedTreePaths, setExpandedTreePaths] = useState<string[]>([]);

  useEffect(() => {
    if (!logisticsRows.length) {
      setSelectedSku("");
      return;
    }
    if (!logisticsRows.some((row) => row.sku === selectedSku)) {
      setSelectedSku(logisticsRows[0].sku);
    }
  }, [logisticsRows, selectedSku]);

  const editableFields: Array<{ field: LogisticsEditableFieldKey; label: string }> = [
    { field: "width_cm", label: "Ширина, см" },
    { field: "length_cm", label: "Длина, см" },
    { field: "height_cm", label: "Высота, см" },
    { field: "weight_kg", label: "Вес, кг" },
  ];
  const visibleSkuLabel = logisticsTreePath ? "SKU выбранной ветки" : "Все SKU магазина";

  return (
    <SectionBlock>
      {loading ? <div className="status">Загрузка контекста...</div> : null}
      {!loading && error ? <div className="status error">{error}</div> : null}
      {!loading && !error ? (
        <>
          {logisticsError ? <div className="status error">{logisticsError}</div> : null}
          {activePlatform !== "yandex_market" && activePlatform !== "ozon" ? (
            <div className={styles.emptyState}>Логистика поддерживается только для Яндекс.Маркета и Ozon.</div>
          ) : logisticsLoading ? (
            <div className="status">Загрузка логистики...</div>
          ) : (
            <div className={styles.logisticsWorkspace}>
              <div className={styles.logisticsCatalogPane}>
                <CatalogBrowser
                  title="Каталог"
                  subtitle="Выбери ветку каталога, затем SKU в соседнем блоке."
                  roots={logisticsTreeRoots}
                  selectedPath={logisticsTreePath}
                  expandedPaths={expandedTreePaths}
                  query={treeQuery}
                  onQueryChange={setTreeQuery}
                  onToggleExpand={(path) =>
                    setExpandedTreePaths((current) =>
                      current.includes(path) ? current.filter((item) => item !== path) : [...current, path],
                    )
                  }
                  onToggleExpandAll={() =>
                    setExpandedTreePaths((current) => (current.length ? [] : collectTreePaths(logisticsTreeRoots)))
                  }
                  onSelectPath={(path) => {
                    setLogisticsTreePath((current) => {
                      const next = current === path ? "" : path;
                      setLogisticsPage(1);
                      return next;
                    });
                  }}
                  emptyText="Нет категорий для выбранного магазина"
                />
              </div>

              <div className={styles.logisticsSidebar}>
                <div className={styles.logisticsSidebarControls}>
                  <div className={styles.logisticsSidebarPanelHead}>
                    <div>
                      <div className={styles.categorySidebarTitle}>Товары</div>
                      <div className={styles.categorySidebarMeta}>{visibleSkuLabel}</div>
                    </div>
                    {logisticsTreePath ? <div className={styles.logisticsBranchChip}>{logisticsTreePath}</div> : null}
                  </div>
                  <ControlField label="Поиск по SKU" className={styles.logisticsSearchField}>
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
                  <div className={styles.logisticsSidebarActionRow}>
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
                    <button
                      type="button"
                      className={`btn ghost ${styles.logisticsImportButton}`}
                      onClick={() => setLogisticsImportOpen(true)}
                      disabled={!activeStoreId || (activePlatform !== "yandex_market" && activePlatform !== "ozon")}
                    >
                      Импорт
                    </button>
                  </div>
                </div>
                <div className={styles.logisticsSidebarList}>
                  {logisticsRows.map((rawRow) => {
                    const row = toLiveLogisticsRow(rawRow);
                    const active = selectedRow?.sku === row.sku;
                    return (
                      <button
                        key={row.sku}
                        type="button"
                        className={`${styles.logisticsSidebarItem} ${active ? styles.logisticsSidebarItemActive : ""}`}
                        onClick={() => setSelectedSku(row.sku)}
                      >
                        <div className={styles.logisticsSidebarItemHead}>
                          <span className={styles.logisticsSidebarSku}>{row.sku}</span>
                          <span className={styles.logisticsSidebarCost}>{fmtCell(row.logistics_cost_display)} {moneySign}</span>
                        </div>
                        <div className={styles.logisticsSidebarName}>{row.name || "Без названия"}</div>
                        <div className={styles.logisticsSidebarMetaRow}>
                          <span className={styles.logisticsSidebarMeta}>Обработка: {fmtCell(row.handling_cost_display)}</span>
                          <span className={styles.categoryTreeStatusInherited}>
                            {row.dimensions_inherited ? "Общие габариты" : "Свои габариты"}
                          </span>
                        </div>
                      </button>
                    );
                  })}
                  {!logisticsRows.length ? (
                    <div className={styles.logisticsEmptyState}>
                      {logisticsTreePath
                        ? "В этой ветке пока нет товаров raw-слоя."
                        : "Нет товаров в raw-слое выбранного магазина."}
                    </div>
                  ) : null}
                </div>
                <div className={styles.logisticsPager}>
                  <div className={styles.inlineInfo}>Всего: {logisticsTotal}. Стр. {logisticsPage} / {totalPages}</div>
                  <div className={styles.platformTabs}>
                    <button type="button" className={`btn inline ${styles.tabButton}`} onClick={() => setLogisticsPage((p) => Math.max(1, p - 1))} disabled={logisticsPage <= 1}>Назад</button>
                    <button type="button" className={`btn inline ${styles.tabButton}`} onClick={() => setLogisticsPage((p) => p + 1)} disabled={logisticsPage >= totalPages}>Вперед</button>
                  </div>
                </div>
              </div>

              <div className={styles.logisticsEditor}>
                {selectedRow ? (
                  <>
                    <div className={styles.categoryEditorHead}>
                      <div className={styles.logisticsEditorIntro}>
                        <div className={styles.categoryEditorEyebrow}>Товар</div>
                        <h3 className={styles.categoryEditorTitle}>{selectedRow.name || "Без названия"}</h3>
                        <div className={styles.categoryEditorPath}>SKU {selectedRow.sku}</div>
                      </div>
                      <div className={styles.categoryEditorMeta}>
                        <span className={styles.categoryEditorMetaChip}>{fmtCell(selectedRow.logistics_cost_display)} {moneySign}</span>
                        <span className={styles.categoryEditorMetaChip}>{selectedRow.dimensions_inherited ? "Общие габариты" : "Свои габариты"}</span>
                      </div>
                    </div>

                    <div className={styles.logisticsEditorGrid}>
                      {editableFields.map(({ field, label }) => {
                        const cellKey = getLogisticsCellKey(selectedRow.sku, field);
                        const draft = logisticsCellDrafts[cellKey];
                        const value = selectedRow[field];
                        return (
                          <div key={field} className={styles.categoryEditorField}>
                            <div className={styles.categoryEditorFieldHead}>
                              <div className={styles.categoryEditorFieldLabel}>{label}</div>
                              <span className={selectedRow.dimensions_inherited ? styles.categoryTreeStatusInherited : styles.categoryTreeStatusCustom}>
                                {selectedRow.dimensions_inherited ? "Общее" : "Свое"}
                              </span>
                            </div>
                            <div className={styles.cellInputWrap}>
                              <input
                                className={`input ${styles.cellInput}`}
                                value={draft ?? (value == null ? "" : String(value))}
                                onChange={(e) => setLogisticsCellDraftByKey(cellKey, e.target.value)}
                                onBlur={() => commitLogisticsCell(selectedRow, field)}
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
                          </div>
                        );
                      })}
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Вес объемный, кг</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.volumetric_weight_kg)}</div>
                      </div>
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Макс. вес, кг</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.max_weight_kg)}</div>
                      </div>
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Стоимость за кг, {moneySign}</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.cost_per_kg)}</div>
                      </div>
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Обработка, {moneySign}</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.handling_cost_display)}</div>
                      </div>
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Доставка до клиента, {moneySign}</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.delivery_to_client_cost)}</div>
                      </div>
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Обработка возврата, {moneySign}</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.return_processing_cost)}</div>
                      </div>
                      <div className={styles.logisticsMetricCard}>
                        <div className={styles.categoryEditorFieldLabel}>Утилизация, {moneySign}</div>
                        <div className={styles.logisticsReadOnlyValue}>{fmtCell(selectedRow.disposal_cost)}</div>
                      </div>
                    </div>
                  </>
                ) : (
                  <div className={styles.emptyState}>Выбери SKU слева, чтобы редактировать габариты и смотреть расчёт логистики.</div>
                )}
              </div>
            </div>
          )}
        </>
      ) : null}
    </SectionBlock>
  );
}
