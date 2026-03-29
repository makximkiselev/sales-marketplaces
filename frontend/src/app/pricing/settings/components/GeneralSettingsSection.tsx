"use client";

import { useEffect, useState } from "react";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { EditableFieldKey, PricingCategoryRow, PricingTableColumn } from "../types";
import { BulkFillColumnModal } from "./BulkFillColumnModal";

type Props = {
  loading: boolean;
  error: string;
  itemsError: string;
  itemsLoading: boolean;
  categoryRows: PricingCategoryRow[];
  tableColumns: PricingTableColumn[];
  cellDrafts: Record<string, string>;
  cellSaving: Record<string, boolean>;
  getCellKey: (leafPath: string, field: EditableFieldKey) => string;
  defaultFieldValue: (field: EditableFieldKey) => string;
  formatNum: (value: number | null | undefined) => string;
  queueSaveCell: (row: PricingCategoryRow, field: EditableFieldKey, rawValue: string) => void;
  flushSaveCell: (row: PricingCategoryRow, field: EditableFieldKey, rawValue?: string) => void;
  applyColumnValue: (field: EditableFieldKey, rawValue: string) => Promise<void> | void;
};

export function GeneralSettingsSection({
  loading,
  error,
  itemsError,
  itemsLoading,
  categoryRows,
  tableColumns,
  cellDrafts,
  cellSaving,
  getCellKey,
  defaultFieldValue,
  formatNum,
  queueSaveCell,
  flushSaveCell,
  applyColumnValue,
}: Props) {
  const [bulkField, setBulkField] = useState<EditableFieldKey | null>(null);
  const [selectedKey, setSelectedKey] = useState("");
  const bulkColumn = bulkField ? tableColumns.find((col) => col.field === bulkField) ?? null : null;
  const selectedRow = categoryRows.find((row) => row.key === selectedKey) ?? categoryRows[0] ?? null;
  const inputColumns = tableColumns.filter((col) => col.kind === "input" && col.field);

  useEffect(() => {
    if (!categoryRows.length) {
      setSelectedKey("");
      return;
    }
    if (!selectedKey || !categoryRows.some((row) => row.key === selectedKey)) {
      setSelectedKey(categoryRows[0].key);
    }
  }, [categoryRows, selectedKey]);

  return (
    <SectionBlock>
        {loading ? <div className="status">Загрузка контекста...</div> : null}
        {!loading && error ? <div className="status error">{error}</div> : null}
        {!loading && !error ? (
          <>
            {itemsError ? <div className="status error">{itemsError}</div> : null}
            {itemsLoading ? (
              <div className="status">Загрузка категорий...</div>
            ) : (
              <>
                {selectedRow ? (
                  <div className={styles.categoryWorkspace}>
                    <aside className={styles.categorySidebar}>
                      <div className={styles.categorySidebarHead}>
                        <div className={styles.categorySidebarTitle}>Категории</div>
                        <div className={styles.categorySidebarMeta}>{categoryRows.length} записей</div>
                      </div>
                      <div className={styles.categorySidebarList}>
                        {categoryRows.map((row) => (
                          <button
                            key={row.key}
                            type="button"
                            className={`${styles.categorySidebarItem} ${selectedRow.key === row.key ? styles.categorySidebarItemActive : ""}`}
                            onClick={() => setSelectedKey(row.key)}
                          >
                            <span className={styles.categorySidebarItemTitle}>{row.category || "-"}</span>
                            {row.subcategoryLevels.length ? (
                              <span className={styles.categorySidebarItemPath}>{row.subcategoryLevels.join(" / ")}</span>
                            ) : null}
                          </button>
                        ))}
                      </div>
                    </aside>

                    <div className={styles.categoryEditor}>
                      <div className={styles.categoryEditorHead}>
                        <div>
                          <div className={styles.categoryEditorEyebrow}>Редактор категории</div>
                          <h3 className={styles.categoryEditorTitle}>{selectedRow.category || "-"}</h3>
                          <div className={styles.categoryEditorPath}>
                            {selectedRow.subcategoryLevels.length ? selectedRow.subcategoryLevels.join(" / ") : "Корневая категория"}
                          </div>
                        </div>
                        <div className={styles.categoryEditorMeta}>
                          <span className={styles.categoryEditorMetaChip}>{selectedRow.itemsCount} SKU</span>
                        </div>
                      </div>

                      <div className={styles.categoryEditorGrid}>
                        {inputColumns.map((col) => {
                          const field = col.field!;
                          const cellKey = getCellKey(selectedRow.leafPath || selectedRow.key, field);
                          const baseVal = selectedRow.values[field];
                          const fallbackVal = baseVal == null ? defaultFieldValue(field) : "";
                          const value = cellDrafts[cellKey] ?? (baseVal == null ? fallbackVal : formatNum(baseVal));
                          const inherited = baseVal == null;
                          return (
                            <label key={col.id} className={styles.categoryEditorField}>
                              <span className={styles.categoryEditorFieldHead}>
                                <span className={styles.categoryEditorFieldLabel}>{col.label}</span>
                                <span className={`${styles.categoryEditorFieldBadge} ${inherited ? styles.categoryEditorFieldBadgeInherited : styles.categoryEditorFieldBadgeCustom}`}>
                                  {inherited ? "Наследуется" : "Переопределено"}
                                </span>
                              </span>
                              <div className={styles.cellInputWrap}>
                                <input
                                  className={`input ${styles.cellInput}`}
                                  value={value}
                                  onChange={(e) => queueSaveCell(selectedRow, field, e.target.value)}
                                  onBlur={(e) => flushSaveCell(selectedRow, field, e.target.value)}
                                  inputMode="decimal"
                                />
                                {cellSaving[cellKey] ? <span className={styles.cellSavingDot} /> : null}
                              </div>
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                ) : null}

                <div className={`${styles.pricingTableWrap} ${styles.pricingDesktopTableWrap}`}>
                  <table className={styles.pricingTable}>
                    <thead>
                      <tr>
                        {tableColumns.map((col) => (
                          <th key={col.id}>
                            <div className={styles.tableHeaderCell}>
                              <span>{col.label}</span>
                              {col.field === "commission_percent" ? (
                                <button
                                  type="button"
                                  className={styles.columnActionButton}
                                  onClick={() => setBulkField("commission_percent")}
                                >
                                  Заполнить всем
                                </button>
                              ) : null}
                            </div>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {categoryRows.map((row) => (
                        <tr key={row.key} className={selectedRow?.key === row.key ? styles.pricingTableRowActive : ""}>
                          {tableColumns.map((col) => {
                            if (col.id === "category") return <td key={col.id} className={styles.colText}>{row.category || "-"}</td>;
                            if (col.id.startsWith("subcategory_")) {
                              const idx = col.subIndex ?? 0;
                              return <td key={col.id} className={styles.colText}>{row.subcategoryLevels[idx] || "-"}</td>;
                            }
                            const field = col.field;
                            if (col.kind === "input" && field) {
                              const cellKey = getCellKey(row.leafPath || row.key, field);
                              const baseVal = row.values[field];
                              const fallbackVal = baseVal == null ? defaultFieldValue(field) : "";
                              const value = cellDrafts[cellKey] ?? (baseVal == null ? fallbackVal : formatNum(baseVal));
                              return (
                                <td key={col.id}>
                                  <div className={styles.cellInputWrap}>
                                    <input
                                      className={`input ${styles.cellInput}`}
                                      value={value}
                                      onChange={(e) => queueSaveCell(row, field, e.target.value)}
                                      onBlur={(e) => flushSaveCell(row, field, e.target.value)}
                                      inputMode="decimal"
                                    />
                                    {cellSaving[cellKey] ? <span className={styles.cellSavingDot} /> : null}
                                  </div>
                                </td>
                              );
                            }
                            return <td key={col.id}><span className={styles.placeholderDash}>-</span></td>;
                          })}
                        </tr>
                      ))}
                      {!categoryRows.length ? (
                        <tr>
                          <td colSpan={tableColumns.length}>Нет загруженных категорий для выбранного магазина.</td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
                <div className={styles.pricingMobileList}>
                  {categoryRows.map((row) => (
                    <article key={row.key} className={styles.pricingMobileCard}>
                      <div className={styles.pricingMobileCardHead}>
                        <div className={styles.pricingMobileCardTitle}>{row.category || "-"}</div>
                        {row.subcategoryLevels.length ? (
                          <div className={styles.pricingMobileCardPath}>
                            {row.subcategoryLevels.join(" / ")}
                          </div>
                        ) : null}
                      </div>
                      <div className={styles.pricingMobileFields}>
                        {tableColumns.map((col) => {
                          if (col.kind !== "input" || !col.field) return null;
                          const field = col.field;
                          const cellKey = getCellKey(row.leafPath || row.key, field);
                          const baseVal = row.values[field];
                          const fallbackVal = baseVal == null ? defaultFieldValue(field) : "";
                          const value = cellDrafts[cellKey] ?? (baseVal == null ? fallbackVal : formatNum(baseVal));
                          return (
                            <label key={col.id} className={styles.pricingMobileField}>
                              <span className={styles.pricingMobileFieldLabel}>{col.label}</span>
                              <div className={styles.cellInputWrap}>
                                <input
                                  className={`input ${styles.cellInput}`}
                                  value={value}
                                  onChange={(e) => queueSaveCell(row, field, e.target.value)}
                                  onBlur={(e) => flushSaveCell(row, field, e.target.value)}
                                  inputMode="decimal"
                                />
                                {cellSaving[cellKey] ? <span className={styles.cellSavingDot} /> : null}
                              </div>
                            </label>
                          );
                        })}
                      </div>
                    </article>
                  ))}
                  {!categoryRows.length ? (
                    <div className={styles.emptyState}>Нет загруженных категорий для выбранного магазина.</div>
                  ) : null}
                </div>
              </>
            )}
          </>
        ) : null}
        {bulkField && bulkColumn?.field ? (
          <BulkFillColumnModal
            field={bulkColumn.field}
            label={bulkColumn.label}
            onClose={() => setBulkField(null)}
            onConfirm={async (value) => {
              await applyColumnValue(bulkColumn.field!, value);
              setBulkField(null);
            }}
          />
        ) : null}
    </SectionBlock>
  );
}
