"use client";

import { useState } from "react";
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
  const bulkColumn = bulkField ? tableColumns.find((col) => col.field === bulkField) ?? null : null;

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
              <div className={styles.pricingTableWrap}>
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
                      <tr key={row.key}>
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
