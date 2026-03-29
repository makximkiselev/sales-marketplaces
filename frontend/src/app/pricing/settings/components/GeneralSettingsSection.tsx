import { useEffect, useState } from "react";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { EditableFieldKey, PricingCategoryRow, PricingTableColumn } from "../types";

type CategoryTreeNode = {
  id: string;
  label: string;
  depth: number;
  pathLabel: string;
  row: PricingCategoryRow | null;
  children: CategoryTreeNode[];
};

function buildCategoryTree(rows: PricingCategoryRow[]): CategoryTreeNode[] {
  const root: CategoryTreeNode[] = [];

  for (const row of rows) {
    const segments = [row.category, ...row.subcategoryLevels].filter(Boolean);
    let level = root;
    let currentPath = "";

    segments.forEach((segment, index) => {
      currentPath = currentPath ? `${currentPath} / ${segment}` : segment;
      let node = level.find((candidate) => candidate.id === currentPath);
      if (!node) {
        node = {
          id: currentPath,
          label: segment,
          depth: index,
          pathLabel: currentPath,
          row: null,
          children: [],
        };
        level.push(node);
      }
      if (index === segments.length - 1) {
        node.row = row;
      }
      level = node.children;
    });
  }

  const normalize = (nodes: CategoryTreeNode[]): CategoryTreeNode[] =>
    nodes
      .map((node) => ({
        ...node,
        children: normalize(node.children).sort((a, b) => a.label.localeCompare(b.label, "ru")),
      }))
      .sort((a, b) => a.label.localeCompare(b.label, "ru"));
  return normalize(root);
}

function collectExpandedPathIds(row: PricingCategoryRow | null): string[] {
  if (!row) return [];
  const segments = [row.category, ...row.subcategoryLevels].filter(Boolean);
  const ids: string[] = [];
  let path = "";
  for (const segment of segments) {
    path = path ? `${path} / ${segment}` : segment;
    ids.push(path);
  }
  return ids;
}

function findFirstLeaf(nodes: CategoryTreeNode[]): PricingCategoryRow | null {
  for (const node of nodes) {
    if (node.row) return node.row;
    const nested = findFirstLeaf(node.children);
    if (nested) return nested;
  }
  return null;
}

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
}: Props) {
  const [selectedKey, setSelectedKey] = useState("");
  const [expandedPaths, setExpandedPaths] = useState<string[]>([]);
  const [treeQuery, setTreeQuery] = useState("");
  const categoryTree = buildCategoryTree(categoryRows);
  const fallbackRow = findFirstLeaf(categoryTree);
  const selectedRow = categoryRows.find((row) => row.key === selectedKey) ?? fallbackRow;
  const inputColumns = tableColumns.filter((col) => col.kind === "input" && col.field);
  const normalizedTreeQuery = treeQuery.trim().toLowerCase();

  useEffect(() => {
    if (!categoryRows.length) {
      setSelectedKey("");
      setExpandedPaths([]);
      return;
    }
    if (!selectedKey || !categoryRows.some((row) => row.key === selectedKey)) {
      setSelectedKey(categoryRows[0].key);
    }
  }, [categoryRows, selectedKey]);

  useEffect(() => {
    if (!selectedRow) return;
    setExpandedPaths((current) => {
      const next = new Set(current);
      for (const id of collectExpandedPathIds(selectedRow)) {
        next.add(id);
      }
      return Array.from(next);
    });
  }, [selectedRow]);

  function toggleNode(pathId: string) {
    setExpandedPaths((current) =>
      current.includes(pathId) ? current.filter((id) => id !== pathId) : [...current, pathId],
    );
  }

  function rowHasOverrides(row: PricingCategoryRow | null) {
    if (!row) return false;
    return inputColumns.some((column) => {
      const field = column.field;
      return field ? row.values[field] != null : false;
    });
  }

  function filterTree(nodes: CategoryTreeNode[]): CategoryTreeNode[] {
    if (!normalizedTreeQuery) return nodes;
    const walk = (items: CategoryTreeNode[]): CategoryTreeNode[] =>
      items.flatMap((node) => {
        const filteredChildren = walk(node.children);
        const selfMatch = node.pathLabel.toLowerCase().includes(normalizedTreeQuery);
        if (!selfMatch && !filteredChildren.length) return [];
        return [{ ...node, children: filteredChildren }];
      });
    return walk(nodes);
  }

  function renderTree(nodes: CategoryTreeNode[]) {
    return nodes.map((node) => {
      const expanded = expandedPaths.includes(node.id);
      const selectableRow = node.row;
      const isSelected = selectableRow ? selectedRow?.key === selectableRow.key : false;
      const hasOverrides = rowHasOverrides(selectableRow);
      return (
        <div key={node.id} className={styles.categoryTreeNode}>
          <div
            className={`${styles.categoryTreeRow} ${isSelected ? styles.categoryTreeRowActive : ""}`}
            style={{ paddingLeft: `${12 + node.depth * 14}px` }}
          >
            <button
              type="button"
              className={`${styles.categoryTreeToggle} ${!node.children.length ? styles.categoryTreeToggleGhost : ""}`}
              onClick={() => {
                if (node.children.length) {
                  toggleNode(node.id);
                } else if (selectableRow) {
                  setSelectedKey(selectableRow.key);
                }
              }}
              aria-label={expanded ? "Свернуть категорию" : "Раскрыть категорию"}
            >
              {node.children.length ? (expanded ? "−" : "+") : "•"}
            </button>
            <button
              type="button"
              className={`${styles.categoryTreeSelect} ${isSelected ? styles.categoryTreeSelectActive : ""}`}
              onClick={() => {
                if (selectableRow) {
                  setSelectedKey(selectableRow.key);
                } else if (node.children.length) {
                  toggleNode(node.id);
                }
              }}
            >
              <span className={styles.categoryTreeLabel}>{node.label}</span>
              <span className={styles.categoryTreeMetaRow}>
                {selectableRow ? (
                  <span className={styles.categoryTreeMeta}>{selectableRow.itemsCount} SKU</span>
                ) : (
                  <span className={styles.categoryTreeMeta}>{node.children.length} веток</span>
                )}
                <span className={`${styles.categoryTreeStatus} ${hasOverrides ? styles.categoryTreeStatusCustom : styles.categoryTreeStatusInherited}`}>
                  {hasOverrides ? "Свои настройки" : "Общие настройки"}
                </span>
              </span>
            </button>
          </div>
          {node.children.length && expanded ? (
            <div className={styles.categoryTreeChildren}>{renderTree(node.children)}</div>
          ) : null}
        </div>
      );
    });
  }

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
                        <div>
                          <div className={styles.categorySidebarTitle}>Каталог категорий</div>
                          <div className={styles.categorySidebarMeta}>Раскрывай только нужную ветку, а не весь список сразу.</div>
                        </div>
                      </div>
                      <div className={styles.categorySidebarSearch}>
                        <input
                          className={`input ${styles.categorySidebarSearchInput}`}
                          value={treeQuery}
                          onChange={(e) => setTreeQuery(e.target.value)}
                          placeholder="Поиск по категории или ветке"
                        />
                      </div>
                      <div className={styles.categorySidebarList}>
                        {renderTree(filterTree(categoryTree))}
                      </div>
                    </aside>

                    <div className={styles.categoryEditor}>
                      <div className={styles.categoryEditorHead}>
                        <div>
                          <div className={styles.categoryEditorEyebrow}>Категория</div>
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
                                  {inherited ? "Общее" : "Свое"}
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
    </SectionBlock>
  );
}
