import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { CatalogBrowser } from "../../../../components/page/CatalogBrowser";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { EditableFieldKey, PricingCategoryRow, PricingTableColumn } from "../types";
import type { TreeNode } from "../../../_shared/catalogState";

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

function collectExpandableNodeIds(nodes: CategoryTreeNode[]): string[] {
  const ids: string[] = [];
  const walk = (items: CategoryTreeNode[]) => {
    for (const node of items) {
      if (node.children.length) {
        ids.push(node.id);
        walk(node.children);
      }
    }
  };
  walk(nodes);
  return ids;
}

function toCatalogRoots(nodes: CategoryTreeNode[]): TreeNode[] {
  return nodes.map((node) => ({
    name: node.label,
    children: toCatalogRoots(node.children),
  }));
}

function findNodeById(nodes: CategoryTreeNode[], id: string): CategoryTreeNode | null {
  for (const node of nodes) {
    if (node.id === id) return node;
    const nested = findNodeById(node.children, id);
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
  mobileMode?: boolean;
  mobileCatalogOpen?: boolean;
  onOpenMobileCatalog?: () => void;
  onCloseMobileCatalog?: () => void;
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
  mobileMode = false,
  mobileCatalogOpen = false,
  onOpenMobileCatalog,
  onCloseMobileCatalog,
}: Props) {
  const [mounted, setMounted] = useState(false);
  const [selectedKey, setSelectedKey] = useState("");
  const [expandedPaths, setExpandedPaths] = useState<string[]>([]);
  const [treeQuery, setTreeQuery] = useState("");
  const categoryTree = buildCategoryTree(categoryRows);
  const catalogRoots = toCatalogRoots(categoryTree);
  const expandableNodeIds = collectExpandableNodeIds(categoryTree);
  const fallbackRow = findFirstLeaf(categoryTree);
  const selectedRow = categoryRows.find((row) => row.key === selectedKey) ?? fallbackRow;
  const inputColumns = tableColumns.filter((col) => col.kind === "input" && col.field);

  useEffect(() => {
    setMounted(true);
    return () => setMounted(false);
  }, []);

  useEffect(() => {
    if (!mounted || !mobileMode || typeof document === "undefined") return;
    const previousOverflow = document.body.style.overflow;
    if (mobileCatalogOpen) {
      document.body.style.overflow = "hidden";
      document.body.classList.add("pricing-mobile-catalog-open");
    }
    return () => {
      document.body.style.overflow = previousOverflow;
      document.body.classList.remove("pricing-mobile-catalog-open");
    };
  }, [mounted, mobileMode, mobileCatalogOpen]);

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

  function toggleAllNodes() {
    setExpandedPaths((current) => (current.length ? [] : expandableNodeIds));
  }

  function rowHasOverrides(row: PricingCategoryRow | null) {
    if (!row) return false;
    return inputColumns.some((column) => {
      const field = column.field;
      return field ? row.values[field] != null : false;
    });
  }

  function renderSidebarContent() {
    const selectedPath = selectedRow?.leafPath || "";
    return (
      <CatalogBrowser
        title="Каталог категорий"
        subtitle="Выбери нужную ветку и редактируй только её параметры."
        roots={catalogRoots}
        selectedPath={selectedPath}
        expandedPaths={expandedPaths}
        query={treeQuery}
        onQueryChange={setTreeQuery}
        onToggleExpand={toggleNode}
        onToggleExpandAll={toggleAllNodes}
        onSelectPath={(path) => {
          const node = findNodeById(categoryTree, path);
          if (!node) return;
          const nextRow = node.row ?? findFirstLeaf(node.children);
          if (nextRow) {
            setSelectedKey(nextRow.key);
            onCloseMobileCatalog?.();
          } else if (node.children.length) {
            toggleNode(node.id);
          }
        }}
        getNodeMeta={(path) => {
          const node = findNodeById(categoryTree, path);
          if (!node) return {};
          const selectableRow = node.row;
          const hasOverrides = rowHasOverrides(selectableRow);
          return {
            secondary: selectableRow ? `${selectableRow.itemsCount} SKU` : `${node.children.length} веток`,
            badge: hasOverrides ? "Свои настройки" : "Общие настройки",
            badgeTone: hasOverrides ? "success" : "muted",
          };
        }}
      />
    );
  }

  return (
    <>
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
                  <div className={`${styles.categoryWorkspace} ${mobileMode ? styles.categoryWorkspaceMobile : ""}`}>
                    {!mobileMode ? (
                      <aside className={styles.categorySidebar}>
                        {renderSidebarContent()}
                      </aside>
                    ) : null}

                    <div className={`${styles.categoryEditor} ${mobileMode ? styles.categoryEditorMobile : ""}`}>
                      <div className={styles.categoryEditorRail}>
                        <div className={styles.categoryEditorHead}>
                          <div>
                            <div className={styles.categoryEditorEyebrow}>Рабочая категория</div>
                            <h3 className={styles.categoryEditorTitle}>
                              {selectedRow.subcategoryLevels.at(-1) || selectedRow.category || "-"}
                            </h3>
                            <div className={styles.categoryEditorPath}>
                              {selectedRow.subcategoryLevels.length
                                ? [selectedRow.category, ...selectedRow.subcategoryLevels].filter(Boolean).join(" / ")
                                : "Корневая категория"}
                            </div>
                          </div>
                          <div className={styles.categoryEditorMeta}>
                            <span className={styles.categoryEditorMetaChip}>{selectedRow.itemsCount} SKU</span>
                            <span className={styles.categoryEditorMetaChip}>
                              {rowHasOverrides(selectedRow) ? "Свои правила" : "Общие правила"}
                            </span>
                            {mobileMode ? (
                              <button type="button" className="btn ghost" onClick={() => onOpenMobileCatalog?.()}>
                                Каталог
                              </button>
                            ) : null}
                          </div>
                        </div>

                        <div className={styles.categoryEditorSummary}>
                          <div className={styles.categoryEditorSummaryTitle}>Параметры расчета</div>
                          <div className={styles.categoryEditorSummaryText}>
                            Изменяй только активную ветку. Пустое значение наследует общие правила выше по дереву.
                          </div>
                        </div>
                      </div>

                      <div className={`${styles.categoryEditorGrid} ${mobileMode ? styles.categoryEditorGridMobile : ""}`}>
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
                                  className={`input input-size-md ${styles.cellInput}`}
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
              </>
            )}
          </>
        ) : null}
    </SectionBlock>
    {mounted && mobileCatalogOpen
      ? createPortal(
          <div className={styles.mobileSheetBackdrop} onClick={() => onCloseMobileCatalog?.()}>
            <div className={styles.mobileSheet} onClick={(e) => e.stopPropagation()}>
              <div className={styles.mobileSheetHead}>
                <div className={styles.mobileSheetTitle}>Каталог</div>
                <button type="button" className="btn ghost" onClick={() => onCloseMobileCatalog?.()}>
                  Закрыть
                </button>
              </div>
              <aside className={`${styles.categorySidebar} ${styles.categorySidebarMobile}`}>
                {renderSidebarContent()}
              </aside>
            </div>
          </div>,
          document.body,
        )
      : null}
    </>
  );
}
