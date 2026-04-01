import { useMemo, useState } from "react";
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
  const [treeQuery, setTreeQuery] = useState("");
  const [expandedTreePaths, setExpandedTreePaths] = useState<string[]>([]);
  const [catalogOpen, setCatalogOpen] = useState(false);

  const editableFields: Array<{ field: LogisticsEditableFieldKey; label: string }> = [
    { field: "width_cm", label: "Ширина, см" },
    { field: "length_cm", label: "Длина, см" },
    { field: "height_cm", label: "Высота, см" },
    { field: "weight_kg", label: "Вес, кг" },
  ];
  const visibleSkuLabel = logisticsTreePath ? "SKU выбранной ветки" : "Все SKU магазина";
  const selectedBranchLabel = logisticsTreePath || "Весь каталог";

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
            <div className={`${styles.logisticsTablePane} ${styles.logisticsTablePaneFull}`}>
              <div className={styles.logisticsTableHead}>
                <div className={styles.logisticsTableIntro}>
                  <div className={styles.categorySidebarTitle}>Логистические затраты</div>
                  <div className={styles.categorySidebarMeta}>
                    Управляй размерами товара и смотри расчетные логистические издержки по raw-слою.
                  </div>
                </div>
                <div className={styles.logisticsTableInfo}>
                  <span className={styles.logisticsBranchChip}>{selectedBranchLabel}</span>
                  <span>{visibleSkuLabel}</span>
                </div>
              </div>

              <div className={styles.logisticsSummaryGrid}>
                <div className={styles.logisticsSummaryCard}>
                  <div className={styles.logisticsSummaryLabel}>Каталог</div>
                  <div className={styles.logisticsSummaryValue}>
                    {logisticsTreePath ? "Выбрана ветка" : "Весь каталог"}
                  </div>
                  <div className={styles.logisticsSummaryMeta}>{selectedBranchLabel}</div>
                </div>
                <div className={styles.logisticsSummaryCard}>
                  <div className={styles.logisticsSummaryLabel}>SKU в выборке</div>
                  <div className={styles.logisticsSummaryValue}>{logisticsTotal}</div>
                  <div className={styles.logisticsSummaryMeta}>{visibleSkuLabel}</div>
                </div>
              </div>

              <div className={styles.logisticsFilterBar}>
                <button
                  type="button"
                  className={`${styles.logisticsCatalogLauncher} ${catalogOpen ? styles.logisticsCatalogLauncherActive : ""}`}
                  onClick={() => setCatalogOpen((current) => !current)}
                >
                  <span className={styles.logisticsCatalogLauncherTitle}>
                    {catalogOpen ? "Каталог открыт" : "Открыть каталог"}
                  </span>
                  <span className={styles.logisticsCatalogLauncherMeta}>{selectedBranchLabel}</span>
                </button>
                <ControlField label="Поиск по SKU" className={styles.logisticsSearchField}>
                  <div className={styles.inputWithSuffix}>
                    <input
                      className={`input input-size-fluid ${styles.settingInput}`}
                      value={logisticsSearch}
                      onChange={(e) => {
                        setLogisticsSearch(e.target.value);
                        setLogisticsPage(1);
                      }}
                      placeholder="Поиск по SKU или наименованию"
                    />
                  </div>
                </ControlField>
                <div className={styles.logisticsToolbarMeta}>
                  <div className={styles.logisticsToolbarChipRow}>
                    <span className={styles.logisticsBranchChip}>{selectedBranchLabel}</span>
                    {logisticsTreePath ? (
                      <button
                        type="button"
                        className={`btn ghost ${styles.logisticsResetButton}`}
                        onClick={() => {
                          setLogisticsTreePath("");
                          setLogisticsPage(1);
                        }}
                      >
                        Весь каталог
                      </button>
                    ) : null}
                  </div>
                  <ControlField label="На странице" className={styles.logisticsPageSizeBox}>
                    <select
                      className={`input input-size-sm ${styles.logisticsPageSizeSelect}`}
                      value={String(logisticsPageSize)}
                      onChange={(e) => {
                        setLogisticsPageSize(Number(e.target.value));
                        setLogisticsPage(1);
                      }}
                    >
                      {logisticsPageSizeOptions.map((n) => <option key={n} value={n}>{n}</option>)}
                    </select>
                  </ControlField>
                </div>
                <button
                  type="button"
                  className={`btn ${styles.logisticsImportButton}`}
                  onClick={() => setLogisticsImportOpen(true)}
                  disabled={!activeStoreId || (activePlatform !== "yandex_market" && activePlatform !== "ozon")}
                >
                  Импорт размеров
                </button>
              </div>

              {catalogOpen ? (
                <div className={styles.logisticsCatalogInline}>
                  <div className={styles.logisticsCatalogInlineHead}>
                    <div>
                      <div className={styles.categorySidebarTitle}>Каталог</div>
                      <div className={styles.categorySidebarMeta}>
                        Выбери ветку каталога, чтобы отфильтровать таблицу логистики.
                      </div>
                    </div>
                    {logisticsTreePath ? (
                      <button
                        type="button"
                        className={`btn ghost ${styles.logisticsResetButton}`}
                        onClick={() => {
                          setLogisticsTreePath("");
                          setLogisticsPage(1);
                        }}
                      >
                        Сбросить ветку
                      </button>
                    ) : null}
                  </div>
                  <div className={styles.logisticsCatalogWorkspace}>
                    <CatalogBrowser
                      title="Каталог"
                      subtitle="Выбери ветку каталога, чтобы сузить таблицу товаров."
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
                      actionLabel={expandedTreePaths.length ? "Свернуть все" : "Развернуть все"}
                    />
                    <div className={styles.logisticsCatalogSelection}>
                      <div className={styles.logisticsCatalogSelectionLabel}>Текущий выбор</div>
                      <div className={styles.logisticsCatalogSelectionValue}>{selectedBranchLabel}</div>
                      <div className={styles.logisticsCatalogSelectionHint}>{visibleSkuLabel}</div>
                    </div>
                  </div>
                </div>
              ) : null}

              <div className={`${styles.pricingTableWrap} ${styles.logisticsTableWrap}`}>
                <table className={`${styles.pricingTable} ${styles.logisticsTable}`}>
                    <thead>
                      <tr>
                        <th>SKU</th>
                        <th>Товар</th>
                        {editableFields.map(({ label }) => <th key={label}>{label}</th>)}
                        <th>Вес объёмный, кг</th>
                        <th>Макс. вес, кг</th>
                        <th>Стоимость за кг, {moneySign}</th>
                        <th>Обработка, {moneySign}</th>
                        <th>Доставка, {moneySign}</th>
                        <th>Возврат, {moneySign}</th>
                        <th>Утилизация, {moneySign}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {logisticsRows.length === 0 ? (
                        <tr>
                          <td colSpan={11} className={styles.emptyCell}>
                            {logisticsTreePath
                              ? "В выбранной ветке пока нет товаров raw-слоя."
                              : "Нет товаров в raw-слое выбранного магазина."}
                          </td>
                        </tr>
                      ) : logisticsRows.map((rawRow) => {
                        const row = toLiveLogisticsRow(rawRow);
                        return (
                          <tr key={row.sku}>
                            <td>{row.sku}</td>
                            <td className={styles.logisticsNameCell}>
                              <div className={styles.logisticsNameWrap}>
                                <div className={styles.logisticsProductName}>{row.name || "Без названия"}</div>
                                <div className={styles.logisticsRowMeta}>
                                  <span className={row.dimensions_inherited ? styles.categoryTreeStatusInherited : styles.categoryTreeStatusCustom}>
                                    {row.dimensions_inherited ? "Общие" : "Свои"}
                                  </span>
                                  {Array.isArray(row.tree_path) && row.tree_path.length ? (
                                    <span className={styles.logisticsPathHint}>{row.tree_path.join(" / ")}</span>
                                  ) : null}
                                </div>
                              </div>
                            </td>
                            {editableFields.map(({ field }) => {
                              const cellKey = getLogisticsCellKey(row.sku, field);
                              const draft = logisticsCellDrafts[cellKey];
                              const value = row[field];
                              return (
                                <td key={cellKey}>
                                  <div className={styles.cellInputWrap}>
                                    <input
                                      className={`input input-size-sm ${styles.cellInput}`}
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
                    </tbody>
                </table>
              </div>

              <div className={styles.logisticsPager}>
                <div className={styles.logisticsPagerMeta}>
                  <div className={styles.inlineInfo}>Страница {logisticsPage} / {totalPages}</div>
                  <div className={styles.logisticsPagerHint}>{logisticsTotal} SKU в выборке</div>
                </div>
                <div className={styles.logisticsPagerActions}>
                  <button type="button" className="btn ghost" onClick={() => setLogisticsPage((p) => Math.max(1, p - 1))} disabled={logisticsPage <= 1}>Назад</button>
                  <button type="button" className="btn ghost" onClick={() => setLogisticsPage((p) => p + 1)} disabled={logisticsPage >= totalPages}>Вперед</button>
                </div>
              </div>
            </div>
          )}
        </>
      ) : null}
    </SectionBlock>
  );
}
