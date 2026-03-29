import { ReactNode, useEffect, useState } from "react";
import styles from "./PricingCatalogFrame.module.css";
import { PageFrame, PageSectionTitle } from "../../../components/page/PageKit";

export type PricingTreeNodeFlat = {
  path: string;
  name: string;
  depth: number;
  hasChildren: boolean;
};

type Props = {
  title: string;
  subtitle: string;
  tabs: ReactNode;
  tabsRight?: ReactNode;
  summaryPanel?: ReactNode;
  searchValue: string;
  onSearchChange: (value: string) => void;
  searchPlaceholder?: string;
  hideSearchPanel?: boolean;
  error?: string;
  treeSelector?: ReactNode;
  treeMeta: string;
  flatTree: PricingTreeNodeFlat[];
  selectedTreePath: string;
  expandedSize: number;
  isExpanded: (path: string) => boolean;
  onToggleExpandAll: () => void;
  onToggleExpand: (path: string) => void;
  onToggleTree: (path: string) => void;
  treeLoadingText: string;
  tableTitle?: string;
  tableTitleControls?: ReactNode;
  tableMeta: ReactNode;
  table: ReactNode;
  page: number;
  totalPages: number;
  onPageChange?: (value: number) => void;
  onPrevPage: () => void;
  onNextPage: () => void;
  canPrev: boolean;
  canNext: boolean;
  pageSize: number;
  onPageSizeChange: (value: number) => void;
  pageSizeOptions?: number[];
};

export default function PricingCatalogFrame(props: Props) {
  const {
    title,
    subtitle,
    tabs,
    tabsRight,
    summaryPanel,
    searchValue,
    onSearchChange,
    searchPlaceholder = "Поиск по SKU или наименованию",
    hideSearchPanel = false,
    error,
    treeSelector,
    treeMeta,
    flatTree,
    selectedTreePath,
    expandedSize,
    isExpanded,
    onToggleExpandAll,
    onToggleExpand,
    onToggleTree,
    treeLoadingText,
    tableTitle,
    tableTitleControls,
    tableMeta,
    table,
    page,
    totalPages,
    onPageChange,
    onPrevPage,
    onNextPage,
    canPrev,
    canNext,
    pageSize,
    onPageSizeChange,
    pageSizeOptions = [25, 50, 100, 200, -1],
  } = props;
  const [pageInput, setPageInput] = useState(String(page));

  useEffect(() => {
    setPageInput(String(page));
  }, [page]);

  function commitPageInput() {
    if (!onPageChange) {
      setPageInput(String(page));
      return;
    }
    const next = Math.max(1, Math.min(totalPages, Number(pageInput) || page));
    setPageInput(String(next));
    if (next !== page) onPageChange(next);
  }

  return (
    <PageFrame
      title={title}
      subtitle={subtitle}
      className={styles.pageCard}
    >
      {summaryPanel ? <div className={styles.summaryPanel}>{summaryPanel}</div> : null}

      {!hideSearchPanel ? (
        <div className={styles.controlPanel}>
          <div className={styles.searchRow}>
            <div className={styles.searchBlock}>
              <label className={styles.fieldLabel} htmlFor="pricing-frame-search">Поиск</label>
              <input
                id="pricing-frame-search"
                className={`input ${styles.searchInput}`}
                value={searchValue}
                onChange={(e) => onSearchChange(e.target.value)}
                placeholder={searchPlaceholder}
              />
            </div>
          </div>
        </div>
      ) : null}

      {error ? <div className={styles.errorBox}>{error}</div> : null}

      <div className={styles.layout}>
        <aside className={`${styles.treePanel} ${treeSelector ? styles.treePanelAll : ""}`}>
          {treeSelector ? <div className={styles.treeSelectorWrap}>{treeSelector}</div> : null}
          <PageSectionTitle
            title="Древо каталога"
            actions={<button className="btn inline sm" onClick={onToggleExpandAll}>{expandedSize ? "Свернуть все" : "Развернуть все"}</button>}
          />
          <div className={styles.treeMeta}>{treeMeta}</div>
          <div className={styles.treeList}>
            {flatTree.length === 0 ? (
              <div className={styles.treeEmpty}>{treeLoadingText}</div>
            ) : (
              flatTree.map((node) => {
                const selected = selectedTreePath === node.path;
                return (
                  <div key={node.path} className={`${styles.treeRow} ${selected ? styles.treeRowSelected : ""}`} style={{ paddingLeft: 10 + node.depth * 18 }}>
                    {node.hasChildren ? (
                      <button className={styles.expandBtn} onClick={() => onToggleExpand(node.path)}>{isExpanded(node.path) ? "−" : "+"}</button>
                    ) : (
                      <span className={styles.expandStub} />
                    )}
                    <button className={styles.treeLabelBtn} onClick={() => onToggleTree(node.path)} title={node.path}>{node.name}</button>
                  </div>
                );
              })
            )}
          </div>
        </aside>

        <div className={styles.contentColumn}>
          <div className={styles.tabsBar}>
            <div className={styles.tabsRow}>{tabs}</div>
            {tabsRight ? <div className={styles.tabsRight}>{tabsRight}</div> : null}
          </div>

          <section className={styles.tablePanel}>
          {(tableTitle || tableMeta || tableTitleControls) ? (
            <div className={styles.tableHeaderRow}>
              {tableTitle ? (
                <div className={styles.tableHeaderMain}>
                  <PageSectionTitle title={tableTitle} meta={tableMeta} />
                </div>
              ) : null}
              {tableTitleControls ? <div className={styles.tableTitleControls}>{tableTitleControls}</div> : null}
            </div>
          ) : null}

          <div className={styles.tableWrap}>{table}</div>

          <div className={styles.paginationRow}>
            <div className={styles.pageLeft}>
              <span>На странице</span>
              <select className={`input ${styles.pageSizeSelect}`} value={pageSize} onChange={(e) => onPageSizeChange(Number(e.target.value) || 50)}>
                {pageSizeOptions.map((n) => (
                  <option key={n} value={n}>{n < 0 ? "Все" : n}</option>
                ))}
              </select>
            </div>

            <div className={styles.pageRightStack}>
              <div className={styles.pageTotals}>{tableMeta}</div>
              <div className={styles.pageRight}>
                <button className={styles.arrowBtn} onClick={onPrevPage} disabled={!canPrev} aria-label="Предыдущая страница">‹</button>
                <div className={styles.pageInfo}>
                  <input
                    className={`input ${styles.pageInput}`}
                    value={pageInput}
                    onChange={(e) => setPageInput(e.target.value.replace(/[^\d]/g, ""))}
                    onBlur={commitPageInput}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") commitPageInput();
                    }}
                    inputMode="numeric"
                    aria-label="Текущая страница"
                  />
                  <span>/ {totalPages}</span>
                </div>
                <button className={styles.arrowBtn} onClick={onNextPage} disabled={!canNext} aria-label="Следующая страница">›</button>
              </div>
            </div>
          </div>
          </section>
        </div>
      </div>
    </PageFrame>
  );
}
