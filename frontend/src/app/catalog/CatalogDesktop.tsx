import { WorkspaceSurface } from "../../components/page/WorkspaceKit";
import styles from "./CatalogPage.module.css";
import type { CatalogController } from "./CatalogRendererTypes";
import layoutStyles from "../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../_shared/WorkspacePageHero";
import { WorkspacePageFrame } from "../_shared/WorkspacePageFrame";

type Props = {
  controller: CatalogController;
  treeSelector: React.ReactNode;
  table: React.ReactNode;
};

export function CatalogDesktop({ controller, treeSelector, table }: Props) {
  const {
    error,
    tab,
    setTab,
    searchDraft,
    setSearchDraft,
    page,
    setPage,
    pageSize,
    setPageSize,
    selectedTreePath,
    flatTree,
    totalCount,
    totalPages,
    tableLoading,
    activeStoreLabel,
    expandedSize,
    isExpanded,
    onToggleExpand,
    onToggleExpandAll,
    onToggleTree,
    tabItems,
  } = controller;

  return (
    <WorkspacePageFrame className={styles.catalogPageCard}>
      <div className={styles.catalogShell}>
        <WorkspacePageHero
          title="Каталог"
          subtitle="Чистый product-workspace для фильтрации, навигации по древу и работы с таблицей товаров."
          tabs={{
            items: tabItems.map((item) => ({
              id: item.id,
              label: item.label,
              meta: "badge" in item ? item.badge : undefined,
            })),
            activeId: tab,
            onChange: setTab,
          }}
          meta={
            <div className={layoutStyles.heroMeta}>
              <span className={layoutStyles.metaChip}>{tab === "all" ? "Все магазины" : activeStoreLabel}</span>
              <span className={layoutStyles.metaChip}>{tableLoading ? "Обновление..." : `Всего: ${totalCount}`}</span>
            </div>
          }
        />

        {error ? <div className="status error">{error}</div> : null}

        <div className={styles.catalogWorkspace}>
          <WorkspaceSurface className={styles.catalogSidebarSurface}>
            <div className={styles.catalogSidebarBlock}>
              {treeSelector}
            </div>
            <div className={styles.catalogSidebarHeader}>
              <div>
                <div className={styles.catalogSidebarTitle}>Древо каталога</div>
                <div className={styles.catalogSidebarMeta}>{tab === "all" ? "Сводный список товаров" : activeStoreLabel}</div>
              </div>
              <button type="button" className="btn ghost" onClick={onToggleExpandAll}>
                {expandedSize ? "Свернуть все" : "Развернуть все"}
              </button>
            </div>
            <div className={styles.catalogTreeList}>
              {flatTree.length === 0 ? (
                <div className={styles.catalogTreeEmpty}>{tableLoading ? "Загрузка..." : "Нет данных для дерева"}</div>
              ) : (
                flatTree.map((node) => {
                  const selected = selectedTreePath === node.path;
                  return (
                    <div
                      key={node.path}
                      className={styles.catalogTreeRow}
                      style={{ "--catalog-tree-indent": `${node.depth * 16}px` } as React.CSSProperties}
                    >
                      {node.hasChildren ? (
                        <button
                          type="button"
                          className={styles.catalogTreeExpand}
                          onClick={() => onToggleExpand(node.path)}
                        >
                          {isExpanded(node.path) ? "−" : "+"}
                        </button>
                      ) : (
                        <span className={styles.catalogTreeDot}>•</span>
                      )}
                      <button
                        type="button"
                        className={`${styles.catalogTreeNode} ${selected ? styles.catalogTreeNodeActive : ""}`.trim()}
                        onClick={() => onToggleTree(node.path)}
                        title={node.path}
                      >
                        {node.name}
                      </button>
                    </div>
                  );
                })
              )}
            </div>
          </WorkspaceSurface>

          <WorkspaceSurface className={styles.catalogTableSurface}>
            <div className={styles.catalogToolbar}>
              <div className={styles.catalogSearchBlock}>
                <label className={styles.catalogFieldLabel} htmlFor="catalog-desktop-search">Поиск по SKU</label>
                <input
                  id="catalog-desktop-search"
                  className={`input input-size-xl ${styles.catalogSearchInput}`}
                  value={searchDraft}
                  onChange={(e) => setSearchDraft(e.target.value)}
                  placeholder="Поиск по SKU или наименованию"
                />
              </div>
              <div className={styles.catalogToolbarMeta}>
                <div className={styles.catalogToolbarChipRow}>
                  {selectedTreePath ? <span className={styles.catalogMetaChip}>{selectedTreePath}</span> : <span className={styles.catalogMetaChip}>Весь каталог</span>}
                </div>
                <div className={styles.catalogToolbarActions}>
                  <div className={styles.catalogPageSizeBlock}>
                    <label className={styles.catalogFieldLabel} htmlFor="catalog-desktop-page-size">На странице</label>
                    <select
                      id="catalog-desktop-page-size"
                      className={`input input-size-sm ${styles.catalogPageSizeSelect}`}
                      value={pageSize}
                      onChange={(e) => {
                        setPage(1);
                        setPageSize(Number(e.target.value) || 50);
                      }}
                    >
                      {[25, 50, 100, 200, -1].map((n) => (
                        <option key={n} value={n}>{n < 0 ? "Все" : n}</option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>
            </div>

            <div className={styles.catalogTableWrap}>{table}</div>

            <div className={styles.catalogPager}>
              <div className={styles.catalogPagerMeta}>Страница {page} / {totalPages}</div>
              <div className={styles.catalogPagerActions}>
                <button type="button" className="btn ghost" onClick={() => setPage((current) => Math.max(1, current - 1))} disabled={page <= 1}>
                  Назад
                </button>
                <button type="button" className="btn ghost" onClick={() => setPage((current) => Math.min(totalPages, current + 1))} disabled={page >= totalPages}>
                  Дальше
                </button>
              </div>
            </div>
          </WorkspaceSurface>
        </div>
      </div>
    </WorkspacePageFrame>
  );
}
