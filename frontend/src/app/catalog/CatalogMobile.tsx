import { PageFrame } from "../../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceTabs } from "../../components/page/WorkspaceKit";
import styles from "./CatalogPage.module.css";
import type { CatalogController } from "./CatalogRendererTypes";

type Props = {
  controller: CatalogController;
  treeSelector: React.ReactNode;
};

export function CatalogMobile({ controller, treeSelector }: Props) {
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
    rows,
    totalCount,
    totalPages,
    tableLoading,
    tabItems,
  } = controller;

  return (
    <PageFrame
      title="Каталог"
      subtitle="Мобильный конструктор каталога без горизонтального скролла."
      className={styles.mobilePageCard}
    >
      <div className={styles.mobileCatalogShell}>
        <WorkspaceSurface className={styles.mobileCatalogHero}>
          <WorkspaceTabs
            className={styles.mobileCatalogTabs}
            items={tabItems.map((item) => ({ id: item.id, label: item.label, meta: "badge" in item ? item.badge : undefined }))}
            activeId={tab}
            onChange={setTab}
          />
          <WorkspaceHeader
            title="Каталог"
            subtitle="Все фильтры и карточки товаров собраны в один мобильный workspace."
            meta={<span className={styles.catalogMetaChip}>{tableLoading ? "Обновление..." : `Всего: ${totalCount}`}</span>}
          />
          <div className={styles.mobileCatalogControls}>
            <div className={styles.treeSourcePanel}>{treeSelector}</div>
            <div className={styles.mobileSearchBlock}>
              <label className={styles.catalogFieldLabel} htmlFor="catalog-mobile-search">Поиск</label>
              <input
                id="catalog-mobile-search"
                className={`input input-size-fluid ${styles.mobileSearchInput}`}
                value={searchDraft}
                onChange={(e) => setSearchDraft(e.target.value)}
                placeholder="Поиск по SKU или наименованию"
              />
            </div>
          </div>
        </WorkspaceSurface>

        {error ? <div className="status error">{error}</div> : null}

        <div className={styles.mobileCatalogMetaRow}>
          <div className={styles.mobileCatalogMeta}>
            {tableLoading ? "Обновление..." : `Всего: ${totalCount}`}
          </div>
          {selectedTreePath ? <div className={styles.mobileCatalogActivePath}>{selectedTreePath}</div> : null}
        </div>

        <div className={styles.mobileCatalogCards}>
          {rows.length === 0 ? (
            <div className={styles.placeholder}>{tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}</div>
          ) : (
            rows.map((row) => (
              <article key={row.sku} className={styles.mobileCatalogCard}>
                <div className={styles.mobileCatalogCardHead}>
                  <div>
                    <div className={styles.mobileCatalogSku}>{row.sku}</div>
                    <div className={styles.mobileCatalogUpdated}>
                      Обновлено: {row.updated_at ? new Date(row.updated_at).toLocaleString("ru-RU") : "—"}
                    </div>
                  </div>
                </div>
                <div className={styles.mobileCatalogName}>{row.name || "—"}</div>
                <div className={styles.mobileCatalogPath}>{(row.tree_path || []).join(" / ") || "Не определено"}</div>
                <div className={styles.mobileCatalogStoreGrid}>
                  {controller.visibleStores.map((store) => (
                    <div key={`${row.sku}-${store.store_uid}`} className={styles.mobileCatalogStoreCard}>
                      <div className={styles.mobileCatalogStoreTitle}>
                        <span>{store.label}</span>
                        <span className={styles.mobileCatalogStoreSub}>{store.store_id}</span>
                      </div>
                      <div className={styles.mobileCatalogMetric}>
                        <span>Размещение</span>
                        <strong>{row.placements?.[store.store_uid] ? "Есть" : "Нет"}</strong>
                      </div>
                      <div className={styles.mobileCatalogMetricMuted}>Цены появятся после расчета</div>
                      <div className={styles.mobileCatalogMetricMuted}>Заработок появится после расчета</div>
                    </div>
                  ))}
                </div>
              </article>
            ))
          )}
        </div>

        <div className={styles.mobileCatalogPager}>
          <div className={styles.mobileCatalogPagerMeta}>
            <span>Страница {page} / {totalPages}</span>
            <select
              className={`input input-size-sm ${styles.mobileCatalogPageSize}`}
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
          <div className={styles.mobileCatalogPagerActions}>
            <button type="button" className="btn ghost" onClick={() => setPage((current) => Math.max(1, current - 1))} disabled={page <= 1}>
              Назад
            </button>
            <button className="btn ghost" onClick={() => setPage((current) => Math.min(totalPages, current + 1))} disabled={page >= totalPages}>
              Дальше
            </button>
          </div>
        </div>
      </div>
    </PageFrame>
  );
}
