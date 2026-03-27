"use client";

import { ControlField, ControlTabs } from "../../components/page/ControlKit";
import { PageFrame } from "../../components/page/PageKit";
import { SectionBlock } from "../../components/page/SectionKit";
import { CatalogItem, CatalogSummaryTreeNode, RunStatus, sourceShortLabel } from "./catalogShared";
import { useCatalogSummaryController } from "./useCatalogSummaryController";

type Props = {
  initialItems: CatalogItem[];
  initialTotalCount: number;
  initialRun: RunStatus | null;
  initialTree: CatalogSummaryTreeNode[];
  initialSelectedSources: string[];
  sourceLabels: Record<string, string>;
};

export default function CatalogSummaryClient(props: Props) {
  const { sourceLabels } = props;
  const {
    selectedPath,
    activeTab,
    search,
    setSearch,
    items,
    totalCount,
    run,
    selectedSources,
    page,
    setPage,
    loading,
    error,
    flatTree,
    totalPages,
    visibleSourceColumns,
    expandedPaths,
    togglePath,
    switchTab,
    toggleExpand,
    toggleExpandAll,
  } = useCatalogSummaryController(props);

  const tabs = [{ id: "all", label: "Все площадки" }, ...selectedSources.map((sourceId) => ({ id: sourceId, label: sourceLabels[sourceId] || sourceId }))];

  return (
    <>
      <PageFrame
        title="Сводка каталога"
        subtitle={`Всего позиций: ${totalCount}. Страница ${Math.min(page, totalPages)} из ${totalPages}.`}
        meta={
          run ? (
            <div>
              Последний импорт: {run.status === "ok" ? "успешно" : "ошибка"}, загружено {run.imported ?? 0} из {run.total ?? 0}, ошибок {run.failed ?? 0}.
            </div>
          ) : (
            <div>Импорт каталога еще не запускался.</div>
          )
        }
      >
        <div className="catalog-toolbar">
          <ControlField label="Поиск">
            <input
              className="input"
              placeholder="Поиск по SKU или наименованию"
              value={search}
              onChange={(e) => {
                setPage(1);
                setSearch(e.target.value);
              }}
            />
          </ControlField>
        </div>
        <ControlTabs
          items={tabs.map((tab) => ({
            id: tab.id,
            label: tab.id === "all" ? tab.label : sourceShortLabel(tab.id, sourceLabels),
          }))}
          activeId={activeTab}
          onChange={switchTab}
        />
      </PageFrame>

      <section className="catalog-layout">
        <SectionBlock title="Дерево категорий" className="catalog-tree-panel">
          <h3 className="subtitle">Дерево категорий</h3>
          <div className="catalog-tree-actions">
            <button className="btn inline" onClick={toggleExpandAll}>
              {expandedPaths.size > 0 ? "Свернуть все" : "Развернуть все"}
            </button>
          </div>
          {flatTree.length ? (
            <div className="table-wrap catalog-tree-scroll">
              <table>
                <tbody>
                  {flatTree.map((node) => {
                    const isExpanded = expandedPaths.has(node.path);
                    const isSelected = selectedPath === node.path;
                    return (
                      <tr key={`cat-${node.id}`} className={`clickable-row ${isSelected ? "catalog-tree-selected" : ""}`}>
                        <td style={{ paddingLeft: `${12 + node.depth * 18}px` }}>
                          {node.hasChildren ? (
                            <button className="btn inline catalog-tree-toggle" onClick={() => toggleExpand(node.path)}>
                              {isExpanded ? "−" : "+"}
                            </button>
                          ) : (
                            <span className="catalog-tree-toggle-spacer" />
                          )}
                          <button className="btn inline catalog-tree-node" onClick={() => togglePath(node.path)}>
                            {node.name}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="status">Категории пока не загружены.</div>
          )}
        </SectionBlock>

        <SectionBlock title="Товары каталога">
          <div className="table-wrap">
            <h3 className="subtitle" style={{ marginBottom: 10 }}>Товары каталога</h3>
            {selectedPath ? (
              <div className="status">
                Фильтр по дереву: {selectedPath}{" "}
                <button className="btn inline" onClick={() => togglePath(selectedPath)}>
                  Сбросить
                </button>
              </div>
            ) : null}
            {loading ? <div className="status">Загрузка...</div> : null}
            {error ? <div className="status error">{error}</div> : null}

            <table>
              <thead>
                <tr>
                  <th>SKU</th>
                  <th>Наименование</th>
                  {visibleSourceColumns.map((sourceId) => (
                    <th className="source-col" key={`src-col-${sourceId}`} title={sourceLabels[sourceId] || sourceId}>
                      {sourceShortLabel(sourceId, sourceLabels)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {items.map((item, index) => (
                  <tr key={`${item.sku_primary || item.sku || "row"}-${index}`}>
                    <td>{item.sku_primary || item.sku || "-"}</td>
                    <td>{item.title || item.name || "-"}</td>
                    {visibleSourceColumns.map((sourceId) => (
                      <td className="source-col" key={`src-val-${item.sku || item.sku_primary}-${sourceId}`}>
                        {(item.source_flags || {})[sourceId] ? "✓" : "—"}
                      </td>
                    ))}
                  </tr>
                ))}
                {!items.length ? (
                  <tr>
                    <td colSpan={2 + Math.max(visibleSourceColumns.length, 1)}>Нет товаров для выбранного фильтра.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>

            <div className="catalog-pagination">
              <button className="btn" disabled={page <= 1 || loading} onClick={() => setPage((current) => Math.max(1, current - 1))}>
                Назад
              </button>
              <span className="status-time">Страница {Math.min(page, totalPages)} / {totalPages}</span>
              <button className="btn" disabled={page >= totalPages || loading} onClick={() => setPage((current) => Math.min(totalPages, current + 1))}>
                Вперед
              </button>
            </div>
          </div>
        </SectionBlock>
      </section>
    </>
  );
}
