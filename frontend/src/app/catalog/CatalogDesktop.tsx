import PricingCatalogFrame from "../pricing/_components/PricingCatalogFrame";
import commonStyles from "../pricing/_components/PricingPageCommon.module.css";
import styles from "./CatalogPage.module.css";
import type { CatalogController } from "./CatalogRendererTypes";

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
    <PricingCatalogFrame
      title="Список товаров"
      subtitle="Быстрый свод по товарам, размещению и будущим ценовым/рекламным метрикам."
      tabs={(
        <>
          {tabItems.map((item) => (
            <button
              key={item.id}
              className={`btn inline ${commonStyles.tabBtn} ${tab === item.id ? commonStyles.tabBtnActive : ""}`}
              onClick={() => setTab(item.id)}
            >
              <span>{item.label}</span>
              {"badge" in item && item.badge ? <span className={commonStyles.tabBadge}>{item.badge}</span> : null}
            </button>
          ))}
        </>
      )}
      searchValue={searchDraft}
      onSearchChange={setSearchDraft}
      searchPlaceholder="Поиск по SKU или наименованию"
      error={error}
      treeSelector={treeSelector}
      treeMeta={tab === "all" ? "Сводный список товаров" : activeStoreLabel}
      flatTree={flatTree}
      selectedTreePath={selectedTreePath}
      expandedSize={expandedSize}
      isExpanded={isExpanded}
      onToggleExpandAll={onToggleExpandAll}
      onToggleExpand={onToggleExpand}
      onToggleTree={onToggleTree}
      treeLoadingText={flatTree.length === 0 ? (tableLoading ? "Загрузка..." : "Нет данных для дерева") : ""}
      tableTitle="Товары"
      tableMeta={
        <>
          {tableLoading ? "Обновление..." : `Всего: ${totalCount}`}
          {selectedTreePath ? ` • Фильтр: ${selectedTreePath}` : ""}
        </>
      }
      table={table}
      page={page}
      totalPages={totalPages}
      onPrevPage={() => setPage((current) => Math.max(1, current - 1))}
      onNextPage={() => setPage((current) => Math.min(totalPages, current + 1))}
      canPrev={page > 1}
      canNext={page < totalPages}
      pageSize={pageSize}
      onPageSizeChange={(value) => {
        setPage(1);
        setPageSize(value);
      }}
    />
  );
}
