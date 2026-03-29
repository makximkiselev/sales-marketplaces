import PricingCatalogFrame from "../pricing/_components/PricingCatalogFrame";
import commonStyles from "../pricing/_components/PricingPageCommon.module.css";
import { tabKeyForStore } from "../_shared/catalogState";
import { useCatalogPageController } from "./useCatalogPageController";
import styles from "./CatalogPage.module.css";

export default function CatalogPage() {
  const controller = useCatalogPageController();
  const {
    loading,
    error,
    stores,
    treeMode,
    setTreeMode,
    tab,
    setTab,
    treeSourceStoreId,
    setTreeSourceStoreId,
    externalSourceType,
    setExternalSourceType,
    externalSourceId,
    setExternalSourceId,
    searchDraft,
    setSearchDraft,
    page,
    setPage,
    pageSize,
    setPageSize,
    selectedTreePath,
    flatTree,
    rows,
    visibleStores,
    totalCount,
    totalPages,
    tableLoading,
    activeStoreLabel,
    expandedSize,
    isExpanded,
    onToggleTree,
    onToggleExpand,
    onToggleExpandAll,
    tabItems,
    context,
  } = controller;

  const treeSelector = (
    <div className={styles.treeSourcePanel}>
      <div className={styles.treeSourceBlock}>
        <label className={commonStyles.fieldLabel}>Источник древа</label>
        <div className={styles.modeSwitch}>
          <button
            type="button"
            className={`btn inline ${commonStyles.tabBtn} ${treeMode === "marketplaces" ? commonStyles.tabBtnActive : ""}`}
            onClick={() => setTreeMode("marketplaces")}
          >
            Маркетплейсы
          </button>
          <button
            type="button"
            className={`btn inline ${commonStyles.tabBtn} ${treeMode === "external" ? commonStyles.tabBtnActive : ""}`}
            onClick={() => setTreeMode("external")}
          >
            Внешний источник
          </button>
        </div>
      </div>

      {treeMode === "marketplaces" && tab === "all" ? (
        <div className={styles.treeSourceBlock}>
          <label className={commonStyles.fieldLabel} htmlFor="catalog-tree-source-store">Магазин для каталога</label>
          <select
            id="catalog-tree-source-store"
            className={`input ${commonStyles.select}`}
            value={treeSourceStoreId}
            onChange={(e) => setTreeSourceStoreId(e.target.value)}
          >
            {stores.map((store) => (
              <option key={store.store_uid} value={store.store_id}>
                {store.platform_label}: {store.label}
              </option>
            ))}
          </select>
        </div>
      ) : null}

      {treeMode === "external" ? (
        <>
          <div className={styles.treeSourceBlock}>
            <label className={commonStyles.fieldLabel} htmlFor="catalog-ext-type">Тип источника</label>
            <select
              id="catalog-ext-type"
              className={`input ${commonStyles.select}`}
              value={externalSourceType}
              onChange={(e) => setExternalSourceType(e.target.value)}
            >
              {(context?.external_tree_source_types || []).map((item) => (
                <option key={item.id} value={item.id}>{item.label}</option>
              ))}
            </select>
          </div>
          <div className={styles.treeSourceBlock}>
            <label className={commonStyles.fieldLabel} htmlFor="catalog-ext-source">Источник</label>
            <select
              id="catalog-ext-source"
              className={`input ${commonStyles.select}`}
              value={externalSourceId}
              onChange={(e) => setExternalSourceId(e.target.value)}
            >
              {(context?.external_sources || [])
                .filter((source) => (externalSourceType === "tables"
                  ? ["gsheets", "yandex_tables"].includes(source.type)
                  : !["gsheets", "yandex_tables"].includes(source.type)))
                .map((source) => (
                  <option key={source.id} value={source.id}>{source.label}</option>
                ))}
            </select>
          </div>
        </>
      ) : null}
    </div>
  );

  const table = (
    <table className={styles.matrixTable}>
      <thead>
        <tr>
          <th rowSpan={2}>SKU</th>
          <th rowSpan={2}>Наименование</th>
          <th className={styles.groupPlacement} colSpan={tab === "all" ? visibleStores.length || 1 : 1}>Размещение</th>
          <th className={styles.groupPrice} colSpan={tab === "all" ? visibleStores.length || 1 : 1}>Цены</th>
          <th className={styles.groupProfit} colSpan={tab === "all" ? visibleStores.length || 1 : 1}>Заработок</th>
          <th rowSpan={2}>Обновлено</th>
        </tr>
        <tr>
          {(tab === "all" ? visibleStores : visibleStores.slice(0, 1)).map((store) => (
            <th className={styles.placementHead} key={`p-${store.store_uid}`}>
              <span className={styles.storeHeadText}>{store.label}</span>
              <span className={styles.subHead}>{store.store_id}</span>
            </th>
          ))}
          {(tab === "all" ? visibleStores : visibleStores.slice(0, 1)).map((store) => (
            <th className={styles.priceHead} key={`c-${store.store_uid}`}>
              <span className={styles.storeHeadText}>{store.label}</span>
              <span className={styles.subHead}>{store.store_id}</span>
            </th>
          ))}
          {(tab === "all" ? visibleStores : visibleStores.slice(0, 1)).map((store) => (
            <th className={styles.earnHead} key={`e-${store.store_uid}`}>
              <span className={styles.storeHeadText}>{store.label}</span>
              <span className={styles.subHead}>{store.store_id}</span>
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <tr>
            <td colSpan={6 + (tab === "all" ? (visibleStores.length * 3 - 1) : 2)} className={styles.emptyCell}>
              {tableLoading ? "Загрузка..." : "Нет товаров для выбранных параметров"}
            </td>
          </tr>
        ) : rows.map((row) => (
          <tr key={row.sku}>
            <td className={styles.skuCell}>{row.sku}</td>
            <td className={styles.nameCell}>
              <div>{row.name || "—"}</div>
              <div className={styles.pathHint}>{(row.tree_path || []).join(" / ") || "Не определено"}</div>
            </td>
            {(tab === "all" ? visibleStores : visibleStores.slice(0, 1)).map((store) => (
              <td key={`pv-${row.sku}-${store.store_uid}`} className={`${styles.centerCell} ${styles.placementCell}`}>
                {row.placements?.[store.store_uid] ? <span className={styles.okMark}>✓</span> : <span className={styles.noMark}>—</span>}
              </td>
            ))}
            {(tab === "all" ? visibleStores : visibleStores.slice(0, 1)).map((store) => (
              <td key={`cv-${row.sku}-${store.store_uid}`} className={`${styles.placeholderCell} ${styles.priceCell}`}>
                <div>Кабинетная: —</div>
                <div>Промо: —</div>
                <div>Витрина: —</div>
              </td>
            ))}
            {(tab === "all" ? visibleStores : visibleStores.slice(0, 1)).map((store) => (
              <td key={`ev-${row.sku}-${store.store_uid}`} className={`${styles.placeholderCell} ${styles.earnCell}`}>
                <div>Данные появятся</div>
                <div>после расчета цен</div>
              </td>
            ))}
            <td className={styles.updatedCell}>{row.updated_at ? new Date(row.updated_at).toLocaleString("ru-RU") : "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );

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
      treeLoadingText={flatTree.length === 0 ? (loading || tableLoading ? "Загрузка..." : "Нет данных для дерева") : ""}
      tableTitle="Товары"
      tableMeta={
        <>
          {tableLoading ? "Обновление..." : `Всего: ${totalCount}`}
          {selectedTreePath ? ` • Фильтр: ${selectedTreePath}` : ""}
        </>
      }
      table={treeMode === "external" ? (
        <div className={styles.placeholder}>
          Режим внешнего источника древа подготовлен в интерфейсе. Подключение дерева из таблиц и внешних систем будет следующим шагом.
        </div>
      ) : table}
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
