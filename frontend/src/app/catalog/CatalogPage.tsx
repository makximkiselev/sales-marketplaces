import { useEffect, useState } from "react";
import PricingCatalogFrame from "../pricing/_components/PricingCatalogFrame";
import commonStyles from "../pricing/_components/PricingPageCommon.module.css";
import { useCatalogPageController } from "./useCatalogPageController";
import { CatalogDesktop } from "./CatalogDesktop";
import { CatalogMobile } from "./CatalogMobile";
import styles from "./CatalogPage.module.css";
import { WorkspaceTabs } from "../../components/page/WorkspaceKit";

function useCatalogMobile() {
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const media = window.matchMedia("(max-width: 960px)");
    const sync = () => setIsMobile(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  return isMobile;
}

export default function CatalogPage() {
  const controller = useCatalogPageController();
  const isMobile = useCatalogMobile();
  const {
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
    tableLoading,
    tabItems,
    context,
  } = controller;

  const treeSelector = (
    <div className={styles.treeSourcePanel}>
      <div className={styles.treeSourceBlock}>
        <label className={commonStyles.fieldLabel}>Источник древа</label>
        <WorkspaceTabs
          items={[
            { id: "marketplaces", label: "Маркетплейсы" },
            { id: "external", label: "Внешний источник" },
          ]}
          activeId={treeMode}
          onChange={setTreeMode}
        />
      </div>

      {treeMode === "marketplaces" && tab === "all" ? (
        <div className={styles.treeSourceBlock}>
          <label className={commonStyles.fieldLabel} htmlFor="catalog-tree-source-store">Магазин для каталога</label>
          <select
            id="catalog-tree-source-store"
            className={`input input-size-md ${commonStyles.select}`}
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
              className={`input input-size-md ${commonStyles.select}`}
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
              className={`input input-size-md ${commonStyles.select}`}
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

  if (isMobile) {
    return <CatalogMobile controller={controller} treeSelector={treeSelector} />;
  }

  return <CatalogDesktop controller={controller} treeSelector={treeSelector} table={treeMode === "external" ? (
    <div className={styles.placeholder}>
      Режим внешнего источника древа подготовлен в интерфейсе. Подключение дерева из таблиц и внешних систем будет следующим шагом.
    </div>
  ) : table} />;
}
