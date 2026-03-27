"use client";

import commonStyles from "./PricingPageCommon.module.css";
import type { StoreCtx } from "../_shared/catalogPageShared";

type StockFilterValue = "all" | "in_stock" | "out_of_stock";

type Props = {
  selectId: string;
  stores: StoreCtx[];
  treeSourceStoreId: string;
  onTreeSourceStoreChange: (value: string) => void;
  showStoreSelector?: boolean;
  stockFilter?: StockFilterValue;
  onStockFilterChange?: (value: StockFilterValue) => void;
};

export default function CatalogTreeControls(props: Props) {
  const {
    selectId,
    stores,
    treeSourceStoreId,
    onTreeSourceStoreChange,
    showStoreSelector = true,
    stockFilter,
    onStockFilterChange,
  } = props;

  return (
    <>
      {showStoreSelector ? (
        <>
          <label className={commonStyles.fieldLabel} htmlFor={selectId}>Магазин для каталога</label>
          <select
            id={selectId}
            className={`input ${commonStyles.select}`}
            value={treeSourceStoreId}
            onChange={(e) => onTreeSourceStoreChange(e.target.value)}
          >
            {stores.map((store) => (
              <option key={store.store_uid} value={store.store_uid}>
                {store.platform_label}: {store.label}
              </option>
            ))}
          </select>
        </>
      ) : null}
      {onStockFilterChange ? (
        <>
          <label className={commonStyles.fieldLabel} htmlFor={`${selectId}-stock-filter`}>Наличие</label>
          <select
            id={`${selectId}-stock-filter`}
            className={`input ${commonStyles.select}`}
            value={stockFilter || "all"}
            onChange={(e) => onStockFilterChange(e.target.value as StockFilterValue)}
          >
            <option value="all">Все товары</option>
            <option value="in_stock">В наличии</option>
            <option value="out_of_stock">Нет в наличии</option>
          </select>
        </>
      ) : null}
    </>
  );
}
