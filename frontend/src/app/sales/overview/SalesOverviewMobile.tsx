import { PageFrame } from "../../../components/page/PageKit";
import { ControlTabs } from "../../../components/page/ControlKit";
import styles from "./SalesOverviewPage.module.css";

type Props = {
  vm: any;
};

export function SalesOverviewMobile({ vm }: Props) {
  const {
    loading,
    error,
    stores,
    trackingStores,
    activeStore,
    activeTrackingStore,
    summaryCards,
    tab,
    setTab,
    storeId,
    setStoreId,
    trackingStoreId,
    setTrackingStoreId,
    orderRows,
    problemRows,
    skuRows,
    categoryRows,
    formatMoney,
    formatPercent,
    formatDateTime,
    activeStoreCurrencyCode,
  } = vm;

  const rows = tab === "orders" ? orderRows : tab === "problems" ? problemRows : tab === "sku" ? skuRows : categoryRows;

  return (
    <PageFrame
      className={styles.pageFrame}
      innerClassName={styles.pageFrameInner}
      title="Обзор продаж"
      subtitle="Мобильный слой продаж без широких таблиц."
    >
      <div className={styles.mobileOverviewShell}>
        <ControlTabs
          className={styles.mobileOverviewTabs}
          items={[
            { id: "orders", label: "Заказы" },
            { id: "problems", label: "Проблемные" },
            { id: "tracking", label: "Трекинг" },
            { id: "sku", label: "Товары" },
            { id: "category", label: "Категории" },
          ]}
          activeId={tab}
          onChange={setTab}
        />

        {tab === "tracking" ? (
          <select className={`input ${styles.dateInput}`} value={trackingStoreId} onChange={(e) => setTrackingStoreId(e.target.value)}>
            {trackingStores.map((store: any) => (
              <option key={store.store_uid} value={store.store_id}>{store.label}</option>
            ))}
          </select>
        ) : (
          <select className={`input ${styles.dateInput}`} value={storeId} onChange={(e) => setStoreId(e.target.value)}>
            {stores.map((store: any) => (
              <option key={store.store_uid} value={store.store_id}>{store.label}</option>
            ))}
          </select>
        )}

        <div className={styles.summaryGrid}>
          {summaryCards.map((card: any) => (
            <div key={card.label} className={styles.summaryCard}>
              <div className={styles.summaryLabel}>{card.label}</div>
              <div className={styles.summaryValue}>{card.value}</div>
              {card.detail ? <div className={styles.summaryDetail}>{card.detail}</div> : null}
            </div>
          ))}
        </div>

        {tab === "tracking"
          ? (activeTrackingStore ? <div className={styles.pageInfo}>Магазин: {activeTrackingStore.label}</div> : null)
          : (activeStore ? <div className={styles.pageInfo}>Магазин: {activeStore.label}</div> : null)}

        {loading ? <div className={styles.empty}>Загрузка...</div> : null}
        {error ? <div className={styles.errorBox}>{error}</div> : null}

        {!loading && !error ? (
          <div className={styles.mobileOverviewCards}>
            {rows.length === 0 ? (
              <div className={styles.empty}>Нет данных для выбранного режима</div>
            ) : rows.slice(0, 50).map((row: any) => (
              <article key={`${row.key || row.order_id || row.sku || Math.random()}`} className={styles.mobileOverviewCard}>
                <div className={styles.mobileOverviewCardHead}>
                  <div className={styles.mobileOverviewCardTitle}>
                    {row.item_name || row.label || row.order_id || row.sku || "Запись"}
                  </div>
                  <div className={styles.mobileOverviewCardMeta}>
                    {row.order_created_at ? formatDateTime(row.order_created_at) : row.sku || "—"}
                  </div>
                </div>
                {row.sku ? <div className={styles.mobileOverviewCardSub}>SKU: {row.sku}</div> : null}
                {row.category_path ? <div className={styles.mobileOverviewCardSub}>{row.category_path}</div> : null}
                <div className={styles.mobileOverviewMetrics}>
                  {"sale_price" in row ? (
                    <>
                      <div className={styles.mobileOverviewMetric}><span>Продажа</span><strong>{formatMoney(row.sale_price, activeStoreCurrencyCode)}</strong></div>
                      <div className={styles.mobileOverviewMetric}><span>Прибыль</span><strong>{formatMoney(row.profit, activeStoreCurrencyCode)}</strong></div>
                      <div className={styles.mobileOverviewMetric}><span>Маржа</span><strong>{formatPercent(row.profit && row.sale_price ? (Number(row.profit) / Number(row.sale_price)) * 100 : null)}</strong></div>
                    </>
                  ) : (
                    <>
                      <div className={styles.mobileOverviewMetric}><span>Оборот</span><strong>{formatMoney(row.revenue, activeStoreCurrencyCode)}</strong></div>
                      <div className={styles.mobileOverviewMetric}><span>Прибыль</span><strong>{formatMoney(row.profit_amount, activeStoreCurrencyCode)}</strong></div>
                      <div className={styles.mobileOverviewMetric}><span>Маржа</span><strong>{formatPercent(row.profit_pct)}</strong></div>
                    </>
                  )}
                </div>
              </article>
            ))}
          </div>
        ) : null}
      </div>
    </PageFrame>
  );
}
