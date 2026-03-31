import { PageFrame } from "../../../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceTabs, WorkspaceToolbar } from "../../../components/page/WorkspaceKit";
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
    period,
    setPeriod,
    itemStatus,
    setItemStatus,
    availableStatuses,
    dateMode,
    setDateMode,
    grain,
    setGrain,
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
    ORDERS_PERIOD_OPTIONS,
  } = vm;

  const rows = tab === "orders" ? orderRows : tab === "problems" ? problemRows : tab === "sku" ? skuRows : categoryRows;
  const currentStoreLabel = tab === "tracking" ? activeTrackingStore?.label : activeStore?.label;

  return (
    <PageFrame
      className={styles.pageFrame}
      innerClassName={styles.pageFrameInner}
      title="Обзор продаж"
      subtitle="Мобильный слой продаж без широких таблиц."
    >
      <div className={styles.mobileOverviewShell}>
        <WorkspaceSurface className={styles.overviewHeroSurface}>
          <WorkspaceTabs
            className={styles.overviewTabs}
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
        <WorkspaceHeader
          title="Аналитика продаж"
          subtitle="Компактный mobile-workspace для быстрых срезов по заказам и ретроспективам."
            meta={currentStoreLabel ? <span className={styles.overviewMetaChip}>{currentStoreLabel}</span> : undefined}
        />
          <WorkspaceToolbar className={styles.overviewToolbarMobile}>
            {tab === "tracking" ? (
              <>
                <select className={`input input-size-fluid ${styles.dateInput}`} value={trackingStoreId} onChange={(e) => setTrackingStoreId(e.target.value)}>
                  {trackingStores.map((store: any) => (
                    <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                  ))}
                </select>
                <select className={`input input-size-fluid ${styles.dateInput}`} value={dateMode} onChange={(e) => setDateMode(e.target.value)}>
                  <option value="created">Дата заказа</option>
                  <option value="delivery">Дата доставки</option>
                </select>
              </>
            ) : (
              <>
                <select className={`input input-size-fluid ${styles.dateInput}`} value={storeId} onChange={(e) => setStoreId(e.target.value)}>
                  {stores.map((store: any) => (
                    <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                  ))}
                </select>
                {tab === "orders" || tab === "problems" ? (
                  <select className={`input input-size-fluid ${styles.dateInput}`} value={period} onChange={(e) => setPeriod(e.target.value)}>
                    {ORDERS_PERIOD_OPTIONS.map((option: any) => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                ) : null}
                {tab === "orders" ? (
                  <select className={`input input-size-fluid ${styles.dateInput}`} value={itemStatus} onChange={(e) => setItemStatus(e.target.value)}>
                    <option value="">Все статусы</option>
                    {availableStatuses.map((status: string) => (
                      <option key={status} value={status}>{status}</option>
                    ))}
                  </select>
                ) : null}
                {tab === "sku" || tab === "category" ? (
                  <>
                    <select className={`input input-size-fluid ${styles.dateInput}`} value={dateMode} onChange={(e) => setDateMode(e.target.value)}>
                      <option value="created">Дата заказа</option>
                      <option value="delivery">Дата доставки</option>
                    </select>
                    <select className={`input input-size-fluid ${styles.dateInput}`} value={grain} onChange={(e) => setGrain(e.target.value)}>
                      <option value="month">По месяцам</option>
                      <option value="day">По дням</option>
                    </select>
                  </>
                ) : null}
              </>
            )}
          </WorkspaceToolbar>
        </WorkspaceSurface>

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
          ? (activeTrackingStore ? <div className={styles.pageInfoChip}>Магазин: {activeTrackingStore.label}</div> : null)
          : (activeStore ? <div className={styles.pageInfoChip}>Магазин: {activeStore.label}</div> : null)}

        {loading ? <div className={styles.empty}>Загрузка...</div> : null}
        {error ? <div className={styles.errorBox}>{error}</div> : null}

        {!loading && !error ? (
          <div className={styles.mobileOverviewCards}>
            {rows.length === 0 ? (
              <div className={styles.empty}>Нет данных для выбранного режима</div>
            ) : rows.slice(0, 50).map((row: any, index: number) => (
              <article key={String(row.key || row.order_id || row.sku || row.category_path || row.label || index)} className={styles.mobileOverviewCard}>
                <div className={styles.mobileOverviewCardHead}>
                  <div className={styles.mobileOverviewCardTitle}>
                    {row.item_name || row.label || row.order_id || row.sku || "Запись"}
                  </div>
                  <div className={styles.mobileOverviewCardMeta}>
                    {row.order_created_at ? formatDateTime(row.order_created_at) : row.sku || "—"}
                  </div>
                </div>
                <div className={styles.mobileOverviewInfoRow}>
                  {row.sku ? <div className={styles.mobileOverviewCardSub}>SKU: {row.sku}</div> : null}
                  {row.category_path ? <div className={styles.mobileOverviewCardPath}>{row.category_path}</div> : null}
                </div>
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
