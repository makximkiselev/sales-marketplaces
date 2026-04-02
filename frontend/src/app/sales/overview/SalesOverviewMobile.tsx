import { PageFrame } from "../../../components/page/PageKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";
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
    setPage,
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
  const modeLabel = dateMode === "delivery" ? "Дата доставки" : "Дата заказа";
  const grainLabel = grain === "day" ? "По дням" : "По месяцам";
  const periodLabel = ORDERS_PERIOD_OPTIONS.find((option: any) => option.value === period)?.label || "Период";
  const tabCopy: Record<string, { eyebrow: string; title: string; subtitle: string }> = {
    orders: {
      eyebrow: "Заказы",
      title: "Операционный слой продаж",
      subtitle: "Быстрый вход в заказы, экономику и статусы без широких таблиц.",
    },
    problems: {
      eyebrow: "Проблемные",
      title: "Контроль потерь и исключений",
      subtitle: "Смотри проблемные заказы отдельно, без смешивания с чистой продажей.",
    },
    tracking: {
      eyebrow: "Трекинг",
      title: "План и факт по периодам",
      subtitle: "Выручка, прибыль и реклама в компактном mobile-срезе.",
    },
    sku: {
      eyebrow: "Товары",
      title: "SKU в динамике",
      subtitle: "Ретроспектива по товарам с акцентом на оборот и прибыль.",
    },
    category: {
      eyebrow: "Категории",
      title: "Категории в динамике",
      subtitle: "Сводный мобильный срез по категориям и периодам.",
    },
  };
  const activeTabCopy = tabCopy[tab] || tabCopy.orders;

  const resetPage = () => setPage(1);

  return (
    <PageFrame
      className={styles.pageFrame}
      innerClassName={styles.pageFrameInner}
      title="Обзор продаж"
      subtitle="Мобильный слой продаж без широких таблиц."
    >
      <div className={styles.mobileOverviewShell}>
        <WorkspacePageHero
          className={styles.overviewHeroSurface}
          title="Обзор продаж"
          subtitle="Мобильный слой продаж без широких таблиц."
          tabs={{
            items: [
              { id: "orders", label: "Заказы" },
              { id: "problems", label: "Проблемные" },
              { id: "tracking", label: "Трекинг" },
              { id: "sku", label: "Товары" },
              { id: "category", label: "Категории" },
            ],
            activeId: tab,
            onChange: setTab,
          }}
          meta={currentStoreLabel ? <span className={layoutStyles.metaChip}>{currentStoreLabel}</span> : undefined}
        >
          <div className={styles.overviewHeroIntro}>
            <div className={styles.overviewEyebrow}>{activeTabCopy.eyebrow}</div>
            <h2 className={styles.overviewHeroTitle}>{activeTabCopy.title}</h2>
            <p className={styles.overviewHeroSubtitle}>{activeTabCopy.subtitle}</p>
          </div>
          <div className={styles.mobileFilterGrid}>
            {tab === "tracking" ? (
              <>
                <div className={styles.mobileFilterGroup}>
                  <div className={styles.mobileFilterLabel}>Магазин</div>
                  <select className={`input input-size-fluid ${styles.dateInput}`} value={trackingStoreId} onChange={(e) => setTrackingStoreId(e.target.value)}>
                    {trackingStores.map((store: any) => (
                      <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                    ))}
                  </select>
                </div>
                <div className={styles.mobileFilterGroup}>
                  <div className={styles.mobileFilterLabel}>Срез</div>
                  <select className={`input input-size-fluid ${styles.dateInput}`} value={dateMode} onChange={(e) => setDateMode(e.target.value)}>
                    <option value="created">Дата заказа</option>
                    <option value="delivery">Дата доставки</option>
                  </select>
                </div>
              </>
            ) : (
              <>
                <div className={styles.mobileFilterGroup}>
                  <div className={styles.mobileFilterLabel}>Магазин</div>
                  <select className={`input input-size-fluid ${styles.dateInput}`} value={storeId} onChange={(e) => { resetPage(); setStoreId(e.target.value); }}>
                    {stores.map((store: any) => (
                      <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                    ))}
                  </select>
                </div>
                {tab === "orders" || tab === "problems" ? (
                  <div className={styles.mobileFilterGroup}>
                    <div className={styles.mobileFilterLabel}>Период</div>
                    <select className={`input input-size-fluid ${styles.dateInput}`} value={period} onChange={(e) => { resetPage(); setPeriod(e.target.value); }}>
                      {ORDERS_PERIOD_OPTIONS.map((option: any) => (
                        <option key={option.value} value={option.value}>{option.label}</option>
                      ))}
                    </select>
                  </div>
                ) : null}
                {tab === "orders" ? (
                  <div className={styles.mobileFilterGroup}>
                    <div className={styles.mobileFilterLabel}>Статус</div>
                    <select className={`input input-size-fluid ${styles.dateInput}`} value={itemStatus} onChange={(e) => { resetPage(); setItemStatus(e.target.value); }}>
                      <option value="">Все статусы</option>
                      {availableStatuses.map((status: string) => (
                        <option key={status} value={status}>{status}</option>
                      ))}
                    </select>
                  </div>
                ) : null}
                {tab === "sku" || tab === "category" ? (
                  <>
                    <div className={styles.mobileFilterGroup}>
                      <div className={styles.mobileFilterLabel}>Срез</div>
                      <select className={`input input-size-fluid ${styles.dateInput}`} value={dateMode} onChange={(e) => { resetPage(); setDateMode(e.target.value); }}>
                        <option value="created">Дата заказа</option>
                        <option value="delivery">Дата доставки</option>
                      </select>
                    </div>
                    <div className={styles.mobileFilterGroup}>
                      <div className={styles.mobileFilterLabel}>Гранулярность</div>
                      <select className={`input input-size-fluid ${styles.dateInput}`} value={grain} onChange={(e) => { resetPage(); setGrain(e.target.value); }}>
                        <option value="month">По месяцам</option>
                        <option value="day">По дням</option>
                      </select>
                    </div>
                  </>
                ) : null}
              </>
            )}
          </div>
          <div className={styles.mobileFilterChips}>
            {currentStoreLabel ? <span className={layoutStyles.metaChip}>{currentStoreLabel}</span> : null}
            {tab === "orders" || tab === "problems" ? <span className={layoutStyles.metaChip}>{periodLabel}</span> : null}
            {tab === "tracking" || tab === "sku" || tab === "category" ? <span className={layoutStyles.metaChip}>{modeLabel}</span> : null}
            {tab === "sku" || tab === "category" ? <span className={layoutStyles.metaChip}>{grainLabel}</span> : null}
            {tab === "orders" && itemStatus ? <span className={layoutStyles.metaChip}>{itemStatus}</span> : null}
          </div>
        </WorkspacePageHero>

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
