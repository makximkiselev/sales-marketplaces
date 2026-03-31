import { Fragment } from "react";
import { PageFrame, PageSectionTitle } from "../../../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceTabs, WorkspaceToolbar } from "../../../components/page/WorkspaceKit";
import styles from "./SalesOverviewPage.module.css";

type Props = {
  vm: any;
};

export function SalesOverviewDesktop({ vm }: Props) {
  const {
    stylesRef,
    loading,
    error,
    stores,
    trackingStores,
    availableStatuses,
    activeStore,
    activeTrackingStore,
    activeStoreCurrencyCode,
    activeTrackingCurrencyCode,
    trackingYears,
    orderRows,
    problemRows,
    flowRows,
    skuRows,
    categoryRows,
    totalCount,
    totalPages,
    summaryCards,
    tab,
    setTab,
    storeId,
    setStoreId,
    dateMode,
    setDateMode,
    period,
    setPeriod,
    itemStatus,
    setItemStatus,
    page,
    setPage,
    pageSize,
    setPageSize,
    grain,
    setGrain,
    trackingStoreId,
    setTrackingStoreId,
    expandedMonthKey,
    setExpandedMonthKey,
    tracking,
    orders,
    problemOrders,
    skuRetrospective,
    categoryRetrospective,
    formatMoney,
    formatPercent,
    formatNumber,
    formatDateTime,
    formatDate,
    formatDelta,
    percentOfBase,
    statusTone,
    ORDERS_PERIOD_OPTIONS,
  } = vm;

  const s = stylesRef as typeof styles;
  const overviewTabs = [
    { id: "orders", label: "По заказам" },
    { id: "problems", label: "Проблемные" },
    { id: "tracking", label: "Трекинг" },
    { id: "sku", label: "Товары" },
    { id: "category", label: "Категории" },
  ] as const;
  const currentStoreLabel = tab === "tracking" ? activeTrackingStore?.label : activeStore?.label;
  const currentCurrencySymbol = String(tab === "tracking" ? activeTrackingCurrencyCode : activeStoreCurrencyCode).trim().toUpperCase() === "USD"
    ? "$"
    : "₽";

  return (
    <PageFrame
      className={s.pageFrame}
      innerClassName={s.pageFrameInner}
      title="Обзор продаж"
      subtitle="Единый слой 'по заказам': история загружается один раз, текущий месяц пополняется инкрементально, а в течение дня заказы донасыщаются оперативной экономикой."
    >
      <WorkspaceSurface className={s.overviewHeroSurface}>
        <WorkspaceTabs
          className={s.overviewTabs}
          items={overviewTabs.map((item) => ({ id: item.id, label: item.label }))}
          activeId={tab}
          onChange={setTab}
        />
        <WorkspaceHeader
          title="Аналитика продаж"
          subtitle="Операционный sales workspace для заказов, проблемных кейсов, трекинга и ретроспектив по товарам и категориям."
          meta={(
            <div className={s.overviewHeroMeta}>
              {currentStoreLabel ? <span className={s.overviewMetaChip}>{currentStoreLabel}</span> : null}
              <span className={s.overviewMetaChip}>{currentCurrencySymbol}</span>
            </div>
          )}
        />
        <WorkspaceToolbar className={s.overviewToolbar}>
          <div className={s.toolbarGroup}>
            {tab === "tracking" ? (
              <select className={`input input-size-xl ${s.dateInput}`} value={trackingStoreId} onChange={(e) => setTrackingStoreId(e.target.value)}>
                {trackingStores.map((store: any) => (
                  <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                ))}
              </select>
            ) : (
              <select className={`input input-size-xl ${s.dateInput}`} value={storeId} onChange={(e) => setStoreId(e.target.value)}>
                {stores.map((store: any) => (
                  <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                ))}
              </select>
            )}
          </div>
          <div className={s.toolbarGroup}>
            {tab === "tracking" || tab === "sku" || tab === "category" ? (
              <>
                <select className={`input input-size-md ${s.dateInput}`} value={dateMode} onChange={(e) => setDateMode(e.target.value)}>
                  <option value="created">По дате заказа</option>
                  <option value="delivery">По дате доставки</option>
                </select>
                {tab === "sku" || tab === "category" ? (
                  <select className={`input input-size-sm ${s.dateInput}`} value={grain} onChange={(e) => setGrain(e.target.value)}>
                    <option value="month">По месяцам</option>
                    <option value="day">По дням</option>
                  </select>
                ) : null}
              </>
            ) : (
              <>
                <select className={`input input-size-md ${s.dateInput}`} value={period} onChange={(e) => { setPage(1); setPeriod(e.target.value); }}>
                  {ORDERS_PERIOD_OPTIONS.map((option: any) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
                <select className={`input input-size-lg ${s.dateInput}`} value={itemStatus} onChange={(e) => { setPage(1); setItemStatus(e.target.value); }}>
                  <option value="">Все статусы</option>
                  {availableStatuses.map((status: string) => (
                    <option key={status} value={status}>{status}</option>
                  ))}
                </select>
              </>
            )}
          </div>
        </WorkspaceToolbar>
      </WorkspaceSurface>

      <div className={s.summaryGrid}>
        {summaryCards.map((card: any) => (
          <div key={card.label} className={s.summaryCard}>
            <div className={s.summaryLabel}>{card.label}</div>
            <div className={s.summaryValue}>{card.value}</div>
            {card.detail ? <div className={s.summaryDetail}>{card.detail}</div> : null}
          </div>
        ))}
      </div>

      {!loading && !error && flowRows.length > 0 ? (
        <div className={s.summaryGrid}>
          {flowRows.map((flow: any) => (
            <div key={flow.code} className={s.summaryCard}>
              <div className={s.summaryLabel}>{flow.label}</div>
              <div className={s.summaryValue}>{flow.date_from && flow.date_to ? `${formatDate(flow.date_from)} - ${formatDate(flow.date_to)}` : "—"}</div>
              <div className={s.summaryDetail}>
                {flow.loaded_at ? `${flow.description || ""} Обновлено: ${formatDateTime(flow.loaded_at)}`.trim() : flow.description}
              </div>
            </div>
          ))}
        </div>
      ) : null}

      {tab === "tracking" ? (
        <WorkspaceSurface className={s.trackingStoreSurface}>
          <WorkspaceTabs
            className={s.trackingStoreTabs}
            items={trackingStores.map((store: any) => ({
              id: store.store_id,
              label: store.label,
            }))}
            activeId={trackingStoreId}
            onChange={setTrackingStoreId}
          />
        </WorkspaceSurface>
      ) : null}
      {tab === "tracking"
        ? (activeTrackingStore ? <div className={s.pageInfo}>Магазин: {activeTrackingStore.label}</div> : null)
        : (activeStore ? <div className={s.pageInfo}>Магазин: {activeStore.label}</div> : null)}
      {loading ? <div className={s.empty}>Загрузка...</div> : null}
      {error ? <div className={s.errorBox}>{error}</div> : null}

      {!loading && !error && tab === "orders" ? (
        <section>
          <PageSectionTitle title="Заказы" meta={`Всего: ${totalCount}`} />
          <div className={s.tableWrap}>
            <table className={s.table}>
              <thead>
                <tr>
                  <th>Дата</th><th>Заказ</th><th>SKU</th><th className={s.nameCell}>Наименование</th><th>Статус</th><th>Продажа</th><th>С соинвестом</th><th>Цена стратегии</th><th>Отклонение</th><th>Реклама</th><th>Себестоимость</th><th>Комиссия</th><th>Эквайринг</th><th>Логистика</th><th>Налог</th><th>Расходы</th><th>Прибыль</th>
                </tr>
              </thead>
              <tbody>
                {orderRows.length === 0 ? (
                  <tr><td colSpan={17} className={s.empty}>Нет заказов для выбранных параметров</td></tr>
                ) : orderRows.map((row: any) => {
                  const totalCosts = Number(row.commission || 0) + Number(row.acquiring || 0) + Number(row.delivery || 0) + Number(row.tax || 0) + Number(row.ads || 0);
                  return (
                    <tr key={`${row.order_id || ""}-${row.sku || ""}`}>
                      <td>{formatDateTime(row.order_created_at)}</td>
                      <td>{row.order_id || "—"}</td>
                      <td>{row.sku || "—"}</td>
                      <td className={s.nameCell}>{row.item_name || "—"}</td>
                      <td><span className={`${s.statusBadge} ${s[`tone_${statusTone(row.item_status)}` as keyof typeof s]}`}>{row.item_status || "—"}</span></td>
                      <td>{formatMoney(row.sale_price, activeStoreCurrencyCode)}</td>
                      <td>{formatMoney(row.sale_price_with_coinvest, activeStoreCurrencyCode)}</td>
                      <td><div>{formatMoney(row.strategy_installed_price, activeStoreCurrencyCode)}</div><div className={s.subtleText}>{formatDateTime(row.strategy_snapshot_at)}</div></td>
                      <td>{formatDelta(row.sale_price, row.strategy_installed_price, activeStoreCurrencyCode)}</td>
                      <td><div>{formatMoney(row.ads, activeStoreCurrencyCode)}</div><div className={s.subtleText}>План: {formatPercent(row.strategy_boost_bid_percent)} / Факт: {formatPercent(row.strategy_market_boost_bid_percent)}</div></td>
                      <td>{formatMoney(row.cogs_price, activeStoreCurrencyCode)}</td>
                      <td><div>{formatMoney(row.commission, activeStoreCurrencyCode)}</div><div className={s.subtleText}>{percentOfBase(row.commission, row.sale_price)}</div></td>
                      <td><div>{formatMoney(row.acquiring, activeStoreCurrencyCode)}</div><div className={s.subtleText}>{percentOfBase(row.acquiring, row.sale_price)}</div></td>
                      <td>{formatMoney(row.delivery, activeStoreCurrencyCode)}</td>
                      <td><div>{formatMoney(row.tax, activeStoreCurrencyCode)}</div><div className={s.subtleText}>{percentOfBase(row.tax, row.sale_price)}</div></td>
                      <td>{formatMoney(totalCosts, activeStoreCurrencyCode)}</td>
                      <td><div>{formatMoney(row.profit, activeStoreCurrencyCode)}</div><div className={s.subtleText}>{percentOfBase(row.profit, row.sale_price)}</div></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className={s.toolbar}>
            <div className={s.pager}>
              <button className="btn inline" disabled={page <= 1} onClick={() => setPage((prev: number) => Math.max(1, prev - 1))}>Назад</button>
              <span className={s.pageInfo}>{page} / {totalPages}</span>
              <button className="btn inline" disabled={page >= totalPages} onClick={() => setPage((prev: number) => Math.min(totalPages, prev + 1))}>Дальше</button>
            </div>
            <label className={s.pageSize}>
              На странице
              <select className={`input input-size-sm ${s.dateInput}`} value={pageSize} onChange={(e) => { setPage(1); setPageSize(Number(e.target.value) || 50); }}>
                {[25, 50, 100, 200].map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
            </label>
          </div>
        </section>
      ) : null}

      {!loading && !error && tab === "problems" ? (
        <section>
          <PageSectionTitle title="Проблемные заказы" meta={`Всего: ${formatNumber(problemOrders?.total_count)}`} />
          <div className={s.tableWrap}>
            <table className={s.table}>
              <thead><tr><th>Дата</th><th>Доставка</th><th>Заказ</th><th>SKU</th><th className={s.nameCell}>Наименование</th><th>Статус</th><th>Продажа</th><th>Себестоимость</th><th>Причина</th></tr></thead>
              <tbody>
                {problemRows.length === 0 ? <tr><td colSpan={9} className={s.empty}>Нет проблемных заказов</td></tr> : problemRows.map((row: any) => (
                  <tr key={`${row.order_id || ""}-${row.sku || ""}`}>
                    <td>{formatDateTime(row.order_created_at)}</td><td>{formatDate(row.delivery_date)}</td><td>{row.order_id || "—"}</td><td>{row.sku || "—"}</td><td className={s.nameCell}>{row.item_name || "—"}</td><td><span className={`${s.statusBadge} ${s.tone_warn}`}>{row.item_status || "—"}</span></td><td>{formatMoney(row.sale_price, activeStoreCurrencyCode)}</td><td>{formatMoney(row.cogs_price, activeStoreCurrencyCode)}</td><td className={s.nameCell}>Доставлен, но нет себестоимости. Заказ исключён из чистой аналитики.</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {!loading && !error && tab === "tracking" ? (
        <section>
          <PageSectionTitle title="Трекинг" meta={tracking?.loaded_at ? `Обновлено: ${formatDateTime(tracking.loaded_at)}` : ""} />
          {trackingYears.length === 0 ? <div className={s.empty}>Нет данных для трекинга</div> : trackingYears.map((year: any) => (
            <div key={year.year} className={s.trackingYearSection}>
              <div className={s.trackingYearTitle}>{year.year}</div>
              <div className={s.tableWrap}>
                <table className={s.table}>
                  <thead><tr><th className={s.nameCell}>Период</th><th>Оборот</th><th>Прибыль</th><th>Маржинальность</th><th>Соинвест</th><th>Возвраты</th><th>Реклама</th><th>Ошибки</th><th>Доставка</th></tr></thead>
                  <tbody>
                    {year.months.map((month: any) => {
                      const open = expandedMonthKey === month.month_key;
                      return (
                        <Fragment key={month.month_key}>
                          <tr className={`${s.trackingMonthRow} ${open ? s.trackingMonthRowActive : ""}`} onClick={() => setExpandedMonthKey((prev: string) => prev === month.month_key ? "" : month.month_key)}>
                            <td className={s.trackingMonthCell}><span className={s.trackingChevron}>{open ? "▾" : "▸"}</span>{month.month_label}</td>
                            <td><div>{formatMoney(month.revenue, activeTrackingCurrencyCode)}</div>{month.revenue_plan_amount != null ? <div className={s.trackingPlanText}>План: {formatMoney(month.revenue_plan_amount, activeTrackingCurrencyCode)}</div> : null}</td>
                            <td><div>{formatMoney(month.profit_amount, activeTrackingCurrencyCode)}</div>{month.profit_plan_amount != null ? <div className={s.trackingPlanText}>План: {formatMoney(month.profit_plan_amount, activeTrackingCurrencyCode)}</div> : null}</td>
                            <td>{formatPercent(month.profit_pct)}</td><td>{formatPercent(month.revenue && month.coinvest_amount ? (month.coinvest_amount / month.revenue) * 100 : 0)}</td><td>{formatPercent(month.returns_pct)}</td><td>{formatMoney(month.ads_amount, activeTrackingCurrencyCode)}</td><td>{formatMoney(month.operational_errors, activeTrackingCurrencyCode)}</td><td>{formatNumber(month.delivery_time_days)}</td>
                          </tr>
                        </Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </section>
      ) : null}

      {!loading && !error && tab === "sku" ? (
        <section>
          <PageSectionTitle title="Товары во времени" meta={`Рядов: ${formatNumber(skuRetrospective?.total_count)}`} />
          <div className={s.tableWrap}>
            <table className={s.table}>
              <thead><tr><th className={s.nameCell}>SKU / товар</th><th>Категория</th><th>Оборот</th><th>Прибыль</th><th>Маржинальность</th><th>Соинвест</th><th>Возвраты</th><th>Периоды</th></tr></thead>
              <tbody>
                {skuRows.length === 0 ? <tr><td colSpan={8} className={s.empty}>Нет данных по товарам</td></tr> : skuRows.map((row: any) => (
                  <tr key={row.key}>
                    <td className={s.nameCell}><div>{row.sku || "—"}</div><div className={s.subtleText}>{row.item_name || row.label || "—"}</div></td>
                    <td className={s.nameCell}>{row.category_path || "—"}</td><td>{formatMoney(row.revenue, activeStoreCurrencyCode)}</td><td>{formatMoney(row.profit_amount, activeStoreCurrencyCode)}</td><td>{formatPercent(row.profit_pct)}</td><td>{formatMoney(row.coinvest_amount, activeStoreCurrencyCode)}</td><td>{formatPercent(row.returns_pct)}</td>
                    <td className={s.nameCell}>{(row.periods || []).slice(0, 4).map((period: any) => <div key={`${row.key}-${period.period_key}`} className={s.subtleText}>{period.period_label}: {formatMoney(period.revenue, activeStoreCurrencyCode)} / {formatMoney(period.profit_amount, activeStoreCurrencyCode)}</div>)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {!loading && !error && tab === "category" ? (
        <section>
          <PageSectionTitle title="Категории во времени" meta={`Рядов: ${formatNumber(categoryRetrospective?.total_count)}`} />
          <div className={s.tableWrap}>
            <table className={s.table}>
              <thead><tr><th className={s.nameCell}>Категория</th><th>Оборот</th><th>Прибыль</th><th>Маржинальность</th><th>Соинвест</th><th>Возвраты</th><th>Периоды</th></tr></thead>
              <tbody>
                {categoryRows.length === 0 ? <tr><td colSpan={7} className={s.empty}>Нет данных по категориям</td></tr> : categoryRows.map((row: any) => (
                  <tr key={row.key}>
                    <td className={s.nameCell}>{row.label || row.category_path || "—"}</td><td>{formatMoney(row.revenue, activeStoreCurrencyCode)}</td><td>{formatMoney(row.profit_amount, activeStoreCurrencyCode)}</td><td>{formatPercent(row.profit_pct)}</td><td>{formatMoney(row.coinvest_amount, activeStoreCurrencyCode)}</td><td>{formatPercent(row.returns_pct)}</td>
                    <td className={s.nameCell}>{(row.periods || []).slice(0, 4).map((period: any) => <div key={`${row.key}-${period.period_key}`} className={s.subtleText}>{period.period_label}: {formatMoney(period.revenue, activeStoreCurrencyCode)} / {formatMoney(period.profit_amount, activeStoreCurrencyCode)}</div>)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </PageFrame>
  );
}
