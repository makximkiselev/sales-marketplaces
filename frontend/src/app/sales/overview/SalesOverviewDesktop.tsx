import { Fragment } from "react";
import { PageSectionTitle } from "../../../components/page/PageKit";
import styles from "./SalesOverviewPage.module.css";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";
import { WorkspacePageFrame } from "../../_shared/WorkspacePageFrame";

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
    skuRows,
    categoryRows,
    totalCount,
    totalPages,
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
    categoryLevel,
    setCategoryLevel,
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

  const adsSourceLabel = (source?: string | null): string => {
    switch (String(source || "")) {
      case "market_boost_fact":
        return "Маркет";
      case "category_ads_percent":
        return "Категория";
      case "store_target_drr":
        return "Магазин";
      default:
        return "—";
    }
  };
  const priceCurrencyCode = activeStoreCurrencyCode || "RUB";
  const economicsCurrencyCode = "RUB";
  const hasNativeCurrency = String(priceCurrencyCode).trim().toUpperCase() === "USD";
  const nativeValue = (row: any, key: string) => row?.[`${key}_native`] ?? row?.[key];
  const moneyWithNative = (rubValue: number | null | undefined, nativeVal: number | null | undefined) => (
    <>
      <div>{formatMoney(rubValue, "RUB")}</div>
      {hasNativeCurrency ? <div className={s.subtleText}>{formatMoney(nativeVal, priceCurrencyCode)}</div> : null}
    </>
  );
  const rubWithOptionalNative = (rubValue: number | null | undefined, nativeVal: number | null | undefined) => (
    <>
      <div>{formatMoney(rubValue, economicsCurrencyCode)}</div>
      {hasNativeCurrency ? <div className={s.subtleText}>{formatMoney(nativeVal, priceCurrencyCode)}</div> : null}
    </>
  );
  const orderFxLabel = (row: any) => {
    const code = String(row?.currency_code || priceCurrencyCode || "RUB").trim().toUpperCase() || "RUB";
    if (code !== "USD") return "1.00";
    const rate = Number(row?.fx_usd_rub_rate || 0);
    if (!rate) return "—";
    return `${rate.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 4 })} ₽/$`;
  };

  const s = stylesRef as typeof styles;
  const overviewTabs = [
    { id: "tracking", label: "Трекинг" },
    { id: "orders", label: "По заказам" },
    { id: "category", label: "Категории" },
    { id: "sku", label: "Товары" },
    { id: "problems", label: "Проблемные" },
  ] as const;
  const tabCopy: Record<string, { eyebrow: string; title: string; subtitle: string }> = {
    orders: {
      eyebrow: "Операционный поток",
      title: "Заказы и экономика в одном слое",
      subtitle: "Смотри продажи, стратегическую цену, расходы и прибыль без разрыва между заказом и экономикой.",
    },
    problems: {
      eyebrow: "Контроль качества",
      title: "Проблемные заказы без шумных таблиц",
      subtitle: "Быстрый срез по проблемным кейсам, чтобы сразу видеть потери, причины и исключенные строки.",
    },
    tracking: {
      eyebrow: "План и факт",
      title: "Трекинг выполнения по периодам",
      subtitle: "Оценивай оборот, прибыль, рекламу и возвраты в динамике по месяцам и дням.",
    },
    sku: {
      eyebrow: "Ретроспектива товаров",
      title: "Товары во времени",
      subtitle: "Собирай выручку, прибыль и возвраты по SKU без перехода в отдельные отчеты.",
    },
    category: {
      eyebrow: "Ретроспектива категорий",
      title: "Категории во времени",
      subtitle: "Смотри, как категории двигают оборот и маржу по дням и месяцам.",
    },
  };
  const activeTabCopy = tabCopy[tab] || tabCopy.orders;
  const currentCurrencyLabel = (() => {
    if (tab === "tracking" || tab === "sku" || tab === "category") return "Аналитика: ₽";
    if (hasNativeCurrency) return "По заказам: ₽ · Справочно: $";
    return "По заказам: ₽";
  })();

  return (
    <WorkspacePageFrame className={s.pageFrame} innerClassName={s.pageFrameInner}>
      <WorkspacePageHero
        className={s.overviewHeroSurface}
        title="Обзор продаж"
        subtitle="Рабочее пространство по заказам, проблемам, трекингу, товарам и категориям."
        tabs={{
          items: overviewTabs.map((item) => ({ id: item.id, label: item.label })),
          activeId: tab,
          onChange: setTab,
        }}
        meta={(
          <div className={layoutStyles.heroMeta}>
            <span className={layoutStyles.metaChip}>{currentCurrencyLabel}</span>
          </div>
        )}
      >
        <div className={s.overviewHeroBody}>
          <div className={s.overviewControlDeck}>
            <div className={s.overviewControlCopy}>
              <div className={s.overviewEyebrow}>{activeTabCopy.eyebrow}</div>
              <h2 className={s.overviewHeroTitle}>{activeTabCopy.title}</h2>
              <p className={s.overviewHeroSubtitle}>{activeTabCopy.subtitle}</p>
            </div>
            <div className={s.overviewControlGrid}>
              <label className={s.overviewControlField}>
                <span className={s.overviewControlLabel}>{tab === "tracking" ? "Магазин трекинга" : "Магазин"}</span>
                {tab === "tracking" ? (
                  <select className={`input input-size-fluid ${s.dateInput}`} value={trackingStoreId} onChange={(e) => setTrackingStoreId(e.target.value)}>
                    {trackingStores.map((store: any) => (
                      <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                    ))}
                  </select>
                ) : (
                  <select className={`input input-size-fluid ${s.dateInput}`} value={storeId} onChange={(e) => setStoreId(e.target.value)}>
                    {stores.map((store: any) => (
                      <option key={store.store_uid} value={store.store_id}>{store.label}</option>
                    ))}
                  </select>
                )}
              </label>

              {tab === "tracking" || tab === "sku" || tab === "category" ? (
                <label className={s.overviewControlField}>
                  <span className={s.overviewControlLabel}>Срез данных</span>
                  <select className={`input input-size-fluid ${s.dateInput}`} value={dateMode} onChange={(e) => setDateMode(e.target.value)}>
                    <option value="created">По дате заказа</option>
                    <option value="delivery">По дате доставки</option>
                  </select>
                </label>
              ) : (
                <label className={s.overviewControlField}>
                  <span className={s.overviewControlLabel}>Период</span>
                  <select className={`input input-size-fluid ${s.dateInput}`} value={period} onChange={(e) => { setPage(1); setPeriod(e.target.value); }}>
                    {ORDERS_PERIOD_OPTIONS.map((option: any) => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                </label>
              )}

              {tab === "sku" || tab === "category" ? (
                <label className={s.overviewControlField}>
                  <span className={s.overviewControlLabel}>Гранулярность</span>
                  <select className={`input input-size-fluid ${s.dateInput}`} value={grain} onChange={(e) => setGrain(e.target.value)}>
                    <option value="month">По месяцам</option>
                    <option value="day">По дням</option>
                  </select>
                </label>
              ) : null}

              {tab === "category" ? (
                <label className={s.overviewControlField}>
                  <span className={s.overviewControlLabel}>Уровень категорий</span>
                  <select className={`input input-size-fluid ${s.dateInput}`} value={categoryLevel} onChange={(e) => setCategoryLevel(e.target.value)}>
                    <option value="level1">Уровень 1</option>
                    <option value="level2">Уровень 2</option>
                    <option value="level3">Уровень 3</option>
                  </select>
                </label>
              ) : null}

              {tab === "orders" ? (
                <label className={s.overviewControlField}>
                  <span className={s.overviewControlLabel}>Статус заказа</span>
                  <select className={`input input-size-fluid ${s.dateInput}`} value={itemStatus} onChange={(e) => { setPage(1); setItemStatus(e.target.value); }}>
                    <option value="">Все статусы</option>
                    {availableStatuses.map((status: string) => (
                      <option key={status} value={status}>{status}</option>
                    ))}
                  </select>
                </label>
              ) : null}
            </div>
          </div>
        </div>
      </WorkspacePageHero>
      {loading ? <div className={s.empty}>Загрузка...</div> : null}
      {error ? <div className={s.errorBox}>{error}</div> : null}

      {!loading && !error && tab === "orders" ? (
        <section>
          <PageSectionTitle title="Заказы" meta={`Всего: ${totalCount}`} />
          <div className={s.tableWrap}>
            <table className={s.table}>
              <thead>
                <tr>
                  <th>Дата</th><th>Заказ</th><th>SKU</th><th className={s.nameCell}>Наименование</th><th>Статус</th><th>Курс</th><th>Продажа</th><th>С соинвестом</th><th>Цена стратегии</th><th>Отклонение</th><th>Реклама</th><th>Себестоимость</th><th>Комиссия</th><th>Эквайринг</th><th>Логистика</th><th>Налог</th><th>Расходы</th><th>Прибыль</th>
                </tr>
              </thead>
              <tbody>
                {orderRows.length === 0 ? (
                  <tr><td colSpan={18} className={s.empty}>Нет заказов для выбранных параметров</td></tr>
                ) : orderRows.map((row: any) => {
                  const totalCosts = Number(row.commission || 0) + Number(row.acquiring || 0) + Number(row.delivery || 0) + Number(row.tax || 0) + Number(row.ads || 0);
                  return (
                    <tr key={`${row.order_id || ""}-${row.sku || ""}`}>
                      <td>{formatDateTime(row.order_created_at)}</td>
                      <td>{row.order_id || "—"}</td>
                      <td>{row.sku || "—"}</td>
                      <td className={s.nameCell}>{row.item_name || "—"}</td>
                      <td><span className={`${s.statusBadge} ${s[`tone_${statusTone(row.item_status)}` as keyof typeof s]}`}>{row.item_status || "—"}</span></td>
                      <td>{orderFxLabel(row)}</td>
                      <td>{moneyWithNative(row.sale_price, nativeValue(row, "sale_price"))}</td>
                      <td>{moneyWithNative(row.sale_price_with_coinvest, nativeValue(row, "sale_price_with_coinvest"))}</td>
                      <td><div>{moneyWithNative(row.strategy_installed_price, nativeValue(row, "strategy_installed_price"))}</div><div className={s.subtleText}>{formatDateTime(row.strategy_snapshot_at)}</div></td>
                      <td>
                        <div>{formatDelta(row.sale_price, row.strategy_installed_price, "RUB")}</div>
                        {hasNativeCurrency ? <div className={s.subtleText}>{formatDelta(nativeValue(row, "sale_price"), nativeValue(row, "strategy_installed_price"), priceCurrencyCode)}</div> : null}
                      </td>
                      <td>
                        {rubWithOptionalNative(row.ads, row.ads_native)}
                        <div className={s.subtleText}>
                          Буст план: {formatPercent(row.strategy_boost_bid_percent)} / Факт: {formatPercent(row.actual_market_boost_bid_percent)}
                        </div>
                        <div className={s.subtleText}>
                          Источник: {adsSourceLabel(row.ads_source)} {formatPercent(row.ads_rate_percent)}
                        </div>
                      </td>
                      <td>{rubWithOptionalNative(row.cogs_price, row.cogs_price_native)}</td>
                      <td><div>{rubWithOptionalNative(row.commission, row.commission_native)}</div><div className={s.subtleText}>{percentOfBase(row.commission, row.sale_price)}</div></td>
                      <td><div>{rubWithOptionalNative(row.acquiring, row.acquiring_native)}</div><div className={s.subtleText}>{percentOfBase(row.acquiring, row.sale_price)}</div></td>
                      <td>{rubWithOptionalNative(row.delivery, row.delivery_native)}</td>
                      <td><div>{rubWithOptionalNative(row.tax, row.tax_native)}</div><div className={s.subtleText}>{percentOfBase(row.tax, row.sale_price)}</div></td>
                      <td>{formatMoney(totalCosts, economicsCurrencyCode)}</td>
                      <td><div>{rubWithOptionalNative(row.profit, row.profit_native)}</div><div className={s.subtleText}>{percentOfBase(row.profit, row.sale_price)}</div></td>
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
                    <td>{formatDateTime(row.order_created_at)}</td><td>{formatDate(row.delivery_date)}</td><td>{row.order_id || "—"}</td><td>{row.sku || "—"}</td><td className={s.nameCell}>{row.item_name || "—"}</td><td><span className={`${s.statusBadge} ${s.tone_warn}`}>{row.item_status || "—"}</span></td><td>{moneyWithNative(row.sale_price, nativeValue(row, "sale_price"))}</td><td>{rubWithOptionalNative(row.cogs_price, row.cogs_price_native)}</td><td className={s.nameCell}>Доставлен, но нет себестоимости. Заказ исключён из чистой аналитики.</td>
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
                            <td><div>{formatMoney(month.revenue, "RUB")}</div>{month.revenue_plan_amount != null ? <div className={s.trackingPlanText}>План: {formatMoney(month.revenue_plan_amount, "RUB")}</div> : null}</td>
                            <td><div>{formatMoney(month.profit_amount, "RUB")}</div>{month.profit_plan_amount != null ? <div className={s.trackingPlanText}>План: {formatMoney(month.profit_plan_amount, "RUB")}</div> : null}</td>
                            <td>{formatPercent(month.profit_pct)}</td><td>{formatPercent(month.revenue && month.coinvest_amount ? (month.coinvest_amount / month.revenue) * 100 : 0)}</td><td>{formatPercent(month.returns_pct)}</td><td>{formatMoney(month.ads_amount, "RUB")}</td><td>{formatMoney(month.operational_errors, "RUB")}</td><td>{formatNumber(month.delivery_time_days)}</td>
                          </tr>
                          <tr className={s.trackingDaysHostRow}>
                            <td colSpan={9} className={s.trackingDaysHostCell}>
                              <div className={`${s.trackingDaysWrap} ${open ? s.trackingDaysWrapOpen : ""}`}>
                                {(month.days || []).length ? (
                                  <table className={s.trackingDaysTable}>
                                    <thead>
                                      <tr>
                                        <th className={s.nameCell}>День</th>
                                        <th>Оборот</th>
                                        <th>Прибыль</th>
                                        <th>Маржинальность</th>
                                        <th>Соинвест</th>
                                        <th>Возвраты</th>
                                        <th>Реклама</th>
                                        <th>Ошибки</th>
                                        <th>Доставка</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {(month.days || []).map((day: any) => (
                                        <tr key={`${month.month_key}-${day.date}`}>
                                          <td className={s.nameCell}>{formatDate(day.date)}</td>
                                          <td>{formatMoney(day.revenue, "RUB")}</td>
                                          <td>{formatMoney(day.profit_amount, "RUB")}</td>
                                          <td>{formatPercent(day.profit_pct)}</td>
                                          <td>{formatPercent(day.coinvest_pct)}</td>
                                          <td>{formatPercent(day.returns_pct)}</td>
                                          <td>{formatMoney(day.ads_amount, "RUB")}</td>
                                          <td>{formatMoney(day.operational_errors, "RUB")}</td>
                                          <td>{formatNumber(day.delivery_time_days)}</td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                ) : (
                                  <div className={s.empty}>Внутри месяца пока нет дневных строк</div>
                                )}
                              </div>
                            </td>
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
                    <td className={s.nameCell}>{row.category_path || "—"}</td><td>{formatMoney(row.revenue, "RUB")}</td><td>{formatMoney(row.profit_amount, "RUB")}</td><td>{formatPercent(row.profit_pct)}</td><td>{formatMoney(row.coinvest_amount, "RUB")}</td><td>{formatPercent(row.returns_pct)}</td>
                    <td className={s.nameCell}>{(row.periods || []).slice(0, 4).map((period: any) => <div key={`${row.key}-${period.period_key}`} className={s.subtleText}>{period.period_label}: {formatMoney(period.revenue, "RUB")} / {formatMoney(period.profit_amount, "RUB")}</div>)}</td>
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
              <thead><tr><th className={s.nameCell}>Категория</th><th className={s.nameCell}>Родитель</th><th>Оборот</th><th>Средний соинвест</th><th>Прибыль</th><th>Маржинальность</th><th>Периоды</th></tr></thead>
              <tbody>
                {categoryRows.length === 0 ? <tr><td colSpan={7} className={s.empty}>Нет данных по категориям</td></tr> : categoryRows.map((row: any) => (
                  <tr key={row.key}>
                    <td className={s.nameCell}>{row.label || row.category_path || "—"}</td><td className={s.nameCell}>{row.category_parent_path || "—"}</td><td>{formatMoney(row.revenue, "RUB")}</td><td>{formatPercent(row.coinvest_pct)}</td><td>{formatMoney(row.profit_amount, "RUB")}</td><td>{formatPercent(row.profit_pct)}</td>
                    <td className={s.nameCell}>{(row.periods || []).slice(0, 4).map((period: any) => <div key={`${row.key}-${period.period_key}`} className={s.subtleText}>{period.period_label}: {formatMoney(period.revenue, "RUB")} / {formatPercent(period.coinvest_pct)} / {formatMoney(period.profit_amount, "RUB")}</div>)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </WorkspacePageFrame>
  );
}
