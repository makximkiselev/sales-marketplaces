import { PanelCard } from "../../../../components/page/SectionKit";
import styles from "../DataSourcesPage.module.css";
import type { DeleteRequest, StoreSourceBinding, YandexAccount } from "../types";

type Props = {
  accounts: YandexAccount[];
  totalShops: number;
  actionAccount: YandexAccount | null;
  shopCheckLoading: Record<string, boolean>;
  flowSavingKey: string | null;
  currencySavingKey: string | null;
  fulfillmentSavingKey: string | null;
  sourceBindingSavingKey: string | null;
  storeSourceBindings: Record<string, StoreSourceBinding>;
  ymActionBusinessId: string;
  setYmActionBusinessId: (value: string) => void;
  openStoreSourceModal: (params: {
    target: "cogs" | "stock";
    platform: "yandex_market";
    storeId: string;
    storeName: string;
  }) => void;
  openWizard: () => void;
  openEditAccount: (account: YandexAccount) => void;
  openAddShop: (account: YandexAccount) => void;
  openDeleteConfirm: (request: DeleteRequest) => void;
  updateStoreFulfillment: (payload: {
    platform: "yandex_market";
    business_id: string;
    campaign_id: string;
    fulfillment_model: "FBO" | "FBS" | "DBS" | "EXPRESS";
  }) => Promise<void>;
  updateStoreCurrency: (payload: {
    platform: "yandex_market";
    business_id: string;
    campaign_id: string;
    currency_code: "RUB" | "USD";
  }) => Promise<void>;
  updateDataFlow: (
    payload: {
      scope: "shop";
      platform: "yandex_market";
      business_id: string;
      campaign_id: string;
      import_enabled?: boolean;
      export_enabled?: boolean;
    },
    savingKey: string,
  ) => Promise<void>;
  checkShop: (campaignId: string, businessId: string) => Promise<void>;
  formatDateTime: (value?: string | null) => string;
  description: string;
};

export function YandexMarketPanel({
  accounts,
  totalShops,
  actionAccount,
  shopCheckLoading,
  flowSavingKey,
  currencySavingKey,
  fulfillmentSavingKey,
  sourceBindingSavingKey,
  storeSourceBindings,
  ymActionBusinessId,
  setYmActionBusinessId,
  openStoreSourceModal,
  openWizard,
  openEditAccount,
  openAddShop,
  openDeleteConfirm,
  updateStoreFulfillment,
  updateStoreCurrency,
  updateDataFlow,
  checkShop,
  formatDateTime,
  description,
}: Props) {
  const shops = accounts.flatMap((account) => account.shops || []);
  const healthyShops = shops.filter((shop) => shop.health_status === "ok").length;
  const errorShops = shops.filter((shop) => shop.health_status === "error").length;
  const importEnabled = shops.filter((shop) => Boolean(shop.data_flow?.import_enabled)).length;
  const exportEnabled = shops.filter((shop) => Boolean(shop.data_flow?.export_enabled)).length;

  return (
    <PanelCard
      title={`Яндекс.Маркет (аккаунтов: ${accounts.length})`}
      description={description}
      action={
        <button className={`btn inline ${styles.ymAddAccountBtn}`} onClick={openWizard} title="Добавить кабинет" aria-label="Добавить кабинет">
          Добавить кабинет
        </button>
      }
    >
      <div className={styles.ymContent}>
        <div className={styles.sourceSummaryRow}>
          <div className={styles.sourceSummaryCard}>
            <div className={styles.sourceSummaryLabel}>Кабинеты</div>
            <div className={styles.sourceSummaryValue}>{accounts.length}</div>
            <div className={styles.sourceSummaryMeta}>Магазинов: {shops.length}</div>
          </div>
          <div className={styles.sourceSummaryCard}>
            <div className={styles.sourceSummaryLabel}>Статус</div>
            <div className={styles.sourceSummaryValue}>{healthyShops}</div>
            <div className={styles.sourceSummaryMeta}>{errorShops > 0 ? `Ошибок: ${errorShops}` : "Все доступные магазины в норме"}</div>
          </div>
          <div className={styles.sourceSummaryCard}>
            <div className={styles.sourceSummaryLabel}>Режим обмена</div>
            <div className={styles.sourceSummaryValue}>{importEnabled}/{exportEnabled}</div>
            <div className={styles.sourceSummaryMeta}>Импорт / экспорт по магазинам</div>
          </div>
        </div>
        {accounts.length > 0 ? (
          <>
            <div className={`${styles.ymTableWrap} ${totalShops > 5 ? styles.ymTableScrollable : ""}`}>
              <table className={styles.ymTable}>
                <colgroup>
                  <col className={styles.ymColBusiness} />
                  <col className={styles.ymColCampaign} />
                  <col className={styles.ymColName} />
                  <col className={styles.ymColModel} />
                  <col className={styles.ymColCurrency} />
                  <col className={styles.ymColSource} />
                  <col className={styles.ymColSource} />
                  <col className={styles.ymColToggle} />
                  <col className={styles.ymColToggle} />
                  <col className={styles.ymColStatus} />
                  <col className={styles.ymColDelete} />
                </colgroup>
                <thead>
                  <tr>
                    <th>Business ID</th>
                    <th>Campaign ID</th>
                    <th>Наименование</th>
                    <th>Модель</th>
                    <th>Валюта</th>
                    <th>Себестоимость</th>
                    <th>Остаток</th>
                    <th className={styles.ymToggleHead}>Импорт</th>
                    <th className={styles.ymToggleHead}>Экспорт</th>
                    <th className={styles.ymStatusHead}>Статус</th>
                    <th className={styles.ymActionHead}>Действия</th>
                  </tr>
                </thead>
                <tbody>
                  {accounts.map((acc) => {
                    const shops = acc.shops || [];
                    if (shops.length === 0) {
                      return (
                        <tr key={`ym-empty-${acc.business_id}`}>
                          <td className={styles.ymBusinessCell}>{acc.business_id}</td>
                          <td className={styles.ymMutedCell}>-</td>
                          <td className={styles.ymMutedCell}>Нет подключенных магазинов</td>
                          <td className={styles.ymMutedCell}>-</td>
                          <td className={styles.ymMutedCell}>-</td>
                          <td className={styles.ymMutedCell}>-</td>
                          <td className={styles.ymMutedCell}>-</td>
                          <td className={styles.ymMutedCell}>-</td>
                          <td className={styles.ymMutedCell}>-</td>
                        </tr>
                      );
                    }
                    return shops.map((shop, idx) => {
                      const checkKey = `${acc.business_id}:${shop.campaign_id}`;
                      const sourceKey = `yandex_market:${shop.campaign_id}`;
                      const sourceBinding = storeSourceBindings[sourceKey];
                      const isChecking = Boolean(shopCheckLoading[checkKey]);
                      const statusClass =
                        shop.health_status === "error" ? "err" : shop.health_status === "ok" ? "ok" : "warn";
                      const statusLabel =
                        shop.health_status === "error"
                          ? "Ошибка"
                          : shop.health_status === "ok"
                            ? "Доступен"
                            : "Не проверен";

                      return (
                        <tr key={`${acc.business_id}-${shop.campaign_id}`}>
                          <td
                            data-label="Business ID"
                            className={`${styles.ymBusinessCell} ${styles.ymClickableCell}`}
                            onClick={() => openEditAccount(acc)}
                            title="Открыть редактирование кабинета"
                          >
                            {acc.business_id}
                          </td>
                          <td data-label="Campaign ID" className={styles.ymClickableCell} onClick={() => openEditAccount(acc)} title="Открыть редактирование кабинета">
                            {shop.campaign_id}
                          </td>
                          <td data-label="Наименование" className={styles.ymClickableCell} onClick={() => openEditAccount(acc)} title="Открыть редактирование кабинета">
                            {shop.campaign_name || `Магазин ${shop.campaign_id}`}
                          </td>
                          <td data-label="Модель" className={styles.ymCurrencyCell}>
                            <select
                              className={`input input-size-sm ${styles.sourceInlineSelect}`}
                              value={String(shop.fulfillment_model || "FBO").toUpperCase()}
                              disabled={fulfillmentSavingKey === `fulfill-ym-${acc.business_id}-${shop.campaign_id}`}
                              onChange={(e) =>
                                void updateStoreFulfillment({
                                  platform: "yandex_market",
                                  business_id: acc.business_id,
                                  campaign_id: shop.campaign_id,
                                  fulfillment_model: e.target.value as "FBO" | "FBS" | "DBS" | "EXPRESS",
                                })
                              }
                            >
                              <option value="FBO">FBO</option>
                              <option value="FBS">FBS</option>
                              <option value="DBS">DBS</option>
                              <option value="EXPRESS">EXPRESS</option>
                            </select>
                          </td>
                          <td data-label="Валюта" className={styles.ymCurrencyCell}>
                            <select
                              className={`input input-size-sm ${styles.sourceInlineSelect}`}
                              value={(shop.currency_code || "RUB").toUpperCase()}
                              disabled={currencySavingKey === `currency-ym-${acc.business_id}-${shop.campaign_id}`}
                              onChange={(e) =>
                                void updateStoreCurrency({
                                  platform: "yandex_market",
                                  business_id: acc.business_id,
                                  campaign_id: shop.campaign_id,
                                  currency_code: e.target.value as "RUB" | "USD",
                                })
                              }
                            >
                              <option value="RUB">RUB</option>
                              <option value="USD">USD</option>
                            </select>
                          </td>
                          <td data-label="Себестоимость" className={styles.ymSourceCell}>
                            <div className={styles.storeSourceCell}>
                              <div className={styles.storeSourceName}>
                                {sourceBinding?.cogsSource?.sourceName || "Не выбран"}
                              </div>
                              <div className={styles.storeSourceMeta}>
                                {sourceBinding?.cogsSource ? (
                                  <>
                                    <span>SKU: {sourceBinding.cogsSource.skuColumn}</span>
                                    <span>Значение: {sourceBinding.cogsSource.valueColumn}</span>
                                  </>
                                ) : (
                                  <span>Источник не настроен</span>
                                )}
                              </div>
                              <button
                                type="button"
                                className={`btn inline ${styles.storeSourceButton}`}
                                disabled={sourceBindingSavingKey === `cogs:yandex_market:${shop.campaign_id}`}
                                onClick={() =>
                                  openStoreSourceModal({
                                    target: "cogs",
                                    platform: "yandex_market",
                                    storeId: shop.campaign_id,
                                    storeName: shop.campaign_name || `Магазин ${shop.campaign_id}`,
                                  })
                                }
                              >
                                {sourceBinding?.cogsSource ? "Изменить" : "Выбрать"}
                              </button>
                            </div>
                          </td>
                          <td data-label="Остаток" className={styles.ymSourceCell}>
                            <div className={styles.storeSourceCell}>
                              <div className={styles.storeSourceName}>
                                {sourceBinding?.stockSource?.sourceName || "Не выбран"}
                              </div>
                              <div className={styles.storeSourceMeta}>
                                {sourceBinding?.stockSource ? (
                                  <>
                                    <span>SKU: {sourceBinding.stockSource.skuColumn}</span>
                                    <span>Значение: {sourceBinding.stockSource.valueColumn}</span>
                                  </>
                                ) : (
                                  <span>Источник не настроен</span>
                                )}
                              </div>
                              <button
                                type="button"
                                className={`btn inline ${styles.storeSourceButton}`}
                                disabled={sourceBindingSavingKey === `stock:yandex_market:${shop.campaign_id}`}
                                onClick={() =>
                                  openStoreSourceModal({
                                    target: "stock",
                                    platform: "yandex_market",
                                    storeId: shop.campaign_id,
                                    storeName: shop.campaign_name || `Магазин ${shop.campaign_id}`,
                                  })
                                }
                              >
                                {sourceBinding?.stockSource ? "Изменить" : "Выбрать"}
                              </button>
                            </div>
                          </td>
                          <td data-label="Импорт" className={styles.ymToggleCell}>
                            <div className={styles.ymToggleWrap}>
                              <span className={styles.ymToggleLabel}>OFF</span>
                              <button
                                type="button"
                                className={`toggle sm ${shop.data_flow?.import_enabled ? "on" : ""}`}
                                role="switch"
                                aria-checked={Boolean(shop.data_flow?.import_enabled)}
                                aria-label={`Импорт магазина ${shop.campaign_name || shop.campaign_id}`}
                                disabled={flowSavingKey === `shop-import-${acc.business_id}-${shop.campaign_id}`}
                                onClick={() =>
                                  void updateDataFlow(
                                    {
                                      scope: "shop",
                                      platform: "yandex_market",
                                      business_id: acc.business_id,
                                      campaign_id: shop.campaign_id,
                                      import_enabled: !Boolean(shop.data_flow?.import_enabled),
                                    },
                                    `shop-import-${acc.business_id}-${shop.campaign_id}`,
                                  )
                                }
                              >
                                <span className="toggle-track"><span className="toggle-thumb" /></span>
                              </button>
                              <span className={styles.ymToggleLabel}>ON</span>
                            </div>
                          </td>
                          <td data-label="Экспорт" className={styles.ymToggleCell}>
                            <div className={styles.ymToggleWrap}>
                              <span className={styles.ymToggleLabel}>OFF</span>
                              <button
                                type="button"
                                className={`toggle sm ${shop.data_flow?.export_enabled ? "on" : ""}`}
                                role="switch"
                                aria-checked={Boolean(shop.data_flow?.export_enabled)}
                                aria-label={`Экспорт магазина ${shop.campaign_name || shop.campaign_id}`}
                                disabled={flowSavingKey === `shop-export-${acc.business_id}-${shop.campaign_id}`}
                                onClick={() =>
                                  void updateDataFlow(
                                    {
                                      scope: "shop",
                                      platform: "yandex_market",
                                      business_id: acc.business_id,
                                      campaign_id: shop.campaign_id,
                                      export_enabled: !Boolean(shop.data_flow?.export_enabled),
                                    },
                                    `shop-export-${acc.business_id}-${shop.campaign_id}`,
                                  )
                                }
                              >
                                <span className="toggle-track"><span className="toggle-thumb" /></span>
                              </button>
                              <span className={styles.ymToggleLabel}>ON</span>
                            </div>
                          </td>
                          <td data-label="Статус" className={styles.ymStatusCell}>
                            <div className={styles.ymStatusTop}>
                              <button
                                className="btn inline icon-only"
                                onClick={() => void checkShop(shop.campaign_id, acc.business_id)}
                                disabled={isChecking}
                                title="Проверить подключение"
                              >
                                <span className={`refresh-icon ${isChecking ? "spinning" : ""}`}>↻</span>
                              </button>
                              <span className={`pill ${statusClass}`}>{statusLabel}</span>
                            </div>
                            <div className={styles.ymStatusTime}>Последнее обновление:</div>
                            <div className={styles.ymStatusTimeValue}>{formatDateTime(shop.health_checked_at)}</div>
                            {shop.health_status === "error" && shop.health_message ? (
                              <div className={styles.ymStatusError}>{shop.health_message}</div>
                            ) : null}
                          </td>
                          <td data-label="Действия" className={styles.ymDeleteCell}>
                            <button
                              className={`btn inline ${styles.ymShopDeleteBtn}`}
                              onClick={() =>
                                openDeleteConfirm({
                                  type: "yandex_shop",
                                  business_id: acc.business_id,
                                  campaign_id: shop.campaign_id,
                                  name: shop.campaign_name || `Магазин ${shop.campaign_id}`,
                                })
                              }
                              title="Удалить магазин"
                              aria-label="Удалить магазин"
                            >
                              Удалить
                            </button>
                          </td>
                        </tr>
                      );
                    });
                  })}
                </tbody>
              </table>
            </div>

            <div className={styles.ymAccountActionsList}>
              <div className={styles.ymAccountActionsRow}>
                {accounts.length > 1 ? (
                  <select
                    className={`input input-size-md ${styles.sourceAccountSelect}`}
                    value={ymActionBusinessId || ""}
                    onChange={(e) => setYmActionBusinessId(e.target.value)}
                  >
                    {accounts.map((acc) => (
                      <option key={acc.business_id} value={acc.business_id}>
                        Кабинет {acc.business_id}
                      </option>
                    ))}
                  </select>
                ) : null}
                <div className={styles.ymAccountActionsButtons}>
                  <button className="btn inline" disabled={!actionAccount} onClick={() => actionAccount && openEditAccount(actionAccount)}>
                    Редактировать кабинет
                  </button>
                  <button className="btn inline" disabled={!actionAccount} onClick={() => actionAccount && void openAddShop(actionAccount)}>
                    Добавить магазин
                  </button>
                  <button
                    className={`btn inline ${styles.ymDeleteAccountBtn}`}
                    disabled={!actionAccount}
                    onClick={() =>
                      actionAccount &&
                      openDeleteConfirm({
                        type: "yandex_account",
                        business_id: actionAccount.business_id,
                      })
                    }
                  >
                    Удалить аккаунт
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : (
          <div className="integration-foot">
            <span className="muted-text">Аккаунты не подключены</span>
          </div>
        )}
      </div>
    </PanelCard>
  );
}
