"use client";

import { PanelCard } from "../../../../components/page/SectionKit";
import styles from "../DataSourcesPage.module.css";
import type { DeleteRequest, YandexAccount } from "../types";

type Props = {
  accounts: YandexAccount[];
  totalShops: number;
  actionAccount: YandexAccount | null;
  shopCheckLoading: Record<string, boolean>;
  flowSavingKey: string | null;
  currencySavingKey: string | null;
  fulfillmentSavingKey: string | null;
  ymActionBusinessId: string;
  setYmActionBusinessId: (value: string) => void;
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
  ymActionBusinessId,
  setYmActionBusinessId,
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
                          {idx === 0 ? (
                            <td
                              rowSpan={shops.length}
                              className={`${styles.ymBusinessCell} ${styles.ymClickableCell}`}
                              onClick={() => openEditAccount(acc)}
                              title="Открыть редактирование кабинета"
                            >
                              {acc.business_id}
                            </td>
                          ) : null}
                          <td className={styles.ymClickableCell} onClick={() => openEditAccount(acc)} title="Открыть редактирование кабинета">
                            {shop.campaign_id}
                          </td>
                          <td className={styles.ymClickableCell} onClick={() => openEditAccount(acc)} title="Открыть редактирование кабинета">
                            {shop.campaign_name || `Магазин ${shop.campaign_id}`}
                          </td>
                          <td className={styles.ymCurrencyCell}>
                            <select
                              className={`input ${styles.ymCurrencySelect}`}
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
                          <td className={styles.ymCurrencyCell}>
                            <select
                              className={`input ${styles.ymCurrencySelect}`}
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
                          <td className={styles.ymToggleCell}>
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
                          <td className={styles.ymToggleCell}>
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
                          <td className={styles.ymStatusCell}>
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
                          <td className={styles.ymDeleteCell}>
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
                    className={`input ${styles.ymAccountSelect}`}
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
