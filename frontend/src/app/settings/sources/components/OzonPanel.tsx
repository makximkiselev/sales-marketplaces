"use client";

import { PanelCard } from "../../../../components/page/SectionKit";
import styles from "../DataSourcesPage.module.css";
import type { DeleteRequest, OzonAccount } from "../types";

type Props = {
  accounts: OzonAccount[];
  actionAccount: OzonAccount | null;
  ozActionClientId: string;
  setOzActionClientId: (value: string) => void;
  ozCheckLoading: Record<string, boolean>;
  flowSavingKey: string | null;
  currencySavingKey: string | null;
  fulfillmentSavingKey: string | null;
  openOzonWizard: (account?: {
    client_id: string;
    api_key?: string;
    seller_id?: string;
    seller_name?: string;
  }) => void;
  openDeleteConfirm: (request: DeleteRequest) => void;
  updateStoreFulfillment: (payload: {
    platform: "ozon";
    client_id: string;
    fulfillment_model: "FBO" | "FBS" | "DBS" | "EXPRESS";
  }) => Promise<void>;
  updateStoreCurrency: (payload: {
    platform: "ozon";
    client_id: string;
    currency_code: "RUB" | "USD";
  }) => Promise<void>;
  updateDataFlow: (
    payload: {
      scope: "account";
      platform: "ozon";
      business_id: string;
      import_enabled?: boolean;
      export_enabled?: boolean;
    },
    savingKey: string,
  ) => Promise<void>;
  checkOzonAccount: (clientId: string) => Promise<void>;
  formatDateTime: (value?: string | null) => string;
  description: string;
};

export function OzonPanel({
  accounts,
  actionAccount,
  ozActionClientId,
  setOzActionClientId,
  ozCheckLoading,
  flowSavingKey,
  currencySavingKey,
  fulfillmentSavingKey,
  openOzonWizard,
  openDeleteConfirm,
  updateStoreFulfillment,
  updateStoreCurrency,
  updateDataFlow,
  checkOzonAccount,
  formatDateTime,
  description,
}: Props) {
  return (
    <PanelCard
      title={`Ozon (кабинетов: ${accounts.length})`}
      description={description}
      action={
        <button className={`btn inline ${styles.ymAddAccountBtn}`} onClick={() => openOzonWizard()}>
          Добавить кабинет
        </button>
      }
    >
      <div className={styles.ymContent}>
        {accounts.length > 0 ? (
          <>
            <div className={`${styles.ymTableWrap} ${accounts.length > 6 ? styles.ymTableScrollable : ""}`}>
              <table className={styles.ymTable}>
                <colgroup>
                  <col className={styles.ozColClient} />
                  <col className={styles.ozColSeller} />
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
                    <th>Client ID</th>
                    <th>Seller ID</th>
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
                    const isChecking = Boolean(ozCheckLoading[acc.client_id]);
                    const statusClass = acc.health_status === "error" ? "err" : acc.health_status === "ok" ? "ok" : "warn";
                    const statusLabel = acc.health_status === "error" ? "Ошибка" : acc.health_status === "ok" ? "Доступен" : "Не проверен";
                    return (
                      <tr key={acc.client_id}>
                        <td
                          className={`${styles.ymBusinessCell} ${styles.ymClickableCell}`}
                          onClick={() =>
                            openOzonWizard({
                              client_id: acc.client_id,
                              api_key: acc.api_key,
                              seller_id: acc.seller_id,
                              seller_name: acc.seller_name,
                            })
                          }
                          title="Открыть редактирование кабинета"
                        >
                          {acc.client_id}
                        </td>
                        <td
                          className={styles.ymClickableCell}
                          onClick={() =>
                            openOzonWizard({
                              client_id: acc.client_id,
                              api_key: acc.api_key,
                              seller_id: acc.seller_id,
                              seller_name: acc.seller_name,
                            })
                          }
                          title="Открыть редактирование кабинета"
                        >
                          {acc.seller_id || "-"}
                        </td>
                        <td
                          className={styles.ymClickableCell}
                          onClick={() =>
                            openOzonWizard({
                              client_id: acc.client_id,
                              api_key: acc.api_key,
                              seller_id: acc.seller_id,
                              seller_name: acc.seller_name,
                            })
                          }
                          title="Открыть редактирование кабинета"
                        >
                          {acc.seller_name || `Ozon кабинет ${acc.client_id}`}
                        </td>
                        <td className={styles.ymCurrencyCell}>
                          <select
                            className={`input ${styles.ymCurrencySelect}`}
                            value={String(acc.fulfillment_model || "FBO").toUpperCase()}
                            disabled={fulfillmentSavingKey === `fulfill-oz-${acc.client_id}`}
                            onChange={(e) =>
                              void updateStoreFulfillment({
                                platform: "ozon",
                                client_id: acc.client_id,
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
                            value={(acc.currency_code || "RUB").toUpperCase()}
                            disabled={currencySavingKey === `currency-oz-${acc.client_id}`}
                            onChange={(e) =>
                              void updateStoreCurrency({
                                platform: "ozon",
                                client_id: acc.client_id,
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
                              className={`toggle sm ${acc.data_flow?.import_enabled ? "on" : ""}`}
                              role="switch"
                              aria-checked={Boolean(acc.data_flow?.import_enabled)}
                              aria-label={`Импорт Ozon кабинета ${acc.client_id}`}
                              disabled={flowSavingKey === `ozon-account-import-${acc.client_id}`}
                              onClick={() =>
                                void updateDataFlow(
                                  {
                                    scope: "account",
                                    platform: "ozon",
                                    business_id: acc.client_id,
                                    import_enabled: !Boolean(acc.data_flow?.import_enabled),
                                  },
                                  `ozon-account-import-${acc.client_id}`,
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
                              className={`toggle sm ${acc.data_flow?.export_enabled ? "on" : ""}`}
                              role="switch"
                              aria-checked={Boolean(acc.data_flow?.export_enabled)}
                              aria-label={`Экспорт Ozon кабинета ${acc.client_id}`}
                              disabled={flowSavingKey === `ozon-account-export-${acc.client_id}`}
                              onClick={() =>
                                void updateDataFlow(
                                  {
                                    scope: "account",
                                    platform: "ozon",
                                    business_id: acc.client_id,
                                    export_enabled: !Boolean(acc.data_flow?.export_enabled),
                                  },
                                  `ozon-account-export-${acc.client_id}`,
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
                              onClick={() => void checkOzonAccount(acc.client_id)}
                              disabled={isChecking}
                              title="Проверить подключение Ozon"
                            >
                              <span className={`refresh-icon ${isChecking ? "spinning" : ""}`}>↻</span>
                            </button>
                            <span className={`pill ${statusClass}`}>{statusLabel}</span>
                          </div>
                          <div className={styles.ymStatusTime}>Последнее обновление:</div>
                          <div className={styles.ymStatusTimeValue}>{formatDateTime(acc.health_checked_at)}</div>
                          {acc.health_status === "error" && acc.health_message ? (
                            <div className={styles.ymStatusError}>{acc.health_message}</div>
                          ) : null}
                        </td>
                        <td className={styles.ymDeleteCell}>
                          <button
                            className={`btn inline ${styles.ymShopDeleteBtn}`}
                            onClick={() =>
                              openDeleteConfirm({
                                type: "ozon_account",
                                client_id: acc.client_id,
                                name: acc.seller_name || `Ozon кабинет ${acc.client_id}`,
                              })
                            }
                          >
                            Удалить
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div className={styles.ymAccountActionsList}>
              <div className={styles.ymAccountActionsRow}>
                {accounts.length > 1 ? (
                  <select
                    className={`input ${styles.ymAccountSelect}`}
                    value={ozActionClientId || ""}
                    onChange={(e) => setOzActionClientId(e.target.value)}
                  >
                    {accounts.map((acc) => (
                      <option key={acc.client_id} value={acc.client_id}>
                        Кабинет {acc.client_id}
                      </option>
                    ))}
                  </select>
                ) : null}
                <div className={styles.ymAccountActionsButtons}>
                  <button
                    className="btn inline"
                    disabled={!actionAccount}
                    onClick={() =>
                      actionAccount &&
                      openOzonWizard({
                        client_id: actionAccount.client_id,
                        api_key: actionAccount.api_key,
                        seller_id: actionAccount.seller_id,
                        seller_name: actionAccount.seller_name,
                      })
                    }
                  >
                    Редактировать кабинет
                  </button>
                  <button
                    className={`btn inline ${styles.ymDeleteAccountBtn}`}
                    disabled={!actionAccount}
                    onClick={() =>
                      actionAccount &&
                      openDeleteConfirm({
                        type: "ozon_account",
                        client_id: actionAccount.client_id,
                        name: actionAccount.seller_name || `Ozon кабинет ${actionAccount.client_id}`,
                      })
                    }
                  >
                    Удалить кабинет
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : (
          <div className="integration-foot">
            <span className="muted-text">Кабинеты не подключены</span>
          </div>
        )}
      </div>
    </PanelCard>
  );
}
