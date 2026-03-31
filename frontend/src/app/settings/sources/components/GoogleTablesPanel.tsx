import { PanelCard } from "../../../../components/page/SectionKit";
import styles from "../DataSourcesPage.module.css";
import type { DeleteRequest, SourceItem } from "../types";

type Props = {
  loading: boolean;
  sources: SourceItem[];
  gsSourceCheckLoading: Record<string, boolean>;
  sourceFlowSavingKey: string | null;
  openGsWizard: (item?: SourceItem, mode?: "create" | "edit-select") => void;
  openDeleteConfirm: (request: DeleteRequest) => void;
  updateSourceFlow: (
    sourceId: string,
    patch: Partial<Pick<SourceItem, "mode_import" | "mode_export">>,
    savingKey: string,
  ) => Promise<void>;
  checkGsheetSource: (sourceId: string) => Promise<void>;
  formatDateTime: (value?: string | null) => string;
};

export function GoogleTablesPanel({
  loading,
  sources,
  gsSourceCheckLoading,
  sourceFlowSavingKey,
  openGsWizard,
  openDeleteConfirm,
  updateSourceFlow,
  checkGsheetSource,
  formatDateTime,
}: Props) {
  return (
    <PanelCard
      title={`Google Таблицы (источников: ${sources.length})`}
      description="Подключение и проверка Google Sheets"
      action={
        <div className={styles.ymAccountActionsButtons}>
          <button className={`btn inline ${styles.ymAddAccountBtn}`} onClick={() => openGsWizard()}>
            Добавить источник
          </button>
          {sources.length > 0 ? (
            <button className="btn inline" onClick={() => openGsWizard(undefined, "edit-select")}>
              Редактировать таблицу
            </button>
          ) : null}
        </div>
      }
    >
      <div className={styles.ymContent}>
        {!loading && sources.length === 0 ? (
          <div className="integration-foot"><span className="muted-text">Еще нет подключенных источников.</span></div>
        ) : (
          <>
            <div className={`${styles.ymTableWrap} ${sources.length > 7 ? styles.ymTableScrollable : ""}`}>
              <table className={styles.ymTable}>
                <colgroup>
                  <col className={styles.ymColName} />
                  <col className={styles.gsColSheet} />
                  <col className={styles.ymColToggle} />
                  <col className={styles.ymColToggle} />
                  <col className={styles.ymColStatus} />
                  <col className={styles.ymColDelete} />
                </colgroup>
                <thead>
                  <tr>
                    <th>Наименование</th>
                    <th>Лист</th>
                    <th className={styles.ymToggleHead}>Импорт</th>
                    <th className={styles.ymToggleHead}>Экспорт</th>
                    <th className={styles.ymStatusHead}>Статус</th>
                    <th className={styles.ymActionHead}>Действия</th>
                  </tr>
                </thead>
                <tbody>
                  {sources.map((it) => (
                    <tr key={it.id} className="clickable-row" onClick={() => openGsWizard(it)}>
                      <td data-label="Наименование">{it.title || "-"}</td>
                      <td data-label="Лист">{it.worksheet || "-"}</td>
                      <td data-label="Импорт" className={styles.ymToggleCell}>
                        <div className={styles.ymToggleWrap}>
                          <span className={styles.ymToggleLabel}>OFF</span>
                          <button
                            type="button"
                            className={`toggle sm ${it.mode_import ? "on" : ""}`}
                            role="switch"
                            aria-checked={Boolean(it.mode_import)}
                            aria-label={`Импорт источника ${it.title || it.id}`}
                            disabled={sourceFlowSavingKey === `${it.id}:import`}
                            onClick={(e) => {
                              e.stopPropagation();
                              void updateSourceFlow(it.id, { mode_import: !Boolean(it.mode_import) }, `${it.id}:import`);
                            }}
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
                            className={`toggle sm ${it.mode_export ? "on" : ""}`}
                            role="switch"
                            aria-checked={Boolean(it.mode_export)}
                            aria-label={`Экспорт источника ${it.title || it.id}`}
                            disabled={sourceFlowSavingKey === `${it.id}:export`}
                            onClick={(e) => {
                              e.stopPropagation();
                              void updateSourceFlow(it.id, { mode_export: !Boolean(it.mode_export) }, `${it.id}:export`);
                            }}
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
                            onClick={(e) => {
                              e.stopPropagation();
                              void checkGsheetSource(it.id);
                            }}
                            disabled={Boolean(gsSourceCheckLoading[it.id])}
                            title="Проверить доступность источника"
                          >
                            <span className={`refresh-icon ${gsSourceCheckLoading[it.id] ? "spinning" : ""}`}>↻</span>
                          </button>
                          <span className={`pill ${it.health_status === "error" ? "err" : it.health_status === "ok" ? "ok" : "warn"}`}>
                            {it.health_status === "error" ? "Ошибка" : it.health_status === "ok" ? "Доступен" : "Не проверен"}
                          </span>
                        </div>
                        <div className={styles.ymStatusTime}>Последнее обновление:</div>
                        <div className={styles.ymStatusTimeValue}>{formatDateTime(it.health_checked_at)}</div>
                        {it.health_status === "error" && it.health_message ? (
                          <div className={styles.ymStatusError}>{it.health_message}</div>
                        ) : null}
                      </td>
                      <td data-label="Действия" className={styles.ymDeleteCell}>
                        <button
                          className={`btn inline ${styles.ymShopDeleteBtn}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            openDeleteConfirm({
                              type: "gsheet_source",
                              source_id: it.id,
                              name: it.title || it.id,
                            });
                          }}
                        >
                          Удалить
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </PanelCard>
  );
}
