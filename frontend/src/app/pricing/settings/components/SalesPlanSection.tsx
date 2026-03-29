"use client";

import { useEffect, useMemo, useState } from "react";
import { SectionBlock } from "../../../../components/page/SectionKit";
import styles from "../PricingSettingsPage.module.css";
import type { SalesPlanRowApi } from "../types";

type PlanDraft = {
  earning_mode: "profit" | "margin";
  strategy_mode: "mix" | "mrc";
  planned_revenue: string;
  target_drr_percent: string;
  target_profit_rub: string;
  target_profit_percent: string;
  minimum_profit_percent: string;
  target_margin_rub: string;
  target_margin_percent: string;
  updated_at?: string | null;
};

type Props = {
  loading: boolean;
  error: string;
  rows: SalesPlanRowApi[];
  savingMap: Record<string, boolean>;
  saveError: string;
  onSaveRows: (items: Array<{ row: SalesPlanRowApi; values: Record<string, unknown> }>) => Promise<void>;
};

function toDraft(row: SalesPlanRowApi): PlanDraft {
  return {
    earning_mode: row.earning_mode === "margin" ? "margin" : "profit",
    strategy_mode: row.strategy_mode === "mrc" ? "mrc" : "mix",
    planned_revenue: row.planned_revenue == null ? "" : String(row.planned_revenue),
    target_drr_percent: row.target_drr_percent == null ? "" : String(row.target_drr_percent),
    target_profit_rub: row.target_profit_rub == null ? "" : String(row.target_profit_rub),
    target_profit_percent: row.target_profit_percent == null ? "" : String(row.target_profit_percent),
    minimum_profit_percent: row.minimum_profit_percent == null ? "" : String(row.minimum_profit_percent),
    target_margin_rub: row.target_margin_rub == null ? "" : String(row.target_margin_rub),
    target_margin_percent: row.target_margin_percent == null ? "" : String(row.target_margin_percent),
    updated_at: row.updated_at,
  };
}

function toNum(value: string) {
  if (value == null || value.trim() === "") return null;
  const parsed = Number(String(value).replace(/\s+/g, "").replace(",", "."));
  return Number.isFinite(parsed) ? parsed : null;
}

function fmtCurrencySign(currencyCode: string) {
  return String(currencyCode || "").toUpperCase() === "USD" ? "$" : "₽";
}

function formatGrouped(value: string) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const normalized = raw.replace(/\s+/g, "").replace(",", ".");
  if (!/^-?\d*\.?\d*$/.test(normalized)) return raw;
  const sign = normalized.startsWith("-") ? "-" : "";
  const unsigned = sign ? normalized.slice(1) : normalized;
  const [intPartRaw, fracPart] = unsigned.split(".");
  const intPart = String(intPartRaw || "0").replace(/^0+(?=\d)/, "") || "0";
  const grouped = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, " ");
  return fracPart != null && fracPart !== "" ? `${sign}${grouped}.${fracPart}` : `${sign}${grouped}`;
}

function formatEditableNumber(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "";
  return String(Math.round(value * 100) / 100);
}

function getActiveRubValue(draft: PlanDraft) {
  return draft.earning_mode === "margin" ? draft.target_margin_rub : draft.target_profit_rub;
}

function getActivePctValue(draft: PlanDraft) {
  return draft.earning_mode === "margin" ? draft.target_margin_percent : draft.target_profit_percent;
}

function setActiveRubValue(draft: PlanDraft, value: string) {
  if (draft.earning_mode === "margin") return { ...draft, target_margin_rub: value };
  return { ...draft, target_profit_rub: value };
}

function setActivePctValue(draft: PlanDraft, value: string) {
  if (draft.earning_mode === "margin") return { ...draft, target_margin_percent: value };
  return { ...draft, target_profit_percent: value };
}

function toComparablePayload(draft: PlanDraft) {
  return {
    earning_mode: draft.earning_mode,
    strategy_mode: draft.strategy_mode,
    planned_revenue: toNum(draft.planned_revenue),
    target_drr_percent: toNum(draft.target_drr_percent),
    target_profit_rub: toNum(draft.target_profit_rub),
    target_profit_percent: toNum(draft.target_profit_percent),
    minimum_profit_percent: toNum(draft.minimum_profit_percent),
    target_margin_rub: toNum(draft.target_margin_rub),
    target_margin_percent: toNum(draft.target_margin_percent),
  };
}

function isDraftChanged(row: SalesPlanRowApi, draft: PlanDraft) {
  return JSON.stringify(toComparablePayload(draft)) !== JSON.stringify({
    earning_mode: row.earning_mode === "margin" ? "margin" : "profit",
    strategy_mode: row.strategy_mode === "mrc" ? "mrc" : "mix",
    planned_revenue: row.planned_revenue == null ? null : Number(row.planned_revenue),
    target_drr_percent: row.target_drr_percent == null ? null : Number(row.target_drr_percent),
    target_profit_rub: row.target_profit_rub == null ? null : Number(row.target_profit_rub),
    target_profit_percent: row.target_profit_percent == null ? null : Number(row.target_profit_percent),
    minimum_profit_percent: row.minimum_profit_percent == null ? null : Number(row.minimum_profit_percent),
    target_margin_rub: row.target_margin_rub == null ? null : Number(row.target_margin_rub),
    target_margin_percent: row.target_margin_percent == null ? null : Number(row.target_margin_percent),
  });
}

export function SalesPlanSection({ loading, error, rows, savingMap, saveError, onSaveRows }: Props) {
  const [drafts, setDrafts] = useState<Record<string, PlanDraft>>({});
  const [focusedCell, setFocusedCell] = useState<string>("");
  const [floatingNotice, setFloatingNotice] = useState<string>("");

  useEffect(() => {
    const next: Record<string, PlanDraft> = {};
    for (const row of rows) next[row.store_uid] = toDraft(row);
    setDrafts(next);
  }, [rows]);

  const rowsById = useMemo(() => {
    const map = new Map<string, SalesPlanRowApi>();
    for (const row of rows) map.set(row.store_uid, row);
    return map;
  }, [rows]);

  function patchDraft(storeUid: string, updater: (prev: PlanDraft) => PlanDraft) {
    setDrafts((prev) => {
      const current = prev[storeUid] || toDraft(rowsById.get(storeUid) as SalesPlanRowApi);
      return { ...prev, [storeUid]: updater(current) };
    });
  }

  function recalcByRub(prev: PlanDraft, nextRub: string) {
    const next = setActiveRubValue(prev, nextRub);
    const rub = toNum(nextRub);
    if (rub == null) return next;
    const pctText = getActivePctValue(next);
    const pct = toNum(pctText);
    if (pct != null && pct > 0) {
      return {
        ...next,
        planned_revenue: formatEditableNumber((rub * 100) / pct),
      };
    }
    const plannedRevenue = toNum(next.planned_revenue);
    if (plannedRevenue != null && plannedRevenue > 0) {
      return setActivePctValue(next, formatEditableNumber((rub / plannedRevenue) * 100));
    }
    return next;
  }

  function recalcByPercent(prev: PlanDraft, nextPct: string) {
    const next = setActivePctValue(prev, nextPct);
    const pct = toNum(nextPct);
    if (pct == null) return next;
    const rubText = getActiveRubValue(next);
    const rub = toNum(rubText);
    if (rub != null && pct > 0) {
      return {
        ...next,
        planned_revenue: formatEditableNumber((rub * 100) / pct),
      };
    }
    const plannedRevenue = toNum(next.planned_revenue);
    if (plannedRevenue != null) {
      return setActiveRubValue(next, formatEditableNumber((plannedRevenue * pct) / 100));
    }
    return next;
  }

  function recalcByRevenue(prev: PlanDraft, nextRevenue: string) {
    const next = { ...prev, planned_revenue: nextRevenue };
    const plannedRevenue = toNum(nextRevenue);
    if (plannedRevenue == null) return next;
    const rubText = getActiveRubValue(next);
    const rub = toNum(rubText);
    if (rub != null) {
      return setActivePctValue(next, plannedRevenue > 0 ? formatEditableNumber((rub / plannedRevenue) * 100) : "");
    }
    const pctText = getActivePctValue(next);
    const pct = toNum(pctText);
    if (pct != null) {
      return setActiveRubValue(next, formatEditableNumber((plannedRevenue * pct) / 100));
    }
    return next;
  }

  const changedStoreUids = rows
    .filter((row) => isDraftChanged(row, drafts[row.store_uid] || toDraft(row)))
    .map((row) => row.store_uid);
  const hasChanges = changedStoreUids.length > 0;
  const savingAny = Object.values(savingMap).some(Boolean);

  useEffect(() => {
    if (!floatingNotice) return;
    const timer = window.setTimeout(() => setFloatingNotice(""), 2600);
    return () => window.clearTimeout(timer);
  }, [floatingNotice]);

  async function commitAll() {
    const changedRows = rows.flatMap((row) => {
      const draft = drafts[row.store_uid] || toDraft(row);
      if (!isDraftChanged(row, draft)) return [];
      return [{
        row,
        values: {
          earning_mode: draft.earning_mode,
          strategy_mode: draft.strategy_mode,
          planned_revenue: draft.planned_revenue,
          target_drr_percent: draft.target_drr_percent,
          target_profit_rub: draft.target_profit_rub,
          target_profit_percent: draft.target_profit_percent,
          minimum_profit_percent: draft.minimum_profit_percent,
          target_margin_rub: draft.target_margin_rub,
          target_margin_percent: draft.target_margin_percent,
        },
      }];
    });
    if (!changedRows.length) return;
    await onSaveRows(changedRows);
    setFloatingNotice("Изменения сохранены");
  }

  return (
    <SectionBlock>
      {loading ? <div className="status">Загрузка плана продаж...</div> : null}
      {!loading && error ? <div className="status error">{error}</div> : null}
      {!loading && !error ? (
        <>
          {saveError ? <div className="status error">{saveError}</div> : null}
          <div className={styles.salesPlanGrid}>
            {rows.map((row) => {
              const draft = drafts[row.store_uid] || toDraft(row);
              const currencySign = fmtCurrencySign(row.currency_code);
              const rubValue = draft.earning_mode === "margin" ? draft.target_margin_rub : draft.target_profit_rub;
              const pctValue = draft.earning_mode === "margin" ? draft.target_margin_percent : draft.target_profit_percent;
              const revenueCellKey = `${row.store_uid}:planned_revenue`;
              const rubCellKey = `${row.store_uid}:${draft.earning_mode}:rub`;
              const isDirty = changedStoreUids.includes(row.store_uid);
              const cardState = savingMap[row.store_uid]
                ? "Сохранение..."
                : isDirty
                  ? "Есть несохранённые изменения"
                  : row.updated_at
                    ? `Сохранено ${new Date(row.updated_at).toLocaleString("ru-RU")}`
                    : "Без сохранённых изменений";
              return (
                <section key={row.store_uid} className={styles.salesPlanCard}>
                  <div className={styles.salesPlanCardHead}>
                    <div className={styles.salesPlanCardTitleBlock}>
                      <div className={styles.salesPlanCardEyebrow}>{row.platform_label}</div>
                      <h3 className={styles.salesPlanCardTitle}>
                        <span>{row.store_name}</span>
                        {row.store_id ? <span className={styles.salesPlanCardId}>{row.store_id}</span> : null}
                      </h3>
                    </div>
                    <div className={`${styles.salesPlanCardState} ${isDirty ? styles.salesPlanCardStateDirty : ""}`}>
                      {cardState}
                    </div>
                  </div>

                  <div className={styles.salesPlanModeGrid}>
                    <div className={styles.salesPlanModeCard}>
                      <div className={styles.salesPlanModeLabel}>Режим стратегии</div>
                      <div className={styles.segmentedSwitch} role="tablist" aria-label={`Режим стратегии ${row.store_name}`}>
                        <button
                          type="button"
                          className={`${styles.segmentedSwitchButton} ${draft.strategy_mode === "mix" ? styles.segmentedSwitchButtonActive : ""}`}
                          aria-pressed={draft.strategy_mode === "mix"}
                          onClick={() => patchDraft(row.store_uid, (prev) => ({ ...prev, strategy_mode: "mix" }))}
                        >
                          Выше прибыль
                        </button>
                        <button
                          type="button"
                          className={`${styles.segmentedSwitchButton} ${draft.strategy_mode === "mrc" ? styles.segmentedSwitchButtonActive : ""}`}
                          aria-pressed={draft.strategy_mode === "mrc"}
                          onClick={() => patchDraft(row.store_uid, (prev) => ({ ...prev, strategy_mode: "mrc" }))}
                        >
                          Выше оборот
                        </button>
                      </div>
                    </div>

                    <div className={styles.salesPlanModeCard}>
                      <div className={styles.salesPlanModeLabel}>Целевой показатель</div>
                      <div className={styles.segmentedSwitch} role="tablist" aria-label={`Целевой показатель ${row.store_name}`}>
                        <button
                          type="button"
                          className={`${styles.segmentedSwitchButton} ${draft.earning_mode === "profit" ? styles.segmentedSwitchButtonActive : ""}`}
                          aria-pressed={draft.earning_mode === "profit"}
                          onClick={() => {
                            patchDraft(row.store_uid, (prev) => ({
                              ...prev,
                              earning_mode: "profit",
                              target_profit_rub: prev.target_profit_rub || prev.target_margin_rub,
                              target_profit_percent: prev.target_profit_percent || prev.target_margin_percent,
                            }));
                          }}
                        >
                          Прибыль
                        </button>
                        <button
                          type="button"
                          className={`${styles.segmentedSwitchButton} ${draft.earning_mode === "margin" ? styles.segmentedSwitchButtonActive : ""}`}
                          aria-pressed={draft.earning_mode === "margin"}
                          onClick={() => {
                            patchDraft(row.store_uid, (prev) => ({
                              ...prev,
                              earning_mode: "margin",
                              target_margin_rub: prev.target_margin_rub || prev.target_profit_rub,
                              target_margin_percent: prev.target_margin_percent || prev.target_profit_percent,
                            }));
                          }}
                        >
                          Маржа
                        </button>
                      </div>
                    </div>
                  </div>

                  <div className={styles.salesPlanFields}>
                    <div className={styles.salesPlanField}>
                      <div className={styles.salesPlanFieldLabel}>Плановый оборот</div>
                      <div className={styles.cellInputWrap}>
                        <input
                          className={`input ${styles.cellInput}`}
                          value={focusedCell === revenueCellKey ? draft.planned_revenue : formatGrouped(draft.planned_revenue)}
                          inputMode="numeric"
                          onChange={(e) => patchDraft(row.store_uid, (prev) => recalcByRevenue(prev, e.target.value))}
                          onFocus={() => setFocusedCell(revenueCellKey)}
                          onBlur={() => {
                            setFocusedCell((prev) => (prev === revenueCellKey ? "" : prev));
                          }}
                        />
                        {savingMap[row.store_uid] ? <span className={styles.cellSavingDot} /> : null}
                      </div>
                    </div>

                    <div className={styles.salesPlanField}>
                      <div className={styles.salesPlanFieldLabel}>Целевые рекламные расходы</div>
                      <div className={styles.cellInputWrap}>
                        <input
                          className={`input ${styles.cellInput}`}
                          value={draft.target_drr_percent}
                          inputMode="decimal"
                          onChange={(e) => patchDraft(row.store_uid, (prev) => ({ ...prev, target_drr_percent: e.target.value }))}
                        />
                        <span className={styles.inlineSuffix}>%</span>
                      </div>
                    </div>

                    <div className={`${styles.salesPlanField} ${styles.salesPlanFieldCompact}`}>
                      <div className={styles.salesPlanFieldLabel}>Целевое значение</div>
                      <div className={styles.cellInputWrap}>
                        <input
                          className={`input ${styles.cellInput}`}
                          value={focusedCell === rubCellKey ? rubValue : formatGrouped(rubValue)}
                          inputMode="decimal"
                          onChange={(e) => patchDraft(row.store_uid, (prev) => recalcByRub(prev, e.target.value))}
                          onFocus={() => setFocusedCell(rubCellKey)}
                          onBlur={() => {
                            setFocusedCell((prev) => (prev === rubCellKey ? "" : prev));
                          }}
                        />
                        <span className={styles.inlineSuffix}>{currencySign}</span>
                      </div>
                    </div>

                    <div className={`${styles.salesPlanField} ${styles.salesPlanFieldCompact}`}>
                      <div className={styles.salesPlanFieldLabel}>Целевое значение, %</div>
                      <div className={styles.cellInputWrap}>
                        <input
                          className={`input ${styles.cellInput}`}
                          value={pctValue}
                          inputMode="decimal"
                          onChange={(e) => patchDraft(row.store_uid, (prev) => recalcByPercent(prev, e.target.value))}
                        />
                        <span className={styles.inlineSuffix}>%</span>
                      </div>
                    </div>

                    <div className={`${styles.salesPlanField} ${styles.salesPlanFieldCompact}`}>
                      <div className={styles.salesPlanFieldLabel}>Минимальная прибыль</div>
                      <div className={styles.cellInputWrap}>
                        <input
                          className={`input ${styles.cellInput}`}
                          value={draft.minimum_profit_percent}
                          inputMode="decimal"
                          onChange={(e) => patchDraft(row.store_uid, (prev) => ({ ...prev, minimum_profit_percent: e.target.value }))}
                        />
                        <span className={styles.inlineSuffix}>%</span>
                      </div>
                    </div>
                  </div>
                </section>
              );
            })}
            {!rows.length ? <div className="status">Нет доступных магазинов.</div> : null}
          </div>
          {(hasChanges || savingAny || floatingNotice || saveError) ? (
            <div className={styles.salesPlanFloatingBar}>
              <div className={styles.salesPlanFloatingMeta}>
                <div className={styles.salesPlanFloatingTitle}>
                  {saveError
                    ? "Ошибка сохранения"
                    : savingAny
                      ? "Сохраняем изменения..."
                      : hasChanges
                        ? `Изменено магазинов: ${changedStoreUids.length}`
                        : floatingNotice || "Изменения сохранены"}
                </div>
                {saveError ? (
                  <div className={styles.salesPlanFloatingHint}>{saveError}</div>
                ) : !hasChanges && floatingNotice ? (
                  <div className={styles.salesPlanFloatingHint}>{floatingNotice}</div>
                ) : null}
              </div>
              {hasChanges ? (
                <button
                  type="button"
                  className="btn"
                  disabled={savingAny}
                  onClick={() => void commitAll()}
                >
                  {savingAny ? "Сохранение..." : "Сохранить"}
                </button>
              ) : null}
            </div>
          ) : null}
        </>
      ) : null}
    </SectionBlock>
  );
}
