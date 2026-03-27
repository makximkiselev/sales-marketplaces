"use client";

import styles from "../PricingSettingsPage.module.css";
import type { CogsSource, StockSource } from "../types";

type Props = {
  earningMode: "profit" | "margin";
  earningUnit: "rub" | "percent";
  moneySign: string;
  activeTargetValue: string;
  targetDrr: string;
  cogsSource: CogsSource | null;
  stockSource: StockSource | null;
  activeStoreId: string;
  showTargets?: boolean;
  setEarningMode: React.Dispatch<React.SetStateAction<"profit" | "margin">>;
  setEarningUnit: React.Dispatch<React.SetStateAction<"rub" | "percent">>;
  setActiveTargetValue: (value: string) => void;
  setTargetDrr: React.Dispatch<React.SetStateAction<string>>;
  setCogsModalOpen: React.Dispatch<React.SetStateAction<boolean>>;
  setCogsSource: React.Dispatch<React.SetStateAction<CogsSource | null>>;
  setStockModalOpen: React.Dispatch<React.SetStateAction<boolean>>;
  setStockSource: React.Dispatch<React.SetStateAction<StockSource | null>>;
};

export function GeneralSettingsPanel({
  earningMode,
  earningUnit,
  moneySign,
  activeTargetValue,
  targetDrr,
  cogsSource,
  stockSource,
  activeStoreId,
  showTargets = true,
  setEarningMode,
  setEarningUnit,
  setActiveTargetValue,
  setTargetDrr,
  setCogsModalOpen,
  setCogsSource,
  setStockModalOpen,
  setStockSource,
}: Props) {
  return (
    <div className={styles.globalSettingsGrid}>
      {showTargets ? (
        <div className={styles.settingField}>
          <div className={styles.settingLabel}>Целевой показатель</div>
          <div className={styles.dualToggleRow}>
            <div className={styles.modeToggleWrap}>
              <span className={styles.modeToggleText}>Прибыль</span>
              <button
                type="button"
                className={`toggle sm ${styles.selectorToggle} ${earningMode === "margin" ? "on" : ""}`}
                role="switch"
                aria-checked={earningMode === "margin"}
                aria-label="Переключить целевой заработок: прибыль или маржа"
                onClick={() => setEarningMode((prev) => (prev === "profit" ? "margin" : "profit"))}
              >
                <span className="toggle-track"><span className="toggle-thumb" /></span>
              </button>
              <span className={styles.modeToggleText}>Маржа</span>
            </div>
            <div className={styles.modeToggleWrap}>
              <span className={styles.modeToggleText}>{moneySign}</span>
              <button
                type="button"
                className={`toggle sm ${styles.selectorToggle} ${earningUnit === "percent" ? "on" : ""}`}
                role="switch"
                aria-checked={earningUnit === "percent"}
                aria-label="Переключить единицы целевого значения: рубли или проценты"
                onClick={() => setEarningUnit((prev) => (prev === "rub" ? "percent" : "rub"))}
              >
                <span className="toggle-track"><span className="toggle-thumb" /></span>
              </button>
              <span className={styles.modeToggleText}>%</span>
            </div>
          </div>
        </div>
      ) : null}

      <div className={`${styles.cogsSourceRow} ${earningMode !== "profit" ? styles.cogsSourceRowDisabled : ""}`}>
        <span className={styles.settingLabel}>Источник себестоимости</span>
        <div className={styles.cogsSourceDisplay}>
          <span className={cogsSource ? styles.cogsSourceNameSet : styles.cogsSourceName}>
            {cogsSource ? cogsSource.sourceName : "Не выбран"}
          </span>
          <button
            type="button"
            className={`btn inline ${styles.cogsSelectButton}`}
            onClick={() => setCogsModalOpen(true)}
            disabled={earningMode !== "profit" || !activeStoreId}
          >
            Выбрать
          </button>
          {cogsSource ? (
            <button
              type="button"
              className={`btn inline ${styles.cogsClearButton}`}
              onClick={() => setCogsSource(null)}
              title="Сбросить источник"
              disabled={earningMode !== "profit" || !activeStoreId}
            >
              ✕
            </button>
          ) : null}
        </div>
        {earningMode !== "profit" ? (
          <div className={styles.cogsSourceMeta}>Доступно только в режиме «Прибыль».</div>
        ) : cogsSource ? (
          <div className={styles.cogsSourceMeta}>
            SKU: <b>{cogsSource.skuColumn}</b> · Себестоимость: <b>{cogsSource.valueColumn}</b>
          </div>
        ) : null}
      </div>

      <div className={styles.cogsSourceRow}>
        <span className={styles.settingLabel}>Источник остатка</span>
        <div className={styles.cogsSourceDisplay}>
          <span className={stockSource ? styles.cogsSourceNameSet : styles.cogsSourceName}>
            {stockSource ? stockSource.sourceName : "Не выбран"}
          </span>
          <button
            type="button"
            className={`btn inline ${styles.cogsSelectButton}`}
            onClick={() => setStockModalOpen(true)}
            disabled={!activeStoreId}
          >
            Выбрать
          </button>
          {stockSource ? (
            <button
              type="button"
              className={`btn inline ${styles.cogsClearButton}`}
              onClick={() => setStockSource(null)}
              title="Сбросить источник"
              disabled={!activeStoreId}
            >
              ✕
            </button>
          ) : null}
        </div>
        {stockSource ? (
          <div className={styles.cogsSourceMeta}>
            SKU: <b>{stockSource.skuColumn}</b> · Остаток: <b>{stockSource.valueColumn}</b>
          </div>
        ) : null}
      </div>

      {showTargets ? (
        <>
          <div className={styles.settingField}>
            <label className={styles.settingLabel} htmlFor="target-earning-value">
              {earningMode === "margin" ? "Целевая маржа" : "Целевая прибыль"}
            </label>
            <div className={styles.inputWithSuffix}>
              <input
                id="target-earning-value"
                className={`input ${styles.settingInput}`}
                value={activeTargetValue}
                onChange={(e) => setActiveTargetValue(e.target.value)}
                placeholder={
                  earningMode === "margin"
                    ? earningUnit === "percent"
                      ? "например 18"
                      : "например 2500"
                    : earningUnit === "percent"
                      ? "например 10"
                      : "например 1500"
                }
              />
              <span className={styles.inputSuffix}>{earningUnit === "percent" ? "%" : moneySign}</span>
            </div>
          </div>

          <div className={styles.settingField}>
            <label className={styles.settingLabel} htmlFor="target-drr">Целевой ДРР</label>
            <div className={styles.inputWithSuffix}>
              <input
                id="target-drr"
                className={`input ${styles.settingInput}`}
                value={targetDrr}
                onChange={(e) => setTargetDrr(e.target.value)}
                placeholder="например 12"
              />
              <span className={styles.inputSuffix}>%</span>
            </div>
          </div>
        </>
      ) : null}
    </div>
  );
}
