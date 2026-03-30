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
  showRelay?: boolean;
  showSources?: boolean;
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
  showRelay = !showTargets,
  showSources = true,
  setEarningMode,
  setEarningUnit,
  setActiveTargetValue,
  setTargetDrr,
  setCogsModalOpen,
  setCogsSource,
  setStockModalOpen,
  setStockSource,
}: Props) {
  const targetModeLabel = earningMode === "margin" ? "Маржа" : "Прибыль";
  const targetValueLabel = activeTargetValue?.trim()
    ? `${activeTargetValue}${earningUnit === "percent" ? "%" : ` ${moneySign}`}`
    : "Не задан";
  const targetDrrLabel = targetDrr?.trim() ? `${targetDrr}%` : "Не задан";

  return (
    <div className={styles.globalSettingsGrid}>
      {showRelay ? (
        <div className={styles.planRelayCard}>
          <div className={styles.planRelayHead}>
            <div className={styles.settingLabel}>Цели из плана продаж</div>
            <span className={styles.planRelayBadge}>Транслируется автоматически</span>
          </div>
          <div className={styles.planRelayText}>
            Целевой показатель редактируется в разделе «План продаж», а здесь только применяется к категорийным правилам.
          </div>
          <div className={styles.planRelayGrid}>
            <div className={styles.planRelayItem}>
              <span className={styles.planRelayItemLabel}>Режим</span>
              <strong className={styles.planRelayItemValue}>{targetModeLabel}</strong>
            </div>
            <div className={styles.planRelayItem}>
              <span className={styles.planRelayItemLabel}>Цель</span>
              <strong className={styles.planRelayItemValue}>{targetValueLabel}</strong>
            </div>
            <div className={styles.planRelayItem}>
              <span className={styles.planRelayItemLabel}>Целевой ДРР</span>
              <strong className={styles.planRelayItemValue}>{targetDrrLabel}</strong>
            </div>
          </div>
        </div>
      ) : null}

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

      {showSources ? (
        <>
          <div className={`${styles.cogsSourceRow} ${earningMode !== "profit" ? styles.cogsSourceRowDisabled : ""}`}>
            <span className={styles.settingLabel}>Источник себестоимости</span>
            <div className={styles.cogsSourceCard}>
              <span className={styles.cogsSourceCardLabel}>Выбранный источник</span>
              <span className={cogsSource ? styles.cogsSourceNameSet : styles.cogsSourceName}>
                {cogsSource ? cogsSource.sourceName : "Не выбран"}
              </span>
            </div>
            <div className={styles.cogsSourceActions}>
              <button
                type="button"
                className={`btn inline ${styles.cogsSelectButton}`}
                onClick={() => setCogsModalOpen(true)}
                disabled={earningMode !== "profit" || !activeStoreId}
              >
                {cogsSource ? "Изменить" : "Выбрать"}
              </button>
            </div>
            {earningMode !== "profit" ? (
              <div className={styles.cogsSourceMeta}>Доступно только в режиме «Прибыль».</div>
            ) : cogsSource ? (
              <div className={styles.cogsSourceMeta}>
                <span>SKU: <b>{cogsSource.skuColumn}</b></span>
                <span>Себестоимость: <b>{cogsSource.valueColumn}</b></span>
              </div>
            ) : null}
          </div>

          <div className={styles.cogsSourceRow}>
            <span className={styles.settingLabel}>Источник остатка</span>
            <div className={styles.cogsSourceCard}>
              <span className={styles.cogsSourceCardLabel}>Выбранный источник</span>
              <span className={stockSource ? styles.cogsSourceNameSet : styles.cogsSourceName}>
                {stockSource ? stockSource.sourceName : "Не выбран"}
              </span>
            </div>
            <div className={styles.cogsSourceActions}>
              <button
                type="button"
                className={`btn inline ${styles.cogsSelectButton}`}
                onClick={() => setStockModalOpen(true)}
                disabled={!activeStoreId}
              >
                {stockSource ? "Изменить" : "Выбрать"}
              </button>
            </div>
            {stockSource ? (
              <div className={styles.cogsSourceMeta}>
                <span>SKU: <b>{stockSource.skuColumn}</b></span>
                <span>Остаток: <b>{stockSource.valueColumn}</b></span>
              </div>
            ) : null}
          </div>
        </>
      ) : null}

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
