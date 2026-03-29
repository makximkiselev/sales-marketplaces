import styles from "../PricingSettingsPage.module.css";
import type { LogisticsNumericKey, LogisticsStoreSettingsApi } from "../types";

type FieldErrors = Record<"handling" | "delivery" | "return" | "disposal", string>;

type Props = {
  moneySign: string;
  logisticsStoreSettings: LogisticsStoreSettingsApi;
  logisticsFieldErrors: FieldErrors;
  logisticsStoreSaving: boolean;
  logisticsStoreError: string;
  logisticsStoreSavedAt: string;
  getLogisticsNumericValue: (key: LogisticsNumericKey, current: number | null | undefined) => string;
  setLogisticsField: <K extends keyof LogisticsStoreSettingsApi>(key: K, value: LogisticsStoreSettingsApi[K]) => void;
  setLogisticsNumericField: (key: LogisticsNumericKey, raw: string) => void;
  onLogisticsNumericBlur: (key: LogisticsNumericKey) => void;
};

export function LogisticsSettingsPanel({
  moneySign,
  logisticsStoreSettings,
  logisticsFieldErrors,
  logisticsStoreSaving,
  logisticsStoreError,
  logisticsStoreSavedAt,
  getLogisticsNumericValue,
  setLogisticsField,
  setLogisticsNumericField,
  onLogisticsNumericBlur,
}: Props) {
  return (
    <div className={`${styles.globalSettingsGrid} ${styles.logisticsSettingsGrid}`}>
      <div className={`${styles.settingField} ${logisticsFieldErrors.handling ? styles.settingFieldError : ""}`}>
        <div className={styles.settingLabel}>Стоимость обработки заказа</div>
        <div className={styles.handlingInlineRow}>
          <div className={styles.modeToggleWrap}>
            <span className={styles.modeToggleText}>{moneySign}</span>
            <button
              type="button"
              className={`toggle sm ${styles.selectorToggle} ${logisticsStoreSettings.handling_mode === "percent" ? "on" : ""}`}
              role="switch"
              aria-checked={logisticsStoreSettings.handling_mode === "percent"}
              onClick={() => setLogisticsField("handling_mode", logisticsStoreSettings.handling_mode === "percent" ? "fixed" : "percent")}
            >
              <span className="toggle-track"><span className="toggle-thumb" /></span>
            </button>
            <span className={styles.modeToggleText}>%</span>
          </div>
          {logisticsStoreSettings.handling_mode === "percent" ? (
            <div className={`${styles.inlineThreeInputs} ${styles.inlineThreeInputsCompact}`}>
              <div className={styles.inputWithSuffix}>
                <input
                  className={`input ${styles.settingInput}`}
                  value={getLogisticsNumericValue("handling_percent", logisticsStoreSettings.handling_percent)}
                  onChange={(e) => setLogisticsNumericField("handling_percent", e.target.value)}
                  onBlur={() => onLogisticsNumericBlur("handling_percent")}
                  placeholder="Процент"
                  inputMode="decimal"
                />
                <span className={styles.inputSuffix}>%</span>
              </div>
              <div className={styles.inputWithSuffix}>
                <input
                  className={`input ${styles.settingInput}`}
                  value={getLogisticsNumericValue("handling_min_amount", logisticsStoreSettings.handling_min_amount)}
                  onChange={(e) => setLogisticsNumericField("handling_min_amount", e.target.value)}
                  onBlur={() => onLogisticsNumericBlur("handling_min_amount")}
                  placeholder="Мин"
                  inputMode="decimal"
                />
                <span className={styles.inputSuffix}>{moneySign}</span>
              </div>
              <div className={styles.inputWithSuffix}>
                <input
                  className={`input ${styles.settingInput}`}
                  value={getLogisticsNumericValue("handling_max_amount", logisticsStoreSettings.handling_max_amount)}
                  onChange={(e) => setLogisticsNumericField("handling_max_amount", e.target.value)}
                  onBlur={() => onLogisticsNumericBlur("handling_max_amount")}
                  placeholder="Макс"
                  inputMode="decimal"
                />
                <span className={styles.inputSuffix}>{moneySign}</span>
              </div>
            </div>
          ) : (
            <div className={`${styles.inputWithSuffix} ${styles.handlingFixedWrap}`}>
              <input
                className={`input ${styles.settingInput}`}
                value={getLogisticsNumericValue("handling_fixed_amount", logisticsStoreSettings.handling_fixed_amount)}
                onChange={(e) => setLogisticsNumericField("handling_fixed_amount", e.target.value)}
                onBlur={() => onLogisticsNumericBlur("handling_fixed_amount")}
                placeholder="Фиксированный платеж"
                inputMode="decimal"
              />
              <span className={styles.inputSuffix}>{moneySign}</span>
            </div>
          )}
        </div>
        {logisticsFieldErrors.handling ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.handling}</div> : null}
      </div>

      <div className={`${styles.settingField} ${logisticsFieldErrors.delivery ? styles.settingFieldError : ""}`}>
        <label className={styles.settingLabel}>Стоимость доставки до клиента за 1кг</label>
        <div className={styles.inputWithSuffix}>
          <input
            className={`input ${styles.settingInput}`}
            value={getLogisticsNumericValue("delivery_cost_per_kg", logisticsStoreSettings.delivery_cost_per_kg)}
            onChange={(e) => setLogisticsNumericField("delivery_cost_per_kg", e.target.value)}
            onBlur={() => onLogisticsNumericBlur("delivery_cost_per_kg")}
            inputMode="decimal"
            placeholder="например 4"
          />
          <span className={styles.inputSuffix}>{moneySign}</span>
        </div>
        {logisticsFieldErrors.delivery ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.delivery}</div> : null}
      </div>

      <div className={`${styles.settingField} ${logisticsFieldErrors.return ? styles.settingFieldError : ""}`}>
        <label className={styles.settingLabel}>Стоимость обработки возврата</label>
        <div className={styles.inputWithSuffix}>
          <input
            className={`input ${styles.settingInput}`}
            value={getLogisticsNumericValue("return_processing_cost", logisticsStoreSettings.return_processing_cost)}
            onChange={(e) => setLogisticsNumericField("return_processing_cost", e.target.value)}
            onBlur={() => onLogisticsNumericBlur("return_processing_cost")}
            inputMode="decimal"
            placeholder="например 3"
          />
          <span className={styles.inputSuffix}>{moneySign}</span>
        </div>
        {logisticsFieldErrors.return ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.return}</div> : null}
      </div>

      <div className={`${styles.settingField} ${logisticsFieldErrors.disposal ? styles.settingFieldError : ""}`}>
        <label className={styles.settingLabel}>Стоимость утилизации</label>
        <div className={styles.inputWithSuffix}>
          <input
            className={`input ${styles.settingInput}`}
            value={getLogisticsNumericValue("disposal_cost", logisticsStoreSettings.disposal_cost)}
            onChange={(e) => setLogisticsNumericField("disposal_cost", e.target.value)}
            onBlur={() => onLogisticsNumericBlur("disposal_cost")}
            inputMode="decimal"
            placeholder="например 0"
          />
          <span className={styles.inputSuffix}>{moneySign}</span>
        </div>
        {logisticsFieldErrors.disposal ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.disposal}</div> : null}
        <div className={styles.cogsSourceMeta}>
          {logisticsStoreSaving
            ? "Сохраняем логистику..."
            : logisticsStoreError
              ? `Ошибка: ${logisticsStoreError}`
              : logisticsStoreSavedAt
                ? `Сохранено: ${new Date(logisticsStoreSavedAt).toLocaleString("ru-RU")}`
                : "Настройки логистики еще не сохранены"}
        </div>
      </div>
    </div>
  );
}
