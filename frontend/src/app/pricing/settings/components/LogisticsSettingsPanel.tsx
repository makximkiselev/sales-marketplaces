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
  const isPercentMode = logisticsStoreSettings.handling_mode === "percent";

  return (
    <div className={styles.logisticsPanelShell}>
      <div className={styles.logisticsPanelHead}>
        <div>
          <div className={styles.categoryEditorEyebrow}>Правила магазина</div>
          <div className={styles.logisticsPanelTitle}>Логистические коэффициенты</div>
        </div>
        <div className={styles.logisticsPanelMeta}>
          {logisticsStoreSaving
            ? "Сохраняем логистику..."
            : logisticsStoreError
              ? `Ошибка: ${logisticsStoreError}`
              : logisticsStoreSavedAt
                ? `Сохранено: ${new Date(logisticsStoreSavedAt).toLocaleString("ru-RU")}`
                : "Автосохранение"}
        </div>
      </div>

      <div className={styles.logisticsStoreGrid}>
        <div className={`${styles.logisticsStoreCard} ${styles.logisticsStoreCardWide} ${logisticsFieldErrors.handling ? styles.settingFieldError : ""}`}>
          <div className={styles.logisticsStoreCardHead}>
            <div className={styles.logisticsStoreCardTitle}>Обработка заказа</div>
            <div className={styles.logisticsModeTabs}>
              <button
                type="button"
                className={`${styles.logisticsModeTab} ${!isPercentMode ? styles.logisticsModeTabActive : ""}`}
                onClick={() => setLogisticsField("handling_mode", "fixed")}
              >
                Фикс, {moneySign}
              </button>
              <button
                type="button"
                className={`${styles.logisticsModeTab} ${isPercentMode ? styles.logisticsModeTabActive : ""}`}
                onClick={() => setLogisticsField("handling_mode", "percent")}
              >
                Процент
              </button>
            </div>
          </div>
          {isPercentMode ? (
            <div className={styles.logisticsStoreCardGrid}>
              <div className={styles.logisticsMetricField}>
                <label className={styles.logisticsMetricLabel}>Ставка, %</label>
                <input
                  className={`input ${styles.settingInput}`}
                  value={getLogisticsNumericValue("handling_percent", logisticsStoreSettings.handling_percent)}
                  onChange={(e) => setLogisticsNumericField("handling_percent", e.target.value)}
                  onBlur={() => onLogisticsNumericBlur("handling_percent")}
                  placeholder="Процент"
                  inputMode="decimal"
                />
              </div>
              <div className={styles.logisticsMetricField}>
                <label className={styles.logisticsMetricLabel}>Минимум, {moneySign}</label>
                <input
                  className={`input ${styles.settingInput}`}
                  value={getLogisticsNumericValue("handling_min_amount", logisticsStoreSettings.handling_min_amount)}
                  onChange={(e) => setLogisticsNumericField("handling_min_amount", e.target.value)}
                  onBlur={() => onLogisticsNumericBlur("handling_min_amount")}
                  placeholder="Мин"
                  inputMode="decimal"
                />
              </div>
              <div className={styles.logisticsMetricField}>
                <label className={styles.logisticsMetricLabel}>Максимум, {moneySign}</label>
                <input
                  className={`input ${styles.settingInput}`}
                  value={getLogisticsNumericValue("handling_max_amount", logisticsStoreSettings.handling_max_amount)}
                  onChange={(e) => setLogisticsNumericField("handling_max_amount", e.target.value)}
                  onBlur={() => onLogisticsNumericBlur("handling_max_amount")}
                  placeholder="Макс"
                  inputMode="decimal"
                />
              </div>
            </div>
          ) : (
            <div className={styles.logisticsMetricField}>
              <label className={styles.logisticsMetricLabel}>Стоимость обработки, {moneySign}</label>
              <input
                className={`input ${styles.settingInput}`}
                value={getLogisticsNumericValue("handling_fixed_amount", logisticsStoreSettings.handling_fixed_amount)}
                onChange={(e) => setLogisticsNumericField("handling_fixed_amount", e.target.value)}
                onBlur={() => onLogisticsNumericBlur("handling_fixed_amount")}
                placeholder="Фиксированный платеж"
                inputMode="decimal"
              />
            </div>
          )}
          {logisticsFieldErrors.handling ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.handling}</div> : null}
        </div>

        <div className={`${styles.logisticsStoreCard} ${logisticsFieldErrors.delivery ? styles.settingFieldError : ""}`}>
          <div className={styles.logisticsStoreCardTitle}>Доставка до клиента, {moneySign}/кг</div>
          <div className={styles.logisticsMetricField}>
            <input
              className={`input ${styles.settingInput}`}
              value={getLogisticsNumericValue("delivery_cost_per_kg", logisticsStoreSettings.delivery_cost_per_kg)}
              onChange={(e) => setLogisticsNumericField("delivery_cost_per_kg", e.target.value)}
              onBlur={() => onLogisticsNumericBlur("delivery_cost_per_kg")}
              inputMode="decimal"
              placeholder="например 4"
            />
          </div>
          {logisticsFieldErrors.delivery ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.delivery}</div> : null}
        </div>

        <div className={`${styles.logisticsStoreCard} ${logisticsFieldErrors.return ? styles.settingFieldError : ""}`}>
          <div className={styles.logisticsStoreCardTitle}>Обработка возврата, {moneySign}</div>
          <div className={styles.logisticsMetricField}>
            <input
              className={`input ${styles.settingInput}`}
              value={getLogisticsNumericValue("return_processing_cost", logisticsStoreSettings.return_processing_cost)}
              onChange={(e) => setLogisticsNumericField("return_processing_cost", e.target.value)}
              onBlur={() => onLogisticsNumericBlur("return_processing_cost")}
              inputMode="decimal"
              placeholder="например 3"
            />
          </div>
          {logisticsFieldErrors.return ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.return}</div> : null}
        </div>

        <div className={`${styles.logisticsStoreCard} ${logisticsFieldErrors.disposal ? styles.settingFieldError : ""}`}>
          <div className={styles.logisticsStoreCardTitle}>Утилизация, {moneySign}</div>
          <div className={styles.logisticsMetricField}>
            <input
              className={`input ${styles.settingInput}`}
              value={getLogisticsNumericValue("disposal_cost", logisticsStoreSettings.disposal_cost)}
              onChange={(e) => setLogisticsNumericField("disposal_cost", e.target.value)}
              onBlur={() => onLogisticsNumericBlur("disposal_cost")}
              inputMode="decimal"
              placeholder="например 0"
            />
          </div>
          {logisticsFieldErrors.disposal ? <div className={styles.fieldErrorText}>{logisticsFieldErrors.disposal}</div> : null}
        </div>
      </div>
    </div>
  );
}
