import { useMemo, useState } from "react";
import { WizardModal } from "../../../../components/page/WizardKit";
import styles from "../PricingSettingsPage.module.css";
import type { EditableFieldKey } from "../types";

type Props = {
  field: EditableFieldKey;
  label: string;
  onClose: () => void;
  onConfirm: (value: string) => Promise<void> | void;
};

export function BulkFillColumnModal({ field, label, onClose, onConfirm }: Props) {
  const [value, setValue] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const suffix = useMemo(() => {
    if (field.endsWith("_percent")) return "%";
    if (field.endsWith("_rub")) return "₽ / $";
    return "";
  }, [field]);

  async function handleConfirm() {
    setSaving(true);
    setError("");
    try {
      await onConfirm(value);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setSaving(false);
      return;
    }
    setSaving(false);
  }

  return (
    <WizardModal
      title={`Заполнить столбец: ${label}`}
      onClose={onClose}
      steps={[{ key: "bulk-fill", label: "Заполнение", active: true }]}
      error={error}
      footer={
        <>
          <button type="button" className="btn inline" onClick={onClose} disabled={saving}>
            Отмена
          </button>
          <button type="button" className="btn inline primary" onClick={() => void handleConfirm()} disabled={saving || !value.trim()}>
            {saving ? "Сохранение..." : "Применить всем"}
          </button>
        </>
      }
      width="min(460px, calc(100vw - 24px))"
    >
      <div className={styles.bulkFillModalBody}>
        <div className={styles.bulkFillModalText}>
          Значение будет записано во все ячейки этого столбца для текущего магазина.
        </div>
        <label className={styles.bulkFillField}>
          <span className={styles.columnSelectLabel}>{label}</span>
          <div className={styles.inputWithSuffix}>
            <input
              className={`input ${styles.settingInput}`}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              inputMode="decimal"
              autoFocus
            />
            {suffix ? <span className={styles.inputSuffix}>{suffix}</span> : null}
          </div>
        </label>
      </div>
    </WizardModal>
  );
}
