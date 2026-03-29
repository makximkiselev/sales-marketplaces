import { useEffect, useState } from "react";
import { apiGet } from "../../../../lib/api";
import { WizardModal } from "../../../../components/page/WizardKit";
import styles from "../PricingSettingsPage.module.css";
import type { CogsSource, SourceItem } from "../types";

type Props = {
  current: CogsSource | null;
  onSave: (src: CogsSource) => void;
  onClose: () => void;
  title?: string;
  skuColumnLabel?: string;
  extraColumnLabel?: string;
  valueColumnLabel?: string;
};

export function CogsSourceModal({
  current,
  onSave,
  onClose,
  title = "Источник себестоимости",
  skuColumnLabel = "Столбец с артикулом (SKU)",
  extraColumnLabel = "",
  valueColumnLabel = "Столбец с себестоимостью",
}: Props) {
  const preconfiguredTable = Boolean(current?.sourceId && (current?.type ?? "table") === "table");
  const [step, setStep] = useState<1 | 2 | 3>(preconfiguredTable ? 3 : 1);
  const [selectedType, setSelectedType] = useState<"table" | "system">(current?.type ?? "table");
  const [sources, setSources] = useState<SourceItem[]>([]);
  const [sourcesLoading, setSourcesLoading] = useState(false);
  const [selectedSourceId, setSelectedSourceId] = useState(current?.sourceId ?? "");
  const [selectedSourceName, setSelectedSourceName] = useState(current?.sourceName ?? "");
  const [headers, setHeaders] = useState<string[]>([]);
  const [headersLoading, setHeadersLoading] = useState(false);
  const [skuColumn, setSkuColumn] = useState(current?.skuColumn ?? "");
  const [extraColumn, setExtraColumn] = useState(current?.extraColumn ?? "");
  const [valueColumn, setValueColumn] = useState(current?.valueColumn ?? "");
  const [modalError, setModalError] = useState("");

  async function loadHeadersBySourceId(id: string) {
    setHeadersLoading(true);
    setModalError("");
    try {
      const data = await apiGet<{ ok: boolean; headers?: string[] }>(
        `/api/data/sources/gsheets/headers?which=${encodeURIComponent(id)}`,
      );
      setHeaders(Array.isArray(data.headers) ? data.headers : []);
      return true;
    } catch (e) {
      setModalError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setHeadersLoading(false);
    }
  }

  useEffect(() => {
    setSourcesLoading(true);
    setModalError("");
    void apiGet<{ ok: boolean; items?: SourceItem[] }>("/api/sources")
      .then((data) => {
        const all = Array.isArray(data.items) ? data.items : [];
        setSources(all.filter((s) => String(s.type || "").toLowerCase() === "gsheets"));
      })
      .catch((e: unknown) => setModalError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSourcesLoading(false));
  }, []);

  useEffect(() => {
    if (!preconfiguredTable || !selectedSourceId) return;
    if (headers.length || headersLoading) return;
    void loadHeadersBySourceId(selectedSourceId);
  }, [preconfiguredTable, selectedSourceId, headers.length, headersLoading]);

  async function handleSelectSource(id: string, name: string) {
    setSelectedSourceId(id);
    setSelectedSourceName(name);
    setHeaders([]);
    setSkuColumn("");
    setExtraColumn("");
    setValueColumn("");
    const ok = await loadHeadersBySourceId(id);
    if (ok) setStep(3);
  }

  function handleSave() {
    if (!selectedSourceId || !skuColumn || !valueColumn || (extraColumnLabel && !extraColumn)) return;
    onSave({
      type: selectedType,
      sourceId: selectedSourceId,
      sourceName: selectedSourceName,
      skuColumn,
      extraColumn,
      valueColumn,
    });
  }

  const stepTitles: Record<number, string> = {
    1: "Тип источника",
    2: "Выберите таблицу",
    3: "Настройте столбцы",
  };

  return (
    <WizardModal
      title={`${title} — ${stepTitles[step]}`}
      onClose={onClose}
      steps={[
        { key: "type", label: "1. Тип", active: step === 1, clickable: step > 1, onClick: () => setStep(1) },
        { key: "source", label: "2. Источник", active: step === 2, clickable: step > 2, onClick: () => setStep(2) },
        { key: "columns", label: "3. Столбцы", active: step === 3, clickable: false },
      ]}
      error={modalError}
      footer={
        <>
          {step > 1 ? (
            <button type="button" className="btn inline" onClick={() => setStep((s) => (s - 1) as 1 | 2 | 3)}>
              Назад
            </button>
          ) : null}
          {step === 1 ? (
            <button
              type="button"
              className="btn inline primary"
              onClick={() => setStep((s) => (s + 1) as 2 | 3)}
              disabled={selectedType !== "table"}
            >
              Далее
            </button>
          ) : null}
          {step === 3 ? (
            <button
              type="button"
              className="btn inline primary"
              onClick={handleSave}
              disabled={!skuColumn || !valueColumn || (Boolean(extraColumnLabel) && !extraColumn)}
            >
              Сохранить
            </button>
          ) : null}
        </>
      }
      width="min(720px, calc(100vw - 24px))"
    >
      {step === 1 && (
        <div className={styles.sourceTypeCards}>
          <button
            type="button"
            className={`${styles.sourceTypeCard} ${selectedType === "table" ? styles.selected : ""}`}
            onClick={() => setSelectedType("table")}
          >
            <p className={styles.sourceTypeCardTitle}>Таблица</p>
            <p className={styles.sourceTypeCardDesc}>Google Sheets из подключённых источников</p>
          </button>
          <button type="button" className={styles.sourceTypeCard} disabled>
            <p className={styles.sourceTypeCardTitle}>Система</p>
            <p className={styles.sourceTypeCardDesc}>Скоро: подключение внешних систем</p>
          </button>
        </div>
      )}

      {step === 2 && (
        <div className={styles.sourceList}>
          {sourcesLoading ? <div className="status">Загрузка источников...</div> : null}
          {!sourcesLoading && !sources.length ? (
            <div className="status">Нет подключённых источников. Добавьте источник в разделе «Данные».</div>
          ) : null}
          {sources.map((src) => (
            <button
              key={src.source_id ?? src.id}
              type="button"
              className={`${styles.sourceListItem} ${selectedSourceId === (src.source_id ?? src.id) ? styles.selected : ""}`}
              onClick={() => void handleSelectSource(src.source_id ?? src.id, src.title || src.source_id || src.id)}
              disabled={headersLoading}
            >
              {src.title || src.source_id || src.id}
              {headersLoading && selectedSourceId === (src.source_id ?? src.id) ? " — загрузка столбцов..." : ""}
            </button>
          ))}
        </div>
      )}

      {step === 3 && (
        <div className={styles.columnSelectGroup}>
          <div>
            <div className={styles.columnSelectLabel}>{skuColumnLabel}</div>
            <select className="input" value={skuColumn} onChange={(e) => setSkuColumn(e.target.value)}>
              <option value="">— выберите столбец —</option>
              {headers.map((h) => <option key={h} value={h}>{h}</option>)}
            </select>
          </div>
          {extraColumnLabel ? (
            <div>
              <div className={styles.columnSelectLabel}>{extraColumnLabel}</div>
              <select className="input" value={extraColumn} onChange={(e) => setExtraColumn(e.target.value)}>
                <option value="">— выберите столбец —</option>
                {headers.map((h) => <option key={h} value={h}>{h}</option>)}
              </select>
            </div>
          ) : null}
          <div>
            <div className={styles.columnSelectLabel}>{valueColumnLabel}</div>
            <select className="input" value={valueColumn} onChange={(e) => setValueColumn(e.target.value)}>
              <option value="">— выберите столбец —</option>
              {headers.map((h) => <option key={h} value={h}>{h}</option>)}
            </select>
          </div>
        </div>
      )}
    </WizardModal>
  );
}
