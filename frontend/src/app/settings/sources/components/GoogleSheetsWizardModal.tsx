import { useRef } from "react";
import { WizardDropzone, WizardLabel, WizardModal } from "../../../../components/page/WizardKit";
import type { IntegrationsPayload } from "../types";
import { DeleteIcon } from "./DeleteIcon";

export function GoogleSheetsWizardModal({
  open,
  mode,
  step,
  editingSourceId,
  loading,
  error,
  title,
  spreadsheet,
  selectedAccountId,
  integrations,
  credFileName,
  keyUploading,
  dropActive,
  keyUploadOk,
  keyUploadMessage,
  worksheets,
  worksheet,
  sources,
  onClose,
  onGoToStep,
  onChooseExistingSource,
  onChangeTitle,
  onChangeSpreadsheet,
  onChangeSelectedAccountId,
  onUseExistingAccount,
  onDeleteGoogleAccount,
  onFileSelected,
  onSetDropActive,
  onVerify,
  onConnect,
  onBackFromAccess,
  onBackFromSheet,
  onChangeWorksheet,
}: {
  open: boolean;
  mode: "create" | "edit-select";
  step: 1 | 2 | 3;
  editingSourceId: string;
  loading: boolean;
  error: string;
  title: string;
  spreadsheet: string;
  selectedAccountId: string;
  integrations: IntegrationsPayload;
  credFileName: string;
  keyUploading: boolean;
  dropActive: boolean;
  keyUploadOk: boolean | null;
  keyUploadMessage: string;
  worksheets: string[];
  worksheet: string;
  sources: Array<{ id: string; title: string }>;
  onClose: () => void;
  onGoToStep: (step: 1 | 2 | 3) => void;
  onChooseExistingSource: (sourceId: string) => void;
  onChangeTitle: (value: string) => void;
  onChangeSpreadsheet: (value: string) => void;
  onChangeSelectedAccountId: (value: string) => void;
  onUseExistingAccount: () => void;
  onDeleteGoogleAccount: (accountId: string) => void;
  onFileSelected: (file: File | null) => void;
  onSetDropActive: (active: boolean) => void;
  onVerify: () => void;
  onConnect: () => void;
  onBackFromAccess: () => void;
  onBackFromSheet: () => void;
  onChangeWorksheet: (value: string) => void;
}) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  if (!open) return null;

  return (
    <WizardModal
      title="Подключение Google таблицы"
      onClose={onClose}
      steps={[
        {
          key: "table",
          label: "1. Таблица",
          active: step === 1,
          clickable: Boolean(editingSourceId),
          onClick: () => {
            if (editingSourceId) onGoToStep(1);
          },
        },
        {
          key: "access",
          label: "2. Доступ",
          active: step === 2,
          clickable: Boolean(editingSourceId),
          onClick: () => {
            if (editingSourceId) onGoToStep(2);
          },
        },
        {
          key: "sheet",
          label: "3. Лист",
          active: step === 3,
          clickable: Boolean(editingSourceId),
          onClick: () => {
            if (editingSourceId) onGoToStep(3);
          },
        },
      ]}
      error={error}
      footer={
        step === 1 ? (
          <>
            <button className="btn" onClick={onClose}>Отмена</button>
            {mode === "create" ? (
              <button className="btn primary" disabled={!title.trim() || !spreadsheet.trim()} onClick={() => onGoToStep(2)}>Далее</button>
            ) : null}
          </>
        ) : step === 2 ? (
          <>
            <button className="btn" onClick={onClose}>Отмена</button>
            <button className="btn" onClick={onBackFromAccess}>Назад</button>
            <button
              className={`btn primary ${loading ? "is-loading" : ""}`}
              disabled={loading || keyUploading || !title.trim() || !selectedAccountId || (Boolean(credFileName) && keyUploadOk !== true)}
              onClick={onVerify}
            >
              Далее
            </button>
          </>
        ) : (
          <>
            <button className="btn" onClick={onClose}>Отмена</button>
            <button className="btn" onClick={onBackFromSheet}>Назад</button>
            <button className={`btn primary ${loading ? "is-loading" : ""}`} disabled={loading} onClick={onConnect}>
              Подключить
            </button>
          </>
        )
      }
      width="min(760px, calc(100vw - 24px))"
    >
      {step === 1 ? (
        <>
          {mode === "edit-select" ? (
            <>
              <WizardLabel>Выберите существующую таблицу</WizardLabel>
              <select
                className="input"
                value={editingSourceId}
                onChange={(e) => {
                  const nextId = e.target.value;
                  if (nextId) onChooseExistingSource(nextId);
                }}
              >
                <option value="">— выберите таблицу —</option>
                {sources.map((src) => (
                  <option key={src.id} value={src.id}>
                    {src.title || src.id}
                  </option>
                ))}
              </select>
              <div className="status">После выбора модалка сразу перейдет к листам таблицы.</div>
            </>
          ) : (
            <>
              <WizardLabel>Название источника</WizardLabel>
              <input className="input" value={title} onChange={(e) => onChangeTitle(e.target.value)} placeholder="Например: Каталог + прайс" />
              <WizardLabel>Ссылка/ID таблицы</WizardLabel>
              <input className="input" value={spreadsheet} onChange={(e) => onChangeSpreadsheet(e.target.value)} placeholder="https://docs.google.com/spreadsheets/d/..." />
            </>
          )}
        </>
      ) : null}

      {step === 2 ? (
        <>
          <WizardLabel>Добавить существующий аккаунт</WizardLabel>
          <div className="row" style={{ gap: 8 }}>
            <select className="input" value={selectedAccountId} onChange={(e) => onChangeSelectedAccountId(e.target.value)}>
              <option value="">Выбери аккаунт</option>
              {(integrations.google?.accounts || []).map((acc) => (
                <option key={acc.id} value={acc.id}>
                  {(acc.name || acc.client_email || acc.id) + (acc.private_key_id ? ` (${acc.private_key_id})` : "")}
                </option>
              ))}
            </select>
            <button className="btn inline" disabled={!selectedAccountId || loading} onClick={onUseExistingAccount}>
              Использовать
            </button>
            <button
              className="btn inline icon-only delete-btn"
              disabled={!selectedAccountId || loading}
              onClick={() => onDeleteGoogleAccount(selectedAccountId)}
              title="Удалить аккаунт"
              aria-label="Удалить аккаунт"
            >
              <DeleteIcon />
            </button>
          </div>

          <WizardLabel>Создать новый</WizardLabel>
          <div className="status">Название аккаунта будет сохранено как: <b>{title.trim() || "—"}</b></div>
          <WizardDropzone
            active={dropActive}
            onClick={() => inputRef.current?.click()}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                inputRef.current?.click();
              }
            }}
            onDragOver={(e) => {
              e.preventDefault();
              onSetDropActive(true);
            }}
            onDragLeave={() => onSetDropActive(false)}
            onDrop={(e) => {
              e.preventDefault();
              onSetDropActive(false);
              onFileSelected(e.dataTransfer.files?.[0] || null);
            }}
            title="Перетащите сюда файл или выберите на компьютере"
            subtitle="Поддерживается только JSON ключ Google Service Account"
            input={
              <input
                ref={inputRef}
                type="file"
                accept=".json,application/json"
                hidden
                onChange={(e) => onFileSelected(e.target.files?.[0] || null)}
              />
            }
          />
          {credFileName ? <div className="status-time">Файл: {credFileName}</div> : null}
          {keyUploading ? <div className="status-time">Файл загружается и валидируется...</div> : null}
          {keyUploadOk === true ? <div className="status ok">Ошибки не найдены</div> : null}
          {keyUploadOk === false ? <div className="status error">Есть ошибки, загрузите файл снова</div> : null}
          {keyUploadMessage && keyUploadOk === false ? (
            <div className="status error" style={{ marginTop: 4 }}>{error || keyUploadMessage}</div>
          ) : null}
          {!credFileName ? (
            <div className="status">
              Как получить Key (JSON):
              <br />
              1. Открой Google Cloud Console -&gt; IAM &amp; Admin -&gt; Service Accounts.
              <br />
              2. Создай сервисный аккаунт (или выбери существующий).
              <br />
              3. Внутри аккаунта: Keys -&gt; Add key -&gt; Create new key -&gt; JSON.
            </div>
          ) : null}
        </>
      ) : null}

      {step === 3 ? (
        <>
          <WizardLabel>Лист таблицы</WizardLabel>
          <select className="input" value={worksheet} onChange={(e) => onChangeWorksheet(e.target.value)}>
            {worksheets.map((w) => <option key={w} value={w}>{w}</option>)}
          </select>
        </>
      ) : null}
    </WizardModal>
  );
}
