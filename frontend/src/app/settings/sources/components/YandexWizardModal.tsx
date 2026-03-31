import { WizardLabel, WizardModal } from "../../../../components/page/WizardKit";
import type { CampaignItem } from "../types";

export function YandexWizardModal({
  open,
  step,
  mode,
  apiKey,
  businessId,
  campaigns,
  selectedCampaignIds,
  loading,
  error,
  onClose,
  onGoToStep,
  onChangeApiKey,
  onChangeBusinessId,
  onToggleCampaign,
  onCheck,
  onConnect,
  onBack,
}: {
  open: boolean;
  step: 1 | 2;
  mode: "create" | "edit" | "add_shop";
  apiKey: string;
  businessId: string;
  campaigns: CampaignItem[];
  selectedCampaignIds: string[];
  loading: boolean;
  error: string;
  onClose: () => void;
  onGoToStep: (step: 1 | 2) => void;
  onChangeApiKey: (value: string) => void;
  onChangeBusinessId: (value: string) => void;
  onToggleCampaign: (id: string) => void;
  onCheck: () => void;
  onConnect: () => void;
  onBack: () => void;
}) {
  if (!open) return null;
  return (
    <WizardModal
      title="Подключение кабинета Яндекс.Маркета"
      onClose={onClose}
      steps={[
        {
          key: "access",
          label: "1. Доступы",
          active: step === 1,
          clickable: mode !== "create",
          onClick: () => {
            if (mode !== "create") onGoToStep(1);
          },
        },
        {
          key: "shops",
          label: "2. Магазины",
          active: step === 2,
          clickable: mode !== "create",
          onClick: () => {
            if (mode !== "create") onGoToStep(2);
          },
        },
      ]}
      error={error}
      footer={
        step === 1 ? (
          <>
            <button className="btn" onClick={onClose}>Отмена</button>
            <button
              className={`btn primary ${loading ? "is-loading" : ""}`}
              disabled={loading}
              onClick={onCheck}
            >
              {loading ? "Проверка..." : "Проверить"}
            </button>
          </>
        ) : (
          <>
            <button className="btn" onClick={onBack}>Назад</button>
            <button className="btn" onClick={onClose}>Отмена</button>
            <button className="btn primary" disabled={loading || selectedCampaignIds.length === 0} onClick={onConnect}>
              Подключить
            </button>
          </>
        )
      }
    >
      {step === 1 ? (
        <>
          <WizardLabel>Токен API</WizardLabel>
          <input className="input input-size-fluid" value={apiKey} onChange={(e) => onChangeApiKey(e.target.value)} placeholder="Введите токен" />
          <WizardLabel>Business ID</WizardLabel>
          <input className="input input-size-md" value={businessId} onChange={(e) => onChangeBusinessId(e.target.value)} placeholder="Введите ID" />
        </>
      ) : (
        <>
          <WizardLabel>Выберите магазины</WizardLabel>
          <div className="campaign-checklist">
            {campaigns.map((c) => (
              <label key={c.id} className="campaign-item">
                <input
                  type="checkbox"
                  checked={selectedCampaignIds.includes(c.id)}
                  onChange={() => onToggleCampaign(c.id)}
                />
                <span>{c.name}</span>
              </label>
            ))}
          </div>
          <div className="source-row-meta">Магазинов: {campaigns.length}</div>
        </>
      )}
    </WizardModal>
  );
}
