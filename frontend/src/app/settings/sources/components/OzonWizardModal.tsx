import { WizardLabel, WizardModal } from "../../../../components/page/WizardKit";

export function OzonWizardModal({
  open,
  clientId,
  apiKey,
  sellerId,
  sellerName,
  loading,
  error,
  onClose,
  onChangeClientId,
  onChangeApiKey,
  onConnect,
}: {
  open: boolean;
  clientId: string;
  apiKey: string;
  sellerId: string;
  sellerName: string;
  loading: boolean;
  error: string;
  onClose: () => void;
  onChangeClientId: (value: string) => void;
  onChangeApiKey: (value: string) => void;
  onConnect: () => void;
}) {
  if (!open) return null;
  return (
    <WizardModal
      title="Подключение кабинета Ozon"
      onClose={onClose}
      steps={[{ key: "ozon", label: "1. Кабинет Ozon", active: true }]}
      error={error}
      footer={
        <>
          <button className="btn" onClick={onClose}>Отмена</button>
          <button
            className={`btn primary ${loading ? "is-loading" : ""}`}
            disabled={loading || !clientId.trim() || !apiKey.trim()}
            onClick={onConnect}
          >
            {loading ? "Подключение..." : "Подключить"}
          </button>
        </>
      }
    >
      <WizardLabel>Client ID</WizardLabel>
      <input className="input input-size-md" value={clientId} onChange={(e) => onChangeClientId(e.target.value)} placeholder="Введите Client ID" />

      <WizardLabel>API key</WizardLabel>
      <input className="input input-size-fluid" value={apiKey} onChange={(e) => onChangeApiKey(e.target.value)} placeholder="Введите API key" />

      {sellerId ? (
        <div className="source-row">
          <div>
            <div className="muted-text">{sellerName || "Ozon кабинет"}</div>
            <div className="status-time">Seller ID: {sellerId}</div>
          </div>
        </div>
      ) : null}
    </WizardModal>
  );
}
