import { ModalShell } from "../../../../components/page/PageKit";
import styles from "../DataSourcesPage.module.css";

export function DeleteConfirmModal({
  title,
  error,
  busy,
  onClose,
  onConfirm,
}: {
  title: string;
  error?: string;
  busy: boolean;
  onClose: () => void;
  onConfirm: () => void;
}) {
  return (
    <ModalShell
      title="Подтверждение удаления"
      onClose={onClose}
      width="min(520px, calc(100vw - 24px))"
    >
      <div style={{ fontSize: 14, lineHeight: 1.4, color: "var(--text-soft)" }}>
        {title}
      </div>
      {error ? <div className="status error">{error}</div> : null}
      <div style={{ marginTop: 14, display: "flex", justifyContent: "flex-end", gap: 8 }}>
        <button className="btn" disabled={busy} onClick={onClose}>Отмена</button>
        <button
          className={`btn ${styles.ymDeleteAccountBtn}`}
          disabled={busy}
          onClick={onConfirm}
        >
          {busy ? "Удаление..." : "Удалить"}
        </button>
      </div>
    </ModalShell>
  );
}
