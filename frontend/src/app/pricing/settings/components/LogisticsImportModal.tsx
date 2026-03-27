"use client";

import { useEffect, useRef, useState } from "react";
import { API_BASE } from "../../../../lib/api";
import { ModalShell } from "../../../../components/page/PageKit";
import { ControlTabs } from "../../../../components/page/ControlKit";
import { WizardDropzone } from "../../../../components/page/WizardKit";
import styles from "../PricingSettingsPage.module.css";
import type { LogisticsImportModalProps } from "../types";

export function LogisticsImportModal({ open, platform, storeId, onClose, onDone }: LogisticsImportModalProps) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [scope, setScope] = useState<"store" | "all">("store");
  const [loading, setLoading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) {
      setFile(null);
      setScope("store");
      setLoading(false);
      setDragActive(false);
      setStatus("");
      setError("");
    }
  }, [open]);

  if (!open) return null;

  async function handleDownloadTemplate() {
    if (!platform || !storeId) return;
    setLoading(true);
    setError("");
    setStatus("");
    try {
      const qs = new URLSearchParams({ platform, store_id: storeId }).toString();
      const res = await fetch(`${API_BASE}/api/pricing/settings/logistics/import-template?${qs}`, { cache: "no-store" });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `logistics_template_${platform}_${storeId}.xlsx`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      setStatus("Шаблон скачан. Заполните файл и загрузите его ниже.");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  async function handleImport() {
    if (!file || !platform || !storeId) return;
    setLoading(true);
    setError("");
    setStatus("");
    try {
      const form = new FormData();
      form.append("platform", platform);
      form.append("store_id", storeId);
      form.append("apply_scope", scope);
      form.append("file", file);
      const res = await fetch(`${API_BASE}/api/pricing/settings/logistics/import`, { method: "POST", body: form });
      let data: any = null;
      try {
        data = await res.json();
      } catch {
        data = null;
      }
      if (!res.ok || !data?.ok) throw new Error(data?.message || `Ошибка импорта (${res.status})`);
      setStatus(`Импорт завершен: строк ${Number(data?.rows_in_file || 0)}, применено к магазинам ${Number(data?.target_stores || 0)}.`);
      await onDone();
      setTimeout(() => onClose(), 250);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  function pickFile(next: File | null) {
    setFile(next);
    setError("");
    if (next) setStatus(`Файл выбран: ${next.name}`);
  }

  return (
    <ModalShell title="Импорт логистики" onClose={onClose} width="min(720px, calc(100vw - 24px))">
      <div className={styles.logisticsImportShell}>
          <div className={styles.logisticsImportStep}>
            <div className={styles.settingLabel}>1. Скачать шаблон</div>
            <p className={styles.inlineInfo}>Скачайте Excel-шаблон с товарами и текущими параметрами логистики.</p>
            <button type="button" className={`btn inline primary ${styles.logisticsTemplateButton}`} onClick={() => void handleDownloadTemplate()} disabled={loading}>
              {loading ? "Скачивание..." : "Скачать шаблон"}
            </button>
          </div>

          <div className={styles.logisticsImportStep}>
            <div className={styles.settingLabel}>2. Загрузить файл</div>
            <WizardDropzone
              active={dragActive}
              onDragOver={(e) => {
                e.preventDefault();
                setDragActive(true);
              }}
              onDragLeave={() => setDragActive(false)}
              onDrop={(e) => {
                e.preventDefault();
                setDragActive(false);
                pickFile(e.dataTransfer.files?.[0] || null);
              }}
              onClick={() => fileInputRef.current?.click()}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") fileInputRef.current?.click();
              }}
              title="Перетащите сюда файл или выберите на компьютере"
              subtitle="Поддерживается формат .xlsx"
              input={
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".xlsx"
                  hidden
                  onChange={(e) => pickFile(e.target.files?.[0] || null)}
                />
              }
            />
            {file ? <div className={styles.inlineInfo}>Файл: {file.name}</div> : null}
          </div>

          <div className={styles.logisticsImportStep}>
            <div className={styles.settingLabel}>3. Применение</div>
            <p className={styles.inlineInfo}>Выберите область применения загруженных данных.</p>
            <ControlTabs
              className={styles.platformTabs}
              items={[
                { id: "store", label: "Для текущего магазина" },
                { id: "all", label: "Для всех магазинов" },
              ]}
              activeId={scope}
              onChange={(id) => setScope(id)}
            />
          </div>

          {status ? <div className="status ok">{status}</div> : null}
          {error ? <div className="status error">{error}</div> : null}
        </div>
        <div className={styles.logisticsImportFooter}>
          <button type="button" className="btn inline" onClick={onClose}>Отмена</button>
          <button type="button" className="btn inline primary" onClick={() => void handleImport()} disabled={!file || loading}>
            {loading ? "Импорт..." : "Импортировать"}
          </button>
        </div>
    </ModalShell>
  );
}
