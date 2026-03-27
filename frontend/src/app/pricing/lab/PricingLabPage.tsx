"use client";

import { useState } from "react";
import { API_BASE } from "../../../lib/api";
import { LoadingButton } from "../../../components/page/ControlKit";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";

export default function Page() {
  const [status, setStatus] = useState<string>("");
  const [loading, setLoading] = useState(false);

  async function rebuild() {
    setLoading(true);
    setStatus("Выполняется пересчет...");
    try {
      const res = await fetch(`${API_BASE}/api/pricing/decision`);
      const data = await res.json();
      if (!res.ok || !data.ok) {
        throw new Error(data.message || "Запрос завершился ошибкой");
      }
      setStatus(`Готово: обработано ${data.items?.length || 0} позиций`);
    } catch (e) {
      setStatus(`Ошибка: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <PageFrame title="Лаборатория цен" subtitle="Ручной запуск построения решений по ставкам и цене." />

      <SectionBlock title="Пересчет">
        <LoadingButton loading={loading} idleLabel="Пересчитать сейчас" loadingLabel="Пересчет..." onClick={rebuild} />
        {status ? <div className="status">{status}</div> : null}
      </SectionBlock>
    </>
  );
}
