import { useState } from "react";
import { API_BASE } from "../../../lib/api";
import { LoadingButton } from "../../../components/page/ControlKit";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { KpiCard, KpiGrid } from "../../../components/page/DataKit";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceToolbar } from "../../../components/page/WorkspaceKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";

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
    <PageFrame title="Лаборатория цен" subtitle="Ручной запуск построения решений по ставкам и цене.">
      <div className={layoutStyles.shell}>
        <WorkspaceSurface className={layoutStyles.heroSurface}>
          <WorkspaceHeader
            title="Pricing lab"
            subtitle="Изолированная сервисная зона для ручного пересчета ценовых решений без перехода в основное рабочее пространство."
            meta={<span className={layoutStyles.metaChip}>{loading ? "Выполняется" : "Ручной режим"}</span>}
          />
          <KpiGrid>
            <KpiCard label="Статус" value={loading ? "В процессе" : status ? "Завершено" : "Ожидание"} />
          </KpiGrid>
          <WorkspaceToolbar className={layoutStyles.toolbar}>
            <div className={layoutStyles.toolbarGroup}>
              <LoadingButton loading={loading} idleLabel="Пересчитать сейчас" loadingLabel="Пересчет..." onClick={rebuild} />
            </div>
          </WorkspaceToolbar>
        </WorkspaceSurface>

        <SectionBlock title="Пересчет">
          {status ? <div className="status">{status}</div> : <div className={layoutStyles.statusBox}>Ручной запуск пока не выполнялся.</div>}
        </SectionBlock>
      </div>
    </PageFrame>
  );
}
