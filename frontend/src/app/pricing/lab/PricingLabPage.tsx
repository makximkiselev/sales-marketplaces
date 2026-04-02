import { useState } from "react";
import { API_BASE } from "../../../lib/api";
import { LoadingButton } from "../../../components/page/ControlKit";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { KpiCard, KpiGrid } from "../../../components/page/DataKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";

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
        <WorkspacePageHero
          title="Лаборатория цен"
          subtitle="Изолированная сервисная зона для ручного пересчета ценовых решений без перехода в основное рабочее пространство."
          meta={<span className={layoutStyles.metaChip}>{loading ? "Выполняется" : "Ручной режим"}</span>}
          toolbar={(
            <div className={layoutStyles.toolbarGroup}>
              <LoadingButton loading={loading} idleLabel="Пересчитать сейчас" loadingLabel="Пересчет..." onClick={rebuild} />
            </div>
          )}
        >
          <KpiGrid>
            <KpiCard label="Статус" value={loading ? "В процессе" : status ? "Завершено" : "Ожидание"} />
          </KpiGrid>
        </WorkspacePageHero>

        <SectionBlock title="Пересчет">
          {status ? <div className="status">{status}</div> : <div className={layoutStyles.statusBox}>Ручной запуск пока не выполнялся.</div>}
        </SectionBlock>
      </div>
    </PageFrame>
  );
}
