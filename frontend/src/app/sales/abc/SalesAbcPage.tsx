import { useEffect, useState } from "react";
import { apiGet } from "../../../lib/api";
import { ErrorBox } from "../../../components/ErrorBox";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { KpiCard, KpiGrid, TableCard } from "../../../components/page/DataKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";
import { readFreshPageSnapshot, writePageSnapshot } from "../../_shared/pageCache";
import styles from "../_shared/SalesSimplePage.module.css";

type SalesDashboardResponse = {
  current?: Record<string, { turnover?: number; op_profit?: number; qty?: number }>;
};

export default function Page() {
  const CACHE_KEY = "page_sales_abc_v1";
  const [rows, setRows] = useState<Array<[string, { turnover?: number; op_profit?: number; qty?: number }]>>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const cached = readFreshPageSnapshot<Array<[string, { turnover?: number; op_profit?: number; qty?: number }]>>(CACHE_KEY, 10 * 60 * 1000);
    if (cached && active) {
      setRows(cached);
    }
    apiGet<SalesDashboardResponse>("/api/sales/dashboard?group_by=article")
      .then((data) => {
        if (!active) return;
        const nextRows = Object.entries(data.current || {}).slice(0, 200);
        setRows(nextRows);
        writePageSnapshot(CACHE_KEY, nextRows);
      })
      .catch((e) => {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      active = false;
    };
  }, []);

  if (error) {
    return <ErrorBox message={error} />;
  }

  const totalTurnover = rows.reduce((sum, [, values]) => sum + Number(values.turnover || 0), 0);
  const totalProfit = rows.reduce((sum, [, values]) => sum + Number(values.op_profit || 0), 0);
  const totalQty = rows.reduce((sum, [, values]) => sum + Number(values.qty || 0), 0);

  return (
    <PageFrame
      title="ABC-анализ продаж"
      subtitle={`Топ артикулов по обороту и прибыли. Показаны первые ${rows.length}.`}
    >
      <div className={styles.shell}>
        <WorkspacePageHero
          title="ABC-анализ продаж"
          subtitle="Быстрый обзор топовых артикулов по обороту, операционной прибыли и количеству."
          meta={(
            <div className={layoutStyles.heroMeta}>
              <span className={layoutStyles.metaChip}>Топ: {rows.length}</span>
              <span className={layoutStyles.metaChip}>Артикулы</span>
            </div>
          )}
        >
          <KpiGrid>
            <KpiCard label="Оборот" value={Math.round(totalTurnover).toLocaleString("ru-RU")} />
            <KpiCard label="OP" value={Math.round(totalProfit).toLocaleString("ru-RU")} />
            <KpiCard label="Количество" value={Math.round(totalQty).toLocaleString("ru-RU")} />
          </KpiGrid>
        </WorkspacePageHero>

        <SectionBlock title="Таблица" className={styles.section}>
          <div className={styles.sectionNote}>Список показывает первые 200 артикулов из текущего среза.</div>
          <TableCard>
            <table className={styles.table}>
            <thead>
              <tr>
                <th>Артикул</th>
                <th className={styles.cellRight}>Оборот</th>
                <th className={styles.cellRight}>OP</th>
                <th className={styles.cellRight}>Количество</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(([article, values]) => (
                <tr key={article}>
                  <td className={styles.cellStrong}>{article}</td>
                  <td className={styles.cellRight}>{Math.round(Number(values.turnover || 0)).toLocaleString("ru-RU")}</td>
                  <td className={styles.cellRight}>{Math.round(Number(values.op_profit || 0)).toLocaleString("ru-RU")}</td>
                  <td className={styles.cellRight}>{Math.round(Number(values.qty || 0)).toLocaleString("ru-RU")}</td>
                </tr>
              ))}
            </tbody>
            </table>
          </TableCard>
        </SectionBlock>
      </div>
    </PageFrame>
  );
}
