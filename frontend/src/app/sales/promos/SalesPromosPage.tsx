import { useEffect, useState } from "react";
import { apiGet } from "../../../lib/api";
import { ErrorBox } from "../../../components/ErrorBox";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { KpiCard, KpiGrid, TableCard } from "../../../components/page/DataKit";
import { WorkspaceHeader, WorkspaceSurface } from "../../../components/page/WorkspaceKit";
import styles from "../_shared/SalesSimplePage.module.css";

type SalesPromosResponse = {
  items?: Array<{
    name?: string;
    orders_total?: number;
    orders_promo?: number;
    promo_share?: number;
    op_fact?: number;
  }>;
};

export default function Page() {
  const [items, setItems] = useState<NonNullable<SalesPromosResponse["items"]>>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    apiGet<SalesPromosResponse>("/api/sales/promos?mode=overview&group_by=category&page=1&page_size=200")
      .then((data) => {
        if (!active) return;
        setItems(data.items || []);
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

  const ordersTotal = items.reduce((sum, item) => sum + Number(item.orders_total || 0), 0);
  const promoOrdersTotal = items.reduce((sum, item) => sum + Number(item.orders_promo || 0), 0);
  const avgPromoShare = items.length
    ? items.reduce((sum, item) => sum + Number(item.promo_share || 0), 0) / items.length
    : 0;

  return (
    <PageFrame title="Продажи по акциям" subtitle="Результаты продаж с разбивкой по промо-активности.">
      <div className={styles.shell}>
        <WorkspaceSurface className={styles.heroSurface}>
          <WorkspaceHeader
            title="Promo analytics"
            subtitle="Сводный экран по категориям с фокусом на долю промо-заказов и фактический операционный результат."
            meta={(
              <div className={styles.heroMeta}>
                <span className={styles.metaChip}>Категории: {items.length}</span>
                <span className={styles.metaChip}>Промо-срез</span>
              </div>
            )}
          />
          <KpiGrid>
            <KpiCard label="Заказы всего" value={Math.round(ordersTotal).toLocaleString("ru-RU")} />
            <KpiCard label="Заказы промо" value={Math.round(promoOrdersTotal).toLocaleString("ru-RU")} />
            <KpiCard label="Средняя доля промо" value={`${(Math.round(avgPromoShare * 100) / 100).toLocaleString("ru-RU")}%`} />
          </KpiGrid>
        </WorkspaceSurface>

        <SectionBlock title="Результаты" className={styles.section}>
          <div className={styles.sectionNote}>Показаны категории из обзорного отчета по промо-активности.</div>
          <TableCard>
            <table className={styles.table}>
            <thead>
              <tr>
                <th>Категория</th>
                <th className={styles.cellRight}>Заказы всего</th>
                <th className={styles.cellRight}>Заказы промо</th>
                <th className={styles.cellRight}>Доля промо, %</th>
                <th className={styles.cellRight}>OP факт</th>
              </tr>
            </thead>
            <tbody>
              {items.map((it, idx) => (
                <tr key={`${it.name || "row"}-${idx}`}>
                  <td className={it.name ? styles.cellStrong : styles.cellMuted}>{it.name || "-"}</td>
                  <td className={styles.cellRight}>{Math.round(Number(it.orders_total || 0)).toLocaleString("ru-RU")}</td>
                  <td className={styles.cellRight}>{Math.round(Number(it.orders_promo || 0)).toLocaleString("ru-RU")}</td>
                  <td className={styles.cellRight}>{(Math.round(Number(it.promo_share || 0) * 100) / 100).toLocaleString("ru-RU")}</td>
                  <td className={styles.cellRight}>{Math.round(Number(it.op_fact || 0)).toLocaleString("ru-RU")}</td>
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
