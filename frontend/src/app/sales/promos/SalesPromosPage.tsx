import { useEffect, useState } from "react";
import { apiGet } from "../../../lib/api";
import { ErrorBox } from "../../../components/ErrorBox";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { TableCard } from "../../../components/page/DataKit";

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

  return (
    <>
      <PageFrame title="Продажи по акциям" subtitle="Результаты продаж с разбивкой по промо-активности." />

      <SectionBlock title="Результаты">
        <TableCard>
          <table className="table-wrap">
            <thead>
              <tr>
                <th>Категория</th>
                <th>Заказы всего</th>
                <th>Заказы промо</th>
                <th>Доля промо, %</th>
                <th>OP факт</th>
              </tr>
            </thead>
            <tbody>
              {items.map((it, idx) => (
                <tr key={`${it.name || "row"}-${idx}`}>
                  <td>{it.name || "-"}</td>
                  <td>{it.orders_total ?? 0}</td>
                  <td>{it.orders_promo ?? 0}</td>
                  <td>{it.promo_share ?? 0}</td>
                  <td>{it.op_fact ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableCard>
      </SectionBlock>
    </>
  );
}
