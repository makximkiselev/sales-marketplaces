import { useEffect, useState } from "react";
import { apiGet } from "../../../lib/api";
import { ErrorBox } from "../../../components/ErrorBox";
import { PageFrame } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { TableCard } from "../../../components/page/DataKit";

type SalesDashboardResponse = {
  current?: Record<string, { turnover?: number; op_profit?: number; qty?: number }>;
};

export default function Page() {
  const [rows, setRows] = useState<Array<[string, { turnover?: number; op_profit?: number; qty?: number }]>>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    apiGet<SalesDashboardResponse>("/api/sales/dashboard?group_by=article")
      .then((data) => {
        if (!active) return;
        setRows(Object.entries(data.current || {}).slice(0, 200));
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
      <PageFrame title="ABC-анализ продаж" subtitle={`Топ артикулов по обороту и прибыли. Показаны первые ${rows.length}.`} />

      <SectionBlock title="Таблица">
        <TableCard>
          <table className="table-wrap">
            <thead>
              <tr>
                <th>Артикул</th>
                <th>Оборот</th>
                <th>OP</th>
                <th>Количество</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(([article, values]) => (
                <tr key={article}>
                  <td>{article}</td>
                  <td>{values.turnover ?? 0}</td>
                  <td>{values.op_profit ?? 0}</td>
                  <td>{values.qty ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableCard>
      </SectionBlock>
    </>
  );
}
