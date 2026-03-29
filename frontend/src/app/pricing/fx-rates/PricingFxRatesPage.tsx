import { useEffect, useMemo, useState } from "react";
import { API_BASE } from "../../../lib/api";
import { ControlField, ControlTabs, LoadingButton } from "../../../components/page/ControlKit";
import { PageFrame } from "../../../components/page/PageKit";
import styles from "./PricingFxRatesPage.module.css";

type RateRow = { date: string; rate: number };
type FxResp = {
  ok?: boolean;
  message?: string;
  period?: string;
  date_from?: string;
  date_to?: string;
  tables?: {
    cbr?: { label?: string; rows?: RateRow[] };
    ozon_sales?: { label?: string; rows?: RateRow[] };
    ozon_services?: { label?: string; rows?: RateRow[] };
  };
};

type PeriodMode = "7d" | "14d" | "30d" | "custom";

function toInputDate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function formatRuDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("ru-RU");
}

function formatRate(v: number): string {
  if (typeof v !== "number" || Number.isNaN(v)) return "-";
  return v.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 6 });
}

export default function PricingFxRatesPage() {
  const [period, setPeriod] = useState<PeriodMode>("7d");
  const [dateFrom, setDateFrom] = useState(() => toInputDate(new Date(Date.now() - 6 * 24 * 3600 * 1000)));
  const [dateTo, setDateTo] = useState(() => toInputDate(new Date()));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [updatedAt, setUpdatedAt] = useState("");
  const [data, setData] = useState<FxResp["tables"]>({});

  const titleLabel = useMemo(() => {
    if (period === "custom") return "Пользовательский период";
    if (period === "14d") return "14 дней";
    if (period === "30d") return "30 дней";
    return "7 дней";
  }, [period]);

  async function loadRates() {
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({ period });
      if (period === "custom") {
        params.set("date_from", dateFrom);
        params.set("date_to", dateTo);
      }
      const res = await fetch(`${API_BASE}/api/pricing/fx-rates?${params.toString()}`, { cache: "no-store" });
      const json = (await res.json()) as FxResp;
      if (!res.ok || !json.ok) throw new Error(json.message || "Не удалось загрузить курсы валют");
      setData(json.tables || {});
      setUpdatedAt(new Date().toLocaleString("ru-RU"));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadRates();
  }, [period]);

  const tableList = [
    { key: "cbr", label: data?.cbr?.label || "Курс ЦБ РФ", rows: data?.cbr?.rows || [] },
    { key: "ozon_sales", label: data?.ozon_sales?.label || "Ozon: для продаж", rows: data?.ozon_sales?.rows || [] },
    { key: "ozon_services", label: data?.ozon_services?.label || "Ozon: для услуг", rows: data?.ozon_services?.rows || [] },
  ] as const;

  return (
    <PageFrame
      title="Курс валют"
      subtitle="Курс USD к RUB: ЦБ РФ и таблицы Ozon (для продаж / для услуг)."
      className={styles.pageCard}
      actions={
        <LoadingButton loading={loading} idleLabel="Обновить данные" loadingLabel="Обновление..." onClick={() => void loadRates()} />
      }
      meta={
        <>
          <div>{loading ? "Обновление..." : "Данные по выбранному периоду"}</div>
          <div>{updatedAt ? `Обновлено: ${updatedAt}` : "Еще не загружено"}</div>
        </>
      }
      toolbarLeft={
        <ControlTabs
          items={[
            { id: "7d", label: "7 дней" },
            { id: "14d", label: "14 дней" },
            { id: "30d", label: "30 дней" },
            { id: "custom", label: "Период" },
          ]}
          activeId={period}
          onChange={setPeriod}
        />
      }
      toolbarRight={
        period === "custom" ? (
          <>
            <ControlField label="Дата с" className={styles.dateField}>
              <input
                type="date"
                className={`input ${styles.dateInput}`}
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
              />
            </ControlField>
            <ControlField label="Дата по" className={styles.dateField}>
              <input
                type="date"
                className={`input ${styles.dateInput}`}
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
              />
            </ControlField>
          </>
        ) : null
      }
    >
      {error ? <div className={`status error ${styles.statusBox}`}>{error}</div> : null}
      <div className={styles.tablesGrid}>
          {tableList.map((tbl) => (
            <section key={tbl.key} className={styles.tableCard}>
              <div className={styles.tableHead}>
                <h2 className={styles.tableTitle}>{tbl.label}</h2>
                <div className={styles.tableMeta}>
                  {titleLabel} • строк: {tbl.rows.length}
                </div>
              </div>
              <div className={styles.tableWrap}>
                <table className={styles.ratesTable}>
                  <thead>
                    <tr>
                      <th>Дата</th>
                      <th>$ к ₽</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tbl.rows.map((row) => (
                      <tr key={`${tbl.key}-${row.date}`}>
                        <td className={styles.dateCell}>{formatRuDate(row.date)}</td>
                        <td className={styles.rateCell}>{formatRate(row.rate)}</td>
                      </tr>
                    ))}
                    {!tbl.rows.length ? (
                      <tr>
                        <td colSpan={2} className="emptyCell">
                          Нет данных за выбранный период
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </section>
          ))}
      </div>
    </PageFrame>
  );
}
