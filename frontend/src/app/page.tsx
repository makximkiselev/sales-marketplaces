import { Link } from "react-router-dom";

export default function Page() {
  return (
    <section className="card section-frame" style={{ margin: "24px", padding: "24px" }}>
      <h1 style={{ margin: 0, marginBottom: 12 }}>Дашборд</h1>
      <p style={{ marginTop: 0, color: "var(--text-soft)" }}>
        Быстрые переходы в основные разделы системы.
      </p>
      <div style={{ display: "grid", gap: 20 }}>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 12 }}>
          <Link className="btn" to="/sales/overview">Обзор продаж</Link>
          <Link className="btn" to="/pricing/decision">Стратегия ценообразования</Link>
          <Link className="btn" to="/pricing/prices">Цены</Link>
          <Link className="btn" to="/pricing/promos">Промо</Link>
          <Link className="btn" to="/pricing/attractiveness">Привлекательность</Link>
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 12 }}>
          <Link className="btn" to="/sales/boost">Эффективность буста</Link>
          <Link className="btn" to="/sales/coinvest">Соинвест</Link>
          <Link className="btn" to="/sales/elasticity">Эластичность</Link>
          <Link className="btn" to="/catalog">Каталог</Link>
          <Link className="btn" to="/settings/sources">Источники данных</Link>
          <Link className="btn" to="/settings/monitoring">Мониторинг</Link>
        </div>
      </div>
    </section>
  );
}
