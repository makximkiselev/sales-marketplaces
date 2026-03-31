import { Link } from "react-router-dom";
import { PageFrame } from "../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface } from "../components/page/WorkspaceKit";
import layoutStyles from "./_shared/AppPageLayout.module.css";

export default function Page() {
  return (
    <PageFrame title="Дашборд" subtitle="Быстрые переходы в основные разделы системы.">
      <div className={layoutStyles.shell}>
        <WorkspaceSurface className={layoutStyles.heroSurface}>
          <WorkspaceHeader
            title="Control center"
            subtitle="Стартовая точка для перехода в ключевые рабочие зоны аналитики, ценообразования, каталога и мониторинга."
            meta={(
              <div className={layoutStyles.heroMeta}>
                <span className={layoutStyles.metaChip}>Главная</span>
                <span className={layoutStyles.metaChip}>Навигация</span>
              </div>
            )}
          />
        </WorkspaceSurface>
        <div className={layoutStyles.gridLinks}>
          <section className={layoutStyles.navCard}>
            <div className={layoutStyles.navTitle}>Продажи</div>
            <div className={layoutStyles.navText}>Обзор, буст, соинвест и эластичность в одном контуре.</div>
            <div className={layoutStyles.navActions}>
              <Link className="btn" to="/sales/overview">Обзор продаж</Link>
              <Link className="btn" to="/sales/boost">Эффективность буста</Link>
              <Link className="btn" to="/sales/coinvest">Соинвест</Link>
              <Link className="btn" to="/sales/elasticity">Эластичность</Link>
            </div>
          </section>
          <section className={layoutStyles.navCard}>
            <div className={layoutStyles.navTitle}>Ценообразование</div>
            <div className={layoutStyles.navText}>Основные рабочие пространства по стратегии, ценам, промо и привлекательности.</div>
            <div className={layoutStyles.navActions}>
              <Link className="btn" to="/pricing/decision">Стратегия</Link>
              <Link className="btn" to="/pricing/prices">Цены</Link>
              <Link className="btn" to="/pricing/promos">Промо</Link>
              <Link className="btn" to="/pricing/attractiveness">Привлекательность</Link>
            </div>
          </section>
          <section className={layoutStyles.navCard}>
            <div className={layoutStyles.navTitle}>Системные разделы</div>
            <div className={layoutStyles.navText}>Каталог, источники, мониторинг и сервисные страницы системы.</div>
            <div className={layoutStyles.navActions}>
              <Link className="btn" to="/catalog">Каталог</Link>
              <Link className="btn" to="/settings/sources">Источники данных</Link>
              <Link className="btn" to="/settings/monitoring">Мониторинг</Link>
              <Link className="btn" to="/pricing/fx-rates">Курс валют</Link>
            </div>
          </section>
        </div>
      </div>
    </PageFrame>
  );
}
