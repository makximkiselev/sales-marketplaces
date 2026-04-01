import styles from "../DataSourcesPage.module.css";
import { PanelCard } from "../../../../components/page/SectionKit";

type Props = {
  title: string;
  description: string;
};

export function WildberriesPanel({ title, description }: Props) {
  return (
    <PanelCard
      title={title}
      description={description}
      action={
        <button className={`btn inline ${styles.ymAddAccountBtn}`} disabled>
          Добавить кабинет
        </button>
      }
    >
      <div className={styles.ymContent}>
        <div className={styles.sourceSummaryRow}>
          <div className={styles.sourceSummaryCard}>
            <div className={styles.sourceSummaryLabel}>Статус</div>
            <div className={styles.sourceSummaryValue}>Beta</div>
            <div className={styles.sourceSummaryMeta}>Интеграция еще не открыта в рабочий контур</div>
          </div>
          <div className={styles.sourceSummaryCard}>
            <div className={styles.sourceSummaryLabel}>Режим обмена</div>
            <div className={styles.sourceSummaryValue}>Off</div>
            <div className={styles.sourceSummaryMeta}>Импорт и экспорт будут доступны после запуска коннектора</div>
          </div>
        </div>
        <div className={styles.sourcePlaceholderCard}>
          <div className={styles.sourcePlaceholderTitle}>Wildberries еще не подключен</div>
          <div className={styles.sourcePlaceholderText}>Когда интеграция будет готова, здесь появятся кабинеты, проверки доступа, режимы обмена и настройки источников себестоимости и остатков.</div>
          <div><span className="pill warn">В разработке</span></div>
        </div>
      </div>
    </PanelCard>
  );
}
