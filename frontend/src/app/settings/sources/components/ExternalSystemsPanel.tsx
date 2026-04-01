import { PanelCard } from "../../../../components/page/SectionKit";
import styles from "../DataSourcesPage.module.css";

export function ExternalSystemsPanel() {
  return (
    <PanelCard
      title="PIM / 1C / МойСклад"
      description="Единый шлюз для внешних систем"
      action={
        <button className={`btn inline ${styles.ymAddAccountBtn}`} disabled>
          Добавить систему
        </button>
      }
    >
      <div className={styles.ymContent}>
        <div className={styles.sourcePlaceholderCard}>
          <div className={styles.sourcePlaceholderTitle}>Внешние системы в подготовке</div>
          <div className={styles.sourcePlaceholderText}>Здесь появятся шлюзы для PIM, 1C и МойСклад. Пока секция оставлена как отдельный системный блок без псевдо-таблицы.</div>
          <div><span className="pill warn">В разработке</span></div>
        </div>
      </div>
    </PanelCard>
  );
}
