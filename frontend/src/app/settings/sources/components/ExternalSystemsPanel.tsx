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
        <div className={styles.ymTableWrap}>
          <table className={styles.ymTable}>
            <thead>
              <tr>
                <th>Система</th>
                <th>Статус</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className={styles.ymMutedCell}>Еще нет подключенных источников</td>
                <td><span className="pill warn">В разработке</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </PanelCard>
  );
}
