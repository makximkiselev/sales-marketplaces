import styles from "../DataSourcesPage.module.css";
import { PanelCard } from "../../../../components/page/SectionKit";

export function YandexTablesPanel() {
  return (
    <PanelCard
      title="Яндекс.Таблицы"
      description="Интеграция будет доступна в следующей итерации"
      action={
        <button className={`btn inline ${styles.ymAddAccountBtn}`} disabled>
          Добавить источник
        </button>
      }
    >
      <div className={styles.ymContent}>
        <div className={styles.ymTableWrap}>
          <table className={styles.ymTable}>
            <thead>
              <tr>
                <th>Источник</th>
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
