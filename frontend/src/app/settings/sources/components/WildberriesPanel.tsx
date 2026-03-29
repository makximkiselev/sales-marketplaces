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
        <div className={styles.ymTableWrap}>
          <table className={styles.ymTable}>
            <colgroup>
              <col className={styles.ozColClient} />
              <col className={styles.ozColSeller} />
              <col className={styles.ymColName} />
              <col className={styles.ymColCurrency} />
              <col className={styles.ymColToggle} />
              <col className={styles.ymColToggle} />
              <col className={styles.ymColStatus} />
              <col className={styles.ymColDelete} />
            </colgroup>
            <thead>
              <tr>
                <th>Client ID</th>
                <th>Seller ID</th>
                <th>Наименование</th>
                <th>Валюта</th>
                <th className={styles.ymToggleHead}>Импорт</th>
                <th className={styles.ymToggleHead}>Экспорт</th>
                <th className={styles.ymStatusHead}>Статус</th>
                <th className={styles.ymActionHead}>Действия</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className={styles.ymMutedCell}>-</td>
                <td className={styles.ymMutedCell}>-</td>
                <td className={styles.ymMutedCell}>Интеграция в разработке</td>
                <td className={styles.ymMutedCell}>-</td>
                <td className={styles.ymToggleCell}>
                  <div className={styles.ymToggleWrap}>
                    <span className={styles.ymToggleLabel}>OFF</span>
                    <button type="button" className="toggle sm" disabled>
                      <span className="toggle-track"><span className="toggle-thumb" /></span>
                    </button>
                    <span className={styles.ymToggleLabel}>ON</span>
                  </div>
                </td>
                <td className={styles.ymToggleCell}>
                  <div className={styles.ymToggleWrap}>
                    <span className={styles.ymToggleLabel}>OFF</span>
                    <button type="button" className="toggle sm" disabled>
                      <span className="toggle-track"><span className="toggle-thumb" /></span>
                    </button>
                    <span className={styles.ymToggleLabel}>ON</span>
                  </div>
                </td>
                <td className={styles.ymStatusCell}>
                  <span className="pill warn">В разработке</span>
                  <div className={styles.ymStatusTime}>Последнее обновление:</div>
                  <div className={styles.ymStatusTimeValue}>Не проверялось</div>
                </td>
                <td className={styles.ymDeleteCell}>
                  <button className={`btn inline ${styles.ymShopDeleteBtn}`} disabled>Удалить</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </PanelCard>
  );
}
