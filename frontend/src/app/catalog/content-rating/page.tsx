import { PageFrame } from "../../../components/page/PageKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";

export default function CatalogContentRatingPage() {
  return (
    <PageFrame title="Контент-рейтинг" subtitle="Раздел в разработке. Здесь будет оценка качества карточек и контента.">
      <div className={layoutStyles.shell}>
        <WorkspacePageHero
          title="Контент-рейтинг"
          subtitle="Будущая рабочая зона для контроля качества карточек, полноты контента и приоритетов исправления."
          meta={<span className={layoutStyles.metaChip}>В разработке</span>}
        />
        <section className={layoutStyles.placeholderBlock}>
          <div className={layoutStyles.placeholderText}>
            Здесь появятся рейтинги карточек, сигналы по проблемным атрибутам и очереди задач на доработку контента.
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
