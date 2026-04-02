import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageFrame } from "../../_shared/WorkspacePageFrame";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";

export default function CatalogContentRatingPage() {
  return (
    <WorkspacePageFrame>
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
    </WorkspacePageFrame>
  );
}
