import { PageFrame } from "../../../components/page/PageKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";

export default function CatalogKpisPage() {
  return (
    <PageFrame title="Ключевые показатели каталога" subtitle="Раздел в разработке. Здесь будут KPI по полноте и качеству каталога.">
      <div className={layoutStyles.shell}>
        <WorkspacePageHero
          title="KPI каталога"
          subtitle="Будущий слой для системных KPI по полноте, качеству и структуре каталога."
          meta={<span className={layoutStyles.metaChip}>В разработке</span>}
        />
        <section className={layoutStyles.placeholderBlock}>
          <div className={layoutStyles.placeholderText}>
            Здесь появятся показатели полноты карточек, покрытия атрибутов, качества дерева и другие контрольные метрики каталога.
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
