import { PageFrame } from "../../../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface } from "../../../components/page/WorkspaceKit";
import layoutStyles from "../../_shared/AppPageLayout.module.css";

export default function CatalogKpisPage() {
  return (
    <PageFrame title="Ключевые показатели каталога" subtitle="Раздел в разработке. Здесь будут KPI по полноте и качеству каталога.">
      <div className={layoutStyles.shell}>
        <WorkspaceSurface className={layoutStyles.heroSurface}>
          <WorkspaceHeader
            title="Catalog KPI"
            subtitle="Будущий слой для системных KPI по полноте, качеству и структуре каталога."
            meta={<span className={layoutStyles.metaChip}>В разработке</span>}
          />
        </WorkspaceSurface>
        <section className={layoutStyles.placeholderBlock}>
          <div className={layoutStyles.placeholderText}>
            Здесь появятся показатели полноты карточек, покрытия атрибутов, качества дерева и другие контрольные метрики каталога.
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
