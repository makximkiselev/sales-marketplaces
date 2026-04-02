import styles from "./DataSourcesPage.module.css";
import { PanelGrid, SectionBlock } from "../../../components/page/SectionKit";
import { ExternalSystemsPanel } from "./components/ExternalSystemsPanel";
import { GoogleTablesPanel } from "./components/GoogleTablesPanel";
import { OzonPanel } from "./components/OzonPanel";
import { WildberriesPanel } from "./components/WildberriesPanel";
import { YandexMarketPanel } from "./components/YandexMarketPanel";
import { YandexTablesPanel } from "./components/YandexTablesPanel";
import { platformMeta } from "./types";
import type { SourcesController, SourcesSectionItem } from "./DataSourcesRendererTypes";
import layoutStyles from "../../_shared/AppPageLayout.module.css";
import { WorkspacePageHero } from "../../_shared/WorkspacePageHero";

type Props = {
  controller: SourcesController;
  sectionItems: SourcesSectionItem[];
};

export function DataSourcesDesktop({ controller, sectionItems }: Props) {
  const {
    sectionTab,
    setSectionTab,
    loading,
    refreshAllLoading,
    lastRefreshAt,
    shopCheckLoading,
    gsSourceCheckLoading,
    sourceFlowSavingKey,
    flowSavingKey,
    currencySavingKey,
    fulfillmentSavingKey,
    sourceBindingSavingKey,
    storeSourceBindings,
    flowError,
    ymActionBusinessId,
    ozActionClientId,
    gsheetsSources,
    sortedYmAccounts,
    totalYmShops,
    sortedOzonAccounts,
    ymActionAccount,
    ozActionAccount,
    headerImportOn,
    headerExportOn,
    headerFlowDisabled,
    setYmActionBusinessId,
    setOzActionClientId,
    updateHeaderFlow,
    updateDataFlow,
    updateStoreCurrency,
    updateStoreFulfillment,
    openStoreSourceModal,
    openWizard,
    openAddShop,
    openEditAccount,
    openDeleteConfirm,
    formatDateTime,
    checkGsheetSource,
    updateSourceFlow,
    checkShop,
    openGsWizard,
    openOzonWizard,
    checkOzonAccount,
    refreshAllStatuses,
  } = controller;

  return (
    <div className={styles.sourcesShell}>
      <WorkspacePageHero
        title="Источники данных"
        subtitle="Единое рабочее пространство для маркетплейсов, таблиц и внешних систем с общим управлением обменом."
        tabs={{
          items: sectionItems.map((item) => ({ id: item.id, label: item.label })),
          activeId: sectionTab,
          onChange: setSectionTab,
        }}
        meta={(
          <div className={layoutStyles.heroMeta}>
            <span className={layoutStyles.metaChip}>
              {sectionTab === "all" ? "Все источники" : sectionItems.find((item) => item.id === sectionTab)?.label ?? "Источники"}
            </span>
            <span className={layoutStyles.metaChip}>
              {refreshAllLoading ? "Проверка статусов..." : `Обновлено: ${controller.formatRefreshLabel(lastRefreshAt)}`}
            </span>
          </div>
        )}
        toolbar={(
          <>
          <div className={styles.flowInline}>
            <span className={styles.flowInlineTitle}>Режим обмена</span>
            <div className={styles.flowInlineItem}>
              <span className="muted-text">Импорт</span>
              <button
                type="button"
                className={`toggle sm ${headerImportOn ? "on" : ""}`}
                role="switch"
                aria-checked={headerImportOn}
                aria-label="Переключить импорт данных"
                disabled={headerFlowDisabled || flowSavingKey === `header-${sectionTab}-import`}
                onClick={() => void updateHeaderFlow("import", !headerImportOn)}
              >
                <span className="toggle-track"><span className="toggle-thumb" /></span>
              </button>
            </div>
            <div className={styles.flowInlineItem}>
              <span className="muted-text">Экспорт</span>
              <button
                type="button"
                className={`toggle sm ${headerExportOn ? "on" : ""}`}
                role="switch"
                aria-checked={headerExportOn}
                aria-label="Переключить экспорт данных"
                disabled={headerFlowDisabled || flowSavingKey === `header-${sectionTab}-export`}
                onClick={() => void updateHeaderFlow("export", !headerExportOn)}
              >
                <span className="toggle-track"><span className="toggle-thumb" /></span>
              </button>
            </div>
          </div>
          <div className={styles.refreshBlock}>
            <button
              className={`btn primary ${styles.refreshButton} ${refreshAllLoading ? styles.refreshButtonLoading : ""}`}
              onClick={() => void refreshAllStatuses()}
              disabled={refreshAllLoading}
            >
              {refreshAllLoading ? "Обновление..." : "Обновить источники"}
            </button>
            <div className={styles.refreshMeta}>Последнее обновление: {controller.formatRefreshLabel(lastRefreshAt)}</div>
          </div>
          </>
        )}
      />

      {flowError ? <div className={`status error ${styles.flowErrorInline}`}>{flowError}</div> : null}

      {sectionTab === "all" || sectionTab === "platforms" ? (
        <SectionBlock title="Площадки">
          <PanelGrid className={styles.platformRows}>
            <YandexMarketPanel
              accounts={sortedYmAccounts}
              totalShops={totalYmShops}
              actionAccount={ymActionAccount}
              shopCheckLoading={shopCheckLoading}
              flowSavingKey={flowSavingKey}
              currencySavingKey={currencySavingKey}
              fulfillmentSavingKey={fulfillmentSavingKey}
              sourceBindingSavingKey={sourceBindingSavingKey}
              storeSourceBindings={storeSourceBindings}
              ymActionBusinessId={ymActionBusinessId}
              setYmActionBusinessId={setYmActionBusinessId}
              openStoreSourceModal={openStoreSourceModal}
              openWizard={() => openWizard("yandex_market")}
              openEditAccount={openEditAccount}
              openAddShop={openAddShop}
              openDeleteConfirm={openDeleteConfirm}
              updateStoreFulfillment={updateStoreFulfillment}
              updateStoreCurrency={updateStoreCurrency}
              updateDataFlow={updateDataFlow}
              checkShop={checkShop}
              formatDateTime={formatDateTime}
              description={platformMeta.yandex_market.desc}
            />
            <OzonPanel
              accounts={sortedOzonAccounts}
              actionAccount={ozActionAccount}
              ozActionClientId={ozActionClientId}
              setOzActionClientId={setOzActionClientId}
              ozCheckLoading={controller.ozCheckLoading}
              flowSavingKey={flowSavingKey}
              currencySavingKey={currencySavingKey}
              fulfillmentSavingKey={fulfillmentSavingKey}
              sourceBindingSavingKey={sourceBindingSavingKey}
              storeSourceBindings={storeSourceBindings}
              openStoreSourceModal={openStoreSourceModal}
              openOzonWizard={openOzonWizard}
              openDeleteConfirm={openDeleteConfirm}
              updateStoreFulfillment={updateStoreFulfillment}
              updateStoreCurrency={updateStoreCurrency}
              updateDataFlow={updateDataFlow}
              checkOzonAccount={checkOzonAccount}
              formatDateTime={formatDateTime}
              description={platformMeta.ozon.desc}
            />
            <WildberriesPanel title={platformMeta.wildberries.label} description={platformMeta.wildberries.desc} />
          </PanelGrid>
        </SectionBlock>
      ) : null}

      {sectionTab === "all" || sectionTab === "tables" ? (
        <SectionBlock title="Внешние таблицы">
          <PanelGrid className={styles.platformRows}>
            <GoogleTablesPanel
              loading={loading}
              sources={gsheetsSources}
              gsSourceCheckLoading={gsSourceCheckLoading}
              sourceFlowSavingKey={sourceFlowSavingKey}
              openGsWizard={openGsWizard}
              openDeleteConfirm={openDeleteConfirm}
              updateSourceFlow={updateSourceFlow}
              checkGsheetSource={checkGsheetSource}
              formatDateTime={formatDateTime}
            />
            <YandexTablesPanel />
          </PanelGrid>
        </SectionBlock>
      ) : null}

      {sectionTab === "all" || sectionTab === "external" ? (
        <SectionBlock title="Внешние системы">
          <ExternalSystemsPanel />
        </SectionBlock>
      ) : null}
    </div>
  );
}
