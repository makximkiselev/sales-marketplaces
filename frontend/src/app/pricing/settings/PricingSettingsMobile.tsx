import styles from "./PricingSettingsPage.module.css";
import { MobileDockLayout } from "../../../components/page/PageKit";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceTabs, WorkspaceToolbar } from "../../../components/page/WorkspaceKit";
import { GeneralSettingsSection } from "./components/GeneralSettingsSection";
import { LogisticsSettingsPanel } from "./components/LogisticsSettingsPanel";
import { LogisticsSettingsSection } from "./components/LogisticsSettingsSection";
import { SalesPlanSection } from "./components/SalesPlanSection";
import type { PricingSettingsRendererProps } from "./PricingSettingsRendererTypes";
import layoutStyles from "../../_shared/AppPageLayout.module.css";

export function PricingSettingsMobile({
  controller,
  sectionItems,
  activeSection,
  activeStore,
  isSalesPlanSection,
  currentSaveState,
  setBulkFillOpen,
  mobileCatalogOpen,
  setMobileCatalogOpen,
}: PricingSettingsRendererProps) {
  const {
    loading,
    error,
    activePlatform,
    storeTabs,
    activeStoreTabKey,
    activeStoreId,
    itemsLoading,
    itemsError,
    settingsTab,
    salesPlanRows,
    salesPlanLoading,
    salesPlanError,
    salesPlanSaving,
    monitoringRunning,
    categoryRows,
    cellDrafts,
    cellSaving,
    logisticsStoreSettings,
    logisticsRows,
    logisticsTreeRoots,
    logisticsTreePath,
    logisticsLoading,
    logisticsError,
    logisticsSearch,
    logisticsPage,
    logisticsPageSize,
    logisticsTotal,
    logisticsPageSizeOptions,
    logisticsStoreSaving,
    logisticsStoreSavedAt,
    logisticsStoreError,
    logisticsFieldErrors,
    logisticsCellDrafts,
    logisticsCellSaving,
    moneySign,
    tableColumns,
    setActiveStoreTabKey,
    setSettingsTab,
    setLogisticsPage,
    setLogisticsPageSize,
    setLogisticsSearch,
    setLogisticsTreePath,
    setLogisticsImportOpen,
    saveSalesPlanRows,
    runMonitoringJob,
    getCellKey,
    defaultFieldValue,
    formatNum,
    queueSaveCell,
    flushSaveCell,
    setLogisticsField,
    setLogisticsNumericField,
    onLogisticsNumericBlur,
    getLogisticsNumericValue,
    toLiveLogisticsRow,
    fmtCell,
    getLogisticsCellKey,
    setLogisticsCellDraftByKey,
    commitLogisticsCell,
    setLogisticsCellDrafts,
  } = controller;

  const mobileDockVisible = Boolean(activeStoreId && !isSalesPlanSection && !mobileCatalogOpen);
  const mobileDock = (
    <div className={styles.mobileActionDockPanel}>
      <div className={styles.stickyActionMeta}>
        <div className={styles.stickyActionTitle}>Действия по магазину</div>
        <div className={styles.stickyActionHint}>{currentSaveState}</div>
      </div>
      <div className={styles.stickyActionButtons}>
        {settingsTab === "categories" ? (
          <button
            type="button"
            className="btn ghost"
            onClick={() => setBulkFillOpen(true)}
          >
            Заполнить всем
          </button>
        ) : null}
        <button
          type="button"
          className={`btn ${styles.recalculateButton}`}
          disabled={Boolean(monitoringRunning.strategy_refresh)}
          onClick={() => void runMonitoringJob("strategy_refresh")}
        >
          {monitoringRunning.strategy_refresh ? "Пересчет..." : "Пересчитать цены"}
        </button>
      </div>
    </div>
  );

  return (
    <div className={`${styles.settingsShell} ${styles.mobileSettingsShell}`}>
      <div className={`${styles.settingsMain} ${styles.mobileSettingsMain}`}>
        <MobileDockLayout dock={mobileDock} dockVisible={mobileDockVisible} dockHeight={232} dockOffset={82}>
        <WorkspaceSurface className={layoutStyles.heroSurface}>
          <div className={styles.mobileNavStack}>
            <WorkspaceTabs
              className={styles.pricingPrimaryTabs}
              items={sectionItems.map((item) => ({ id: item.id, label: item.label }))}
              activeId={settingsTab}
              onChange={(id) => setSettingsTab(id)}
            />
            {!isSalesPlanSection ? (
              <div className={styles.mobileStoreStrip}>
                <div className={styles.pricingStripLabel}>Магазин</div>
                <WorkspaceTabs
                  className={styles.pricingStoreTabs}
                  items={storeTabs.map((store) => ({
                    id: store.key,
                    label: store.storeName,
                    meta: store.platformLabel,
                  }))}
                  activeId={activeStoreTabKey}
                onChange={setActiveStoreTabKey}
                />
              </div>
            ) : null}
          </div>

          <WorkspaceHeader
            title="Настройки ценообразования"
            subtitle="Единая рабочая зона для целей продаж, категорийных правил и логистики магазинов."
          />

          {!isSalesPlanSection ? (
            <WorkspaceToolbar className={styles.pricingToolbarMobile}>
              <div className={styles.pricingToolbarBlock}>
                <div className={styles.pricingStripLabel}>Магазин</div>
                <WorkspaceTabs
                  className={styles.pricingStoreTabs}
                  items={storeTabs.map((store) => ({
                    id: store.key,
                    label: store.storeName,
                    meta: store.platformLabel,
                  }))}
                  activeId={activeStoreTabKey}
                  onChange={setActiveStoreTabKey}
                />
              </div>
            </WorkspaceToolbar>
          ) : null}
        </WorkspaceSurface>

        {settingsTab === "sales_plan" ? (
          <SalesPlanSection
            loading={salesPlanLoading}
            error={error || salesPlanError}
            rows={salesPlanRows}
            savingMap={salesPlanSaving}
            saveError={salesPlanError}
            onSaveRows={saveSalesPlanRows}
          />
        ) : null}

        {settingsTab === "categories" ? (
          <GeneralSettingsSection
            mobileMode={true}
            loading={loading}
            error={error}
            itemsError={itemsError}
            itemsLoading={itemsLoading}
            categoryRows={categoryRows}
            tableColumns={tableColumns}
            cellDrafts={cellDrafts}
            cellSaving={cellSaving}
            getCellKey={getCellKey}
            defaultFieldValue={defaultFieldValue}
            formatNum={formatNum}
            queueSaveCell={queueSaveCell}
            flushSaveCell={flushSaveCell}
            mobileCatalogOpen={mobileCatalogOpen}
            onOpenMobileCatalog={() => setMobileCatalogOpen(true)}
            onCloseMobileCatalog={() => setMobileCatalogOpen(false)}
          />
        ) : null}

        {settingsTab === "logistics" ? (
          <>
            <div className={styles.controlsRow}>
              <LogisticsSettingsPanel
                moneySign={moneySign}
                logisticsStoreSettings={logisticsStoreSettings}
                logisticsFieldErrors={logisticsFieldErrors}
                logisticsStoreSaving={logisticsStoreSaving}
                logisticsStoreError={logisticsStoreError}
                logisticsStoreSavedAt={logisticsStoreSavedAt}
                getLogisticsNumericValue={getLogisticsNumericValue}
                setLogisticsField={setLogisticsField}
                setLogisticsNumericField={setLogisticsNumericField}
                onLogisticsNumericBlur={onLogisticsNumericBlur}
              />
            </div>
            <LogisticsSettingsSection
              moneySign={moneySign}
              loading={loading}
              error={error}
              logisticsError={logisticsError}
              logisticsLoading={logisticsLoading}
              logisticsRows={logisticsRows}
              logisticsTreeRoots={logisticsTreeRoots}
              logisticsTreePath={logisticsTreePath}
              logisticsSearch={logisticsSearch}
              logisticsPage={logisticsPage}
              logisticsPageSize={logisticsPageSize}
              logisticsTotal={logisticsTotal}
              logisticsPageSizeOptions={logisticsPageSizeOptions}
              activePlatform={activePlatform}
              activeStoreId={activeStoreId}
              logisticsCellDrafts={logisticsCellDrafts}
              logisticsCellSaving={logisticsCellSaving}
              setLogisticsPage={setLogisticsPage}
              setLogisticsPageSize={setLogisticsPageSize}
              setLogisticsSearch={setLogisticsSearch}
              setLogisticsTreePath={setLogisticsTreePath}
              setLogisticsImportOpen={setLogisticsImportOpen}
              toLiveLogisticsRow={toLiveLogisticsRow}
              fmtCell={fmtCell}
              getLogisticsCellKey={getLogisticsCellKey}
              setLogisticsCellDraftByKey={setLogisticsCellDraftByKey}
              commitLogisticsCell={commitLogisticsCell}
              setLogisticsCellDrafts={setLogisticsCellDrafts}
            />
          </>
        ) : null}

        </MobileDockLayout>
      </div>
    </div>
  );
}
