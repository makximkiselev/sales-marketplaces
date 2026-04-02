import styles from "./PricingSettingsPage.module.css";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceTabs, WorkspaceToolbar } from "../../../components/page/WorkspaceKit";
import { GeneralSettingsSection } from "./components/GeneralSettingsSection";
import { LogisticsSettingsPanel } from "./components/LogisticsSettingsPanel";
import { LogisticsSettingsSection } from "./components/LogisticsSettingsSection";
import { SalesPlanSection } from "./components/SalesPlanSection";
import type { PricingSettingsRendererProps } from "./PricingSettingsRendererTypes";

export function PricingSettingsDesktop({
  controller,
  sectionItems,
  activeSection,
  activeStore,
  isSalesPlanSection,
  currentSaveState,
  setBulkFillOpen,
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
    storeSettingsSaving,
    storeSettingsError,
    storeSettingsSavedAt,
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

  return (
    <div className={styles.settingsShell}>
      <div className={styles.settingsMain}>
        <div className={styles.pricingWorkbenchShell}>
          <WorkspaceSurface className={styles.pricingHeroSurface}>
            <div className={styles.pricingModeStrip}>
              <WorkspaceTabs
                className={styles.pricingPrimaryTabs}
                items={sectionItems.map((item) => ({ id: item.id, label: item.label }))}
                activeId={settingsTab}
                onChange={(id) => setSettingsTab(id)}
              />
            </div>

            <WorkspaceHeader
              className={styles.pricingHeroHeader}
              title="Настройки ценообразования"
              subtitle="Единое рабочее пространство для целей продаж, категорийных правил и логистики магазинов."
              meta={(
                <div className={styles.pricingHeroMeta}>
                  <span className={styles.pricingMetaChip}>{activeSection.title}</span>
                  {!isSalesPlanSection && activeStore ? (
                    <span className={styles.pricingMetaChip}>{activeStore.storeName}</span>
                  ) : (
                    <span className={styles.pricingMetaChip}>Все магазины</span>
                  )}
                </div>
              )}
            />

            <WorkspaceToolbar className={styles.pricingToolbar}>
              {!isSalesPlanSection ? (
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
              ) : (
                <div className={styles.pricingToolbarMeta}>
                  <span className={styles.pricingMetaChip}>Все магазины</span>
                </div>
              )}

              {!isSalesPlanSection && activeStore ? (
                <div className={styles.pricingToolbarMeta}>
                  <span className={styles.pricingMetaChip}>{activeStore.platformLabel}</span>
                  <span className={styles.pricingMetaChip}>Валюта {moneySign}</span>
                </div>
              ) : null}
            </WorkspaceToolbar>
          </WorkspaceSurface>

          {settingsTab === "logistics" ? (
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
          ) : null}

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
            />
          ) : null}

          {settingsTab === "logistics" ? (
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
          ) : null}

          {activeStoreId && !isSalesPlanSection ? (
            <div className={`${styles.stickyActionBar} ${styles.stickyActionBarAlways}`}>
              <div className={styles.stickyActionMeta}>
                <div className={styles.stickyActionTitle}>Действия по магазину</div>
                <div className={styles.stickyActionHint}>{currentSaveState}</div>
              </div>
              <div className={styles.stickyActionButtons}>
                {settingsTab === "categories" ? (
                  <button
                    type="button"
                    className={`btn ghost ${styles.categoryBulkAction}`}
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
          ) : null}
        </div>
      </div>
    </div>
  );
}
