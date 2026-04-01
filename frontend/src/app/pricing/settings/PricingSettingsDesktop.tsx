import styles from "./PricingSettingsPage.module.css";
import { WorkspaceTabs } from "../../../components/page/WorkspaceKit";
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
          <div className={styles.pricingWorkbenchHead}>
            <div className={styles.pricingWorkbenchTabs}>
              <WorkspaceTabs
                className={styles.pricingWorkbenchSectionTabs}
                items={sectionItems.map((item) => ({ id: item.id, label: item.label }))}
                activeId={settingsTab}
                onChange={(id) => setSettingsTab(id)}
              />
              {!isSalesPlanSection ? (
                <WorkspaceTabs
                  className={styles.pricingWorkbenchStoreTabs}
                  items={storeTabs.map((store) => ({
                    id: store.key,
                    label: store.storeName,
                    meta: store.platformLabel,
                  }))}
                  activeId={activeStoreTabKey}
                  onChange={setActiveStoreTabKey}
                />
              ) : null}
            </div>

            <div className={styles.pricingWorkbenchContext}>
              <div className={styles.pricingWorkbenchTitleBlock}>
                <h1 className={styles.pricingWorkbenchTitle}>{activeSection.title}</h1>
                <p className={styles.pricingWorkbenchSubtitle}>
                  {isSalesPlanSection
                    ? "Цели продаж и стратегия по всем магазинам."
                    : activeSection.description}
                </p>
              </div>
              <div className={styles.pricingWorkbenchMeta}>
                {!isSalesPlanSection && activeStore ? (
                  <>
                    <span className={styles.pricingWorkbenchMetaChip}>{activeStore.platformLabel}</span>
                    <span className={styles.pricingWorkbenchMetaChip}>{activeStore.storeName}</span>
                    <span className={styles.pricingWorkbenchMetaChip}>Валюта {moneySign}</span>
                  </>
                ) : (
                  <span className={styles.pricingWorkbenchMetaChip}>Все магазины</span>
                )}
              </div>
            </div>
          </div>

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
