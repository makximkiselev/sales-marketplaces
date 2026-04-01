import styles from "./PricingSettingsPage.module.css";
import { MobileDockLayout } from "../../../components/page/PageKit";
import { WorkspaceHeader, WorkspaceTabs } from "../../../components/page/WorkspaceKit";
import { GeneralSettingsPanel } from "./components/GeneralSettingsPanel";
import { GeneralSettingsSection } from "./components/GeneralSettingsSection";
import { LogisticsSettingsPanel } from "./components/LogisticsSettingsPanel";
import { LogisticsSettingsSection } from "./components/LogisticsSettingsSection";
import { SalesPlanSection } from "./components/SalesPlanSection";
import type { PricingSettingsRendererProps } from "./PricingSettingsRendererTypes";

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
    earningMode,
    earningUnit,
    targetDrr,
    cogsSource,
    stockSource,
    activeTargetValue,
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
    setEarningMode,
    setEarningUnit,
    setTargetDrr,
    setCogsSource,
    setCogsModalOpen,
    setStockSource,
    setStockModalOpen,
    setActiveTargetValue,
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
        <div className={styles.mobileCommandDeck}>
          <div className={styles.mobileNavStack}>
            <WorkspaceTabs
              className={styles.mobileWorkspaceTabs}
              items={sectionItems.map((item) => ({ id: item.id, label: item.label }))}
              activeId={settingsTab}
              onChange={(id) => setSettingsTab(id)}
            />
            {!isSalesPlanSection ? (
              <div className={styles.mobileStoreStrip}>
                <div className={styles.pricingStripLabel}>Магазин</div>
                <WorkspaceTabs
                  className={styles.mobileWorkspaceTabs}
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

          <div className={styles.pricingHeroPanel}>
            <WorkspaceHeader
              className={styles.pricingHeroHeader}
              title={activeSection.title}
              subtitle={
                isSalesPlanSection
                  ? "Store-level цели, режимы прибыли и стратегия для всех магазинов."
                  : activeSection.description
              }
              meta={
                !isSalesPlanSection && activeStore ? (
                  <div className={styles.workspaceHeroChips}>
                    <span className={styles.workspaceHeroChip}>{activeStore.platformLabel}</span>
                    <span className={styles.workspaceHeroChip}>{activeStore.storeName}</span>
                    <span className={styles.workspaceHeroChip}>Валюта {moneySign}</span>
                  </div>
                ) : isSalesPlanSection ? <span className={styles.workspaceHeroChip}>Все магазины</span> : undefined
              }
            />
          </div>
        </div>

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
          <>
            <div className={styles.controlsRow}>
              <GeneralSettingsPanel
                earningMode={earningMode}
                earningUnit={earningUnit}
                moneySign={moneySign}
                activeTargetValue={activeTargetValue}
                targetDrr={targetDrr}
                cogsSource={cogsSource}
                stockSource={stockSource}
                activeStoreId={activeStoreId}
                showTargets={false}
                showRelay={true}
                showSources={true}
                setEarningMode={setEarningMode}
                setEarningUnit={setEarningUnit}
                setActiveTargetValue={setActiveTargetValue}
                setTargetDrr={setTargetDrr}
                setCogsModalOpen={setCogsModalOpen}
                setCogsSource={setCogsSource}
                setStockModalOpen={setStockModalOpen}
                setStockSource={setStockSource}
              />
            </div>
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
          </>
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
