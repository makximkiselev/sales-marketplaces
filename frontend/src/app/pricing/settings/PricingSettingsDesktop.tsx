import styles from "./PricingSettingsPage.module.css";
import { ControlTabs } from "../../../components/page/ControlKit";
import { GeneralSettingsPanel } from "./components/GeneralSettingsPanel";
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
    earningMode,
    earningUnit,
    targetDrr,
    itemsLoading,
    itemsError,
    cogsSource,
    stockSource,
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
    activeTargetValue,
    setActiveStoreTabKey,
    setEarningMode,
    setEarningUnit,
    setTargetDrr,
    setCogsModalOpen,
    setStockModalOpen,
    setSettingsTab,
    setLogisticsPage,
    setLogisticsPageSize,
    setLogisticsSearch,
    setLogisticsImportOpen,
    setActiveTargetValue,
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
    <div className={`${styles.settingsShell} ${isSalesPlanSection ? styles.settingsShellFull : ""}`}>
      <div className={`${styles.settingsMain} ${isSalesPlanSection ? styles.settingsMainCompact : ""}`}>
        <div className={`${styles.workspaceHero} ${isSalesPlanSection ? styles.workspaceHeroCompact : ""}`}>
          <div className={styles.desktopSectionTabs}>
            <ControlTabs
              className={styles.desktopSectionTabsRow}
              items={sectionItems.map((item) => ({ id: item.id, label: item.label }))}
              activeId={settingsTab}
              onChange={(id) => setSettingsTab(id)}
            />
          </div>
          {!isSalesPlanSection ? (
            <div className={styles.desktopStoreTabs}>
              <ControlTabs
                className={styles.storeTabsBar}
                items={storeTabs.map((store) => ({
                  id: store.key,
                  label: store.storeName,
                  badge: store.platformLabel,
                }))}
                activeId={activeStoreTabKey}
                onChange={setActiveStoreTabKey}
              />
            </div>
          ) : null}
          <div className={styles.workspaceHeroMain}>
            <div className={styles.workspaceTitleRow}>
              <h2 className={styles.workspaceTitle}>{activeSection.title}</h2>
              {isSalesPlanSection ? <span className={styles.workspaceHeroChip}>Все магазины</span> : null}
            </div>
            <p className={styles.workspaceSubtitle}>
              {isSalesPlanSection
                ? "Store-level цели, режимы прибыли и стратегия для всех магазинов в одном рабочем пространстве."
                : activeSection.description}
            </p>
            {!isSalesPlanSection && activeStore ? (
              <div className={styles.workspaceHeroChips}>
                <span className={styles.workspaceHeroChip}>{activeStore.platformLabel}</span>
                <span className={styles.workspaceHeroChip}>Валюта {moneySign}</span>
              </div>
            ) : null}
          </div>
        </div>

        {settingsTab === "sources" ? (
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
              showRelay={false}
              showSources={true}
              setEarningMode={setEarningMode}
              setEarningUnit={setEarningUnit}
              setActiveTargetValue={setActiveTargetValue}
              setTargetDrr={setTargetDrr}
              setCogsModalOpen={setCogsModalOpen}
              setCogsSource={() => {}}
              setStockModalOpen={setStockModalOpen}
              setStockSource={() => {}}
            />
          </div>
        ) : null}

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
            setLogisticsImportOpen={setLogisticsImportOpen}
            toLiveLogisticsRow={toLiveLogisticsRow}
            fmtCell={fmtCell}
            getLogisticsCellKey={getLogisticsCellKey}
            setLogisticsCellDraftByKey={setLogisticsCellDraftByKey}
            commitLogisticsCell={commitLogisticsCell}
            setLogisticsCellDrafts={setLogisticsCellDrafts}
          />
        ) : null}

        {activeStoreId && !isSalesPlanSection && settingsTab !== "sources" ? (
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
  );
}
