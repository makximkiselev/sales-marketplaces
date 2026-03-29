"use client";

import styles from "./PricingSettingsPage.module.css";
import { PageFrame } from "../../../components/page/PageKit";
import { ControlTabs } from "../../../components/page/ControlKit";
import { CogsSourceModal } from "./components/CogsSourceModal";
import { LogisticsImportModal } from "./components/LogisticsImportModal";
import { GeneralSettingsPanel } from "./components/GeneralSettingsPanel";
import { LogisticsSettingsPanel } from "./components/LogisticsSettingsPanel";
import { GeneralSettingsSection } from "./components/GeneralSettingsSection";
import { LogisticsSettingsSection } from "./components/LogisticsSettingsSection";
import { SalesPlanSection } from "./components/SalesPlanSection";

import { usePricingSettingsController } from "./usePricingSettingsController";

export default function PricingSettingsPage() {
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
    cogsModalOpen,
    stockSource,
    stockModalOpen,
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
    logisticsImportOpen,
    moneySign,
    tableColumns,
    activeTargetValue,
    setActiveStoreTabKey,
    setEarningMode,
    setEarningUnit,
    setTargetDrr,
    setCogsSource,
    setCogsModalOpen,
    setStockSource,
    setStockModalOpen,
    setSettingsTab,
    setLogisticsPage,
    setLogisticsPageSize,
    setLogisticsSearch,
    setLogisticsImportOpen,
    setLogisticsCellDrafts,
    setActiveTargetValue,
    saveSalesPlanRows,
    runMonitoringJob,
    getCellKey,
    defaultFieldValue,
    formatNum,
    queueSaveCell,
    setLogisticsField,
    setLogisticsNumericField,
    onLogisticsNumericBlur,
    getLogisticsNumericValue,
    toLiveLogisticsRow,
    fmtCell,
    getLogisticsCellKey,
    setLogisticsCellDraftByKey,
    commitLogisticsCell,
    handleLogisticsImportDone,
  } = usePricingSettingsController();

  return (
    <>
      <PageFrame
        title="Настройки ценообразования"
        subtitle="План продаж, категорийные и логистические затраты по магазинам."
        className={styles.headCard}
        meta={
          <>
            {(settingsTab === "categories" || settingsTab === "logistics") && activeStoreId ? (
              <div className={styles.storeSettingsMeta}>
                {storeSettingsSaving
                  ? "Настройки магазина: сохраняются..."
                  : storeSettingsError
                    ? `Ошибка сохранения: ${storeSettingsError}`
                    : storeSettingsSavedAt
                      ? `Настройки магазина сохранены: ${new Date(storeSettingsSavedAt).toLocaleString("ru-RU")}`
                      : "Настройки магазина еще не сохранены"}
              </div>
            ) : null}
          </>
        }
        toolbarLeft={
          <ControlTabs
            className={styles.settingsTabsRow}
            items={[
              { id: "sales_plan", label: "План продаж" },
              { id: "categories", label: "Категорийные затраты" },
              { id: "logistics", label: "Логистические затраты" },
            ]}
            activeId={settingsTab}
            onChange={(id) => setSettingsTab(id)}
          />
        }
        toolbarRight={
          <div className={styles.toolbarActions}>
            {activeStoreId ? (
              <button
                type="button"
                className={`btn ${styles.recalculateButton}`}
                disabled={Boolean(monitoringRunning.strategy_refresh)}
                onClick={() => void runMonitoringJob("strategy_refresh")}
              >
                {monitoringRunning.strategy_refresh ? "Пересчет..." : "Пересчитать цены"}
              </button>
            ) : null}
            {settingsTab !== "sales_plan" ? (
              <ControlTabs
                className={styles.storeTabsRow}
                items={storeTabs.map((store) => ({
                  id: store.key,
                  label: store.storeName,
                  badge: store.platformLabel,
                }))}
                activeId={activeStoreTabKey}
                onChange={setActiveStoreTabKey}
              />
            ) : null}
          </div>
        }
      >
        {settingsTab === "categories" ? (
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
      </PageFrame>

      {cogsModalOpen && earningMode === "profit" && activeStoreId && (
        <CogsSourceModal
          current={cogsSource}
          onSave={(src) => {
            setCogsSource(src);
            setCogsModalOpen(false);
          }}
          onClose={() => setCogsModalOpen(false)}
        />
      )}

      {stockModalOpen && activeStoreId && (
        <CogsSourceModal
          current={stockSource}
          title="Источник остатка"
          valueColumnLabel="Столбец с остатком"
          onSave={(src) => {
            setStockSource(src);
            setStockModalOpen(false);
          }}
          onClose={() => setStockModalOpen(false)}
        />
      )}

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

      <LogisticsImportModal
        open={logisticsImportOpen}
        platform={activePlatform}
        storeId={activeStoreId}
        onClose={() => setLogisticsImportOpen(false)}
        onDone={handleLogisticsImportDone}
      />
    </>
  );
}
