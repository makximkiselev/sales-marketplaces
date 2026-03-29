import { useState } from "react";
import styles from "./PricingSettingsPage.module.css";
import { PageFrame } from "../../../components/page/PageKit";
import { ControlTabs } from "../../../components/page/ControlKit";
import { CogsSourceModal } from "./components/CogsSourceModal";
import { LogisticsImportModal } from "./components/LogisticsImportModal";
import { BulkFillColumnModal } from "./components/BulkFillColumnModal";
import { GeneralSettingsPanel } from "./components/GeneralSettingsPanel";
import { LogisticsSettingsPanel } from "./components/LogisticsSettingsPanel";
import { GeneralSettingsSection } from "./components/GeneralSettingsSection";
import { LogisticsSettingsSection } from "./components/LogisticsSettingsSection";
import { SalesPlanSection } from "./components/SalesPlanSection";

import { usePricingSettingsController } from "./usePricingSettingsController";
import type { EditableFieldKey } from "./types";

export default function PricingSettingsPage() {
  const [bulkField, setBulkField] = useState<EditableFieldKey>("commission_percent");
  const [bulkFillOpen, setBulkFillOpen] = useState(false);
  const {
    loading,
    refreshing,
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
    refreshStoreDataFromPlatform,
    runMonitoringJob,
    getCellKey,
    defaultFieldValue,
    formatNum,
    queueSaveCell,
    flushSaveCell,
    applyColumnValue,
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

  const sectionItems = [
    {
      id: "sales_plan" as const,
      label: "План продаж",
      title: "План продаж",
      description: "Базовые цели по выручке и количеству для магазинов.",
    },
    {
      id: "categories" as const,
      label: "Категорийные затраты",
      title: "Категорийные затраты",
      description: "Комиссии, реклама и правила расчёта по категориям.",
    },
    {
      id: "sources" as const,
      label: "Источники данных",
      title: "Источники данных",
      description: "Источники себестоимости и остатков для активного магазина.",
    },
    {
      id: "logistics" as const,
      label: "Логистические затраты",
      title: "Логистические затраты",
      description: "Магазинные коэффициенты и затраты по товарам.",
    },
  ];
  const activeSection = sectionItems.find((item) => item.id === settingsTab) ?? sectionItems[0];
  const activeStore = storeTabs.find((store) => store.key === activeStoreTabKey) ?? null;
  const isSalesPlanSection = settingsTab === "sales_plan";
  const bulkFillColumns = tableColumns
    .filter((column) => column.kind === "input" && column.field)
    .map((column) => ({ field: column.field as EditableFieldKey, label: column.label }));
  const currentSavedAt = settingsTab === "logistics" ? logisticsStoreSavedAt : storeSettingsSavedAt;
  const currentSaveState = settingsTab === "logistics"
    ? logisticsStoreSaving
      ? "Сохранение логистики..."
      : logisticsStoreError
        ? `Ошибка: ${logisticsStoreError}`
        : currentSavedAt
          ? `Сохранено ${new Date(currentSavedAt).toLocaleString("ru-RU")}`
          : "Изменения ещё не зафиксированы"
    : settingsTab === "categories" || settingsTab === "sources"
      ? storeSettingsSaving
        ? "Автосохранение..."
        : storeSettingsError
          ? `Ошибка: ${storeSettingsError}`
          : currentSavedAt
            ? `Сохранено ${new Date(currentSavedAt).toLocaleString("ru-RU")}`
            : "Изменения ещё не зафиксированы"
      : salesPlanError
        ? `Ошибка: ${salesPlanError}`
        : "Редактирование вручную";

  return (
    <>
      <PageFrame
        title="Настройки ценообразования"
        subtitle="Единая рабочая зона для целей продаж, категорийных правил и логистики магазинов."
        className={styles.headCard}
        innerClassName={`${styles.headCardInner} ${isSalesPlanSection ? styles.headCardInnerCompact : ""}`}
      >
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
            ) : null}
          </div>
        </div>
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

      {settingsTab === "categories" && bulkFillOpen && bulkFillColumns.length ? (
        <BulkFillColumnModal
          fields={bulkFillColumns}
          initialField={bulkField}
          onClose={() => setBulkFillOpen(false)}
          onConfirm={async (field, value) => {
            setBulkField(field);
            await applyColumnValue(field, value);
            setBulkFillOpen(false);
          }}
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
