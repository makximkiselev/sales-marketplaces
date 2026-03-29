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
      id: "logistics" as const,
      label: "Логистические затраты",
      title: "Логистические затраты",
      description: "Магазинные коэффициенты и затраты по товарам.",
    },
  ];
  const activeSection = sectionItems.find((item) => item.id === settingsTab) ?? sectionItems[0];
  const activeStore = storeTabs.find((store) => store.key === activeStoreTabKey) ?? null;
  const isSalesPlanSection = settingsTab === "sales_plan";
  const showWorkspaceHero = !isSalesPlanSection;
  const currentSavedAt = settingsTab === "logistics" ? logisticsStoreSavedAt : storeSettingsSavedAt;
  const currentSaveState = settingsTab === "logistics"
    ? logisticsStoreSaving
      ? "Сохранение логистики..."
      : logisticsStoreError
        ? `Ошибка: ${logisticsStoreError}`
        : currentSavedAt
          ? `Сохранено ${new Date(currentSavedAt).toLocaleString("ru-RU")}`
          : "Изменения ещё не зафиксированы"
    : settingsTab === "categories"
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
        toolbarLeft={
          <div className={styles.headerControlStack}>
            <div className={styles.mobileSectionTabs}>
              <ControlTabs
                className={styles.settingsTabsRow}
                items={sectionItems.map((item) => ({ id: item.id, label: item.label }))}
                activeId={settingsTab}
                onChange={(id) => setSettingsTab(id)}
              />
            </div>
            {!isSalesPlanSection ? (
              <div className={styles.mobileStoreTabs}>
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
              </div>
            ) : null}
          </div>
        }
      >
        <div className={styles.settingsShell}>
          <aside className={styles.desktopRail}>
            <section className={styles.desktopRailCard}>
              <div className={styles.desktopRailTitle}>Разделы</div>
              <div className={styles.desktopRailNav}>
                {sectionItems.map((item) => (
                  <button
                    key={item.id}
                    type="button"
                    className={`${styles.sectionNavButton} ${settingsTab === item.id ? styles.sectionNavButtonActive : ""}`}
                    onClick={() => setSettingsTab(item.id)}
                  >
                    <span className={styles.sectionNavLabel}>{item.label}</span>
                    <span className={styles.sectionNavHint}>{item.description}</span>
                  </button>
                ))}
              </div>
            </section>

            {!isSalesPlanSection ? (
              <section className={styles.desktopRailCard}>
                <div className={styles.desktopRailTitle}>Магазины</div>
                <div className={styles.desktopStoreList}>
                  {storeTabs.map((store) => (
                    <button
                      key={store.key}
                      type="button"
                      className={`${styles.desktopStoreButton} ${activeStoreTabKey === store.key ? styles.desktopStoreButtonActive : ""}`}
                      onClick={() => setActiveStoreTabKey(store.key)}
                    >
                      <span className={styles.desktopStoreButtonTitle}>{store.storeName}</span>
                      <span className={styles.desktopStoreButtonMeta}>{store.platformLabel}</span>
                    </button>
                  ))}
                </div>
              </section>
            ) : null}
          </aside>

          <div className={styles.settingsMain}>
            {showWorkspaceHero ? (
              <div className={styles.workspaceHero}>
                <div className={styles.workspaceHeroMain}>
                  <div className={styles.workspaceEyebrow}>{activeSection.title}</div>
                  <h2 className={styles.workspaceTitle}>{activeStore?.storeName || activeSection.title}</h2>
                  <p className={styles.workspaceSubtitle}>{activeSection.description}</p>
                  <div className={styles.workspaceHeroChips}>
                    {activeStore?.platformLabel ? (
                      <span className={styles.workspaceHeroChip}>{activeStore.platformLabel}</span>
                    ) : null}
                    <span className={styles.workspaceHeroChip}>Валюта {moneySign}</span>
                  </div>
                </div>
              </div>
            ) : (
              <div className={styles.salesPlanIntro}>
                <div className={styles.salesPlanIntroTitle}>План продаж по всем магазинам</div>
                <div className={styles.salesPlanIntroText}>
                  Здесь задаются store-level цели и режим расчёта, которые дальше используются в ценообразовании и стратегии.
                </div>
              </div>
            )}

            {activeStoreId && !isSalesPlanSection ? (
              <div className={styles.stickyActionBar}>
                <div className={styles.stickyActionMeta}>
                  <div className={styles.stickyActionTitle}>Изменения сохраняются автоматически</div>
                  <div className={styles.stickyActionHint}>{currentSaveState}</div>
                </div>
                <div className={styles.stickyActionButtons}>
                  <button
                    type="button"
                    className="btn ghost"
                    disabled={refreshing}
                    onClick={() => void refreshStoreDataFromPlatform()}
                  >
                    {refreshing ? "Обновление..." : "Обновить данные"}
                  </button>
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
                  showTargets={false}
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
                applyColumnValue={applyColumnValue}
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
