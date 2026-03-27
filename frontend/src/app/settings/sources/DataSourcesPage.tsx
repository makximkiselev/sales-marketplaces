"use client";

import styles from "./DataSourcesPage.module.css";
import { PageFrame } from "../../../components/page/PageKit";
import { ControlTabs } from "../../../components/page/ControlKit";
import { PanelCard, PanelGrid, SectionBlock } from "../../../components/page/SectionKit";
import { DeleteConfirmModal } from "./components/DeleteConfirmModal";
import { ExternalSystemsPanel } from "./components/ExternalSystemsPanel";
import { GoogleSheetsWizardModal } from "./components/GoogleSheetsWizardModal";
import { GoogleTablesPanel } from "./components/GoogleTablesPanel";
import { OzonPanel } from "./components/OzonPanel";
import { OzonWizardModal } from "./components/OzonWizardModal";
import { WildberriesPanel } from "./components/WildberriesPanel";
import { YandexMarketPanel } from "./components/YandexMarketPanel";
import { YandexTablesPanel } from "./components/YandexTablesPanel";
import { YandexWizardModal } from "./components/YandexWizardModal";
import { useSourcesPageController } from "./useSourcesPageController";
import { platformMeta } from "./types";

export default function DataSourcesPage() {
  const {
    sectionTab,
    setSectionTab,
    integrations,
    loading,
    refreshAllLoading,
    lastRefreshAt,
    wizardOpen,
    ymWizardMode,
    step,
    apiKey,
    businessId,
    campaigns,
    selectedCampaignIds,
    wizardLoading,
    wizardError,
    shopCheckLoading,
    gsSourceCheckLoading,
    sourceFlowSavingKey,
    flowSavingKey,
    currencySavingKey,
    fulfillmentSavingKey,
    flowError,
    gsWizardOpen,
    gsWizardMode,
    gsStep,
    gsLoading,
    gsError,
    gsTitle,
    gsSpreadsheet,
    gsCredFileName,
    gsKeyUploading,
    gsDropActive,
    gsKeyUploadOk,
    gsKeyUploadMessage,
    gsSelectedAccountId,
    gsWorksheets,
    gsWorksheet,
    gsEditingSourceId,
    ozWizardOpen,
    ozClientId,
    ozApiKey,
    ozSellerId,
    ozSellerName,
    ozLoading,
    ozError,
    ozCheckLoading,
    ymActionBusinessId,
    ozActionClientId,
    deleteRequest,
    deleteBusy,
    deleteError,
    gsheetsSources,
    ymAccounts,
    ozAccounts,
    sortedYmAccounts,
    totalYmShops,
    sortedOzonAccounts,
    ymActionAccount,
    ozActionAccount,
    headerImportOn,
    headerExportOn,
    headerFlowDisabled,
    setApiKey,
    setBusinessId,
    setOzClientId,
    setOzApiKey,
    setGsTitle,
    setGsSpreadsheet,
    setGsSelectedAccountId,
    setGsWorksheet,
    setGsDropActive,
    setYmActionBusinessId,
    setOzActionClientId,
    setStep,
    setGsStep,
    updateHeaderFlow,
    updateDataFlow,
    updateStoreCurrency,
    updateStoreFulfillment,
    openWizard,
    openAddShop,
    openEditAccount,
    closeWizard,
    goToYmStep,
    proceedFromStep2,
    connectYandex,
    toggleCampaign,
    formatDateTime,
    formatRefreshLabel,
    checkGsheetSource,
    updateSourceFlow,
    checkShop,
    openGsWizard,
    chooseExistingGsSource,
    closeGsWizard,
    goToGsStep,
    verifyGsheets,
    connectGsheets,
    onGoogleKeyFileSelected,
    openDeleteConfirm,
    closeDeleteConfirm,
    getDeleteConfirmText,
    confirmDelete,
    useExistingGoogleAccount,
    openOzonWizard,
    closeOzonWizard,
    connectOzon,
    checkOzonAccount,
    refreshAllStatuses,
  } = useSourcesPageController();

  return (
    <>
      <PageFrame
        title="Источники данных"
        subtitle="Интеграции, таблицы и внешние системы."
        className={styles.sourcesHeadCard}
        actions={
          <div className={styles.refreshBlock}>
            <button
              className={`btn primary ${styles.refreshButton} ${refreshAllLoading ? styles.refreshButtonLoading : ""}`}
              onClick={() => void refreshAllStatuses()}
              disabled={refreshAllLoading}
            >
              {refreshAllLoading ? "Обновление..." : "Обновить источники"}
            </button>
          </div>
        }
        meta={<div className={styles.refreshMeta}>Последнее обновление: {formatRefreshLabel(lastRefreshAt)}</div>}
        toolbarLeft={
          <ControlTabs
            className={styles.sourcesTabs}
            items={[
              { id: "all", label: "Все источники" },
              { id: "platforms", label: "Площадки" },
              { id: "tables", label: "Внешние таблицы" },
              { id: "external", label: "Внешние системы" },
            ]}
            activeId={sectionTab}
            onChange={setSectionTab}
          />
        }
        toolbarRight={
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
        }
      >
        {flowError ? <div className={`status error ${styles.flowErrorInline}`}>{flowError}</div> : null}
      </PageFrame>

      {(sectionTab === "all" || sectionTab === "platforms") ? (
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
              ymActionBusinessId={ymActionBusinessId}
              setYmActionBusinessId={setYmActionBusinessId}
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
              ozCheckLoading={ozCheckLoading}
              flowSavingKey={flowSavingKey}
              currencySavingKey={currencySavingKey}
              fulfillmentSavingKey={fulfillmentSavingKey}
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

      {(sectionTab === "all" || sectionTab === "tables") ? (
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

      {(sectionTab === "all" || sectionTab === "external") ? (
      <SectionBlock title="Внешние системы">
          <ExternalSystemsPanel />
      </SectionBlock>
      ) : null}

      {deleteRequest ? (
        <DeleteConfirmModal
          title={getDeleteConfirmText(deleteRequest)}
          error={deleteError}
          busy={deleteBusy}
          onClose={closeDeleteConfirm}
          onConfirm={() => void confirmDelete()}
        />
      ) : null}

      <OzonWizardModal
        open={ozWizardOpen}
        clientId={ozClientId}
        apiKey={ozApiKey}
        sellerId={ozSellerId}
        sellerName={ozSellerName}
        loading={ozLoading}
        error={ozError}
        onClose={closeOzonWizard}
        onChangeClientId={setOzClientId}
        onChangeApiKey={setOzApiKey}
        onConnect={() => void connectOzon()}
      />

      <YandexWizardModal
        open={wizardOpen}
        step={step}
        mode={ymWizardMode}
        apiKey={apiKey}
        businessId={businessId}
        campaigns={campaigns}
        selectedCampaignIds={selectedCampaignIds}
        loading={wizardLoading}
        error={wizardError}
        onClose={closeWizard}
        onGoToStep={(nextStep) => void goToYmStep(nextStep)}
        onChangeApiKey={setApiKey}
        onChangeBusinessId={setBusinessId}
        onToggleCampaign={toggleCampaign}
        onCheck={() => void proceedFromStep2()}
        onConnect={() => void connectYandex()}
        onBack={() => setStep(1)}
      />

      <GoogleSheetsWizardModal
        open={gsWizardOpen}
        mode={gsWizardMode}
        step={gsStep}
        editingSourceId={gsEditingSourceId}
        loading={gsLoading}
        error={gsError}
        title={gsTitle}
        spreadsheet={gsSpreadsheet}
        selectedAccountId={gsSelectedAccountId}
        integrations={integrations}
        credFileName={gsCredFileName}
        keyUploading={gsKeyUploading}
        dropActive={gsDropActive}
        keyUploadOk={gsKeyUploadOk}
        keyUploadMessage={gsKeyUploadMessage}
        worksheets={gsWorksheets}
        worksheet={gsWorksheet}
        sources={gsheetsSources}
        onClose={closeGsWizard}
        onGoToStep={(nextStep) => void goToGsStep(nextStep)}
        onChooseExistingSource={(sourceId) => void chooseExistingGsSource(sourceId, gsheetsSources)}
        onChangeTitle={setGsTitle}
        onChangeSpreadsheet={setGsSpreadsheet}
        onChangeSelectedAccountId={setGsSelectedAccountId}
        onUseExistingAccount={() => void useExistingGoogleAccount()}
        onDeleteGoogleAccount={(accountId) => {
          const acc = (integrations.google?.accounts || []).find((a) => a.id === accountId);
          openDeleteConfirm({
            type: "google_account",
            account_id: accountId,
            name: acc?.name || acc?.client_email || accountId,
          });
        }}
        onFileSelected={(file) => void onGoogleKeyFileSelected(file)}
        onSetDropActive={setGsDropActive}
        onVerify={() => void verifyGsheets()}
        onConnect={() => void connectGsheets()}
        onBackFromAccess={() => setGsStep(1)}
        onBackFromSheet={() => setGsStep(2)}
        onChangeWorksheet={setGsWorksheet}
      />
    </>
  );
}
