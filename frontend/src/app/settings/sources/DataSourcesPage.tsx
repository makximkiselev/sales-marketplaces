import { useEffect, useState } from "react";
import styles from "./DataSourcesPage.module.css";
import { PageFrame } from "../../../components/page/PageKit";
import { DeleteConfirmModal } from "./components/DeleteConfirmModal";
import { GoogleSheetsWizardModal } from "./components/GoogleSheetsWizardModal";
import { OzonWizardModal } from "./components/OzonWizardModal";
import { YandexWizardModal } from "./components/YandexWizardModal";
import { CogsSourceModal } from "../../pricing/settings/components/CogsSourceModal";
import { useSourcesPageController } from "./useSourcesPageController";
import { DataSourcesDesktop } from "./DataSourcesDesktop";
import { DataSourcesMobile } from "./DataSourcesMobile";
import type { SourcesSectionItem } from "./DataSourcesRendererTypes";

function useSourcesMobile() {
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const media = window.matchMedia("(max-width: 960px)");
    const sync = () => setIsMobile(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  return isMobile;
}

export default function DataSourcesPage() {
  const isMobile = useSourcesMobile();
  const controller = useSourcesPageController();
  const {
    sectionTab,
    integrations,
    wizardOpen,
    ymWizardMode,
    step,
    apiKey,
    businessId,
    campaigns,
    selectedCampaignIds,
    wizardLoading,
    wizardError,
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
    gsheetsSources,
    ozClientId,
    ozApiKey,
    ozSellerId,
    ozSellerName,
    ozLoading,
    ozError,
    ozCheckLoading,
    ymActionBusinessId,
    ozActionClientId,
    sourceBindingModal,
    storeSourceBindings,
    deleteRequest,
    deleteBusy,
    deleteError,
    setApiKey,
    setBusinessId,
    setOzClientId,
    setOzApiKey,
    setGsTitle,
    setGsSpreadsheet,
    setGsSelectedAccountId,
    setGsWorksheet,
    setGsDropActive,
    setStep,
    setGsStep,
    openWizard,
    closeWizard,
    goToYmStep,
    proceedFromStep2,
    connectYandex,
    toggleCampaign,
    formatDateTime,
    formatRefreshLabel,
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
    closeOzonWizard,
    connectOzon,
    closeStoreSourceModal,
    saveStoreSourceBinding,
  } = controller;

  const sectionItems: SourcesSectionItem[] = [
    { id: "all", label: "Все источники" },
    { id: "platforms", label: "Площадки" },
    { id: "tables", label: "Внешние таблицы" },
    { id: "external", label: "Внешние системы" },
  ];

  return (
    <>
      <PageFrame
        title="Источники данных"
        subtitle="Интеграции, таблицы и внешние системы."
        className={styles.sourcesHeadCard}
      >
        {isMobile ? (
          <DataSourcesMobile controller={controller} sectionItems={sectionItems} />
        ) : (
          <DataSourcesDesktop controller={controller} sectionItems={sectionItems} />
        )}
      </PageFrame>

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

      {sourceBindingModal ? (
        <CogsSourceModal
          current={
            sourceBindingModal.target === "cogs"
              ? storeSourceBindings[`${sourceBindingModal.platform}:${sourceBindingModal.storeId}`]?.cogsSource ?? null
              : storeSourceBindings[`${sourceBindingModal.platform}:${sourceBindingModal.storeId}`]?.stockSource ?? null
          }
          title={sourceBindingModal.target === "cogs" ? "Источник себестоимости" : "Источник остатка"}
          valueColumnLabel={sourceBindingModal.target === "cogs" ? "Столбец с себестоимостью" : "Столбец с остатком"}
          onSave={saveStoreSourceBinding}
          onClose={closeStoreSourceModal}
        />
      ) : null}
    </>
  );
}
