import { useEffect, useMemo, useState } from "react";
import { useDeleteConfirmController } from "./useDeleteConfirmController";
import { useGsheetsSourcesController } from "./useGsheetsSourcesController";
import { useOzonSourcesController } from "./useOzonSourcesController";
import { useYandexSourcesController } from "./useYandexSourcesController";
import {
  applyHeaderFlow,
  applyIntegrationDataFlow,
  applySourceDataFlow,
  applyStoreCurrency,
  applyStoreFulfillment,
  loadSourcesContext,
  refreshAllSourceStatuses,
  refreshIntegrationsContext,
} from "./controllerServices";
import {
  checkGsheetSource as apiCheckGsheetSource,
  checkOzonAccount as apiCheckOzonAccount,
  checkYandexShop as apiCheckYandexShop,
  connectGsheetsSource,
  connectOzonAccount,
  connectYandexAccount,
  deleteGoogleAccount as apiDeleteGoogleAccount,
  deleteGsheetSource as apiDeleteGsheetSource,
  deleteOzonAccount as apiDeleteOzonAccount,
  deleteYandexAccount as apiDeleteYandexAccount,
  deleteYandexShop as apiDeleteYandexShop,
  fetchYandexCampaigns,
  selectGoogleAccount,
  uploadGoogleAccountKey as apiUploadGoogleAccountKey,
  verifyGsheets as apiVerifyGsheets,
} from "./api";
import { formatDateTime, formatRefreshLabel, getSortedOzonAccounts, getSortedYandexAccounts } from "./controllerUtils";
import type { IntegrationsPayload, SourceItem } from "./types";

export function useSourcesPageController() {
  const [sectionTab, setSectionTab] = useState<"all" | "platforms" | "tables" | "external">("all");
  const [sources, setSources] = useState<SourceItem[]>([]);
  const [integrations, setIntegrations] = useState<IntegrationsPayload>({});
  const [loading, setLoading] = useState(true);
  const [refreshAllLoading, setRefreshAllLoading] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState<string | null>(null);

  const [gsSourceCheckLoading, setGsSourceCheckLoading] = useState<Record<string, boolean>>({});
  const [sourceFlowSavingKey, setSourceFlowSavingKey] = useState<string | null>(null);
  const [flowSavingKey, setFlowSavingKey] = useState<string | null>(null);
  const [currencySavingKey, setCurrencySavingKey] = useState<string | null>(null);
  const [fulfillmentSavingKey, setFulfillmentSavingKey] = useState<string | null>(null);
  const [flowError, setFlowError] = useState("");

  const gsheetsSources = useMemo(
    () => sources.filter((s) => (s.type || "").toLowerCase() === "gsheets"),
    [sources],
  );
  const ymAccounts = integrations.yandex_market?.accounts || [];
  const ozAccounts = integrations.ozon?.accounts || [];
  const sortedYmAccounts = useMemo(() => getSortedYandexAccounts(ymAccounts), [ymAccounts]);
  const totalYmShops = useMemo(
    () => sortedYmAccounts.reduce((acc, item) => acc + (item.shops?.length || 0), 0),
    [sortedYmAccounts],
  );
  const sortedOzonAccounts = useMemo(() => getSortedOzonAccounts(ozAccounts), [ozAccounts]);
  const platformsImportOn = useMemo(
    () =>
      [
        Boolean(integrations.yandex_market?.data_flow?.import_enabled),
        Boolean(integrations.ozon?.data_flow?.import_enabled),
        Boolean(integrations.data_flow?.platforms?.wildberries?.import_enabled),
      ].some(Boolean),
    [integrations],
  );
  const platformsExportOn = useMemo(
    () =>
      [
        Boolean(integrations.yandex_market?.data_flow?.export_enabled),
        Boolean(integrations.ozon?.data_flow?.export_enabled),
        Boolean(integrations.data_flow?.platforms?.wildberries?.export_enabled),
      ].some(Boolean),
    [integrations],
  );
  const tablesImportOn = useMemo(() => gsheetsSources.some((src) => Boolean(src.mode_import)), [gsheetsSources]);
  const tablesExportOn = useMemo(() => gsheetsSources.some((src) => Boolean(src.mode_export)), [gsheetsSources]);
  const headerImportOn =
    sectionTab === "platforms"
      ? platformsImportOn
      : sectionTab === "tables"
        ? tablesImportOn
        : sectionTab === "all"
          ? platformsImportOn || tablesImportOn
          : false;
  const headerExportOn =
    sectionTab === "platforms"
      ? platformsExportOn
      : sectionTab === "tables"
        ? tablesExportOn
        : sectionTab === "all"
          ? platformsExportOn || tablesExportOn
          : false;
  const headerFlowDisabled = sectionTab === "external" || (sectionTab === "tables" && gsheetsSources.length === 0);

  const yandex = useYandexSourcesController({
    fetchCampaigns: fetchYandexCampaigns,
    connectYandexAccount,
    checkYandexShop: apiCheckYandexShop,
    refreshIntegrationsOnly,
    loadData,
  });

  const gsheets = useGsheetsSourcesController({
    activeGoogleAccountId: integrations.google?.active_account_id || "",
    verifyGsheetsRequest: apiVerifyGsheets,
    connectGsheetsSource,
    uploadGoogleAccountKey: apiUploadGoogleAccountKey,
    deleteGoogleAccount: apiDeleteGoogleAccount,
    selectGoogleAccount,
    checkGsheetSource: apiCheckGsheetSource,
    loadData,
  });

  const ozon = useOzonSourcesController({
    connectOzonAccount,
    checkOzonAccount: apiCheckOzonAccount,
    loadData,
  });

  const deleteConfirm = useDeleteConfirmController({
    deleteYandexAccount: async (businessId) => { await apiDeleteYandexAccount(businessId); await loadData(); },
    deleteYandexShop: async (businessId, campaignId) => { await apiDeleteYandexShop(businessId, campaignId); await loadData(); },
    deleteOzonAccount: async (clientId) => { await apiDeleteOzonAccount(clientId); await loadData(); },
    deleteGsheetSource: async (sourceId) => { await apiDeleteGsheetSource(sourceId); await loadData(); },
    deleteGoogleAccount: async (accountId) => {
      const data = await apiDeleteGoogleAccount(accountId);
      gsheets.setGsSelectedAccountId(data.active_account_id || "");
      await loadData();
    },
  });

  const ymActionAccount = useMemo(
    () => sortedYmAccounts.find((a) => a.business_id === yandex.ymActionBusinessId) || sortedYmAccounts[0] || null,
    [sortedYmAccounts, yandex.ymActionBusinessId],
  );
  const ozActionAccount = useMemo(
    () => sortedOzonAccounts.find((a) => a.client_id === ozon.ozActionClientId) || sortedOzonAccounts[0] || null,
    [sortedOzonAccounts, ozon.ozActionClientId],
  );

  async function loadData() {
    setLoading(true);
    try {
      const data = await loadSourcesContext();
      setSources(data.sources);
      setIntegrations(data.integrations);
      if (data.lastRefreshAt) setLastRefreshAt(data.lastRefreshAt);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadData();
  }, []);

  useEffect(() => {
    if (!sortedYmAccounts.length) {
      if (yandex.ymActionBusinessId) yandex.setYmActionBusinessId("");
      return;
    }
    if (!sortedYmAccounts.some((a) => a.business_id === yandex.ymActionBusinessId)) {
      yandex.setYmActionBusinessId(sortedYmAccounts[0].business_id);
    }
  }, [sortedYmAccounts, yandex.ymActionBusinessId]);

  useEffect(() => {
    if (!sortedOzonAccounts.length) {
      if (ozon.ozActionClientId) ozon.setOzActionClientId("");
      return;
    }
    if (!sortedOzonAccounts.some((a) => a.client_id === ozon.ozActionClientId)) {
      ozon.setOzActionClientId(sortedOzonAccounts[0].client_id);
    }
  }, [sortedOzonAccounts, ozon.ozActionClientId]);

  async function refreshIntegrationsOnly() {
    const intData = await refreshIntegrationsContext();
    setIntegrations(intData);
  }

  async function updateHeaderFlow(kind: "import" | "export", nextValue: boolean) {
    if (headerFlowDisabled) return;
    const key = `header-${sectionTab}-${kind}`;
    setFlowSavingKey(key);
    setFlowError("");
    try {
      await applyHeaderFlow({ sectionTab, kind, nextValue, gsheetsSources, sortedOzonAccounts });
      await loadData();
    } catch (e) {
      setFlowError(e instanceof Error ? e.message : String(e));
      await loadData();
    } finally {
      setFlowSavingKey(null);
    }
  }

  async function updateDataFlow(
    payload: {
      scope?: "global" | "platform" | "account" | "shop";
      platform?: "yandex_market" | "ozon" | "wildberries";
      business_id?: string;
      campaign_id?: string;
      import_enabled?: boolean;
      export_enabled?: boolean;
    },
    savingKey: string,
  ) {
    const hasImport = typeof payload.import_enabled === "boolean";
    const hasExport = typeof payload.export_enabled === "boolean";
    if (!hasImport && !hasExport) return;

    setFlowError("");
    setFlowSavingKey(savingKey);
    try {
      await applyIntegrationDataFlow(payload);
      await refreshIntegrationsOnly();
    } catch (e) {
      setFlowError(e instanceof Error ? e.message : String(e));
      await refreshIntegrationsOnly();
    } finally {
      setFlowSavingKey(null);
    }
  }

  async function updateStoreCurrency(payload: {
    platform: "yandex_market" | "ozon";
    currency_code: "RUB" | "USD";
    business_id?: string;
    campaign_id?: string;
    client_id?: string;
  }) {
    setFlowError("");
    const saveKey =
      payload.platform === "yandex_market"
        ? `currency-ym-${payload.business_id}-${payload.campaign_id}`
        : `currency-oz-${payload.client_id}`;
    setCurrencySavingKey(saveKey);
    try {
      await applyStoreCurrency(payload);
      await loadData();
    } catch (e) {
      setFlowError(e instanceof Error ? e.message : String(e));
    } finally {
      setCurrencySavingKey(null);
    }
  }

  async function updateStoreFulfillment(payload: {
    platform: "yandex_market" | "ozon";
    fulfillment_model: "FBO" | "FBS" | "DBS" | "EXPRESS";
    business_id?: string;
    campaign_id?: string;
    client_id?: string;
  }) {
    setFlowError("");
    const saveKey =
      payload.platform === "yandex_market"
        ? `fulfill-ym-${payload.business_id}-${payload.campaign_id}`
        : `fulfill-oz-${payload.client_id}`;
    setFulfillmentSavingKey(saveKey);
    try {
      await applyStoreFulfillment(payload);
      await loadData();
    } catch (e) {
      setFlowError(e instanceof Error ? e.message : String(e));
    } finally {
      setFulfillmentSavingKey(null);
    }
  }











  async function updateSourceFlow(sourceId: string, payload: { mode_import?: boolean; mode_export?: boolean }, savingKey: string) {
    setSourceFlowSavingKey(savingKey);
    setFlowError("");
    try {
      await applySourceDataFlow(sourceId, payload);
      await loadData();
    } catch (e) {
      setFlowError(e instanceof Error ? e.message : String(e));
    } finally {
      setSourceFlowSavingKey(null);
    }
  }






















  async function refreshAllStatuses() {
    setRefreshAllLoading(true);
    setFlowError("");
    try {
      await refreshAllSourceStatuses({ ymAccounts, ozAccounts, gsheetsSources });
      await loadData();
      setLastRefreshAt(new Date().toISOString());
    } catch (e) {
      setFlowError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshAllLoading(false);
    }
  }

  return {
    sectionTab,
    setSectionTab,
    sources,
    integrations,
    loading,
    refreshAllLoading,
    lastRefreshAt,
    wizardOpen: yandex.wizardOpen,
    ymWizardMode: yandex.ymWizardMode,
    step: yandex.step,
    apiKey: yandex.apiKey,
    businessId: yandex.businessId,
    campaigns: yandex.campaigns,
    selectedCampaignIds: yandex.selectedCampaignIds,
    wizardLoading: yandex.wizardLoading,
    wizardError: yandex.wizardError,
    shopCheckLoading: yandex.shopCheckLoading,
    gsSourceCheckLoading: gsheets.gsSourceCheckLoading,
    sourceFlowSavingKey,
    flowSavingKey,
    currencySavingKey,
    fulfillmentSavingKey,
    flowError,
    gsWizardOpen: gsheets.gsWizardOpen,
    gsWizardMode: gsheets.gsWizardMode,
    gsStep: gsheets.gsStep,
    gsLoading: gsheets.gsLoading,
    gsError: gsheets.gsError,
    gsTitle: gsheets.gsTitle,
    gsSpreadsheet: gsheets.gsSpreadsheet,
    gsModeImport: gsheets.gsModeImport,
    gsModeExport: gsheets.gsModeExport,
    gsCredFileName: gsheets.gsCredFileName,
    gsCredFile: gsheets.gsCredFile,
    gsKeyUploading: gsheets.gsKeyUploading,
    gsDropActive: gsheets.gsDropActive,
    gsKeyUploadOk: gsheets.gsKeyUploadOk,
    gsKeyUploadMessage: gsheets.gsKeyUploadMessage,
    gsSelectedAccountId: gsheets.gsSelectedAccountId,
    gsWorksheets: gsheets.gsWorksheets,
    gsWorksheet: gsheets.gsWorksheet,
    gsEditingSourceId: gsheets.gsEditingSourceId,
    ozWizardOpen: ozon.ozWizardOpen,
    ozClientId: ozon.ozClientId,
    ozApiKey: ozon.ozApiKey,
    ozSellerId: ozon.ozSellerId,
    ozSellerName: ozon.ozSellerName,
    ozLoading: ozon.ozLoading,
    ozError: ozon.ozError,
    ozCheckLoading: ozon.ozCheckLoading,
    ymActionBusinessId: yandex.ymActionBusinessId,
    ozActionClientId: ozon.ozActionClientId,
    deleteRequest: deleteConfirm.deleteRequest,
    deleteBusy: deleteConfirm.deleteBusy,
    deleteError: deleteConfirm.deleteError,
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
    setApiKey: yandex.setApiKey,
    setBusinessId: yandex.setBusinessId,
    setOzClientId: ozon.setOzClientId,
    setOzApiKey: ozon.setOzApiKey,
    setOzSellerId: ozon.setOzSellerId,
    setOzSellerName: ozon.setOzSellerName,
    setGsTitle: gsheets.setGsTitle,
    setGsSpreadsheet: gsheets.setGsSpreadsheet,
    setGsModeImport: gsheets.setGsModeImport,
    setGsModeExport: gsheets.setGsModeExport,
    setGsSelectedAccountId: gsheets.setGsSelectedAccountId,
    setGsWorksheet: gsheets.setGsWorksheet,
    setGsDropActive: gsheets.setGsDropActive,
    setYmActionBusinessId: yandex.setYmActionBusinessId,
    setOzActionClientId: ozon.setOzActionClientId,
    setStep: yandex.setStep,
    setGsStep: gsheets.setGsStep,
    loadData,
    updateHeaderFlow,
    updateDataFlow,
    updateStoreCurrency,
    updateStoreFulfillment,
    openWizard: yandex.openWizard,
    openAddShop: yandex.openAddShop,
    openEditAccount: yandex.openEditAccount,
    closeWizard: yandex.closeWizard,
    goToYmStep: yandex.goToYmStep,
    proceedFromStep2: yandex.proceedFromStep2,
    connectYandex: yandex.connectYandex,
    toggleCampaign: yandex.toggleCampaign,
    formatDateTime,
    formatRefreshLabel,
    checkGsheetSource: gsheets.checkGsheetSource,
    updateSourceFlow,
    checkShop: yandex.checkShop,
    openGsWizard: gsheets.openGsWizard,
    chooseExistingGsSource: gsheets.chooseExistingGsSource,
    closeGsWizard: gsheets.closeGsWizard,
    goToGsStep: gsheets.goToGsStep,
    verifyGsheets: gsheets.verifyGsheets,
    connectGsheets: gsheets.connectGsheets,
    onGoogleKeyFileSelected: gsheets.onGoogleKeyFileSelected,
    openDeleteConfirm: deleteConfirm.openDeleteConfirm,
    closeDeleteConfirm: deleteConfirm.closeDeleteConfirm,
    getDeleteConfirmText: deleteConfirm.getDeleteConfirmText,
    confirmDelete: deleteConfirm.confirmDelete,
    useExistingGoogleAccount: gsheets.useExistingGoogleAccount,
    openOzonWizard: ozon.openOzonWizard,
    closeOzonWizard: ozon.closeOzonWizard,
    connectOzon: ozon.connectOzon,
    checkOzonAccount: ozon.checkOzonAccount,
    refreshAllStatuses,
  };
}
