import { useMemo, useState } from "react";
import type { CampaignItem } from "./types";

export function useYandexSourcesController(deps: {
  fetchCampaigns: (apiKey: string, businessId: string) => Promise<CampaignItem[]>;
  connectYandexAccount: (payload: { api_key: string; business_id: string; campaign_ids: string[] }) => Promise<unknown>;
  checkYandexShop: (campaignId: string, businessId: string) => Promise<unknown>;
  refreshIntegrationsOnly: () => Promise<unknown>;
  loadData: () => Promise<unknown>;
}) {
  const [wizardOpen, setWizardOpen] = useState(false);
  const [ymWizardMode, setYmWizardMode] = useState<"create" | "edit" | "add_shop">("create");
  const [step, setStep] = useState<1 | 2>(1);
  const [apiKey, setApiKey] = useState("");
  const [businessId, setBusinessId] = useState("");
  const [campaigns, setCampaigns] = useState<CampaignItem[]>([]);
  const [selectedCampaignIds, setSelectedCampaignIds] = useState<string[]>([]);
  const [wizardLoading, setWizardLoading] = useState(false);
  const [wizardError, setWizardError] = useState("");
  const [shopCheckLoading, setShopCheckLoading] = useState<Record<string, boolean>>({});
  const [ymActionBusinessId, setYmActionBusinessId] = useState("");

  const hasSelectedCampaigns = useMemo(() => selectedCampaignIds.length > 0, [selectedCampaignIds]);

  function openWizard(businessIdPreset = "", apiKeyPreset = "") {
    setYmWizardMode("create");
    setWizardOpen(true);
    setStep(1);
    setApiKey(apiKeyPreset);
    setBusinessId(businessIdPreset);
    setCampaigns([]);
    setSelectedCampaignIds([]);
    setWizardError("");
  }

  async function openAddShop(account: { business_id: string; api_key?: string }) {
    setYmWizardMode("add_shop");
    setWizardOpen(true);
    setApiKey(account.api_key || "");
    setBusinessId(account.business_id || "");
    setWizardError("");
    setWizardLoading(true);
    try {
      const items = await deps.fetchCampaigns(account.api_key || "", account.business_id || "");
      setCampaigns(items);
      setSelectedCampaignIds(items.length === 1 ? [items[0].id] : []);
      setStep(2);
    } catch (e) {
      setStep(1);
      setWizardError(e instanceof Error ? e.message : String(e));
    } finally {
      setWizardLoading(false);
    }
  }

  function openEditAccount(account: { business_id: string; api_key?: string }) {
    setYmWizardMode("edit");
    setWizardOpen(true);
    setApiKey(account.api_key || "");
    setBusinessId(account.business_id || "");
    setCampaigns([]);
    setSelectedCampaignIds([]);
    setWizardError("");
    setStep(1);
  }

  function closeWizard() {
    setWizardOpen(false);
    setYmWizardMode("create");
    setWizardError("");
    setWizardLoading(false);
  }

  async function goToYmStep(targetStep: 1 | 2) {
    if (targetStep === 1) {
      setStep(1);
      return;
    }
    if (ymWizardMode === "create") return;
    if (campaigns.length > 0) {
      setStep(2);
      return;
    }
    if (!apiKey.trim() || !businessId.trim()) return;
    setWizardError("");
    setWizardLoading(true);
    try {
      const items = await deps.fetchCampaigns(apiKey, businessId);
      setCampaigns(items);
      setSelectedCampaignIds((prev) => (prev.length ? prev : items.length === 1 ? [items[0].id] : []));
      setStep(2);
    } catch (e) {
      setWizardError(e instanceof Error ? e.message : String(e));
    } finally {
      setWizardLoading(false);
    }
  }

  async function proceedFromStep2() {
    setWizardError("");
    if (!apiKey.trim()) {
      setWizardError("Укажите токен API.");
      return;
    }
    if (!businessId.trim()) {
      setWizardError("Укажите Business ID.");
      return;
    }
    setWizardLoading(true);
    try {
      const items = await deps.fetchCampaigns(apiKey, businessId);
      setCampaigns(items);
      setSelectedCampaignIds(items.length === 1 ? [items[0].id] : []);
      setStep(2);
    } catch (e) {
      setWizardError(e instanceof Error ? e.message : String(e));
    } finally {
      setWizardLoading(false);
    }
  }

  async function connectYandex() {
    setWizardError("");
    setWizardLoading(true);
    try {
      await deps.connectYandexAccount({ api_key: apiKey, business_id: businessId, campaign_ids: selectedCampaignIds });
      closeWizard();
      await deps.loadData();
    } catch (e) {
      setWizardError(e instanceof Error ? e.message : String(e));
    } finally {
      setWizardLoading(false);
    }
  }

  function toggleCampaign(id: string) {
    setSelectedCampaignIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  }

  async function checkShop(campaignId: string, businessIdForCheck: string) {
    const checkKey = `${businessIdForCheck}:${campaignId}`;
    setShopCheckLoading((prev) => ({ ...prev, [checkKey]: true }));
    try {
      await deps.checkYandexShop(campaignId, businessIdForCheck);
    } finally {
      setShopCheckLoading((prev) => ({ ...prev, [checkKey]: false }));
      await deps.refreshIntegrationsOnly();
    }
  }

  return {
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
    ymActionBusinessId,
    hasSelectedCampaigns,
    setApiKey,
    setBusinessId,
    setStep,
    setYmActionBusinessId,
    openWizard,
    openAddShop,
    openEditAccount,
    closeWizard,
    goToYmStep,
    proceedFromStep2,
    connectYandex,
    toggleCampaign,
    checkShop,
  };
}
