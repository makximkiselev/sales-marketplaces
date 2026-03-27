"use client";

import { useState } from "react";

export function useOzonSourcesController(deps: {
  connectOzonAccount: (payload: { client_id: string; api_key: string }) => Promise<unknown>;
  checkOzonAccount: (clientId: string) => Promise<unknown>;
  loadData: () => Promise<unknown>;
}) {
  const [ozWizardOpen, setOzWizardOpen] = useState(false);
  const [ozClientId, setOzClientId] = useState("");
  const [ozApiKey, setOzApiKey] = useState("");
  const [ozSellerId, setOzSellerId] = useState("");
  const [ozSellerName, setOzSellerName] = useState("");
  const [ozLoading, setOzLoading] = useState(false);
  const [ozError, setOzError] = useState("");
  const [ozCheckLoading, setOzCheckLoading] = useState<Record<string, boolean>>({});
  const [ozActionClientId, setOzActionClientId] = useState("");

  function openOzonWizard(account?: { client_id: string; api_key?: string; seller_id?: string; seller_name?: string }) {
    setOzWizardOpen(true);
    setOzClientId(account?.client_id || "");
    setOzApiKey(account?.api_key || "");
    setOzSellerId(account?.seller_id || "");
    setOzSellerName(account?.seller_name || "");
    setOzError("");
  }

  function closeOzonWizard() {
    setOzWizardOpen(false);
    setOzError("");
    setOzLoading(false);
  }

  async function connectOzon() {
    setOzError("");
    setOzLoading(true);
    try {
      await deps.connectOzonAccount({ client_id: ozClientId, api_key: ozApiKey });
      closeOzonWizard();
      await deps.loadData();
    } catch (e) {
      setOzError(e instanceof Error ? e.message : String(e));
    } finally {
      setOzLoading(false);
    }
  }

  async function checkOzonAccount(clientId: string) {
    setOzCheckLoading((prev) => ({ ...prev, [clientId]: true }));
    try {
      await deps.checkOzonAccount(clientId);
      await deps.loadData();
    } finally {
      setOzCheckLoading((prev) => ({ ...prev, [clientId]: false }));
    }
  }

  return {
    ozWizardOpen,
    ozClientId,
    ozApiKey,
    ozSellerId,
    ozSellerName,
    ozLoading,
    ozError,
    ozCheckLoading,
    ozActionClientId,
    setOzClientId,
    setOzApiKey,
    setOzSellerId,
    setOzSellerName,
    setOzActionClientId,
    openOzonWizard,
    closeOzonWizard,
    connectOzon,
    checkOzonAccount,
  };
}
