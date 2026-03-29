import {
  fetchIntegrations,
  fetchSources,
  postGsheetSourceCheckRaw,
  postIntegrationFlow,
  postOzonAccountCheckRaw,
  postSourceFlow,
  postStoreCurrency,
  postStoreFulfillment,
  postYandexShopCheckRaw,
} from "./api";
import type { IntegrationsPayload, OzonAccount, SourceItem, YandexAccount } from "./types";

type SectionTab = "all" | "platforms" | "tables" | "external";
type FlowKind = "import" | "export";

type LoadSourcesContextResult = {
  sources: SourceItem[];
  integrations: IntegrationsPayload;
  lastRefreshAt: string | null;
};

type RefreshAllStatusesArgs = {
  ymAccounts: YandexAccount[];
  ozAccounts: OzonAccount[];
  gsheetsSources: SourceItem[];
};

function _extractLastRefreshAt(sources: SourceItem[]): string | null {
  const values = sources
    .map((item) => {
      const row = item as SourceItem & { health_checked_at?: string };
      return String(item.last_refreshed || row.health_checked_at || "").trim();
    })
    .filter(Boolean)
    .sort();
  return values.length ? values[values.length - 1] : null;
}

async function _consumeResponse(res: Response): Promise<void> {
  try {
    await res.text();
  } catch {
    // ignore
  }
}

export async function loadSourcesContext(): Promise<LoadSourcesContextResult> {
  const [sourcesResp, integrations] = await Promise.all([fetchSources(), fetchIntegrations()]);
  const sources = Array.isArray(sourcesResp.items) ? sourcesResp.items : [];
  return {
    sources,
    integrations,
    lastRefreshAt: _extractLastRefreshAt(sources),
  };
}

export async function refreshIntegrationsContext(): Promise<IntegrationsPayload> {
  return fetchIntegrations();
}

export async function applyIntegrationDataFlow(payload: {
  scope?: "global" | "platform" | "account" | "shop";
  platform?: "yandex_market" | "ozon" | "wildberries";
  business_id?: string;
  campaign_id?: string;
  import_enabled?: boolean;
  export_enabled?: boolean;
}): Promise<void> {
  await postIntegrationFlow(payload);
}

export async function applySourceDataFlow(
  sourceId: string,
  payload: { mode_import?: boolean; mode_export?: boolean },
): Promise<void> {
  await postSourceFlow(sourceId, payload);
}

export async function applyStoreCurrency(payload: {
  platform: "yandex_market" | "ozon";
  currency_code: "RUB" | "USD";
  business_id?: string;
  campaign_id?: string;
  client_id?: string;
}): Promise<void> {
  await postStoreCurrency(payload);
}

export async function applyStoreFulfillment(payload: {
  platform: "yandex_market" | "ozon";
  fulfillment_model: "FBO" | "FBS" | "DBS" | "EXPRESS";
  business_id?: string;
  campaign_id?: string;
  client_id?: string;
}): Promise<void> {
  await postStoreFulfillment(payload);
}

export async function applyHeaderFlow(args: {
  sectionTab: SectionTab;
  kind: FlowKind;
  nextValue: boolean;
  gsheetsSources: SourceItem[];
  sortedOzonAccounts: OzonAccount[];
}): Promise<void> {
  const field = args.kind === "import" ? "import_enabled" : "export_enabled";
  const promises: Promise<unknown>[] = [];

  if (args.sectionTab === "all" || args.sectionTab === "platforms") {
    promises.push(
      postIntegrationFlow({
        scope: "platform",
        platform: "yandex_market",
        [field]: args.nextValue,
      }),
      postIntegrationFlow({
        scope: "platform",
        platform: "ozon",
        [field]: args.nextValue,
      }),
      postIntegrationFlow({
        scope: "platform",
        platform: "wildberries",
        [field]: args.nextValue,
      }),
    );
  }

  if (args.sectionTab === "all" || args.sectionTab === "tables") {
    for (const source of args.gsheetsSources) {
      const sourceId = String(source.id || "").trim();
      if (!sourceId) continue;
      promises.push(
        postSourceFlow(
          sourceId,
          args.kind === "import"
            ? { mode_import: args.nextValue }
            : { mode_export: args.nextValue },
        ),
      );
    }
  }

  await Promise.all(promises);
}

export async function refreshAllSourceStatuses({
  ymAccounts,
  ozAccounts,
  gsheetsSources,
}: RefreshAllStatusesArgs): Promise<void> {
  const checks: Promise<unknown>[] = [];

  for (const account of ymAccounts || []) {
    const businessId = String(account.business_id || "").trim();
    for (const shop of account.shops || []) {
      const campaignId = String(shop.campaign_id || "").trim();
      if (!campaignId || !businessId) continue;
      checks.push(
        postYandexShopCheckRaw(campaignId, businessId).then(_consumeResponse),
      );
    }
  }

  for (const account of ozAccounts || []) {
    const clientId = String(account.client_id || "").trim();
    if (!clientId) continue;
    checks.push(postOzonAccountCheckRaw(clientId).then(_consumeResponse));
  }

  for (const source of gsheetsSources || []) {
    const sourceId = String(source.id || "").trim();
    if (!sourceId) continue;
    checks.push(postGsheetSourceCheckRaw(sourceId).then(_consumeResponse));
  }

  await Promise.allSettled(checks);
}
