import { apiGetOk } from "../../../lib/api";
import type { ContextResp } from "./catalogPageShared";

export async function fetchPricingCatalogContext(endpoint: string) {
  return apiGetOk<ContextResp>(endpoint);
}
