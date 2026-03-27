import { API_BASE, buildApiUrl } from "./api";

const INTERNAL_API_BASE = import.meta.env.VITE_API_BASE_INTERNAL || API_BASE;

export async function fetchServer(path: string) {
  const base = INTERNAL_API_BASE === "/api" ? "" : INTERNAL_API_BASE;
  const url = buildApiUrl(path).replace(API_BASE, base);
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${text}`);
  }
  return res.json();
}
