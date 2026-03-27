import { API_BASE } from "./api";

const INTERNAL_API_BASE = import.meta.env.VITE_API_BASE_INTERNAL || API_BASE;

export async function fetchServer(path: string) {
  const res = await fetch(`${INTERNAL_API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${text}`);
  }
  return res.json();
}
