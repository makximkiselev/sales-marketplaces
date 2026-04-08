const RAW_API_BASE = String(import.meta.env.VITE_API_BASE || "/api").trim();
export const API_BASE = RAW_API_BASE === "/api" ? "" : RAW_API_BASE.replace(/\/+$/, "");

export function buildApiUrl(path: string): string {
  const rawPath = String(path || "").trim();
  if (!rawPath) return API_BASE;
  if (/^https?:\/\//i.test(rawPath)) return rawPath;
  if (API_BASE.endsWith("/api") && rawPath.startsWith("/api/")) {
    return rawPath;
  }
  if (API_BASE.endsWith("/") && rawPath.startsWith("/")) {
    return `${API_BASE}${rawPath.slice(1)}`;
  }
  if (!API_BASE.endsWith("/") && !rawPath.startsWith("/")) {
    return `${API_BASE}/${rawPath}`;
  }
  return `${API_BASE}${rawPath}`;
}

type ApiRequestOptions = {
  signal?: AbortSignal;
};

export async function apiGet<T>(path: string, options?: ApiRequestOptions): Promise<T> {
  const res = await fetch(buildApiUrl(path), {
    cache: "no-store",
    credentials: "include",
    signal: options?.signal,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GET ${path} failed: ${res.status} ${text}`);
  }
  return res.json() as Promise<T>;
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(buildApiUrl(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
    cache: "no-store",
    credentials: "include",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`POST ${path} failed: ${res.status} ${text}`);
  }
  return res.json() as Promise<T>;
}

// Хелперы для API-ответов со структурой { ok: boolean; message?: string }
// Выбрасывают ошибку если ok === false, возвращают данные напрямую

export async function apiGetOk<T extends { ok: boolean; message?: string }>(
  path: string,
  options?: ApiRequestOptions,
): Promise<T> {
  const data = await apiGet<T>(path, options);
  if (!data.ok) throw new Error(data.message || `GET ${path} failed`);
  return data;
}

export async function apiPostOk<T extends { ok: boolean; message?: string }>(
  path: string,
  body?: unknown
): Promise<T> {
  const data = await apiPost<T>(path, body);
  if (!data.ok) throw new Error(data.message || `POST ${path} failed`);
  return data;
}

export async function apiGetParams<T extends { ok: boolean; message?: string }>(
  path: string,
  params: Record<string, string>,
  options?: ApiRequestOptions,
): Promise<T> {
  const qs = new URLSearchParams(params).toString();
  return apiGetOk<T>(`${path}?${qs}`, options);
}
