export const API_BASE = import.meta.env.VITE_API_BASE || "/api";

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    cache: "no-store"
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GET ${path} failed: ${res.status} ${text}`);
  }
  return res.json() as Promise<T>;
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
    cache: "no-store"
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
  path: string
): Promise<T> {
  const data = await apiGet<T>(path);
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
  params: Record<string, string>
): Promise<T> {
  const qs = new URLSearchParams(params).toString();
  return apiGetOk<T>(`${path}?${qs}`);
}
