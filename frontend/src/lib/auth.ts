import { buildApiUrl } from "./api";

export type AuthUser = {
  user_id: string;
  identifier: string;
  display_name: string;
  role: string;
  is_active: boolean;
};

type AuthMeResponse = {
  ok?: boolean;
  user?: AuthUser;
};

const AUTH_USER_STORAGE_KEY = "app_auth_user_v1";

let authUserCache: AuthUser | null | undefined;
let authUserPromise: Promise<AuthUser | null> | null = null;

function readStoredAuthUser(): AuthUser | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.sessionStorage.getItem(AUTH_USER_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as AuthUser | null;
    if (!parsed || typeof parsed.identifier !== "string") return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeStoredAuthUser(user: AuthUser | null) {
  if (typeof window === "undefined") return;
  try {
    if (user) {
      window.sessionStorage.setItem(AUTH_USER_STORAGE_KEY, JSON.stringify(user));
    } else {
      window.sessionStorage.removeItem(AUTH_USER_STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures and keep working with in-memory cache.
  }
}

function requestAuthUser(): Promise<AuthUser | null> {
  if (authUserPromise) return authUserPromise;
  authUserPromise = fetch(buildApiUrl("/api/auth/me"), {
    cache: "no-store",
    credentials: "include",
  })
    .then((res) => (res.ok ? res.json() : null))
    .then((data: AuthMeResponse | null) => {
      authUserCache = data?.ok && data.user ? data.user : null;
      writeStoredAuthUser(authUserCache);
      return authUserCache;
    })
    .catch(() => {
      authUserCache = null;
      writeStoredAuthUser(null);
      return null;
    })
    .finally(() => {
      authUserPromise = null;
    });
  return authUserPromise;
}

export async function fetchAuthUser(force = false): Promise<AuthUser | null> {
  if (!force && authUserCache !== undefined) {
    return authUserCache;
  }
  if (!force) {
    const storedUser = readStoredAuthUser();
    if (storedUser) {
      authUserCache = storedUser;
      void requestAuthUser();
      return storedUser;
    }
  }
  return requestAuthUser();
}

export function getAuthUserSnapshot(): AuthUser | null {
  if (authUserCache !== undefined) return authUserCache;
  const storedUser = readStoredAuthUser();
  if (storedUser) {
    authUserCache = storedUser;
    return storedUser;
  }
  return null;
}

export function primeAuthUser(user: AuthUser | null) {
  authUserCache = user;
  writeStoredAuthUser(user);
}

export function clearAuthUserCache() {
  authUserCache = undefined;
  authUserPromise = null;
  writeStoredAuthUser(null);
}
