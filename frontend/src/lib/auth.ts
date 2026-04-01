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

let authUserCache: AuthUser | null | undefined;
let authUserPromise: Promise<AuthUser | null> | null = null;

export async function fetchAuthUser(force = false): Promise<AuthUser | null> {
  if (!force && authUserCache !== undefined) {
    return authUserCache;
  }
  if (!force && authUserPromise) {
    return authUserPromise;
  }
  authUserPromise = fetch(buildApiUrl("/api/auth/me"), {
    cache: "no-store",
    credentials: "include",
  })
    .then((res) => (res.ok ? res.json() : null))
    .then((data: AuthMeResponse | null) => {
      authUserCache = data?.ok && data.user ? data.user : null;
      return authUserCache;
    })
    .catch(() => {
      authUserCache = null;
      return null;
    })
    .finally(() => {
      authUserPromise = null;
    });
  return authUserPromise;
}

export function primeAuthUser(user: AuthUser | null) {
  authUserCache = user;
}

export function clearAuthUserCache() {
  authUserCache = undefined;
  authUserPromise = null;
}
