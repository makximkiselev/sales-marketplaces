type SnapshotEnvelope<T> = {
  savedAt: number;
  value: T;
};

function canUseStorage() {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

export function readPageSnapshot<T>(key: string): T | null {
  try {
    if (!canUseStorage()) return null;
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as SnapshotEnvelope<T> | null;
    return parsed?.value ?? null;
  } catch {
    return null;
  }
}

export function readFreshPageSnapshot<T>(key: string, ttlMs: number): T | null {
  try {
    if (!canUseStorage()) return null;
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as SnapshotEnvelope<T> | null;
    if (!parsed || typeof parsed.savedAt !== "number" || !Number.isFinite(parsed.savedAt)) return null;
    if (Date.now() - parsed.savedAt > ttlMs) return null;
    return parsed.value ?? null;
  } catch {
    return null;
  }
}

export function writePageSnapshot(key: string, value: unknown) {
  try {
    if (!canUseStorage()) return;
    const payload: SnapshotEnvelope<unknown> = { savedAt: Date.now(), value };
    window.localStorage.setItem(key, JSON.stringify(payload));
  } catch {
    // noop
  }
}

export function clearPageSnapshot(key: string) {
  try {
    if (!canUseStorage()) return;
    window.localStorage.removeItem(key);
  } catch {
    // noop
  }
}

export function clearPageSnapshotsByPrefix(prefix: string) {
  try {
    if (!canUseStorage()) return;
    const keys: string[] = [];
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const key = window.localStorage.key(i);
      if (key && key.startsWith(prefix)) keys.push(key);
    }
    keys.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // noop
  }
}
