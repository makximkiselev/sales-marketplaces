"use client";

export type AppToastDetail = {
  message: string;
  tone?: "success" | "error";
  durationMs?: number;
};

export const APP_TOAST_EVENT = "app-toast";

export function showAppToast(detail: AppToastDetail) {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new CustomEvent<AppToastDetail>(APP_TOAST_EVENT, { detail }));
}
