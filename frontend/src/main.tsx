import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import "./styles/globals.css";
import "./components/ui/primitives.css";

if (typeof window !== "undefined") {
  const originalFetch = window.fetch.bind(window);
  window.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const target = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const isSameOrigin = target.startsWith("/") || target.startsWith(window.location.origin);
    if (!isSameOrigin) {
      return originalFetch(input, init);
    }
    return originalFetch(input, { ...(init || {}), credentials: init?.credentials ?? "include" });
  }) as typeof window.fetch;
}

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.getRegistrations().then((registrations) => {
      registrations.forEach((registration) => {
        registration.unregister().catch(() => undefined);
      });
    }).catch(() => undefined);
    if ("caches" in window) {
      caches.keys().then((keys) => {
        keys.forEach((key) => {
          caches.delete(key).catch(() => undefined);
        });
      }).catch(() => undefined);
    }
  });
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
