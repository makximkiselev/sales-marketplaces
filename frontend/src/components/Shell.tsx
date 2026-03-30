import { ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { buildApiUrl } from "../lib/api";
import { APP_TOAST_EVENT, type AppToastDetail } from "./ui/toastBus";

type NavItem = { href: string; label: string };
type NavSection = { title: string; items: NavItem[] };
type NavGroup = { title: string; shortLabel: string; items?: NavItem[]; sections?: NavSection[] };
type AuthUser = { user_id: string; identifier: string; display_name: string; role: string; is_active: boolean };

const groups: NavGroup[] = [
  { title: "Сводка", shortLabel: "Сводка", items: [{ href: "/", label: "Дашборд" }] },
  {
    title: "Каталог",
    shortLabel: "Каталог",
    sections: [
      { title: "Товары", items: [{ href: "/catalog", label: "Список товаров" }] },
      {
        title: "Статистика",
        items: [
          { href: "/catalog/content-rating", label: "Контент-рейтинг" },
          { href: "/catalog/kpis", label: "Ключевые показатели" },
        ],
      },
    ],
  },
  {
    title: "Ценообразование",
    shortLabel: "Цены",
    items: [
      { href: "/pricing/decision", label: "Стратегия ценообразования" },
      { href: "/pricing/prices", label: "Цены" },
      { href: "/pricing/attractiveness", label: "Привлекательность" },
      { href: "/pricing/promos", label: "Промо" },
      { href: "/sales/elasticity", label: "Эластичность" },
      { href: "/sales/coinvest", label: "Соинвест" },
      { href: "/pricing/lab", label: "Лаборатория цены" },
    ],
  },
  {
    title: "Продажи",
    shortLabel: "Продажи",
    sections: [
      { title: "Наши данные", items: [{ href: "/sales/overview", label: "Обзор продаж" }] },
      {
        title: "Данные площадки",
        items: [
          { href: "/sales/abc", label: "ABC-анализ" },
          { href: "/sales/boost", label: "Эффективность буста" },
          { href: "/sales/promos", label: "Продажи в промо" },
        ],
      },
    ],
  },
  {
    title: "Настройки",
    shortLabel: "Настройки",
    items: [
      { href: "/settings/sources", label: "Источники" },
      { href: "/settings/pricing", label: "Настройки ценообразования" },
      { href: "/settings/fx-rates", label: "Курс валют" },
      { href: "/settings/monitoring", label: "Мониторинг" },
    ],
  },
];

const groupItems = (group: NavGroup): NavItem[] =>
  group.items ?? group.sections?.flatMap((section) => section.items) ?? [];

const PULL_REFRESH_MAX = 84;
const PULL_REFRESH_TRIGGER = 64;

function isActive(pathname: string, href: string): boolean {
  if (href === "/") return pathname === "/";
  const allHrefs = groups.flatMap((group) => groupItems(group).map((item) => item.href));
  const hasDeeper = allHrefs.some((candidate) => candidate !== href && candidate.startsWith(`${href}/`));
  if (hasDeeper) return pathname === href;
  return pathname === href || pathname.startsWith(`${href}/`);
}

function flattenCurrentGroupSections(group: NavGroup) {
  if (group.sections) {
    return group.sections;
  }
  return [
    {
      title: group.title,
      items: groupItems(group),
    },
  ];
}

export function Shell({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  const [toast, setToast] = useState<AppToastDetail | null>(null);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [authUser, setAuthUser] = useState<AuthUser | null>(null);
  const [mobileRouteLoading, setMobileRouteLoading] = useState(false);
  const [pullDistance, setPullDistance] = useState(0);
  const [pullReady, setPullReady] = useState(false);
  const [pullRefreshing, setPullRefreshing] = useState(false);
  const pullStartYRef = useRef<number | null>(null);
  const pullActiveRef = useRef(false);

  const currentGroup = useMemo(
    () => {
      if (pathname.startsWith("/settings/admin")) {
        return groups.find((group) => group.title === "Настройки") ?? groups[0];
      }
      return groups.find((group) => groupItems(group).some((item) => isActive(pathname, item.href))) ?? groups[0];
    },
    [pathname],
  );
  const currentItem = useMemo(
    () => groupItems(currentGroup).find((item) => isActive(pathname, item.href)) ?? groupItems(currentGroup)[0],
    [currentGroup, pathname],
  );
  const currentSections = useMemo(() => flattenCurrentGroupSections(currentGroup), [currentGroup]);

  useEffect(() => {
    let cancelled = false;
    fetch(buildApiUrl("/api/auth/me"), { cache: "no-store", credentials: "include" })
      .then((res) => (res.ok ? res.json() : null))
      .then((data: { ok?: boolean; user?: AuthUser } | null) => {
        if (!cancelled) setAuthUser(data?.ok && data.user ? data.user : null);
      })
      .catch(() => {
        if (!cancelled) setAuthUser(null);
      });
    return () => {
      cancelled = true;
    };
  }, [pathname]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!window.matchMedia("(max-width: 960px) and (pointer: coarse)").matches) return;
    setMobileRouteLoading(true);
    const timer = window.setTimeout(() => setMobileRouteLoading(false), 900);
    return () => window.clearTimeout(timer);
  }, [pathname]);

  useEffect(() => {
    setMobileMenuOpen(false);
    setPullDistance(0);
    setPullReady(false);
    setPullRefreshing(false);
    pullStartYRef.current = null;
    pullActiveRef.current = false;
  }, [pathname]);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    function handleToast(event: Event) {
      const detail = (event as CustomEvent<AppToastDetail>).detail;
      if (!detail?.message) return;
      if (timer) clearTimeout(timer);
      setToast(detail);
      timer = setTimeout(() => setToast(null), Math.max(1200, Number(detail.durationMs || 2200)));
    }
    window.addEventListener(APP_TOAST_EVENT, handleToast as EventListener);
    return () => {
      if (timer) clearTimeout(timer);
      window.removeEventListener(APP_TOAST_EVENT, handleToast as EventListener);
    };
  }, []);

  useEffect(() => {
    if (!mobileMenuOpen || typeof document === "undefined") return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [mobileMenuOpen]);

  function canPullRefresh() {
    if (mobileMenuOpen || pullRefreshing || typeof window === "undefined") return false;
    if (!window.matchMedia("(max-width: 960px) and (pointer: coarse)").matches) return false;
    const scrollTop = window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
    return scrollTop <= 0;
  }

  function handlePullStart(clientY: number) {
    if (!canPullRefresh()) return;
    pullStartYRef.current = clientY;
    pullActiveRef.current = true;
    setPullReady(false);
  }

  function handlePullMove(clientY: number) {
    if (!pullActiveRef.current || pullStartYRef.current == null) return;
    const delta = clientY - pullStartYRef.current;
    if (delta <= 0) {
      setPullDistance(0);
      setPullReady(false);
      return;
    }
    const scrollTop = window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
    if (scrollTop > 0) {
      pullActiveRef.current = false;
      pullStartYRef.current = null;
      setPullDistance(0);
      setPullReady(false);
      return;
    }
    const nextDistance = Math.min(PULL_REFRESH_MAX, delta * 0.45);
    setPullDistance(nextDistance);
    setPullReady(nextDistance >= PULL_REFRESH_TRIGGER);
  }

  function handlePullEnd() {
    if (!pullActiveRef.current) return;
    pullActiveRef.current = false;
    pullStartYRef.current = null;
    if (pullReady) {
      setPullReady(false);
      setPullRefreshing(true);
      setPullDistance(PULL_REFRESH_TRIGGER);
      window.setTimeout(() => window.location.reload(), 120);
      return;
    }
    setPullReady(false);
    setPullDistance(0);
  }

  useEffect(() => {
    if (typeof window === "undefined") return;
    function onTouchStart(event: TouchEvent) {
      if (event.touches.length !== 1) return;
      handlePullStart(event.touches[0].clientY);
    }
    function onTouchMove(event: TouchEvent) {
      if (event.touches.length !== 1) return;
      handlePullMove(event.touches[0].clientY);
    }
    function onTouchEnd() {
      handlePullEnd();
    }
    window.addEventListener("touchstart", onTouchStart, { passive: true });
    window.addEventListener("touchmove", onTouchMove, { passive: true });
    window.addEventListener("touchend", onTouchEnd, { passive: true });
    window.addEventListener("touchcancel", onTouchEnd, { passive: true });
    return () => {
      window.removeEventListener("touchstart", onTouchStart);
      window.removeEventListener("touchmove", onTouchMove);
      window.removeEventListener("touchend", onTouchEnd);
      window.removeEventListener("touchcancel", onTouchEnd);
    };
  }, [mobileMenuOpen, pullReady, pullRefreshing]);

  function renderMobileMenu() {
    if (!mobileMenuOpen) return null;
    return (
      <div className="app-drawer-overlay" onClick={() => setMobileMenuOpen(false)}>
        <aside className="app-drawer" onClick={(e) => e.stopPropagation()}>
          <div className="app-drawer-head">
            <div className="app-shell-brand">
              <div className="logo" />
              <div className="app-shell-brand-copy">
                <div className="app-shell-brand-title">Аналитика данных</div>
                <div className="app-shell-brand-subtitle">{currentItem?.label ?? currentGroup.title}</div>
              </div>
            </div>
            <button type="button" className="btn icon-only" onClick={() => setMobileMenuOpen(false)} aria-label="Закрыть меню">
              ×
            </button>
          </div>
          <div className="app-drawer-body">
            {groups.map((group) => (
              <section key={group.title} className="app-drawer-group">
                <div className="app-drawer-group-title">{group.title}</div>
                <div className="app-drawer-links">
                  {flattenCurrentGroupSections(group).map((section) => (
                    <div key={section.title} className="app-drawer-section">
                      {group.sections ? <div className="app-drawer-section-title">{section.title}</div> : null}
                      {section.items.map((item) => (
                        <Link
                          key={item.href}
                          to={item.href}
                          className={`app-drawer-link${isActive(pathname, item.href) ? " active" : ""}`}
                          onClick={() => setMobileMenuOpen(false)}
                        >
                          {item.label}
                        </Link>
                      ))}
                    </div>
                  ))}
                </div>
              </section>
            ))}
          </div>
        </aside>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <header className="app-shell-header">
        <div className="app-shell-header-inner">
          <div className="app-shell-mobilebar">
            <button type="button" className="btn mobile-menu-trigger" onClick={() => setMobileMenuOpen(true)}>
              Меню
            </button>
            <div className="app-shell-mobile-title">
              <div className="app-shell-mobile-label">{currentGroup.title}</div>
              <div className="app-shell-mobile-page">{currentItem?.label ?? currentGroup.title}</div>
            </div>
            <Link to="/" className="app-shell-mobile-home" aria-label="На главную">
              <div className="logo" />
            </Link>
          </div>

          <div className="app-shell-desktopbar">
            <Link to="/" className="app-shell-brand">
              <div className="logo" />
              <div className="app-shell-brand-copy">
                <div className="app-shell-brand-title">Аналитика данных</div>
                <div className="app-shell-brand-subtitle">Pricing, catalog, sales, settings</div>
              </div>
            </Link>

            <nav className="app-shell-primary-nav" aria-label="Основные разделы">
              {groups.map((group) => {
                const directHref = groupItems(group)[0]?.href || "/";
                const active = currentGroup.title === group.title;
                return (
                  <Link key={group.title} to={directHref} className={`app-shell-primary-link${active ? " active" : ""}`}>
                    {group.title}
                  </Link>
                );
              })}
            </nav>
            <div className="app-shell-userzone">
              {authUser?.role === "owner" ? (
                <Link to="/settings/admin" className={`app-shell-utility-link${isActive(pathname, "/settings/admin") ? " active" : ""}`}>
                  Пользователи
                </Link>
              ) : null}
              {authUser ? <div className="app-shell-userchip">{authUser.display_name || authUser.identifier}</div> : null}
            </div>
          </div>
        </div>
      </header>

      <div className="app-shell-subnav-wrap">
        <div className="app-shell-subnav">
          {currentSections.flatMap((section) =>
            section.items.map((item) => (
              <Link key={item.href} to={item.href} className={`app-shell-subnav-link${isActive(pathname, item.href) ? " active" : ""}`}>
                {item.label}
              </Link>
            )),
          )}
        </div>
      </div>

      {renderMobileMenu()}

      <div className={`pull-refresh-shell${pullDistance > 0 || pullRefreshing ? " active" : ""}`}>
        <div
          className={`pull-refresh-indicator${pullReady ? " ready" : ""}${pullRefreshing ? " loading" : ""}`}
          style={{ height: `${pullDistance}px` }}
          aria-hidden="true"
        >
          <div className="pull-refresh-spinner" />
          <span>{pullRefreshing ? "Обновляем..." : pullReady ? "Отпустите, чтобы обновить" : "Потяните вниз для обновления"}</span>
        </div>
        <main className="wrap">{children}</main>
      </div>

      {toast ? <div className={`app-toast${toast.tone === "error" ? " error" : ""}`}>{toast.message}</div> : null}

      <div className={`mobile-route-progress${mobileRouteLoading ? " active" : ""}`} aria-hidden="true">
        <div className="mobile-route-progress-bar" />
      </div>

      <nav className="app-shell-bottom-nav" aria-label="Нижняя навигация">
        {groups.map((group) => {
          const directHref = groupItems(group)[0]?.href || "/";
          const active = currentGroup.title === group.title;
          return (
            <Link key={group.title} to={directHref} className={`app-shell-bottom-link${active ? " active" : ""}`}>
              <span>{group.shortLabel}</span>
            </Link>
          );
        })}
      </nav>
    </div>
  );
}
