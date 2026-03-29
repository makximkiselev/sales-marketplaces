"use client";

import { ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { APP_TOAST_EVENT, type AppToastDetail } from "./ui/toastBus";

type NavItem = { href: string; label: string };
type NavSection = { title: string; items: NavItem[] };
type NavGroup = { title: string; items?: NavItem[]; sections?: NavSection[] };

const groups: NavGroup[] = [
  {
    title: "Сводка",
    items: [{ href: "/", label: "Дашборд" }],
  },
  {
    title: "Каталог",
    sections: [
      {
        title: "Товары",
        items: [
          { href: "/catalog", label: "Список товаров" },
        ],
      },
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
    sections: [
      {
        title: "Наши данные",
        items: [{ href: "/sales/overview", label: "Обзор продаж" }],
      },
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
  if (href === "/") {
    return pathname === "/";
  }
  const allHrefs = groups.flatMap((group) => groupItems(group).map((item) => item.href));
  const hasDeeper = allHrefs.some((h) => h !== href && h.startsWith(`${href}/`));
  if (hasDeeper) {
    return pathname === href;
  }
  return pathname === href || pathname.startsWith(`${href}/`);
}

export function Shell({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  const [openMenuTitle, setOpenMenuTitle] = useState<string | null>(null);
  const [suppressHoverTitle, setSuppressHoverTitle] = useState<string | null>(null);
  const [toast, setToast] = useState<AppToastDetail | null>(null);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [mobileGroupTitle, setMobileGroupTitle] = useState<string | null>(null);
  const [pullDistance, setPullDistance] = useState(0);
  const [pullReady, setPullReady] = useState(false);
  const [pullRefreshing, setPullRefreshing] = useState(false);
  const pullStartYRef = useRef<number | null>(null);
  const pullActiveRef = useRef(false);
  const currentGroupTitle = useMemo(() => {
    const match = groups.find((group) => groupItems(group).some((item) => isActive(pathname, item.href)));
    return match?.title ?? groups[0].title;
  }, [pathname]);
  const currentItem = useMemo(() => {
    for (const group of groups) {
      for (const item of groupItems(group)) {
        if (isActive(pathname, item.href)) {
          return item;
        }
      }
    }
    return groupItems(groups[0])[0] ?? { href: "/", label: "Дашборд" };
  }, [pathname]);

  useEffect(() => {
    setOpenMenuTitle(null);
    setSuppressHoverTitle(null);
    setMobileMenuOpen(false);
    setMobileGroupTitle(null);
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

  function closeMenuAfterClick(groupTitle: string) {
    setOpenMenuTitle(null);
    setSuppressHoverTitle(groupTitle);
    if (typeof document !== "undefined") {
      const el = document.activeElement as HTMLElement | null;
      el?.blur?.();
    }
  }

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
      window.setTimeout(() => {
        window.location.reload();
      }, 120);
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
      <div className="mobile-nav-overlay" onClick={() => setMobileMenuOpen(false)}>
        <aside className="mobile-nav-drawer" onClick={(e) => e.stopPropagation()}>
          <div className="mobile-nav-head">
            <div className="mobile-nav-brand">
              <div className="logo" />
              <div>
                <div className="mobile-nav-title">Аналитика данных</div>
                <div className="mobile-nav-subtitle">{currentItem.label}</div>
              </div>
            </div>
            <button type="button" className="btn icon-only" onClick={() => setMobileMenuOpen(false)} aria-label="Закрыть меню">
              ×
            </button>
          </div>
          <div className="mobile-nav-body">
            {groups.map((group) => {
              const items = groupItems(group);
              const selected = currentGroupTitle === group.title;
              const expanded = mobileGroupTitle === group.title;
              const directHref = items[0]?.href || "/";
              const isSingleLink = group.title === "Сводка";
              const sectionItems = group.sections
                ? group.sections
                    .map((section) => ({
                      ...section,
                      items: section.items.filter((item) => !isActive(pathname, item.href)),
                    }))
                    .filter((section) => section.items.length > 0)
                : null;
              const standaloneItems = group.sections
                ? null
                : items.filter((item) => !isActive(pathname, item.href));
              if (isSingleLink) {
                if (selected) {
                  return null;
                }
                return (
                  <Link key={group.title} to={directHref} className={`mobile-nav-direct${selected ? " active" : ""}`} onClick={() => setMobileMenuOpen(false)}>
                    {group.title}
                  </Link>
                );
              }
              return (
                <section key={group.title} className={`mobile-nav-group${expanded ? " expanded" : ""}`}>
                  <button
                    type="button"
                    className={`mobile-nav-group-toggle${selected ? " active" : ""}`}
                    onClick={() => setMobileGroupTitle((prev) => (prev === group.title ? null : group.title))}
                  >
                    <span>{group.title}</span>
                    <span className="mobile-nav-chevron">{expanded ? "−" : "+"}</span>
                  </button>
                  {expanded ? (
                    <div className="mobile-nav-links">
                      {group.sections
                        ? sectionItems?.map((section) => (
                            <div key={section.title} className="mobile-nav-section">
                              <div className="mobile-nav-section-title">{section.title}</div>
                              {section.items.map((item) => (
                                <Link
                                  key={item.href}
                                  to={item.href}
                                  className={`mobile-nav-link${isActive(pathname, item.href) ? " active" : ""}`}
                                  onClick={() => setMobileMenuOpen(false)}
                                >
                                  {item.label}
                                </Link>
                              ))}
                            </div>
                          ))
                        : standaloneItems?.map((item) => (
                            <Link
                              key={item.href}
                              to={item.href}
                              className={`mobile-nav-link${isActive(pathname, item.href) ? " active" : ""}`}
                              onClick={() => setMobileMenuOpen(false)}
                            >
                              {item.label}
                            </Link>
                          ))}
                      {group.sections && !sectionItems?.length ? <div className="mobile-nav-empty">Все страницы этого раздела скрыты</div> : null}
                      {!group.sections && !standaloneItems?.length ? <div className="mobile-nav-empty">Текущая страница уже открыта</div> : null}
                    </div>
                  ) : null}
                </section>
              );
            })}
          </div>
        </aside>
      </div>
    );
  }

  return (
    <div className="app">
      <main className="main">
        <header className="topbar">
          <div className="mobile-topbar">
            <button
              type="button"
              className="btn mobile-menu-trigger"
              onClick={() => {
                setMobileGroupTitle(currentGroupTitle);
                setMobileMenuOpen(true);
              }}
            >
              Меню
            </button>
            <div className="mobile-topbar-center">
              <div className="mobile-topbar-title">{currentItem.label}</div>
              <div className="mobile-topbar-subtitle">{currentGroupTitle}</div>
            </div>
            <Link to="/" className="mobile-topbar-home">
              <div className="logo" />
            </Link>
          </div>
          <div className="topbar-main topbar-main-inline">
            <div className="brand-inline">
              <div className="logo" />
              <div className="name">Аналитика данных</div>
            </div>
            <div className="mega-menu">
              <nav className="primary-nav">
                {groups.map((group) => {
                  const selected = currentGroupTitle === group.title;
                  const isSingleLink = group.title === "Сводка";
                  const directHref = groupItems(group)[0]?.href || "/";

                  if (isSingleLink) {
                    return (
                      <Link
                        key={group.title}
                        to={directHref}
                        className={`primary-nav-link primary-nav-link-direct${selected ? " active" : ""}`}
                      >
                        <span>{group.title}</span>
                      </Link>
                    );
                  }

                  const isOpen = openMenuTitle === group.title;
                  return (
                    <div
                      key={group.title}
                      className={`primary-nav-item${isOpen ? " open" : ""}`}
                      onMouseEnter={() => {
                        if (suppressHoverTitle === group.title) return;
                        setOpenMenuTitle(group.title);
                      }}
                      onMouseLeave={() => {
                        setOpenMenuTitle((prev) => (prev === group.title ? null : prev));
                        setSuppressHoverTitle((prev) => (prev === group.title ? null : prev));
                      }}
                    >
                      <button type="button" className={`primary-nav-link${selected ? " active" : ""}`}>
                        <span>{group.title}</span>
                      </button>

                      <div className={`mega-panel ${group.sections ? "mega-panel-sections" : "mega-panel-list"}`}>
                        {group.sections ? (
                          group.sections.map((section) => (
                            <div key={section.title} className="mega-panel-section">
                              <div className="mega-panel-section-title">{section.title}</div>
                              <div className="mega-panel-section-items">
                                {section.items.map((item) => {
                                  const active = isActive(pathname, item.href);
                                  return (
                                    <Link
                                      key={item.href}
                                      to={item.href}
                                      className={`mega-panel-link${active ? " active" : ""}`}
                                      onClick={() => closeMenuAfterClick(group.title)}
                                    >
                                      {item.label}
                                    </Link>
                                  );
                                })}
                              </div>
                            </div>
                          ))
                        ) : (
                          groupItems(group).map((item) => {
                            const active = isActive(pathname, item.href);
                            return (
                              <Link
                                key={item.href}
                                to={item.href}
                                className={`mega-panel-link${active ? " active" : ""}`}
                                onClick={() => closeMenuAfterClick(group.title)}
                              >
                                {item.label}
                              </Link>
                            );
                          })
                        )}
                      </div>
                    </div>
                  );
                })}
              </nav>
            </div>
          </div>
        </header>
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
          <div className="wrap">{children}</div>
        </div>
        {toast ? (
          <div className={`app-toast${toast.tone === "error" ? " error" : ""}`}>
            {toast.message}
          </div>
        ) : null}
      </main>
    </div>
  );
}
