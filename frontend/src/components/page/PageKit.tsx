"use client";

import { ReactNode, useEffect, useState } from "react";
import { createPortal } from "react-dom";
import styles from "./PageKit.module.css";

type PageFrameProps = {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  meta?: ReactNode;
  toolbarLeft?: ReactNode;
  toolbarRight?: ReactNode;
  className?: string;
  innerClassName?: string;
  children?: ReactNode;
};

export function PageFrame({
  title,
  subtitle,
  actions,
  meta,
  toolbarLeft,
  toolbarRight,
  className = "",
  innerClassName = "",
  children,
}: PageFrameProps) {
  return (
    <section className={`card section-frame ${styles.frame} ${className}`.trim()}>
      <div className={`${styles.frameInner} ${innerClassName}`.trim()}>
        <div className={styles.hero}>
          <div className={styles.heroMain}>
            <h1 className={styles.heroTitle}>{title}</h1>
            {subtitle ? <p className={styles.heroSubtitle}>{subtitle}</p> : null}
          </div>
          {(actions || meta) ? (
            <div className={styles.heroAside}>
              {actions}
              {meta ? <div className={styles.heroMeta}>{meta}</div> : null}
            </div>
          ) : null}
        </div>
        {(toolbarLeft || toolbarRight) ? (
          <div className={styles.toolbar}>
            <div className={styles.toolbarLeft}>{toolbarLeft}</div>
            <div className={styles.toolbarRight}>{toolbarRight}</div>
          </div>
        ) : null}
        {children}
      </div>
    </section>
  );
}

export function PageSectionTitle({
  title,
  meta,
  actions,
}: {
  title: string;
  meta?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className={styles.sectionRow}>
      <div className={styles.sectionTitleBlock}>
        <h3 className={styles.sectionTitle}>{title}</h3>
        {meta ? <div className={styles.sectionMeta}>{meta}</div> : null}
      </div>
      {actions}
    </div>
  );
}

export function ModalShell({
  title,
  subtitle,
  onClose,
  width,
  children,
}: {
  title: string;
  subtitle?: string;
  onClose: () => void;
  width?: string;
  children: ReactNode;
}) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    return () => setMounted(false);
  }, []);

  if (!mounted) {
    return null;
  }

  return createPortal(
    <div className={styles.modalOverlay} onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className={styles.modalCard} style={width ? { width } : undefined} onClick={(e) => e.stopPropagation()}>
        <div className={styles.modalHead}>
          <div>
            <h2 className={styles.modalTitle}>{title}</h2>
            {subtitle ? <p className={styles.modalSubtitle}>{subtitle}</p> : null}
          </div>
          <button type="button" className="btn inline icon-only" onClick={onClose} aria-label="Закрыть">
            ×
          </button>
        </div>
        <div className={styles.modalBody}>{children}</div>
      </div>
    </div>,
    document.body,
  );
}
