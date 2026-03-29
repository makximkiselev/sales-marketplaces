import { ReactNode } from "react";
import styles from "./SectionKit.module.css";

export function SectionBlock({
  title,
  children,
  className = "",
  bodyClassName = "",
}: {
  title?: string;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <section className={`section-frame card ${styles.section} ${className}`.trim()}>
      {title ? <div className={`section-title ${styles.sectionTitle}`.trim()}>{title}</div> : null}
      <div className={`inner ${styles.sectionBody} ${bodyClassName}`.trim()}>{children}</div>
    </section>
  );
}

export function PanelGrid({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={`grid integration-cards ${styles.panelGrid} ${className}`.trim()}>{children}</div>;
}

export function PanelCard({
  title,
  description,
  action,
  children,
  footer,
  className = "",
}: {
  title: string;
  description?: ReactNode;
  action?: ReactNode;
  children?: ReactNode;
  footer?: ReactNode;
  className?: string;
}) {
  return (
    <article className={`integration-card ${styles.panelCard} ${className}`.trim()}>
      <div className={`integration-head ${styles.panelHead}`.trim()}>
        <div className={styles.panelTitleBlock}>
          <div className="integration-title">{title}</div>
          {description ? <div className="integration-desc">{description}</div> : null}
        </div>
        {action}
      </div>
      {children}
      {footer ? <div className={styles.panelFooter}>{footer}</div> : null}
    </article>
  );
}
