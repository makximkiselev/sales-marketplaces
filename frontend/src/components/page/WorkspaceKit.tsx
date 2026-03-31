import { ReactNode } from "react";
import { ControlTabs } from "./ControlKit";
import styles from "./WorkspaceKit.module.css";

type TabItem<T extends string> = {
  id: T;
  label: ReactNode;
  meta?: ReactNode;
};

export function WorkspaceStack({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={`${styles.stack} ${className}`.trim()}>{children}</div>;
}

export function WorkspaceSurface({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <section className={`${styles.surface} ${className}`.trim()}>{children}</section>;
}

export function WorkspaceHeader({
  title,
  subtitle,
  meta,
  children,
  className = "",
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  meta?: ReactNode;
  children?: ReactNode;
  className?: string;
}) {
  return (
    <div className={`${styles.header} ${className}`.trim()}>
      <div className={styles.headerMain}>
        <h2 className={styles.title}>{title}</h2>
        {subtitle ? <p className={styles.subtitle}>{subtitle}</p> : null}
      </div>
      {(meta || children) ? (
        <div className={styles.headerAside}>
          {meta ? <div className={styles.meta}>{meta}</div> : null}
          {children}
        </div>
      ) : null}
    </div>
  );
}

export function WorkspaceTabs<T extends string>({
  items,
  activeId,
  onChange,
  className = "",
}: {
  items: Array<TabItem<T>>;
  activeId: T;
  onChange: (id: T) => void;
  className?: string;
}) {
  return (
    <ControlTabs
      className={`${styles.tabs} ${className}`.trim()}
      items={items.map((item) => ({
        id: item.id,
        label: item.label,
        badge: item.meta,
      }))}
      activeId={activeId}
      onChange={onChange}
    />
  );
}

export function WorkspaceToolbar({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={`${styles.toolbar} ${className}`.trim()}>{children}</div>;
}
