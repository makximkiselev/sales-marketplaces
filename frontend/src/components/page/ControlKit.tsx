import { ReactNode } from "react";
import styles from "./ControlKit.module.css";

export function ControlField({
  label,
  children,
  className = "",
}: {
  label: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={`${styles.field} ${className}`.trim()}>
      <div className={styles.label}>{label}</div>
      <div className={styles.fieldBody}>{children}</div>
    </div>
  );
}

export function ControlTabs<T extends string>({
  items,
  activeId,
  onChange,
  className = "",
  variant = "pill",
}: {
  items: Array<{ id: T; label: ReactNode; badge?: ReactNode }>;
  activeId: T;
  onChange: (id: T) => void;
  className?: string;
  variant?: "pill" | "underline";
}) {
  return (
    <div className={`${styles.tabs} ${variant === "underline" ? styles.tabsUnderline : ""} ${className}`.trim()}>
      {items.map((item) => (
        <button
          key={item.id}
          type="button"
          className={`btn inline ${styles.tabBtn} ${variant === "underline" ? styles.tabBtnUnderline : ""} ${activeId === item.id ? (variant === "underline" ? styles.tabBtnUnderlineActive : styles.tabBtnActive) : ""}`.trim()}
          data-active={activeId === item.id ? "true" : "false"}
          aria-current={activeId === item.id ? "page" : undefined}
          onClick={() => onChange(item.id)}
        >
          <span>{item.label}</span>
          {item.badge ? <span className={`${styles.tabBadge} ${variant === "underline" ? styles.tabBadgeUnderline : ""}`.trim()}>{item.badge}</span> : null}
        </button>
      ))}
    </div>
  );
}

export function LoadingButton({
  loading,
  idleLabel,
  loadingLabel,
  className = "",
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  loading?: boolean;
  idleLabel: ReactNode;
  loadingLabel?: ReactNode;
}) {
  return (
    <button
      {...props}
      type={props.type || "button"}
      className={`btn inline ${styles.actionButton} ${loading ? styles.actionButtonLoading : ""} ${className}`.trim()}
    >
      {loading ? (
        <>
          <span className={styles.spinner} aria-hidden="true" />
          <span>{loadingLabel ?? idleLabel}</span>
        </>
      ) : (
        idleLabel
      )}
    </button>
  );
}
