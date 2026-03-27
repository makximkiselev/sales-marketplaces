"use client";

import { ReactNode } from "react";
import { ModalShell } from "./PageKit";
import styles from "./WizardKit.module.css";

type WizardStepItem = {
  key: string;
  label: string;
  active?: boolean;
  clickable?: boolean;
  onClick?: () => void;
};

export function WizardModal({
  title,
  onClose,
  steps,
  width,
  children,
  footer,
  error,
}: {
  title: string;
  onClose: () => void;
  steps: WizardStepItem[];
  width?: string;
  children: ReactNode;
  footer?: ReactNode;
  error?: ReactNode;
}) {
  return (
    <ModalShell title={title} onClose={onClose} width={width}>
      <div className={styles.shell}>
        <WizardSteps steps={steps} columns={steps.length} />
        <div className="grid">{children}</div>
        {error ? <div className="status error">{error}</div> : null}
        {footer ? <WizardFooter>{footer}</WizardFooter> : null}
      </div>
    </ModalShell>
  );
}

export function WizardSteps({
  steps,
  columns,
}: {
  steps: WizardStepItem[];
  columns?: number;
}) {
  return (
    <div
      className={styles.steps}
      style={columns ? { gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` } : undefined}
    >
      {steps.map((step) => (
        <button
          key={step.key}
          type="button"
          className={`${styles.step} ${step.active ? styles.stepActive : ""} ${step.clickable ? styles.stepClickable : ""}`.trim()}
          onClick={step.onClick}
          disabled={!step.clickable && !step.active}
        >
          {step.label}
        </button>
      ))}
    </div>
  );
}

export function WizardLabel({ children }: { children: ReactNode }) {
  return <label className={styles.label}>{children}</label>;
}

export function WizardFooter({ children }: { children: ReactNode }) {
  return <div className={styles.footer}>{children}</div>;
}

export function WizardDropzone({
  active,
  onClick,
  onKeyDown,
  onDragOver,
  onDragLeave,
  onDrop,
  title,
  subtitle,
  input,
}: {
  active?: boolean;
  onClick: () => void;
  onKeyDown: (event: React.KeyboardEvent<HTMLDivElement>) => void;
  onDragOver: (event: React.DragEvent<HTMLDivElement>) => void;
  onDragLeave: () => void;
  onDrop: (event: React.DragEvent<HTMLDivElement>) => void;
  title: string;
  subtitle?: string;
  input: ReactNode;
}) {
  return (
    <div
      className={`${styles.dropzone} ${active ? styles.dropzoneActive : ""}`.trim()}
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={onKeyDown}
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onDrop={onDrop}
    >
      {input}
      <div className={styles.dropzoneContent}>
        <div className={styles.dropzoneTitle}>{title}</div>
        {subtitle ? <div className={styles.dropzoneSubtitle}>{subtitle}</div> : null}
      </div>
    </div>
  );
}
