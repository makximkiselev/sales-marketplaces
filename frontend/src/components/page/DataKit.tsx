"use client";

import { ReactNode } from "react";
import styles from "./DataKit.module.css";

export function KpiGrid({ children }: { children: ReactNode }) {
  return <div className={styles.kpiGrid}>{children}</div>;
}

export function KpiCard({ label, value }: { label: ReactNode; value: ReactNode }) {
  return (
    <div className={styles.kpiCard}>
      <div className={styles.kpiLabel}>{label}</div>
      <div className={styles.kpiValue}>{value}</div>
    </div>
  );
}

export function TableCard({ children }: { children: ReactNode }) {
  return <div className={styles.tableCard}>{children}</div>;
}
