import type { ReactNode } from "react";
import styles from "./PricingMatrixKit.module.css";
import type { StoreCtx } from "../_shared/catalogPageShared";

export function MatrixNameCell({ name, path }: { name: string; path: string[] }) {
  return (
    <td className={styles.nameCell}>
      <div className={styles.nameTitle}>{name || "—"}</div>
    </td>
  );
}

export function MatrixMultiValue({ rows }: { rows: Array<{ key: string; label: string; value: ReactNode }> }) {
  return (
    <div className={styles.multiValue}>
      {rows.map((row) => (
        <div className={styles.multiLine} key={row.key}>
          <span className={styles.multiStore}>{row.label}</span>
          <div className={styles.multiValueContent}>{row.value}</div>
        </div>
      ))}
    </div>
  );
}

export function buildStoreLines(stores: StoreCtx[], map: (store: StoreCtx) => ReactNode, keyPrefix: string) {
  return stores.map((store) => ({
    key: `${keyPrefix}-${store.store_uid}`,
    label: store.label,
    value: map(store),
  }));
}

export { styles as pricingMatrixStyles };
