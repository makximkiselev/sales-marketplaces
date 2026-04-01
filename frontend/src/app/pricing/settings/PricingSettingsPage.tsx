import { useEffect, useState } from "react";
import styles from "./PricingSettingsPage.module.css";
import { LogisticsImportModal } from "./components/LogisticsImportModal";
import { BulkFillColumnModal } from "./components/BulkFillColumnModal";
import { usePricingSettingsController } from "./usePricingSettingsController";
import type { EditableFieldKey } from "./types";
import { PricingSettingsDesktop } from "./PricingSettingsDesktop";
import { PricingSettingsMobile } from "./PricingSettingsMobile";
import type { PricingSettingsSectionItem } from "./PricingSettingsRendererTypes";

function usePricingSettingsMobile() {
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const media = window.matchMedia("(max-width: 960px)");
    const sync = () => setIsMobile(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  return isMobile;
}

export default function PricingSettingsPage() {
  const [bulkField, setBulkField] = useState<EditableFieldKey>("commission_percent");
  const [bulkFillOpen, setBulkFillOpen] = useState(false);
  const [mobileCatalogOpen, setMobileCatalogOpen] = useState(false);
  const isMobile = usePricingSettingsMobile();
  const controller = usePricingSettingsController();
  const {
    activeStoreId,
    settingsTab,
    salesPlanError,
    storeSettingsSaving,
    storeSettingsError,
    storeSettingsSavedAt,
    logisticsStoreSaving,
    logisticsStoreSavedAt,
    logisticsStoreError,
    logisticsImportOpen,
    tableColumns,
    setLogisticsImportOpen,
    applyColumnValue,
    handleLogisticsImportDone,
    storeTabs,
    activeStoreTabKey,
    moneySign,
  } = controller;

  const sectionItems: PricingSettingsSectionItem[] = [
    {
      id: "sales_plan" as const,
      label: "План продаж",
      title: "План продаж",
      description: "Базовые цели по выручке и количеству для магазинов.",
    },
    {
      id: "categories" as const,
      label: "Категорийные затраты",
      title: "Категорийные затраты",
      description: "Комиссии, реклама и правила расчёта по категориям.",
    },
    {
      id: "logistics" as const,
      label: "Логистические затраты",
      title: "Логистические затраты",
      description: "Магазинные коэффициенты и затраты по товарам.",
    },
  ];
  const activeSection = sectionItems.find((item) => item.id === settingsTab) ?? sectionItems[0];
  const activeStore = storeTabs.find((store) => store.key === activeStoreTabKey) ?? null;
  const isSalesPlanSection = settingsTab === "sales_plan";
  const bulkFillColumns = tableColumns
    .filter(
      (column) =>
        column.kind === "input" &&
        column.field &&
        ![
          "target_profit_rub",
          "target_profit_percent",
          "target_margin_rub",
          "target_margin_percent",
        ].includes(column.field),
    )
    .map((column) => ({ field: column.field as EditableFieldKey, label: column.label }));
  const activeBulkField =
    bulkFillColumns.find((column) => column.field === bulkField)?.field ?? bulkFillColumns[0]?.field ?? "commission_percent";
  const currentSavedAt = settingsTab === "logistics" ? logisticsStoreSavedAt : storeSettingsSavedAt;
  const currentSaveState = settingsTab === "logistics"
    ? logisticsStoreSaving
      ? "Сохранение логистики..."
      : logisticsStoreError
        ? `Ошибка: ${logisticsStoreError}`
        : currentSavedAt
          ? `Сохранено ${new Date(currentSavedAt).toLocaleString("ru-RU")}`
          : "Изменения ещё не зафиксированы"
    : settingsTab === "categories"
      ? storeSettingsSaving
        ? "Автосохранение..."
        : storeSettingsError
          ? `Ошибка: ${storeSettingsError}`
          : currentSavedAt
            ? `Сохранено ${new Date(currentSavedAt).toLocaleString("ru-RU")}`
            : "Изменения ещё не зафиксированы"
      : salesPlanError
        ? `Ошибка: ${salesPlanError}`
        : "Редактирование вручную";

  return (
    <>
      <div className={styles.settingsPageRoot}>
        {isMobile ? (
          <PricingSettingsMobile
            controller={controller}
            sectionItems={sectionItems}
            activeSection={activeSection}
            activeStore={activeStore}
            isSalesPlanSection={isSalesPlanSection}
            currentSaveState={currentSaveState}
            bulkField={bulkField}
            bulkFillOpen={bulkFillOpen}
            setBulkField={setBulkField}
            setBulkFillOpen={setBulkFillOpen}
            mobileCatalogOpen={mobileCatalogOpen}
            setMobileCatalogOpen={setMobileCatalogOpen}
            bulkFillColumns={bulkFillColumns}
          />
        ) : (
          <PricingSettingsDesktop
            controller={controller}
            sectionItems={sectionItems}
            activeSection={activeSection}
            activeStore={activeStore}
            isSalesPlanSection={isSalesPlanSection}
            currentSaveState={currentSaveState}
            bulkField={bulkField}
            bulkFillOpen={bulkFillOpen}
            setBulkField={setBulkField}
            setBulkFillOpen={setBulkFillOpen}
            mobileCatalogOpen={mobileCatalogOpen}
            setMobileCatalogOpen={setMobileCatalogOpen}
            bulkFillColumns={bulkFillColumns}
          />
        )}
      </div>

      {settingsTab === "categories" && bulkFillOpen && bulkFillColumns.length ? (
        <BulkFillColumnModal
          fields={bulkFillColumns}
          initialField={activeBulkField}
          onClose={() => setBulkFillOpen(false)}
          onConfirm={async (field, value) => {
            setBulkField(field);
            await applyColumnValue(field, value);
            setBulkFillOpen(false);
          }}
        />
      ) : null}

      <LogisticsImportModal
        open={logisticsImportOpen}
        platform={controller.activePlatform}
        storeId={activeStoreId}
        onClose={() => setLogisticsImportOpen(false)}
        onDone={handleLogisticsImportDone}
      />
    </>
  );
}
