import type { usePricingSettingsController } from "./usePricingSettingsController";
import type { EditableFieldKey } from "./types";

export type PricingSettingsSectionId = "sales_plan" | "categories" | "sources" | "logistics";

export type PricingSettingsSectionItem = {
  id: PricingSettingsSectionId;
  label: string;
  title: string;
  description: string;
};

export type PricingSettingsController = ReturnType<typeof usePricingSettingsController>;

export type PricingSettingsRendererProps = {
  controller: PricingSettingsController;
  sectionItems: PricingSettingsSectionItem[];
  activeSection: PricingSettingsSectionItem;
  activeStore: PricingSettingsController["storeTabs"][number] | null;
  isSalesPlanSection: boolean;
  currentSaveState: string;
  bulkField: EditableFieldKey;
  bulkFillOpen: boolean;
  setBulkField: (field: EditableFieldKey) => void;
  setBulkFillOpen: (open: boolean) => void;
  mobileCatalogOpen: boolean;
  setMobileCatalogOpen: (open: boolean) => void;
  bulkFillColumns: Array<{ field: EditableFieldKey; label: string }>;
};
