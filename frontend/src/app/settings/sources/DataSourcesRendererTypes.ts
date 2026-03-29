import type { useSourcesPageController } from "./useSourcesPageController";

export type SourcesSectionId = "all" | "platforms" | "tables" | "external";

export type SourcesSectionItem = {
  id: SourcesSectionId;
  label: string;
};

export type SourcesController = ReturnType<typeof useSourcesPageController>;
