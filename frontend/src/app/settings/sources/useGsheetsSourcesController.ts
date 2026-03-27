"use client";

import { useState } from "react";
import type { SourceItem } from "./types";

export function useGsheetsSourcesController(deps: {
  activeGoogleAccountId: string;
  verifyGsheetsRequest: (payload: { spreadsheet_url: string; account_id?: string; worksheet?: string }) => Promise<{ worksheets?: string[] }>;
  connectGsheetsSource: (payload: {
    source_id: string;
    title: string;
    spreadsheet_url: string;
    worksheet: string;
    mode_import: boolean;
    mode_export: boolean;
    account_id?: string;
  }) => Promise<unknown>;
  uploadGoogleAccountKey: (file: File, accountName: string) => Promise<{ active_account_id?: string }>;
  deleteGoogleAccount: (accountId: string) => Promise<{ active_account_id?: string }>;
  selectGoogleAccount: (accountId: string) => Promise<unknown>;
  checkGsheetSource: (sourceId: string) => Promise<unknown>;
  loadData: () => Promise<unknown>;
}) {
  const [gsWizardMode, setGsWizardMode] = useState<"create" | "edit-select">("create");
  const [gsWizardOpen, setGsWizardOpen] = useState(false);
  const [gsStep, setGsStep] = useState<1 | 2 | 3>(1);
  const [gsLoading, setGsLoading] = useState(false);
  const [gsError, setGsError] = useState("");
  const [gsTitle, setGsTitle] = useState("");
  const [gsSpreadsheet, setGsSpreadsheet] = useState("");
  const [gsModeImport, setGsModeImport] = useState(true);
  const [gsModeExport, setGsModeExport] = useState(false);
  const [gsCredFileName, setGsCredFileName] = useState("");
  const [gsCredFile, setGsCredFile] = useState<File | null>(null);
  const [gsKeyUploading, setGsKeyUploading] = useState(false);
  const [gsDropActive, setGsDropActive] = useState(false);
  const [gsKeyUploadOk, setGsKeyUploadOk] = useState<boolean | null>(null);
  const [gsKeyUploadMessage, setGsKeyUploadMessage] = useState("");
  const [gsSelectedAccountId, setGsSelectedAccountId] = useState("");
  const [gsWorksheets, setGsWorksheets] = useState<string[]>([]);
  const [gsWorksheet, setGsWorksheet] = useState("");
  const [gsEditingSourceId, setGsEditingSourceId] = useState("");
  const [gsSourceCheckLoading, setGsSourceCheckLoading] = useState<Record<string, boolean>>({});

  function openGsWizard(source?: SourceItem, mode: "create" | "edit-select" = "create") {
    setGsWizardMode(mode);
    setGsWizardOpen(true);
    setGsStep(1);
    setGsLoading(false);
    setGsError("");
    setGsEditingSourceId(source?.id || "");
    setGsTitle(source?.title || "");
    setGsSpreadsheet(source?.spreadsheet_id || "");
    setGsModeImport(source?.mode_import ?? true);
    setGsModeExport(source?.mode_export ?? false);
    setGsCredFileName("");
    setGsCredFile(null);
    setGsKeyUploading(false);
    setGsDropActive(false);
    setGsKeyUploadOk(null);
    setGsKeyUploadMessage("");
    setGsSelectedAccountId(deps.activeGoogleAccountId || "");
    setGsWorksheets([]);
    setGsWorksheet(source?.worksheet || "");
  }

  function closeGsWizard() {
    setGsWizardOpen(false);
    setGsWizardMode("create");
    setGsError("");
    setGsLoading(false);
  }

  async function chooseExistingGsSource(sourceId: string, sources: SourceItem[]) {
    const source = sources.find((it) => it.id === sourceId);
    if (!source) return;
    setGsEditingSourceId(source.id || "");
    setGsTitle(source.title || "");
    setGsSpreadsheet(source.spreadsheet_id || "");
    setGsModeImport(source.mode_import ?? true);
    setGsModeExport(source.mode_export ?? false);
    setGsWorksheets([]);
    setGsWorksheet(source.worksheet || "");
    setGsError("");
    setGsLoading(true);
    try {
      const data = await deps.verifyGsheetsRequest({
        spreadsheet_url: source.spreadsheet_id || "",
        account_id: gsSelectedAccountId || undefined,
        worksheet: source.worksheet || undefined,
      });
      const worksheets: string[] = data.worksheets || [];
      setGsWorksheets(worksheets);
      setGsWorksheet(source.worksheet || worksheets[0] || "");
      setGsStep(3);
    } catch (e) {
      setGsError(e instanceof Error ? e.message : String(e));
    } finally {
      setGsLoading(false);
    }
  }

  async function verifyGsheets(worksheetOverride?: string) {
    setGsError("");
    setGsLoading(true);
    try {
      const data = await deps.verifyGsheetsRequest({
        spreadsheet_url: gsSpreadsheet,
        account_id: gsSelectedAccountId || undefined,
        worksheet: worksheetOverride || gsWorksheet || undefined,
      });
      const worksheets: string[] = data.worksheets || [];
      setGsWorksheets(worksheets);
      const nextWorksheet = worksheetOverride || gsWorksheet || worksheets[0] || "";
      setGsWorksheet(nextWorksheet);
      setGsStep(3);
    } catch (e) {
      setGsError(e instanceof Error ? e.message : String(e));
    } finally {
      setGsLoading(false);
    }
  }

  async function goToGsStep(targetStep: 1 | 2 | 3) {
    if (!gsEditingSourceId && targetStep === 1) {
      setGsStep(1);
      return;
    }
    if (targetStep === 1) {
      setGsStep(1);
      return;
    }
    if (targetStep === 2) {
      if (!gsTitle.trim() || !gsSpreadsheet.trim()) return;
      setGsStep(2);
      return;
    }
    if (!gsSelectedAccountId) return;
    if (gsWorksheets.length > 0) {
      setGsStep(3);
      return;
    }
    await verifyGsheets();
  }

  async function connectGsheets() {
    setGsError("");
    setGsLoading(true);
    try {
      const sourceId = gsEditingSourceId || (gsTitle || "gsheets_source").toLowerCase().replace(/[^a-z0-9_]+/g, "_");
      await deps.connectGsheetsSource({
        source_id: sourceId,
        title: gsTitle || "Google Sheets",
        spreadsheet_url: gsSpreadsheet,
        worksheet: gsWorksheet,
        mode_import: gsModeImport,
        mode_export: gsModeExport,
        account_id: gsSelectedAccountId || undefined,
      });
      closeGsWizard();
      await deps.loadData();
    } catch (e) {
      setGsError(e instanceof Error ? e.message : String(e));
    } finally {
      setGsLoading(false);
    }
  }

  async function uploadGoogleKey(file: File) {
    setGsError("");
    setGsKeyUploading(true);
    setGsKeyUploadOk(null);
    setGsKeyUploadMessage("");
    try {
      const accountName = gsTitle.trim();
      if (!accountName) throw new Error('Заполните поле "Название источника" на шаге 1.');
      const data = await deps.uploadGoogleAccountKey(file, accountName);
      setGsSelectedAccountId(data.active_account_id || "");
      setGsCredFile(file);
      setGsCredFileName(file.name);
      setGsKeyUploadOk(true);
      setGsKeyUploadMessage("Ошибки не найдены");
      await deps.loadData();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setGsError(msg);
      setGsKeyUploadOk(false);
      setGsKeyUploadMessage("Есть ошибки, загрузите файл снова");
    } finally {
      setGsKeyUploading(false);
    }
  }

  async function onGoogleKeyFileSelected(file: File | null) {
    if (!file) {
      setGsCredFileName("");
      setGsCredFile(null);
      setGsKeyUploadOk(null);
      setGsKeyUploadMessage("");
      return;
    }
    await uploadGoogleKey(file);
  }

  async function deleteGoogleAccount(accountId: string) {
    setGsError("");
    setGsLoading(true);
    try {
      const data = await deps.deleteGoogleAccount(accountId);
      setGsSelectedAccountId(data.active_account_id || "");
      await deps.loadData();
    } catch (e) {
      setGsError(e instanceof Error ? e.message : String(e));
    } finally {
      setGsLoading(false);
    }
  }

  async function useExistingGoogleAccount() {
    if (!gsSelectedAccountId) return;
    setGsError("");
    setGsLoading(true);
    try {
      await deps.selectGoogleAccount(gsSelectedAccountId);
    } catch (e) {
      setGsError(e instanceof Error ? e.message : String(e));
    } finally {
      setGsLoading(false);
    }
  }

  async function checkGsheetSource(sourceId: string) {
    setGsSourceCheckLoading((prev) => ({ ...prev, [sourceId]: true }));
    try {
      await deps.checkGsheetSource(sourceId);
    } finally {
      setGsSourceCheckLoading((prev) => ({ ...prev, [sourceId]: false }));
      await deps.loadData();
    }
  }

  return {
    gsWizardOpen,
    gsWizardMode,
    gsStep,
    gsLoading,
    gsError,
    gsTitle,
    gsSpreadsheet,
    gsModeImport,
    gsModeExport,
    gsCredFileName,
    gsCredFile,
    gsKeyUploading,
    gsDropActive,
    gsKeyUploadOk,
    gsKeyUploadMessage,
    gsSelectedAccountId,
    gsWorksheets,
    gsWorksheet,
    gsEditingSourceId,
    gsSourceCheckLoading,
    setGsStep,
    setGsTitle,
    setGsSpreadsheet,
    setGsModeImport,
    setGsModeExport,
    setGsSelectedAccountId,
    setGsWorksheet,
    setGsDropActive,
    openGsWizard,
    chooseExistingGsSource,
    closeGsWizard,
    goToGsStep,
    verifyGsheets,
    connectGsheets,
    onGoogleKeyFileSelected,
    deleteGoogleAccount,
    useExistingGoogleAccount,
    checkGsheetSource,
  };
}
