import { useState } from "react";
import type { DeleteRequest } from "./types";

export function useDeleteConfirmController(actions: {
  deleteYandexAccount: (businessId: string) => Promise<void>;
  deleteYandexShop: (businessId: string, campaignId: string) => Promise<void>;
  deleteOzonAccount: (clientId: string) => Promise<void>;
  deleteGsheetSource: (sourceId: string) => Promise<void>;
  deleteGoogleAccount: (accountId: string) => Promise<void>;
}) {
  const [deleteRequest, setDeleteRequest] = useState<DeleteRequest | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [deleteError, setDeleteError] = useState("");

  function openDeleteConfirm(req: DeleteRequest) {
    setDeleteError("");
    setDeleteRequest(req);
  }

  function closeDeleteConfirm() {
    if (deleteBusy) return;
    setDeleteRequest(null);
    setDeleteError("");
  }

  function getDeleteConfirmText(req: DeleteRequest | null): string {
    if (!req) return "";
    if (req.type === "yandex_shop") return `Вы точно хотите удалить магазин "${req.name}"?`;
    if (req.type === "yandex_account") return `Вы точно хотите удалить аккаунт ${req.business_id}?`;
    if (req.type === "ozon_account") return `Вы точно хотите удалить кабинет Ozon "${req.name || req.client_id}"?`;
    if (req.type === "gsheet_source") return `Вы точно хотите удалить источник "${req.name}"?`;
    return `Вы точно хотите удалить Google-аккаунт "${req.name}"?`;
  }

  async function confirmDelete() {
    if (!deleteRequest) return;
    setDeleteBusy(true);
    setDeleteError("");
    try {
      if (deleteRequest.type === "yandex_account") await actions.deleteYandexAccount(deleteRequest.business_id);
      else if (deleteRequest.type === "yandex_shop") await actions.deleteYandexShop(deleteRequest.business_id, deleteRequest.campaign_id);
      else if (deleteRequest.type === "ozon_account") await actions.deleteOzonAccount(deleteRequest.client_id);
      else if (deleteRequest.type === "gsheet_source") await actions.deleteGsheetSource(deleteRequest.source_id);
      else if (deleteRequest.type === "google_account") await actions.deleteGoogleAccount(deleteRequest.account_id);
      setDeleteRequest(null);
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : String(e));
    } finally {
      setDeleteBusy(false);
    }
  }

  return {
    deleteRequest,
    deleteBusy,
    deleteError,
    openDeleteConfirm,
    closeDeleteConfirm,
    getDeleteConfirmText,
    confirmDelete,
  };
}
