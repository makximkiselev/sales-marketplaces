from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.services.auth_service import (
    AUTH_COOKIE_NAME,
    AUTH_HINT_COOKIE_NAME,
    authenticate_user,
    create_session_for_user,
    create_or_update_user,
    delete_user,
    get_user_by_session_token,
    list_users,
    revoke_session,
    update_user,
)


router = APIRouter()


class LoginPayload(BaseModel):
    identifier: str = ""
    password: str = ""


def _cookie_secure(request: Request) -> bool:
    proto = str(request.headers.get("x-forwarded-proto") or request.url.scheme or "").lower()
    return proto == "https"


def _client_ip(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()[:120]
    return str(request.client.host if request.client else "")[:120]


def _require_owner(request: Request) -> dict:
    user = getattr(request.state, "auth_user", None)
    if not user:
        raise PermissionError("unauthorized")
    if str(user.get("role") or "").strip() != "owner":
        raise PermissionError("forbidden")
    return user


@router.get("/api/auth/me")
async def auth_me(request: Request):
    user = getattr(request.state, "auth_user", None)
    if not user:
        token = request.cookies.get(AUTH_COOKIE_NAME)
        user = await asyncio.to_thread(get_user_by_session_token, token)
    if not user:
        return JSONResponse({"ok": False, "message": "unauthorized"}, status_code=401)
    return {"ok": True, "user": user}


@router.post("/api/auth/login")
async def auth_login(payload: LoginPayload, request: Request, response: Response):
    identifier = str(payload.identifier or "").strip()
    password = str(payload.password or "")
    if not identifier or not password:
        return JSONResponse({"ok": False, "message": "identifier и password обязательны"}, status_code=400)
    try:
        user = await asyncio.to_thread(authenticate_user, identifier=identifier, password=password)
        session_token, expires_at = await asyncio.to_thread(
            create_session_for_user,
            user_id=user["user_id"],
            user_agent=str(request.headers.get("user-agent") or "")[:500],
            ip_address=_client_ip(request),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    response.set_cookie(
        AUTH_COOKIE_NAME,
        session_token,
        httponly=True,
        secure=_cookie_secure(request),
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
        path="/",
    )
    response.set_cookie(
        AUTH_HINT_COOKIE_NAME,
        "1",
        httponly=False,
        secure=_cookie_secure(request),
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
        path="/",
    )
    return {"ok": True, "user": user, "expires_at": expires_at}


@router.post("/api/auth/logout")
async def auth_logout(request: Request, response: Response):
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if token:
        await asyncio.to_thread(revoke_session, token)
    response.delete_cookie(AUTH_COOKIE_NAME, path="/")
    response.delete_cookie(AUTH_HINT_COOKIE_NAME, path="/")
    return {"ok": True}


@router.get("/api/admin/users")
async def admin_users_list(request: Request):
    try:
        _require_owner(request)
    except PermissionError as exc:
        code = 401 if str(exc) == "unauthorized" else 403
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=code)
    return {"ok": True, "rows": list_users()}


@router.post("/api/admin/users")
async def admin_users_create(payload: dict | None, request: Request):
    try:
        _require_owner(request)
    except PermissionError as exc:
        code = 401 if str(exc) == "unauthorized" else 403
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=code)
    body = payload if isinstance(payload, dict) else {}
    try:
        user = create_or_update_user(
            identifier=str(body.get("identifier") or "").strip(),
            password=str(body.get("password") or ""),
            display_name=str(body.get("display_name") or "").strip() or None,
            role=str(body.get("role") or "viewer").strip() or "viewer",
            is_active=bool(body.get("is_active", True)),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    return {"ok": True, "user": user}


@router.post("/api/admin/users/{user_id}")
async def admin_users_update(user_id: str, payload: dict | None, request: Request):
    current_user = None
    try:
        current_user = _require_owner(request)
    except PermissionError as exc:
        code = 401 if str(exc) == "unauthorized" else 403
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=code)
    body = payload if isinstance(payload, dict) else {}
    target_user_id = str(user_id or "").strip()
    if current_user and current_user.get("user_id") == target_user_id and body.get("is_active") is False:
        return JSONResponse({"ok": False, "message": "Нельзя отключить текущего владельца"}, status_code=400)
    try:
        user = update_user(
            user_id=target_user_id,
            display_name=body.get("display_name"),
            role=body.get("role"),
            is_active=body.get("is_active"),
            password=body.get("password"),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    return {"ok": True, "user": user}


@router.post("/api/admin/users/{user_id}/delete")
async def admin_users_delete(user_id: str, request: Request):
    current_user = None
    try:
        current_user = _require_owner(request)
    except PermissionError as exc:
        code = 401 if str(exc) == "unauthorized" else 403
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=code)
    target_user_id = str(user_id or "").strip()
    if current_user and current_user.get("user_id") == target_user_id:
        return JSONResponse({"ok": False, "message": "Нельзя удалить текущего владельца"}, status_code=400)
    try:
        delete_user(user_id=target_user_id)
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
    return {"ok": True}
