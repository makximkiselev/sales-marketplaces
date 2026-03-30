from __future__ import annotations

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from backend.services.auth_service import (
    AUTH_COOKIE_NAME,
    authenticate_user,
    create_session_for_user,
    get_user_by_session_token,
    revoke_session,
)


router = APIRouter()


def _cookie_secure(request: Request) -> bool:
    proto = str(request.headers.get("x-forwarded-proto") or request.url.scheme or "").lower()
    return proto == "https"


def _client_ip(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()[:120]
    return str(request.client.host if request.client else "")[:120]


@router.get("/api/auth/me")
async def auth_me(request: Request):
    user = getattr(request.state, "auth_user", None)
    if not user:
        token = request.cookies.get(AUTH_COOKIE_NAME)
        user = get_user_by_session_token(token)
    if not user:
        return JSONResponse({"ok": False, "message": "unauthorized"}, status_code=401)
    return {"ok": True, "user": user}


@router.post("/api/auth/login")
async def auth_login(payload: dict | None, request: Request, response: Response):
    body = payload if isinstance(payload, dict) else {}
    identifier = str(body.get("identifier") or "").strip()
    password = str(body.get("password") or "")
    if not identifier or not password:
        return JSONResponse({"ok": False, "message": "identifier и password обязательны"}, status_code=400)
    try:
        user = authenticate_user(identifier=identifier, password=password)
        session_token, expires_at = create_session_for_user(
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
    return {"ok": True, "user": user, "expires_at": expires_at}


@router.post("/api/auth/logout")
async def auth_logout(request: Request, response: Response):
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if token:
        revoke_session(token)
    response.delete_cookie(AUTH_COOKIE_NAME, path="/")
    return {"ok": True}
