from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from backend.services.db import SYSTEM_DATABASE_URL, SYSTEM_DB_PATH, apply_sqlite_pragmas, is_postgres_backend
from backend.services.store_data_model import _init_system_store_tables


AUTH_COOKIE_NAME = "daweb_session"
AUTH_HINT_COOKIE_NAME = "daweb_has_session"
AUTH_SESSION_DAYS = int(str(os.getenv("AUTH_SESSION_DAYS") or "30").strip() or "30")
PBKDF2_ROUNDS = int(str(os.getenv("AUTH_PBKDF2_ROUNDS") or "240000").strip() or "240000")
AUTH_LAST_SEEN_UPDATE_SECONDS = int(str(os.getenv("AUTH_LAST_SEEN_UPDATE_SECONDS") or "300").strip() or "300")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _ph() -> str:
    return "%s" if is_postgres_backend() and SYSTEM_DATABASE_URL else "?"


def _connect_system():
    if is_postgres_backend():
        try:
            import psycopg
            from psycopg.rows import dict_row
        except Exception as exc:
            raise RuntimeError("Для PostgreSQL backend требуется psycopg[binary]") from exc
        if not SYSTEM_DATABASE_URL:
            raise RuntimeError("APP_SYSTEM_DATABASE_URL/SYSTEM_DATABASE_URL не задан для auth")
        return psycopg.connect(SYSTEM_DATABASE_URL, row_factory=dict_row)
    conn = sqlite3.connect(str(SYSTEM_DB_PATH))  # pragma: no cover
    return apply_sqlite_pragmas(conn, history=False)  # pragma: no cover


def _row_value(row: Any, key: str, idx: int | None = None) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    if idx is not None:
        try:
            return row[idx]
        except Exception:
            return None
    return None


def _hash_token(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _hash_password(password: str, *, salt: str | None = None, rounds: int = PBKDF2_ROUNDS) -> str:
    raw_password = str(password or "")
    if not raw_password:
        raise ValueError("Пароль обязателен")
    salt_value = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt_value.encode("utf-8"),
        max(100_000, int(rounds)),
    ).hex()
    return f"pbkdf2_sha256${int(rounds)}${salt_value}${digest}"


def _verify_password(password: str, encoded: str) -> bool:
    raw = str(encoded or "").strip()
    if not raw:
        return False
    try:
        scheme, rounds_str, salt, expected = raw.split("$", 3)
    except ValueError:
        return False
    if scheme != "pbkdf2_sha256":
        return False
    actual = _hash_password(password, salt=salt, rounds=int(rounds_str))
    return hmac.compare_digest(actual, raw)


def _public_user(row: Any) -> dict[str, Any]:
    return {
        "user_id": str(_row_value(row, "user_id", 0) or "").strip(),
        "identifier": str(_row_value(row, "identifier", 1) or "").strip(),
        "display_name": str(_row_value(row, "display_name", 2) or "").strip(),
        "role": str(_row_value(row, "role", 4) or "viewer").strip() or "viewer",
        "is_active": bool(int(_row_value(row, "is_active", 5) or 0)),
    }


def create_or_update_user(
    *,
    identifier: str,
    password: str,
    display_name: str | None = None,
    role: str = "viewer",
    is_active: bool = True,
) -> dict[str, Any]:
    ident = str(identifier or "").strip().lower()
    if not ident:
        raise ValueError("identifier обязателен")
    password_hash = _hash_password(password)
    ts = _now_iso()
    uid = uuid4().hex
    shown = str(display_name or ident).strip() or ident
    user_role = str(role or "viewer").strip() or "viewer"
    _init_system_store_tables()
    with _connect_system() as conn:
        conn.execute(
            f"""
            INSERT INTO app_users (user_id, identifier, display_name, password_hash, role, is_active, created_at, updated_at)
            VALUES ({_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()})
            ON CONFLICT(identifier) DO UPDATE SET
                display_name = excluded.display_name,
                password_hash = excluded.password_hash,
                role = excluded.role,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
            """,
            (uid, ident, shown, password_hash, user_role, 1 if is_active else 0, ts, ts),
        )
        row = conn.execute(
            f"SELECT user_id, identifier, display_name, password_hash, role, is_active FROM app_users WHERE identifier = {_ph()}",
            (ident,),
        ).fetchone()
        conn.commit()
    return _public_user(row)


def list_users() -> list[dict[str, Any]]:
    _init_system_store_tables()
    with _connect_system() as conn:
        rows = conn.execute(
            """
            SELECT user_id, identifier, display_name, password_hash, role, is_active, created_at, updated_at
            FROM app_users
            ORDER BY created_at ASC, identifier ASC
            """
        ).fetchall()
    return [
        {
            **_public_user(row),
            "created_at": str(_row_value(row, "created_at", 6) or "").strip(),
            "updated_at": str(_row_value(row, "updated_at", 7) or "").strip(),
        }
        for row in rows
    ]


def update_user(
    *,
    user_id: str,
    display_name: str | None = None,
    role: str | None = None,
    is_active: bool | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        raise ValueError("user_id обязателен")
    _init_system_store_tables()
    with _connect_system() as conn:
        row = conn.execute(
            f"SELECT user_id, identifier, display_name, password_hash, role, is_active, created_at, updated_at FROM app_users WHERE user_id = {_ph()}",
            (uid,),
        ).fetchone()
        if not row:
            raise ValueError("Пользователь не найден")
        next_display_name = str(display_name).strip() if display_name is not None else str(_row_value(row, "display_name", 2) or "").strip()
        next_role = str(role).strip() if role is not None else str(_row_value(row, "role", 4) or "viewer").strip()
        next_active = (1 if bool(is_active) else 0) if is_active is not None else int(_row_value(row, "is_active", 5) or 0)
        next_password_hash = (
            _hash_password(password) if password is not None and str(password).strip() else str(_row_value(row, "password_hash", 3) or "")
        )
        ts = _now_iso()
        conn.execute(
            f"""
            UPDATE app_users
            SET display_name = {_ph()},
                password_hash = {_ph()},
                role = {_ph()},
                is_active = {_ph()},
                updated_at = {_ph()}
            WHERE user_id = {_ph()}
            """,
            (next_display_name, next_password_hash, next_role, next_active, ts, uid),
        )
        updated = conn.execute(
            f"SELECT user_id, identifier, display_name, password_hash, role, is_active, created_at, updated_at FROM app_users WHERE user_id = {_ph()}",
            (uid,),
        ).fetchone()
        conn.commit()
    return {
        **_public_user(updated),
        "created_at": str(_row_value(updated, "created_at", 6) or "").strip(),
        "updated_at": str(_row_value(updated, "updated_at", 7) or "").strip(),
    }


def delete_user(*, user_id: str) -> None:
    uid = str(user_id or "").strip()
    if not uid:
        raise ValueError("user_id обязателен")
    _init_system_store_tables()
    with _connect_system() as conn:
        row = conn.execute(
            f"SELECT user_id, role FROM app_users WHERE user_id = {_ph()}",
            (uid,),
        ).fetchone()
        if not row:
            raise ValueError("Пользователь не найден")
        role = str(_row_value(row, "role", 1) or "viewer").strip()
        if role == "owner":
            owners_left = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM app_users WHERE role = {_ph()} AND is_active = {_ph()} AND user_id <> {_ph()}",
                ("owner", 1, uid),
            ).fetchone()
            if int(_row_value(owners_left, "cnt", 0) or 0) <= 0:
                raise ValueError("Нельзя удалить последнего владельца")
        conn.execute(
            f"DELETE FROM app_sessions WHERE user_id = {_ph()}",
            (uid,),
        )
        conn.execute(
            f"DELETE FROM app_access_links WHERE user_id = {_ph()}",
            (uid,),
        )
        conn.execute(
            f"DELETE FROM app_users WHERE user_id = {_ph()}",
            (uid,),
        )
        conn.commit()


def authenticate_user(*, identifier: str, password: str) -> dict[str, Any]:
    ident = str(identifier or "").strip().lower()
    if not ident:
        raise ValueError("Логин обязателен")
    _init_system_store_tables()
    with _connect_system() as conn:
        row = conn.execute(
            f"SELECT user_id, identifier, display_name, password_hash, role, is_active FROM app_users WHERE identifier = {_ph()}",
            (ident,),
        ).fetchone()
    if not row:
        raise ValueError("Неверный логин или пароль")
    if not bool(int(_row_value(row, "is_active", 5) or 0)):
        raise ValueError("Пользователь отключен")
    if not _verify_password(password, str(_row_value(row, "password_hash", 3) or "")):
        raise ValueError("Неверный логин или пароль")
    return _public_user(row)


def create_session_for_user(*, user_id: str, user_agent: str = "", ip_address: str = "") -> tuple[str, str]:
    uid = str(user_id or "").strip()
    if not uid:
        raise ValueError("user_id обязателен")
    raw_session_token = secrets.token_urlsafe(40)
    session_id = uuid4().hex
    now_iso = _now_iso()
    session_expires_at = (_now() + timedelta(days=max(1, AUTH_SESSION_DAYS))).isoformat()
    _init_system_store_tables()
    with _connect_system() as conn:
        conn.execute(
            f"""
            INSERT INTO app_sessions (
                session_id, user_id, token_hash, user_agent, ip_address, last_seen_at, expires_at, revoked_at, created_at, updated_at
            ) VALUES ({_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()})
            """,
            (
                session_id,
                uid,
                _hash_token(raw_session_token),
                str(user_agent or "")[:500],
                str(ip_address or "")[:120],
                now_iso,
                session_expires_at,
                None,
                now_iso,
                now_iso,
            ),
        )
        conn.commit()
    return raw_session_token, session_expires_at


def get_user_by_session_token(session_token: str | None) -> dict[str, Any] | None:
    token = str(session_token or "").strip()
    if not token:
        return None
    now_iso = _now_iso()
    _init_system_store_tables()
    with _connect_system() as conn:
        row = conn.execute(
            f"""
            SELECT
                s.session_id,
                s.last_seen_at,
                s.expires_at,
                s.revoked_at,
                u.user_id,
                u.identifier,
                u.display_name,
                u.password_hash,
                u.role,
                u.is_active
            FROM app_sessions s
            JOIN app_users u ON u.user_id = s.user_id
            WHERE s.token_hash = {_ph()}
            """,
            (_hash_token(token),),
        ).fetchone()
        if not row:
            return None
        if not bool(int(_row_value(row, "is_active", 9) or 0)):
            return None
        if str(_row_value(row, "revoked_at", 3) or "").strip():
            return None
        expires_at = str(_row_value(row, "expires_at", 2) or "").strip()
        if expires_at and expires_at < now_iso:
            return None
        last_seen_at = str(_row_value(row, "last_seen_at", 1) or "").strip()
        should_touch_last_seen = True
        if last_seen_at:
            try:
                should_touch_last_seen = (
                    (_now() - datetime.fromisoformat(last_seen_at)).total_seconds() >= AUTH_LAST_SEEN_UPDATE_SECONDS
                )
            except Exception:
                should_touch_last_seen = True
        if should_touch_last_seen:
            conn.execute(
                f"UPDATE app_sessions SET last_seen_at = {_ph()}, updated_at = {_ph()} WHERE session_id = {_ph()}",
                (now_iso, now_iso, str(_row_value(row, "session_id", 0) or "").strip()),
            )
            conn.commit()
    return _public_user(row)


def revoke_session(session_token: str | None) -> None:
    token = str(session_token or "").strip()
    if not token:
        return
    now_iso = _now_iso()
    _init_system_store_tables()
    with _connect_system() as conn:
        conn.execute(
            f"UPDATE app_sessions SET revoked_at = {_ph()}, updated_at = {_ph()} WHERE token_hash = {_ph()}",
            (now_iso, now_iso, _hash_token(token)),
        )
        conn.commit()
