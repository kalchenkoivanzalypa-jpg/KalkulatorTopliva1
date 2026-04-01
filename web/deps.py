"""Зависимости FastAPI: БД, пользователь, гость."""
from __future__ import annotations

from typing import Annotated, Optional

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import User, get_session
from web import settings
from web.auth_otp import unsign_session_user_id
from web.users_repo import get_or_create_guest_user, get_user_by_id


async def db_session() -> AsyncSession:
    s = await get_session()
    try:
        yield s
    finally:
        await s.close()


DbSession = Annotated[AsyncSession, Depends(db_session)]


def _guest_tid_from_request(request: Request) -> Optional[int]:
    tid = request.session.get("guest_tid")
    if tid is not None:
        try:
            return int(tid)
        except (TypeError, ValueError):
            return None
    return None


async def require_guest_user(
    request: Request,
    session: DbSession,
) -> User:
    """Пользователь для сохранения расчётов (гость с synthetic telegram_id)."""
    tid = _guest_tid_from_request(request)
    if tid is None:
        raise HTTPException(status_code=400, detail="Сессия не инициализирована. Откройте /calc снова.")
    return await get_or_create_guest_user(session, tid)


async def optional_session_user(
    request: Request,
    session: DbSession,
) -> Optional[User]:
    token = request.cookies.get("session")
    if not token:
        return None
    uid = unsign_session_user_id(token)
    if not uid:
        return None
    return await get_user_by_id(session, uid)


async def require_email_user(
    request: Request,
    session: DbSession,
) -> User:
    u = await optional_session_user(request, session)
    if not u or not u.email:
        raise HTTPException(status_code=401, detail="Требуется вход по email")
    return u


def init_guest_session(request: Request) -> int:
    """Вызывать при первом заходе в /calc: кладёт guest_tid в server-side session."""
    tid = request.session.get("guest_tid")
    if tid is not None:
        try:
            return int(tid)
        except (TypeError, ValueError):
            pass
    from web.users_repo import new_guest_tid

    tid = new_guest_tid()
    request.session["guest_tid"] = tid
    return tid
