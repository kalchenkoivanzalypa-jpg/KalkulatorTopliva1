"""Пользователи веб: гость (отрицательный telegram_id) и вход по email."""
from __future__ import annotations

import secrets
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import User


async def get_user_by_id(session: AsyncSession, user_id: int) -> Optional[User]:
    return await session.get(User, user_id)


async def get_user_by_email(session: AsyncSession, email: str) -> Optional[User]:
    e = (email or "").strip().lower()
    if not e:
        return None
    r = await session.execute(select(User).where(User.email == e).limit(1))
    return r.scalar_one_or_none()


async def get_or_create_guest_user(session: AsyncSession, guest_tid: int) -> User:
    """guest_tid — отрицательное число, уникальное в users.telegram_id."""
    r = await session.execute(select(User).where(User.telegram_id == guest_tid).limit(1))
    u = r.scalar_one_or_none()
    if u:
        return u
    u = User(
        telegram_id=guest_tid,
        username="web_guest",
        first_name="Guest",
        email=None,
        is_active=True,
    )
    session.add(u)
    await session.commit()
    await session.refresh(u)
    return u


async def create_or_update_email_user(session: AsyncSession, email: str) -> User:
    """Пользователь после OTP: email заполнен, telegram_id NULL."""
    e = email.strip().lower()
    u = await get_user_by_email(session, e)
    if u:
        return u
    u = User(
        telegram_id=None,
        username=None,
        first_name="Web",
        email=e,
        is_active=True,
    )
    session.add(u)
    await session.commit()
    await session.refresh(u)
    return u


def new_guest_tid() -> int:
    """Отрицательный synthetic id (не пересекается с реальными Telegram id)."""
    # 12 цифр случайных в отрицательном диапазоне
    return -int(secrets.randbelow(10**12) + 1)
