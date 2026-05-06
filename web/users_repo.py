"""Пользователи веб: гость (отрицательный telegram_id) и вход по email."""
from __future__ import annotations

import secrets
from typing import Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
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
    """Пользователь после OTP: email заполнен; telegram_id — synthetic (гость), если старая БД требует NOT NULL."""
    e = email.strip().lower()
    u = await get_user_by_email(session, e)
    if u:
        return u
    u = User(
        telegram_id=new_guest_tid(),
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


def synthetic_telegram_id_for_max(max_user_id: int) -> int:
    """
    Старая SQLite-схема может требовать NOT NULL у users.telegram_id.
    Для пользователей только MAX (без Telegram) подставляем уникальный отрицательный id:
    диапазон < -10**12 — не пересекается с new_guest_tid() и с реальными Telegram id (>0).
    """
    return -(10**13 + int(max_user_id))


async def link_max_user_to_email(session: AsyncSession, *, max_user_id: int, email: str) -> User:
    """
    Привязать MAX пользователя к email-аккаунту (единый аккаунт).

    - email становится "первичным" идентификатором (одна запись users).
    - max_user_id хранится в той же записи users.
    - данные/подписки из MAX-only пользователя (если он отдельной строкой) переносятся в email-пользователя.
    """
    from db.database import (
        AnomalyAlert,
        BasisDigestSubscription,
        Lead,
        PriceAlert,
        UserRequest,
    )

    max_user_id = int(max_user_id)
    email_norm = (email or "").strip().lower()
    if "@" not in email_norm or "." not in email_norm:
        raise ValueError("email is invalid")

    # 1) email user (destination)
    email_user = await get_user_by_email(session, email_norm)
    if email_user is None:
        email_user = await create_or_update_email_user(session, email_norm)

    # If this email user already has a different max_user_id, reject (would hijack).
    existing_muid = getattr(email_user, "max_user_id", None)
    if existing_muid is not None and int(existing_muid) and int(existing_muid) != int(max_user_id):
        raise ValueError("Email already linked to another MAX user")

    # If some other user already has this max_user_id and it's not the same row — that's MAX-only user.
    r = await session.execute(select(User).where(User.max_user_id == max_user_id).limit(1))
    max_user = r.scalar_one_or_none()

    # 2) Ensure email_user has max_user_id
    if not getattr(email_user, "max_user_id", None):
        email_user.max_user_id = max_user_id
        session.add(email_user)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            # unique(max_user_id) conflict — someone already linked
            raise ValueError("MAX user id already linked to another account")

    # If max_user doesn't exist or is the same record — done.
    if max_user is None or int(getattr(max_user, "id", 0) or 0) == int(getattr(email_user, "id", 0) or 0):
        return email_user

    # 3) Merge: move data from max_user -> email_user
    from_id = int(max_user.id)
    to_id = int(email_user.id)

    # Simple bulk moves (no unique constraints)
    await session.execute(
        UserRequest.__table__.update().where(UserRequest.user_id == from_id).values(user_id=to_id)
    )
    await session.execute(Lead.__table__.update().where(Lead.user_id == from_id).values(user_id=to_id))
    await session.execute(
        PriceAlert.__table__.update().where(PriceAlert.user_id == from_id).values(user_id=to_id)
    )

    # Anomaly: unique(user_id, instrument_code)
    r = await session.execute(select(AnomalyAlert).where(AnomalyAlert.user_id == from_id))
    anoms = list(r.scalars().all())
    for a in anoms:
        a.user_id = to_id
        session.add(a)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            # duplicate — disable this one
            try:
                a.is_active = False
                session.add(a)
                await session.commit()
            except Exception:
                await session.rollback()

    # Digest: partial unique indexes (user_id,basis_id[,product_id][,destination_id] depending on mode)
    r = await session.execute(select(BasisDigestSubscription).where(BasisDigestSubscription.user_id == from_id))
    digests = list(r.scalars().all())
    for d in digests:
        d.user_id = to_id
        session.add(d)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            try:
                d.is_active = False
                session.add(d)
                await session.commit()
            except Exception:
                await session.rollback()

    # Deactivate the old MAX-only user (keep row for audit)
    max_user.is_active = False
    session.add(max_user)
    await session.commit()
    return email_user
