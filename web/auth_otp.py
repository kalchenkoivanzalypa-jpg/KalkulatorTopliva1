"""OTP по email и подписанные cookie сессии."""
from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import EmailOtp
from web import settings


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(
        settings.WEB_SECRET_KEY,
        salt="fuel-web-session",
    )


def sign_session_user_id(user_id: int) -> str:
    return _serializer().dumps({"uid": user_id})


def unsign_session_user_id(token: str, max_age: int = 86400 * 30) -> int | None:
    try:
        data = _serializer().loads(token, max_age=max_age)
        return int(data["uid"])
    except (BadSignature, SignatureExpired, KeyError, TypeError, ValueError):
        return None


def hash_otp(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def generate_otp_code() -> str:
    return f"{secrets.randbelow(10**6):06d}"


async def create_otp(session: AsyncSession, email: str) -> str:
    """Создаёт OTP, возвращает открытый код для отправки по почте."""
    email_norm = email.strip().lower()
    await session.execute(delete(EmailOtp).where(EmailOtp.email == email_norm))
    code = generate_otp_code()
    expires = datetime.now(timezone.utc) + timedelta(minutes=settings.OTP_TTL_MINUTES)
    session.add(
        EmailOtp(
            email=email_norm,
            code_hash=hash_otp(code),
            expires_at=expires,
            attempts=0,
        )
    )
    await session.commit()
    return code


async def verify_otp(session: AsyncSession, email: str, code: str) -> bool:
    email_norm = email.strip().lower()
    r = await session.execute(
        select(EmailOtp).where(EmailOtp.email == email_norm).limit(1)
    )
    row = r.scalar_one_or_none()
    if not row:
        return False
    now = datetime.now(timezone.utc)
    exp = row.expires_at
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    if now > exp:
        await session.delete(row)
        await session.commit()
        return False
    if row.attempts >= settings.OTP_MAX_ATTEMPTS:
        await session.delete(row)
        await session.commit()
        return False
    row.attempts = int(row.attempts or 0) + 1
    await session.commit()
    ok = hmac.compare_digest(row.code_hash, hash_otp(code.strip()))
    if ok:
        await session.delete(row)
        await session.commit()
    return ok
