"""Cookie для админки (пароль из env)."""
from __future__ import annotations

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from web import settings


def _ser() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(settings.WEB_SECRET_KEY, salt="fuel-web-admin")


ADMIN_COOKIE = "admin_web"


def sign_admin_ok() -> str:
    return _ser().dumps({"ok": True})


def verify_admin_token(token: str, max_age: int = 86400 * 7) -> bool:
    if not token:
        return False
    try:
        data = _ser().loads(token, max_age=max_age)
        return bool(data.get("ok"))
    except (BadSignature, SignatureExpired, TypeError):
        return False
