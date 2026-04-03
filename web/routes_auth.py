"""Вход по email + OTP."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from web.auth_otp import create_otp, sign_session_user_id, verify_otp
from web.deps import DbSession
from web.email_util import SMTPNotConfiguredError, send_smtp_email
from web.jinja_env import templates
from web.users_repo import create_or_update_email_user

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login", response_class=HTMLResponse)
async def login_post(request: Request, session: DbSession, email: str = Form(...)):
    email = (email or "").strip().lower()
    if "@" not in email or "." not in email:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Введите корректный email."},
        )
    try:
        code = await create_otp(session, email)
    except Exception:
        logger.exception("create_otp failed for %s", email)
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Не удалось подготовить код входа (ошибка БД). Обновите страницу и попробуйте снова.",
            },
        )
    try:
        await send_smtp_email(
            subject="Код входа — НК калькулятор топлива",
            body=f"Ваш код входа: {code}\n\nЕсли вы не запрашивали код — проигнорируйте письмо.",
            to_addrs=[email],
            require_smtp=True,
        )
    except SMTPNotConfiguredError:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Отправка почты на сервере не настроена. Обратитесь к администратору (SMTP в .env).",
            },
        )
    except Exception:
        logger.exception("SMTP: не удалось отправить OTP на %s", email)
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Не удалось отправить письмо. Если проблема повторяется, на VPS могут блокировать исходящий SMTP — попробуйте в .env порт 587, SMTP_SSL=0, SMTP_TLS=1 или уточните у хостинга.",
            },
        )
    try:
        request.session["otp_email"] = email
        return RedirectResponse("/verify", status_code=302)
    except Exception:
        logger.exception("session/redirect after OTP for %s", email)
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Ошибка сохранения сессии. Если письмо с кодом пришло — откройте /verify и введите код.",
            },
        )


@router.get("/verify", response_class=HTMLResponse)
async def verify_get(request: Request):
    email = request.session.get("otp_email")
    if not email:
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("verify.html", {"request": request, "email": email, "error": None})


@router.post("/verify", response_class=HTMLResponse)
async def verify_post(
    request: Request,
    session: DbSession,
    code: str = Form(...),
):
    email = request.session.get("otp_email")
    if not email:
        return RedirectResponse("/login", status_code=302)

    ok = await verify_otp(session, email, code.strip())
    if not ok:
        return templates.TemplateResponse(
            "verify.html",
            {"request": request, "email": email, "error": "Неверный или просроченный код."},
        )

    user = await create_or_update_email_user(session, email)
    request.session.pop("otp_email", None)
    token = sign_session_user_id(user.id)
    resp = RedirectResponse("/cabinet", status_code=302)
    resp.set_cookie(
        "session",
        token,
        httponly=True,
        max_age=86400 * 30,
        samesite="lax",
    )
    return resp


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie("session")
    return resp
