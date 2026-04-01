"""Вход по email + OTP."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from web.auth_otp import create_otp, sign_session_user_id, verify_otp
from web.deps import DbSession
from web.email_util import send_smtp_email
from web.users_repo import create_or_update_email_user

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")


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
    code = await create_otp(session, email)
    await send_smtp_email(
        subject="Код входа — НК калькулятор топлива",
        body=f"Ваш код входа: {code}\n\nЕсли вы не запрашивали код — проигнорируйте письмо.",
        to_addrs=[email],
    )
    request.session["otp_email"] = email
    return RedirectResponse("/verify", status_code=302)


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
