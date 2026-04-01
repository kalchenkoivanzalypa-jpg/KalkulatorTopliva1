"""Админка: пароль, импорт PDF, бюллетени, лиды, проверка кодов."""
from __future__ import annotations

import csv
import io
import logging
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from db.database import Lead, ProductBasisPrice
from web import settings
from web.admin_auth import ADMIN_COOKIE, sign_admin_ok, verify_admin_token
from web.deps import DbSession

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="web/templates")
logger = logging.getLogger(__name__)


def _require_admin(request: Request) -> bool:
    if not settings.ADMIN_WEB_PASSWORD:
        return False
    tok = request.cookies.get(ADMIN_COOKIE) or ""
    return verify_admin_token(tok)


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def admin_home(request: Request):
    if not settings.ADMIN_WEB_PASSWORD:
        return templates.TemplateResponse(
            "admin_disabled.html",
            {"request": request, "msg": "ADMIN_WEB_PASSWORD не задан в окружении."},
        )
    if _require_admin(request):
        return templates.TemplateResponse("admin_menu.html", {"request": request})
    return templates.TemplateResponse("admin_login.html", {"request": request, "error": None})


@router.post("/login")
async def admin_login(request: Request, password: str = Form("")):
    if not settings.ADMIN_WEB_PASSWORD:
        return RedirectResponse("/admin", status_code=302)
    if password != settings.ADMIN_WEB_PASSWORD:
        return templates.TemplateResponse(
            "admin_login.html",
            {"request": request, "error": "Неверный пароль."},
        )
    resp = RedirectResponse("/admin/import", status_code=302)
    resp.set_cookie(ADMIN_COOKIE, sign_admin_ok(), httponly=True, max_age=86400 * 7, samesite="lax")
    return resp


@router.get("/logout")
async def admin_logout():
    resp = RedirectResponse("/admin", status_code=302)
    resp.delete_cookie(ADMIN_COOKIE)
    return resp


def require_admin_or_redirect(request: Request):
    if not settings.ADMIN_WEB_PASSWORD or not _require_admin(request):
        return RedirectResponse("/admin", status_code=302)
    return None


@router.get("/import", response_class=HTMLResponse)
async def admin_import_page(request: Request):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    return templates.TemplateResponse("admin_import.html", {"request": request, "log": None, "error": None})


@router.post("/import/run")
async def admin_import_run(request: Request, last_n: int = Form(1)):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    last_n = max(1, min(50, int(last_n)))
    try:
        import contextlib
        import import_spimex_prices_from_pdf as spx

        d = spx.default_bulletins_directory()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            await spx.main_async_last_n(
                d,
                last_n,
                only_a_prefix=False,
                strict_market_only=False,
                log_extracted_codes=False,
            )
        log_text = buf.getvalue() or "(готово)"
    except Exception as e:
        logger.exception("admin import")
        return templates.TemplateResponse(
            "admin_import.html",
            {"request": request, "log": None, "error": str(e)},
        )
    return templates.TemplateResponse(
        "admin_import.html",
        {"request": request, "log": log_text, "error": None},
    )


@router.get("/bulletins", response_class=HTMLResponse)
async def admin_bulletins(request: Request):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    try:
        import import_spimex_prices_from_pdf as spx

        d = spx.default_bulletins_directory()
    except Exception:
        d = Path("data/bulletins")
    files: list[Path] = []
    if d.exists():
        files = sorted(d.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
    return templates.TemplateResponse(
        "admin_bulletins.html",
        {"request": request, "directory": str(d.resolve()), "files": files[:200]},
    )


@router.get("/leads", response_class=HTMLResponse)
async def admin_leads(request: Request):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    from db.database import get_session

    session = await get_session()
    try:
        q = await session.execute(select(Lead).order_by(Lead.created_at.desc()).limit(500))
        leads = q.scalars().all()
    finally:
        await session.close()
    return templates.TemplateResponse("admin_leads.html", {"request": request, "leads": leads})


@router.get("/leads.csv")
async def admin_leads_csv(request: Request):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    from db.database import get_session

    session = await get_session()
    try:
        q = await session.execute(select(Lead).order_by(Lead.created_at.desc()).limit(5000))
        leads = q.scalars().all()
    finally:
        await session.close()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id", "user_id", "request_id", "email", "phone", "company", "status", "source", "created_at"])
    for row in leads:
        w.writerow(
            [
                row.id,
                row.user_id,
                row.request_id,
                row.email,
                row.phone,
                row.company,
                row.status,
                row.source,
                row.created_at,
            ]
        )
    return PlainTextResponse(
        out.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="leads.csv"'},
    )


@router.get("/check", response_class=HTMLResponse)
async def admin_check_get(request: Request):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    return templates.TemplateResponse("admin_check.html", {"request": request, "rows": None, "error": None})


@router.post("/check", response_class=HTMLResponse)
async def admin_check_post(request: Request, session: DbSession, instrument_code: str = Form("")):
    redir = require_admin_or_redirect(request)
    if redir:
        return redir
    code = (instrument_code or "").strip().upper()
    if not code:
        return templates.TemplateResponse(
            "admin_check.html",
            {"request": request, "rows": None, "error": "Введите код инструмента."},
        )
    q = await session.execute(
        select(ProductBasisPrice, Basis)
        .join(Basis, Basis.id == ProductBasisPrice.basis_id)
        .where(ProductBasisPrice.instrument_code == code)
        .where(ProductBasisPrice.is_active.is_(True))
        .limit(50)
    )
    rows = q.all()
    return templates.TemplateResponse("admin_check.html", {"request": request, "rows": rows, "code": code, "error": None})
