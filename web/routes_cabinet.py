"""Личный кабинет: подписки, заявки."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from bot.handlers import notify_managers_about_lead, send_order_to_email
from db.database import Basis, CityDestination, Lead, PriceAlert, Product, UserRequest
from web.deps import DbSession, require_email_user
from web.jinja_env import templates
from web.products_util import list_products_for_calc

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/cabinet", response_class=HTMLResponse)
async def cabinet(request: Request, session: DbSession):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet", status_code=302)

    alerts = (
        await session.execute(
            select(PriceAlert)
            .where(PriceAlert.user_id == user.id)
            .where(PriceAlert.is_active.is_(True))
            .order_by(PriceAlert.created_at.desc())
        )
    ).scalars().all()

    reqs = (
        await session.execute(
            select(UserRequest).where(UserRequest.user_id == user.id).order_by(UserRequest.created_at.desc()).limit(50)
        )
    ).scalars().all()

    leads = (
        await session.execute(select(Lead).where(Lead.user_id == user.id).order_by(Lead.created_at.desc()).limit(50))
    ).scalars().all()

    alert_rows = []
    for a in alerts:
        pr = await session.get(Product, a.product_id)
        alert_rows.append({"a": a, "product_name": pr.name if pr else "—"})

    products = await list_products_for_calc(session)

    return templates.TemplateResponse(
        "cabinet.html",
        {
            "request": request,
            "user": user,
            "alerts": alert_rows,
            "requests": reqs,
            "leads": leads,
            "products": products,
            "msg": request.query_params.get("ok"),
            "err": request.query_params.get("err"),
        },
    )


@router.post("/cabinet/subscribe", response_class=HTMLResponse)
async def cabinet_subscribe(
    request: Request,
    session: DbSession,
    product_id: int = Form(...),
    target_price: str = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login", status_code=302)

    try:
        tp = float(target_price.replace(",", ".").strip())
        if tp <= 0:
            raise ValueError
    except ValueError:
        return RedirectResponse("/cabinet?err=price", status_code=302)

    session.add(
        PriceAlert(
            user_id=user.id,
            product_id=product_id,
            target_price=tp,
            volume=None,
            city_destination_id=None,
            email=user.email,
            is_active=True,
        )
    )
    await session.commit()
    return RedirectResponse("/cabinet?ok=sub", status_code=302)


@router.post("/cabinet/lead", response_class=HTMLResponse)
async def cabinet_lead(
    request: Request,
    session: DbSession,
    request_id: int = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login", status_code=302)

    ur = await session.get(UserRequest, request_id)
    if not ur or ur.user_id != user.id:
        return RedirectResponse("/cabinet?err=lead", status_code=302)

    lead = (
        await session.execute(
            select(Lead).where(Lead.user_id == user.id, Lead.request_id == request_id).limit(1)
        )
    ).scalar_one_or_none()
    if lead and lead.status in ("sent", "contacted", "won"):
        return RedirectResponse("/cabinet?ok=lead", status_code=302)

    if not lead:
        lead = Lead(user_id=user.id, request_id=request_id, status="email_pending", source="web")
        session.add(lead)
        await session.flush()

    try:
        await send_order_to_email(user.email or "", ur, session)
        lead.email = user.email
        lead.status = "sent"
        await session.commit()

        product = await session.get(Product, ur.product_id)
        basis = await session.get(Basis, ur.basis_id)
        dest = await session.get(CityDestination, ur.city_destination_id)
        from utils import canonical_fuel_display_name

        lead_text = (
            f"📝 <b>Новая заявка #{ur.id}</b>\n\n"
            f"📧 Email: <b>{user.email}</b>\n"
            f"🛢️ {canonical_fuel_display_name(product.name) if product else '—'}\n"
            f"📍 {basis.name if basis else '—'} → {dest.name if dest else '—'}\n"
            f"📦 {ur.volume:g} т, {('Ж/Д' if ur.transport_type == 'rail' else 'Авто')}\n"
            f"💎 Итого: <b>{float(ur.total_price):,.0f}</b> ₽".replace(",", " ")
        )
        bot = getattr(request.app.state, "bot", None)
        if bot is not None:
            await notify_managers_about_lead(bot, lead_text)
    except Exception:
        logger.exception("cabinet lead")
        await session.rollback()
        return RedirectResponse("/cabinet?err=lead_send", status_code=302)

    return RedirectResponse("/cabinet?ok=lead", status_code=302)
