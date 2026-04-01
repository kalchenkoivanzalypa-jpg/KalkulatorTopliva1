"""Публичная аналитика: тренд, сравнение и заявки."""
from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from bot.handlers import notify_managers_about_lead, send_order_to_email
from db.database import Basis, CityDestination, Lead, Product, ProductBasisPrice, UserRequest
from utils import canonical_fuel_display_name
from utils.rail_logistics import find_rail_station_for_destination, is_sakhalin_destination
from web.deps import DbSession, require_email_user
from web.products_util import list_products_for_basis
from web.services.calc_service import finalize_calculation, resolve_destination_to_id
from web.services.analytics_service import (
    compute_compare_three,
    compute_trend,
    pick_compare_products,
    search_basises,
)

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")

PAGE_SIZE = 12


def _a(request: Request) -> dict:
    return request.session.get("analytics") or {}


def _set_a(request: Request, data: dict) -> None:
    request.session["analytics"] = data


@router.get("/analytics", response_class=HTMLResponse)
async def analytics_menu(request: Request):
    return templates.TemplateResponse("analytics_menu.html", {"request": request})


# ----- Trend -----


@router.get("/analytics/trend", response_class=HTMLResponse)
async def trend_start(request: Request):
    _set_a(request, {"flow": "trend"})
    return templates.TemplateResponse(
        "analytics_trend_search.html",
        {"request": request, "query": "", "offset": 0, "basises": [], "total": 0, "error": None},
    )


@router.post("/analytics/trend/search", response_class=HTMLResponse)
async def trend_search(
    request: Request,
    session: DbSession,
    q: str = Form(""),
    offset: int = Form(0),
):
    _set_a(request, {"flow": "trend"})
    try:
        offset = max(0, int(offset))
    except ValueError:
        offset = 0
    page, total = await search_basises(session, q, offset=offset, page_size=PAGE_SIZE)
    return templates.TemplateResponse(
        "analytics_trend_search.html",
        {
            "request": request,
            "query": q,
            "offset": offset,
            "basises": page,
            "total": total,
            "page_size": PAGE_SIZE,
            "error": None,
        },
    )


@router.get("/analytics/trend/basis/{basis_id}", response_class=HTMLResponse)
async def trend_pick_product(request: Request, session: DbSession, basis_id: int):
    data = _a(request)
    data["flow"] = "trend"
    data["trend_basis_id"] = basis_id
    _set_a(request, data)

    basis = await session.get(Basis, basis_id)
    products = await list_products_for_basis(session, basis_id=basis_id)
    return templates.TemplateResponse(
        "analytics_trend_products.html",
        {"request": request, "basis": basis, "products": products, "error": None},
    )


@router.post("/analytics/trend/result", response_class=HTMLResponse)
async def trend_result(
    request: Request,
    session: DbSession,
    product_id: int = Form(...),
    basis_id: int = Form(...),
):
    tr = await compute_trend(session, basis_id, product_id)
    if not tr:
        basis = await session.get(Basis, basis_id)
        products = await list_products_for_basis(session, basis_id=basis_id)
        return templates.TemplateResponse(
            "analytics_trend_products.html",
            {
                "request": request,
                "basis": basis,
                "products": products,
                "error": "Нет истории по коду на СПбМТСБ — импортируйте бюллетени.",
            },
        )
    return templates.TemplateResponse("analytics_trend_result.html", {"request": request, "tr": tr, "basis_id": basis_id, "product_id": product_id})


# ----- Compare -----


@router.get("/analytics/compare", response_class=HTMLResponse)
async def compare_start(request: Request, session: DbSession):
    _set_a(
        request,
        {
            "flow": "compare",
            "product_id": None,
            "basis_ids": [],
            "basis_names": [],
        },
    )
    q = await session.execute(select(Product).where(Product.is_active.is_(True)).order_by(Product.name))
    products = pick_compare_products(q.scalars().all())
    return templates.TemplateResponse(
        "analytics_compare_products.html",
        {"request": request, "products": products},
    )


@router.post("/analytics/compare/product", response_class=HTMLResponse)
async def compare_set_product(request: Request, product_id: int = Form(...)):
    data = _a(request)
    data.update(
        {
            "flow": "compare",
            "product_id": int(product_id),
            "basis_ids": [],
            "basis_names": [],
        }
    )
    _set_a(request, data)
    return RedirectResponse("/analytics/compare/basis-search", status_code=302)


@router.get("/analytics/compare/basis-search", response_class=HTMLResponse)
async def compare_basis_search_get(request: Request):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/compare", status_code=302)
    if len(data.get("basis_ids") or []) >= 3:
        return RedirectResponse("/analytics/compare/destination", status_code=302)
    return templates.TemplateResponse(
        "analytics_compare_basis_search.html",
        {
            "request": request,
            "query": "",
            "offset": 0,
            "basises": [],
            "total": 0,
            "chosen": list(zip(data.get("basis_ids") or [], data.get("basis_names") or [])),
            "n_needed": 3 - len(data.get("basis_ids") or []),
        },
    )


@router.post("/analytics/compare/basis-search", response_class=HTMLResponse)
async def compare_basis_search_post(
    request: Request,
    session: DbSession,
    q: str = Form(""),
    offset: int = Form(0),
):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/compare", status_code=302)
    try:
        offset = max(0, int(offset))
    except ValueError:
        offset = 0
    page, total = await search_basises(session, q, offset=offset, page_size=PAGE_SIZE)
    chosen = list(zip(data.get("basis_ids") or [], data.get("basis_names") or []))
    return templates.TemplateResponse(
        "analytics_compare_basis_search.html",
        {
            "request": request,
            "query": q,
            "offset": offset,
            "basises": page,
            "total": total,
            "page_size": PAGE_SIZE,
            "chosen": chosen,
            "n_needed": 3 - len(data.get("basis_ids") or []),
        },
    )


@router.post("/analytics/compare/pick-basis", response_class=HTMLResponse)
async def compare_pick_basis(request: Request, session: DbSession, basis_id: int = Form(...)):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/compare", status_code=302)
    basis = await session.get(Basis, basis_id)
    if not basis:
        return RedirectResponse("/analytics/compare/basis-search", status_code=302)
    ids = list(data.get("basis_ids") or [])
    names = list(data.get("basis_names") or [])
    if basis_id not in ids:
        ids.append(basis_id)
        names.append(basis.name)
    data["basis_ids"] = ids
    data["basis_names"] = names
    _set_a(request, data)
    if len(ids) < 3:
        return RedirectResponse("/analytics/compare/basis-search", status_code=302)
    return RedirectResponse("/analytics/compare/destination", status_code=302)


@router.get("/analytics/compare/destination", response_class=HTMLResponse)
async def compare_destination_get(request: Request):
    data = _a(request)
    if len(data.get("basis_ids") or []) != 3:
        return RedirectResponse("/analytics/compare", status_code=302)
    return templates.TemplateResponse(
        "analytics_compare_destination.html",
        {"request": request, "error": None},
    )


@router.post("/analytics/compare/result", response_class=HTMLResponse)
async def compare_result(
    request: Request,
    session: DbSession,
    destination: str = Form(...),
):
    data = _a(request)
    pid = data.get("product_id")
    basis_ids = data.get("basis_ids") or []
    if not pid or len(basis_ids) != 3:
        return RedirectResponse("/analytics/compare", status_code=302)

    res = await compute_compare_three(
        session,
        product_id=int(pid),
        basis_ids=[int(x) for x in basis_ids],
        destination_text=destination.strip(),
    )
    if not res:
        return templates.TemplateResponse(
            "analytics_compare_destination.html",
            {
                "request": request,
                "error": "Не удалось определить координаты назначения. Уточните населённый пункт или станцию.",
            },
        )
    return templates.TemplateResponse(
        "analytics_compare_result.html",
        {"request": request, "res": res, "product_id": int(pid), "basis_ids": [int(x) for x in basis_ids], "destination": destination.strip()},
    )


@router.post("/analytics/order")
async def analytics_order(
    request: Request,
    session: DbSession,
    basis_id: int = Form(...),
    product_id: int = Form(...),
    destination: str = Form(...),
    volume: str = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/analytics", status_code=302)

    try:
        vol = float((volume or "").replace(",", ".").strip())
        if vol <= 0:
            raise ValueError
    except Exception:
        return RedirectResponse("/analytics?err=vol", status_code=302)

    dest_text = (destination or "").strip()
    dest_id, dest_lat, dest_lon, dest_key = await resolve_destination_to_id(session, dest_text)
    if dest_id is None:
        return RedirectResponse("/analytics?err=dest", status_code=302)

    basis = await session.get(Basis, int(basis_id))
    pbp = (
        await session.execute(
            select(ProductBasisPrice)
            .where(ProductBasisPrice.basis_id == int(basis_id))
            .where(ProductBasisPrice.product_id == int(product_id))
            .where(ProductBasisPrice.is_active.is_(True))
            .order_by(ProductBasisPrice.id.asc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if not basis or not pbp:
        return RedirectResponse("/analytics?err=nodata", status_code=302)

    dest_station = await find_rail_station_for_destination(session, dest_text, dest_key)
    sak = is_sakhalin_destination(dest_text, dest_key, dest_station)

    # минимальный distance для UI (finalize_calculation для rail пересчитает по ТР №4)
    from bot.handlers import calculate_distance as hav

    if (basis.transport_type or "").lower() == "rail":
        d_lat = float(dest_station.latitude) if dest_station else float(dest_lat)
        d_lon = float(dest_station.longitude) if dest_station else float(dest_lon)
        o_lat = float(basis.rail_latitude or basis.latitude)
        o_lon = float(basis.rail_longitude or basis.longitude)
        dist = hav(d_lat, d_lon, o_lat, o_lon)
        transport = "rail"
    else:
        dist = hav(float(dest_lat), float(dest_lon), float(basis.latitude), float(basis.longitude))
        transport = "auto"

    selected = {
        "distance": float(dist),
        "basis": basis,
        "price": pbp,
        "transport_type": transport,
        "rate": 0.0,
        "delivery_cost_per_ton": 0.0,
        "total_cost_per_ton": float(pbp.current_price),
        "rail_dest_station_id": int(dest_station.id) if dest_station else None,
        "rail_dest_station_name": str(dest_station.name) if dest_station else None,
        "rail_origin_station_name": None,
        "is_sakhalin_destination": bool(sak),
        "ferry_surcharge_per_ton": 0.0,
    }

    r = await finalize_calculation(
        session,
        user=user,
        product_id=int(product_id),
        destination_id=int(dest_id),
        selected=selected,
        volume=vol,
    )

    lead = Lead(user_id=user.id, request_id=r.request_id, status="email_pending", source="analytics")
    session.add(lead)
    await session.commit()

    ur = await session.get(UserRequest, int(r.request_id))
    if ur:
        try:
            await send_order_to_email(user.email or "", ur, session)
            lead.email = user.email
            lead.status = "sent"
            await session.commit()

            product = await session.get(Product, ur.product_id)
            b = await session.get(Basis, ur.basis_id)
            d = await session.get(CityDestination, ur.city_destination_id)
            lead_text = (
                f"📝 <b>Новая заявка #{ur.id}</b>\n\n"
                f"📧 Email: <b>{user.email}</b>\n"
                f"🛢️ {canonical_fuel_display_name(product.name) if product else '—'}\n"
                f"📍 {b.name if b else '—'} → {d.name if d else '—'}\n"
                f"📦 {ur.volume:g} т, {('Ж/Д' if ur.transport_type == 'rail' else 'Авто')}\n"
                f"💎 Итого: <b>{float(ur.total_price):,.0f}</b> ₽".replace(",", " ")
            )
            bot = getattr(request.app.state, "bot", None)
            if bot is not None:
                await notify_managers_about_lead(bot, lead_text)
        except Exception:
            pass

    return RedirectResponse("/cabinet?ok=lead", status_code=302)
